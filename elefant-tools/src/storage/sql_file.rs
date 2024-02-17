use std::io::BufRead;
use std::ops::Deref;
use std::sync::Arc;
use std::vec;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{pin_mut, Stream, StreamExt};
use itertools::Itertools;
use tokio::fs::File;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};
use crate::models::SimplifiedDataType;
use crate::models::PostgresSchema;
use crate::models::PostgresTable;
use crate::storage::{BaseCopyTarget, CopyDestination, DataFormat, TableData};
use crate::{Result};
use crate::quoting::IdentifierQuoter;

pub struct SqlFileOptions {
    pub max_rows_per_insert: usize,
}

impl Default for SqlFileOptions {
    fn default() -> Self {
        Self {
            max_rows_per_insert: 1000,
        }
    }
}

pub struct SqlFile<F: AsyncWrite + Unpin + Send + Sync> {
    file: F,
    is_empty: bool,
    options: SqlFileOptions,
    quoter: Arc<IdentifierQuoter>,
}

impl<'q, F: AsyncWrite + Unpin + Send + Sync> SqlFile<F> {
    pub async fn new(path: &str, identifier_quoter: Arc<IdentifierQuoter>, options: SqlFileOptions) -> Result<SqlFile<BufWriter<File>>> {
        let file = File::create(path).await?;
        Ok(SqlFile {
            file: BufWriter::new(file),
            is_empty: true,
            options,
            quoter: identifier_quoter,
        })
    }
}

#[async_trait]
impl<F: AsyncWrite + Unpin + Send + Sync> BaseCopyTarget for SqlFile<F> {
    async fn supported_data_format(&self) -> Result<Vec<DataFormat>> {
        Ok(vec![DataFormat::Text])
    }
}

#[async_trait]
impl<F: AsyncWrite + Unpin + Send + Sync> CopyDestination for SqlFile<F> {
    async fn apply_data<S: Stream<Item=Result<Bytes>> + Send>(&mut self, schema: &PostgresSchema, table: &PostgresTable, data: TableData<S>) -> Result<()> {
        let file = &mut self.file;

        let column_types = table.columns.iter().map(|c| c.get_simplified_data_type()).collect_vec();

        let stream = data.into_stream();

        pin_mut!(stream);

        let mut count = 0;
        while let Some(bytes) = stream.next().await {
            if count == 0 {
                file.write_all(b"\n\n").await?;
            }
            match bytes {
                Ok(bytes) => {
                    if count % self.options.max_rows_per_insert == 0 {
                        if count > 0 {
                            file.write_all(b";\n\n").await?;
                        }

                        file.write_all(b"insert into ").await?;
                        file.write_all(schema.name.as_bytes()).await?;
                        file.write_all(b".").await?;
                        file.write_all(table.name.as_bytes()).await?;
                        file.write_all(b" (").await?;
                        for (index, column) in table.columns.iter().enumerate() {
                            if index != 0 {
                                file.write_all(b", ").await?;
                            }
                            file.write_all(column.name.as_bytes()).await?;
                        }
                        file.write_all(b")").await?;
                        file.write_all(b" values").await?;

                        file.write_all(b"\n").await?;
                        count = 0;
                    } else {
                        file.write_all(b",\n").await?;
                    }
                    count += 1;

                    let bytes: &[u8] = bytes.deref();
                    let bytes = &bytes[0..bytes.len() - 1];
                    let cols = BufRead::split(bytes, b'\t').zip(column_types.iter());
                    file.write_all(b"(").await?;
                    for (index, (bytes, col_data_type)) in cols.enumerate() {
                        if index != 0 {
                            file.write_all(b", ").await?;
                        }

                        match bytes {
                            Ok(bytes) => {
                                if bytes == [b'\\', b'N'] {
                                    file.write_all(b"null").await?;
                                    continue;
                                }


                                match col_data_type {
                                    SimplifiedDataType::Number => {
                                        match bytes[..] {
                                            [b'N', b'a', b'N'] | [b'I', b'n', b'f', b'i', b'n', b'i', b't', b'y'] | [b'-', b'I', b'n', b'f', b'i', b'n', b'i', b't', b'y'] => {
                                                file.write_all(b"'").await?;
                                                file.write_all(&bytes).await?;
                                                file.write_all(b"'").await?;
                                            }
                                            _ => {
                                                file.write_all(&bytes).await?;
                                            }
                                        }
                                    }
                                    SimplifiedDataType::Text => {
                                        file.write_all(b"E'").await?;
                                        if bytes.contains(&b'\'') {
                                            let s = std::str::from_utf8(&bytes).unwrap();
                                            let s = s.replace('\'', "''");
                                            file.write_all(s.as_bytes()).await?;
                                        } else {
                                            file.write_all(&bytes).await?;
                                        }
                                        file.write_all(b"'").await?;
                                    }
                                    SimplifiedDataType::Bool => {
                                        let value = bytes[0] == b't';
                                        file.write_all(format!("{}", value).as_bytes()).await?;
                                    }
                                }
                            }
                            Err(e) => panic!("wtf: {:?}", e)
                        }
                    }
                    file.write_all(b")").await?;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        if count > 0 {
            file.write_all(b";").await?;
        }

        file.flush().await?;

        Ok(())
    }

    async fn apply_ddl_statement(&mut self, statement: &str) -> Result<()> {
        if self.is_empty {
            self.file.write_all(statement.as_bytes()).await?;
            self.is_empty = false;
        } else {
            self.file.write_all(b"\n\n").await?;
            self.file.write_all(statement.as_bytes()).await?;
        }

        Ok(())
    }

    fn get_identifier_quoter(&self) -> Arc<IdentifierQuoter> {
        self.quoter.clone()
    }
}

#[cfg(test)]
mod tests {
    use indoc::indoc;
    use super::*;
    use crate::test_helpers::*;
    use tokio::test;
    use crate::copy_data::{copy_data, CopyDataOptions};
    use crate::schema_reader::tests::introspect_schema;
    use crate::storage;
    use crate::storage::postgres_instance::PostgresInstanceStorage;
    use crate::storage::tests::{validate_copy_state};

    async fn export_to_string(source: &TestHelper) -> String {
        let mut result_file = Vec::<u8>::new();


        {
            let quoter = IdentifierQuoter::empty();

            let mut sql_file = SqlFile {
                file: &mut result_file,
                options: SqlFileOptions::default(),
                quoter: Arc::new(quoter),
                is_empty: true,
            };

            let source = PostgresInstanceStorage::new(source.get_conn()).await.unwrap();


            copy_data(&source, &mut sql_file, CopyDataOptions::default()).await.unwrap();
        }

        String::from_utf8(result_file).unwrap()
    }


    #[test]
    async fn exports_to_fake_file_15() {
        let source = get_test_helper("source").await;
        if source.get_conn().version() < 150 {
            return;
        }

        //language=postgresql
        source.execute_not_query(storage::tests::get_copy_source_database_create_script(source.get_conn().version())).await;


        let result_file = export_to_string(&source).await;

        similar_asserts::assert_eq!(result_file, indoc! {r#"
            create schema if not exists public;

            create extension if not exists btree_gin;

            create table public.array_test (
                name text[] not null
            );

            create table public.ext_test_table (
                id int4 not null,
                name text not null,
                search_vector tsvector generated always as (to_tsvector('english'::regconfig, name)) stored,
                constraint ext_test_table_pkey primary key (id)
            );

            create table public.field (
                id int4 not null,
                constraint field_pkey primary key (id)
            );

            create table public.people (
                id int4 not null,
                name text not null,
                age int4 not null,
                constraint people_pkey primary key (id),
                constraint multi_check check (((name <> 'fsgsdfgsdf'::text) AND (age < 9999))),
                constraint people_age_check check ((age > 0))
            );

            create table public.pets (
                id int4 not null,
                name text not null,
                constraint pets_pkey primary key (id),
                constraint pets_name_check check ((length(name) > 1))
            );

            create table public.tree_node (
                id int4 not null,
                field_id int4 not null,
                name text not null,
                parent_id int4,
                constraint tree_node_pkey primary key (id)
            );

            create table public.my_partitioned_table (
                value int4 not null
            ) partition by range (value);

            create table public.my_partitioned_table_1 partition of my_partitioned_table FOR VALUES FROM (1) TO (10);

            create table public.my_partitioned_table_2 partition of my_partitioned_table FOR VALUES FROM (10) TO (20);

            create table public.cats (
                id int4 not null,
                name text not null,
                color text not null,
                constraint pets_name_check check ((length(name) > 1))
            ) inherits (pets);

            create table public.dogs (
                id int4 not null,
                name text not null,
                breed text not null,
                constraint dogs_breed_check check ((length(breed) > 1)),
                constraint pets_name_check check ((length(name) > 1))
            ) inherits (pets);

            insert into public.array_test (name) values
            (E'{foo,bar}'),
            (E'{baz,qux}'),
            (E'{quux,corge}');

            insert into public.cats (id, name, color) values
            (2, E'Fluffy', E'white');

            insert into public.dogs (id, name, breed) values
            (1, E'Fido', E'beagle');

            insert into public.my_partitioned_table_1 (value) values
            (1),
            (9);

            insert into public.my_partitioned_table_2 (value) values
            (11),
            (19);

            insert into public.people (id, name, age) values
            (1, E'foo', 42),
            (2, E'bar', 89),
            (3, E'nice', 69),
            (4, E'str\nange', 420),
            (5, E't\t\tap', 421),
            (6, E'q''t', 12);

            insert into public.pets (id, name) values
            (3, E'Remy');

            create index ext_test_table_name_idx on public.ext_test_table using gin (id, search_vector);

            create index people_age_brin_idx on public.people using brin (age);

            create index people_age_idx on public.people using btree (age desc nulls first) include (name, id) where (age % 2) = 0;

            create unique index people_name_key on public.people using btree (name asc nulls last);

            create index people_name_lower_idx on public.people using btree (lower(name) asc nulls last);

            create unique index field_id_id_unique on public.tree_node using btree (field_id asc nulls last, id asc nulls last);

            create unique index unique_name_per_level on public.tree_node using btree (field_id asc nulls last, parent_id asc nulls last, name asc nulls last) nulls not distinct;

            create sequence public.ext_test_table_id_seq as int4 increment by 1 minvalue 1 maxvalue 2147483647 start 1 cache 1;

            create sequence public.field_id_seq as int4 increment by 1 minvalue 1 maxvalue 2147483647 start 1 cache 1;

            create sequence public.people_id_seq as int4 increment by 1 minvalue 1 maxvalue 2147483647 start 1 cache 1;

            select pg_catalog.setval('public.people_id_seq', 6, true);

            create sequence public.pets_id_seq as int4 increment by 1 minvalue 1 maxvalue 2147483647 start 1 cache 1;

            select pg_catalog.setval('public.pets_id_seq', 3, true);

            create sequence public.tree_node_id_seq as int4 increment by 1 minvalue 1 maxvalue 2147483647 start 1 cache 1;

            alter table public.cats alter column id set default nextval('pets_id_seq'::regclass);

            alter table public.dogs alter column id set default nextval('pets_id_seq'::regclass);

            alter table public.ext_test_table alter column id set default nextval('ext_test_table_id_seq'::regclass);

            alter table public.field alter column id set default nextval('field_id_seq'::regclass);

            alter table public.people alter column id set default nextval('people_id_seq'::regclass);

            alter table public.pets alter column id set default nextval('pets_id_seq'::regclass);

            alter table public.tree_node alter column id set default nextval('tree_node_id_seq'::regclass);

            create view public.people_who_cant_drink (id, name, age) as  SELECT people.id,
                people.name,
                people.age
               FROM people
              WHERE people.age < 18;

            alter table public.people add constraint people_name_key unique using index people_name_key;

            alter table public.tree_node add constraint tree_node_field_id_fkey foreign key (field_id) references public.field (id);

            alter table public.tree_node add constraint tree_node_field_id_parent_id_fkey foreign key (field_id, parent_id) references public.tree_node (field_id, id);

            alter table public.tree_node add constraint field_id_id_unique unique using index field_id_id_unique;

            alter table public.tree_node add constraint unique_name_per_level unique using index unique_name_per_level;"#});

        let destination = get_test_helper("destination").await;
        destination.execute_not_query(&result_file).await;

        let source_schema = introspect_schema(&source).await;
        let destination_schema = introspect_schema(&destination).await;

        assert_eq!(source_schema, destination_schema);

        validate_copy_state(&destination).await;
    }

    #[test]
    async fn exports_to_fake_file_14() {
        let source = get_test_helper("source").await;
        if source.get_conn().version() < 140 || source.get_conn().version() >= 150 {
            return;
        }

        //language=postgresql
        source.execute_not_query(storage::tests::get_copy_source_database_create_script(source.get_conn().version())).await;


        let result_file = export_to_string(&source).await;

        similar_asserts::assert_eq!(result_file, indoc! {r#"
            create schema if not exists public;

            create extension if not exists btree_gin;

            create table public.array_test (
                name text[] not null
            );

            create table public.ext_test_table (
                id int4 not null,
                name text not null,
                search_vector tsvector generated always as (to_tsvector('english'::regconfig, name)) stored,
                constraint ext_test_table_pkey primary key (id)
            );

            create table public.field (
                id int4 not null,
                constraint field_pkey primary key (id)
            );

            create table public.people (
                id int4 not null,
                name text not null,
                age int4 not null,
                constraint people_pkey primary key (id),
                constraint multi_check check (((name <> 'fsgsdfgsdf'::text) AND (age < 9999))),
                constraint people_age_check check ((age > 0))
            );

            create table public.pets (
                id int4 not null,
                name text not null,
                constraint pets_pkey primary key (id),
                constraint pets_name_check check ((length(name) > 1))
            );

            create table public.tree_node (
                id int4 not null,
                field_id int4 not null,
                name text not null,
                parent_id int4,
                constraint tree_node_pkey primary key (id)
            );

            create table public.my_partitioned_table (
                value int4 not null
            ) partition by range (value);

            create table public.my_partitioned_table_1 partition of my_partitioned_table FOR VALUES FROM (1) TO (10);

            create table public.my_partitioned_table_2 partition of my_partitioned_table FOR VALUES FROM (10) TO (20);

            create table public.cats (
                id int4 not null,
                name text not null,
                color text not null,
                constraint pets_name_check check ((length(name) > 1))
            ) inherits (pets);

            create table public.dogs (
                id int4 not null,
                name text not null,
                breed text not null,
                constraint dogs_breed_check check ((length(breed) > 1)),
                constraint pets_name_check check ((length(name) > 1))
            ) inherits (pets);

            insert into public.array_test (name) values
            (E'{foo,bar}'),
            (E'{baz,qux}'),
            (E'{quux,corge}');

            insert into public.cats (id, name, color) values
            (2, E'Fluffy', E'white');

            insert into public.dogs (id, name, breed) values
            (1, E'Fido', E'beagle');

            insert into public.my_partitioned_table_1 (value) values
            (1),
            (9);

            insert into public.my_partitioned_table_2 (value) values
            (11),
            (19);

            insert into public.people (id, name, age) values
            (1, E'foo', 42),
            (2, E'bar', 89),
            (3, E'nice', 69),
            (4, E'str\nange', 420),
            (5, E't\t\tap', 421),
            (6, E'q''t', 12);

            insert into public.pets (id, name) values
            (3, E'Remy');

            create index ext_test_table_name_idx on public.ext_test_table using gin (id, search_vector);

            create index people_age_brin_idx on public.people using brin (age);

            create index people_age_idx on public.people using btree (age desc nulls first) include (name, id) where (age % 2) = 0;

            create unique index people_name_key on public.people using btree (name asc nulls last);

            create index people_name_lower_idx on public.people using btree (lower(name) asc nulls last);

            create unique index field_id_id_unique on public.tree_node using btree (field_id asc nulls last, id asc nulls last);

            create unique index unique_name_per_level on public.tree_node using btree (field_id asc nulls last, parent_id asc nulls last, name asc nulls last);

            create sequence public.ext_test_table_id_seq as int4 increment by 1 minvalue 1 maxvalue 2147483647 start 1 cache 1;

            create sequence public.field_id_seq as int4 increment by 1 minvalue 1 maxvalue 2147483647 start 1 cache 1;

            create sequence public.people_id_seq as int4 increment by 1 minvalue 1 maxvalue 2147483647 start 1 cache 1;

            select pg_catalog.setval('public.people_id_seq', 6, true);

            create sequence public.pets_id_seq as int4 increment by 1 minvalue 1 maxvalue 2147483647 start 1 cache 1;

            select pg_catalog.setval('public.pets_id_seq', 3, true);

            create sequence public.tree_node_id_seq as int4 increment by 1 minvalue 1 maxvalue 2147483647 start 1 cache 1;

            alter table public.cats alter column id set default nextval('pets_id_seq'::regclass);

            alter table public.dogs alter column id set default nextval('pets_id_seq'::regclass);

            alter table public.ext_test_table alter column id set default nextval('ext_test_table_id_seq'::regclass);

            alter table public.field alter column id set default nextval('field_id_seq'::regclass);

            alter table public.people alter column id set default nextval('people_id_seq'::regclass);

            alter table public.pets alter column id set default nextval('pets_id_seq'::regclass);

            alter table public.tree_node alter column id set default nextval('tree_node_id_seq'::regclass);

            create view public.people_who_cant_drink (id, name, age) as  SELECT people.id,
                people.name,
                people.age
               FROM people
              WHERE people.age < 18;

            alter table public.people add constraint people_name_key unique using index people_name_key;

            alter table public.tree_node add constraint tree_node_field_id_fkey foreign key (field_id) references public.field (id);

            alter table public.tree_node add constraint tree_node_field_id_parent_id_fkey foreign key (field_id, parent_id) references public.tree_node (field_id, id);

            alter table public.tree_node add constraint field_id_id_unique unique using index field_id_id_unique;

            alter table public.tree_node add constraint unique_name_per_level unique using index unique_name_per_level;"#});

        let destination = get_test_helper("destination").await;
        destination.execute_not_query(&result_file).await;


        let source_schema = introspect_schema(&source).await;
        let destination_schema = introspect_schema(&destination).await;

        assert_eq!(source_schema, destination_schema);

        validate_copy_state(&destination).await;
    }


    #[test]
    async fn edge_case_values_floats() {
        let source = get_test_helper("source").await;

        //language=postgresql
        source.execute_not_query(r#"
        create table edge_case_values(
            r4 float4,
            r8 float8
        );

        insert into edge_case_values(r4, r8)
        values (1.0, 1.0),
               ('NaN', 'NaN'),
               ('Infinity', 'Infinity'),
               ('-Infinity', '-Infinity'),
               (null, null);
        "#).await;

        let result_file = export_to_string(&source).await;

        similar_asserts::assert_eq!(result_file, indoc! {r#"
            create schema if not exists public;

            create table public.edge_case_values (
                r4 float4,
                r8 float8
            );

            insert into public.edge_case_values (r4, r8) values
            (1, 1),
            ('NaN', 'NaN'),
            ('Infinity', 'Infinity'),
            ('-Infinity', '-Infinity'),
            (null, null);"#});

        let destination = get_test_helper("destination").await;
        destination.execute_not_query(&result_file).await;


        let items = destination.get_results::<(Option<f32>, Option<f64>)>("select r4, r8 from edge_case_values;").await;

        assert_eq!(items.len(), 5);
        assert_eq!(items[0], (Some(1.0), Some(1.0)));
        assert_eq!(items[2], (Some(f32::INFINITY), Some(f64::INFINITY)));
        assert_eq!(items[3], (Some(f32::NEG_INFINITY), Some(f64::NEG_INFINITY)));
        assert_eq!(items[4], (None, None));

        let nan_tuple = items[1];

        assert!(nan_tuple.0.unwrap().is_nan());
        assert!(nan_tuple.1.unwrap().is_nan());
    }

    #[test]
    async fn copy_array_values() {
        let source = get_test_helper("source").await;

        //language=postgresql
        source.execute_not_query(r#"
        create table array_values(
            values int4[]
        );

        insert into array_values(values)
        values (array[1, 2, 3]),
               (array[4, 5, 6]);
        "#).await;

        let result_file = export_to_string(&source).await;

        similar_asserts::assert_eq!(result_file, indoc! {r#"
            create schema if not exists public;

            create table public.array_values (
                values int4[]
            );

            insert into public.array_values (values) values
            (E'{1,2,3}'),
            (E'{4,5,6}');"#});

        let destination = get_test_helper("destination").await;
        destination.execute_not_query(&result_file).await;


        let items = destination.get_results::<(Vec<i32>,)>("select values from array_values;").await;

        assert_eq!(items.len(), 2);
        assert_eq!(items[0].0, vec![1, 2, 3]);
        assert_eq!(items[1].0, vec![4, 5, 6]);
    }
}