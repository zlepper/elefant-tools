use std::sync::Arc;
use std::vec;
use bytes::Bytes;
use futures::{pin_mut, SinkExt, Stream, StreamExt};
use itertools::Itertools;
use tokio::fs::File;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter};
use tracing::{instrument};
use uuid::Uuid;
use crate::models::SimplifiedDataType;
use crate::models::PostgresSchema;
use crate::models::PostgresTable;
use crate::storage::{BaseCopyTarget, CopyDestination};
use crate::{AsyncCleanup, CopyDestinationFactory, ParallelCopyDestinationNotAvailable, PostgresClientWrapper, Result, SequentialOrParallel};
use crate::chunk_reader::{ChunkResult, StringChunkReader};
use crate::quoting::IdentifierQuoter;
use crate::storage::data_format::DataFormat;
use crate::storage::table_data::TableData;

pub struct SqlFileOptions {
    pub max_rows_per_insert: usize,
    pub chunk_separator: String,
    pub max_commands_per_chunk: usize,
    pub data_mode: SqlDataMode,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SqlDataMode{
    InsertStatements,
    CopyStatements
}

impl Default for SqlFileOptions {
    fn default() -> Self {
        Self {
            max_rows_per_insert: 1000,
            chunk_separator: Uuid::new_v4().to_string(),
            max_commands_per_chunk: 10,
            data_mode: SqlDataMode::InsertStatements,
        }
    }
}

pub struct SqlFile<F: AsyncWrite + Unpin + Send + Sync> {
    file: F,
    is_empty: bool,
    options: SqlFileOptions,
    quoter: Arc<IdentifierQuoter>,
    current_command_count: usize,
    chunk_separator: Vec<u8>,
}

impl SqlFile<BufWriter<File>> {
    #[instrument(skip_all)]
    pub async fn new_file(path: &str, identifier_quoter: Arc<IdentifierQuoter>, options: SqlFileOptions) -> Result<Self> {
        let file = File::create(path).await?;

        let file = BufWriter::with_capacity(1048576 * 20, file);

        SqlFile::new(file, identifier_quoter, options).await
    }
}

static CHUNK_SEPARATOR_PREFIX: &str = "-- chunk-separator-";

impl<F: AsyncWrite + Unpin + Send + Sync> SqlFile<F> {
    pub async fn new(file: F, identifier_quoter: Arc<IdentifierQuoter>, options: SqlFileOptions) -> Result<Self> {
        let chunk_separator = format!("{}{} --", CHUNK_SEPARATOR_PREFIX, options.chunk_separator).into_bytes();

        Ok(SqlFile {
            file,
            is_empty: true,
            options,
            quoter: identifier_quoter,
            current_command_count: 0,
            chunk_separator,
        })
    }
}

impl<F: AsyncWrite + Unpin + Send + Sync> BaseCopyTarget for SqlFile<F> {
    async fn supported_data_format(&self) -> Result<Vec<DataFormat>> {
        Ok(vec![DataFormat::Text])
    }
}


impl<'a, F: AsyncWrite + Unpin + Send + Sync + 'a> CopyDestinationFactory<'a> for SqlFile<F> {
    type SequentialDestination = &'a mut SqlFile<F>;
    type ParallelDestination = ParallelCopyDestinationNotAvailable;

    async fn create_destination(&'a mut self) -> Result<SequentialOrParallel<Self::SequentialDestination, Self::ParallelDestination>> {
        Ok(SequentialOrParallel::Sequential(self))
    }
}

impl<F: AsyncWrite + Unpin + Send + Sync> CopyDestination for &mut SqlFile<F> {
    #[instrument(skip_all)]
    async fn apply_data<S: Stream<Item=Result<Bytes>> + Send, C: AsyncCleanup>(&mut self, schema: &PostgresSchema, table: &PostgresTable, data: TableData<S, C>) -> Result<()> {
        let file = &mut self.file;
        if self.current_command_count > 0 {
            file.write_all(b"\n").await?;
            self.current_command_count = 0;
        }

        let stream = data.data;

        pin_mut!(stream);

        if self.options.data_mode == SqlDataMode::InsertStatements {
            self.write_data_stream_to_insert_statements(&mut stream, schema, table).await?;
        } else {
            self.write_data_stream_to_copy_statements(&mut stream, schema, table).await?;
        }

        Ok(())
    }

    #[instrument(skip_all)]
    async fn apply_transactional_statement(&mut self, statement: &str) -> Result<()> {
        if self.current_command_count % self.options.max_commands_per_chunk == 0 {
            if !self.is_empty {
                self.file.write_all(b"\n\n").await?;
            }

            self.file.write_all(&self.chunk_separator).await?;
            self.file.write_all(b"\n").await?;
            // if !self.is_empty {
            //     self.file.write_all(b"\n").await?;
            // }
            self.is_empty = true;
        }

        if self.is_empty {
            self.file.write_all(statement.as_bytes()).await?;
            self.is_empty = false;
        } else {
            self.file.write_all(b"\n\n").await?;
            self.file.write_all(statement.as_bytes()).await?;
        }

        self.current_command_count += 1;

        Ok(())
    }

    #[instrument(skip_all)]
    async fn apply_non_transactional_statement(&mut self, statement: &str) -> Result<()> {
        self.apply_transactional_statement(statement).await
    }

    async fn begin_transaction(&mut self) -> Result<()> {
        Ok(())
    }

    async fn commit_transaction(&mut self) -> Result<()> {
        Ok(())
    }

    fn get_identifier_quoter(&self) -> Arc<IdentifierQuoter> {
        self.quoter.clone()
    }
}

impl<F: AsyncWrite + Unpin + Send + Sync> SqlFile<F> {
    async fn write_data_stream_to_insert_statements<S: Stream<Item=Result<Bytes>> + Send + Unpin>(&mut self, stream: &mut S, schema: &PostgresSchema, table: &PostgresTable) -> Result<()> {

        let file = &mut self.file;

        let column_types = table.columns.iter().map(|c| c.get_simplified_data_type()).collect_vec();

        let mut count = 0;
        while let Some(bytes) = stream.next().await {
            if count == 0 {
                file.write_all(b"\n").await?;
                file.write_all(&self.chunk_separator).await?;
                file.write_all(b"\n").await?;
            }
            match bytes {
                Ok(bytes) => {
                    if count % self.options.max_rows_per_insert == 0 {
                        if count > 0 {
                            file.write_all(b";\n").await?;
                            file.write_all(&self.chunk_separator).await?;
                            file.write_all(b"\n").await?;
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


                    write_row(file, &column_types, bytes).await?;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        if count > 0 {
            file.write_all(b";\n").await?;
        }

        file.flush().await?;

        Ok(())
    }
    
    async fn write_data_stream_to_copy_statements<S: Stream<Item=Result<Bytes>> + Send + Unpin>(&mut self, stream: &mut S, schema: &PostgresSchema, table: &PostgresTable) -> Result<()> {

        let file = &mut self.file;

        let mut count = 0;
        while let Some(bytes) = stream.next().await {
            if count == 0 {
                file.write_all(b"\n").await?;
                file.write_all(&self.chunk_separator).await?;
                file.write_all(b"\n").await?;
                
                let copy_command = table.get_copy_in_command(schema, &DataFormat::Text, &self.quoter);
                file.write_all(copy_command.as_bytes()).await?;
                
                file.write_all(b"\n").await?;
                file.write_all(&self.chunk_separator).await?;
                file.write_all(b"\n").await?;
            }
            match bytes {
                Ok(bytes) => {
                    file.write_all(&bytes).await?;
                    count += 1;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        if count > 0 {
            file.write_all(b"\\.\n").await?;
            file.flush().await?;
        }


        Ok(())
    }
}

async fn write_row<F: AsyncWrite + Unpin + Send + Sync>(file: &mut F, column_types: &[SimplifiedDataType], bytes: Bytes) -> Result<()> {
    let without_line_break = bytes.slice(0..bytes.len() - 1);
    let column_bytes = without_line_break.split(|b| *b == b'\t');
    
    let mut content: Vec<u8> = Vec::with_capacity(bytes.len() * 2);
    
    let cols = column_bytes.zip(column_types.iter());
    content.push(b'(');
    for (index, (bytes, col_data_type)) in cols.enumerate() {
        if index != 0 {
            content.extend_from_slice(b", ");
        }

        write_column(&mut content, bytes, col_data_type);
    }
    content.push(b')');
    
    file.write_all(&content).await?;
    
    Ok(())
}

fn write_column(content: &mut Vec<u8>, bytes: &[u8], col_data_type: &SimplifiedDataType) {
    if bytes == [b'\\', b'N'] {
        content.extend_from_slice(b"null");
        return;
    }


    match col_data_type {
        SimplifiedDataType::Number => {
            write_number_column(content, bytes);
        }
        SimplifiedDataType::Text => {
            write_text_column(content, bytes);
        }
        SimplifiedDataType::Bool => {
            write_bool_column(content, bytes);
        }
    }
}

fn write_bool_column(content: &mut Vec<u8>, bytes: &[u8]) {
    let value = bytes[0] == b't';
    content.extend_from_slice(format!("{}", value).as_bytes());
}

fn write_text_column(content: &mut Vec<u8>, bytes: &[u8]) {
    content.extend_from_slice(b"E'");

    if bytes.contains(&b'\'') {
        let s = std::str::from_utf8(bytes).unwrap();
        let s = s.replace('\'', "''");
        content.extend_from_slice(s.as_bytes());
    } else {
        content.extend_from_slice(bytes);
    }
    content.push(b'\'');
}

fn write_number_column(content: &mut Vec<u8>, bytes: &[u8]) {
    match bytes[..] {
        [b'N', b'a', b'N'] | [b'I', b'n', b'f', b'i', b'n', b'i', b't', b'y'] | [b'-', b'I', b'n', b'f', b'i', b'n', b'i', b't', b'y'] => {
            content.push(b'\'');
            content.extend_from_slice(bytes);
            content.push(b'\'');
        }
        _ => {
            content.extend_from_slice(bytes);
        }
    }
}

#[instrument(skip_all)]
pub async fn apply_sql_file<F: AsyncBufRead + Unpin + Send + Sync>(content: &mut F, target_connection: &PostgresClientWrapper) -> Result<()> {
    let mut sql_chunk = String::with_capacity(10000);

    let read = content.read_line(&mut sql_chunk).await?;

    if read == 0 {
        return Ok(());
    }

    if sql_chunk.starts_with(CHUNK_SEPARATOR_PREFIX) {
        let separator = sql_chunk.clone();

        loop {
            sql_chunk.clear();

            let read = content.read_lines_until_separator_line(&separator, &mut sql_chunk).await?;
            match read {
                ChunkResult::Chunk(_) => {
                    
                    if sql_chunk.starts_with("copy ") && sql_chunk.ends_with(" from stdin with (format text, header false);\n") {
                        
                        let copy_in_stream = target_connection.copy_in::<Bytes>(&sql_chunk).await?;


                        pin_mut!(copy_in_stream);
                        
                        // sql_chunk.clear();
                        // content.read_line(&mut sql_chunk).await?;
                        // 
                        // assert_eq!(sql_chunk, separator);
                        
                        loop {
                            sql_chunk.clear();
                            let read = content.read_line(&mut sql_chunk).await?;
                            if read == 0 {
                                break;
                            }
                            if sql_chunk.starts_with("\\.") {
                                break;
                            }
                            let byt = Bytes::from(sql_chunk.clone());
                            
                            copy_in_stream.send(byt).await?;
                        }
                        
                        copy_in_stream.close().await?;
                        
                    } else {
                        target_connection.execute_non_query(&sql_chunk).await?;
                    }
                }
                ChunkResult::End(read) => {
                    if read > 0 {
                        target_connection.execute_non_query(&sql_chunk).await?;
                    }
                    break;
                }
            }
        }
    } else {
        content.read_to_string(&mut sql_chunk).await?;
        target_connection.execute_non_query(&sql_chunk).await?;
    }

    Ok(())
}

pub async fn apply_sql_string(file_content: &str, target_connection: &PostgresClientWrapper) -> Result<()> {
    let mut bytes = file_content.as_bytes();
    apply_sql_file(&mut bytes, target_connection).await
}

#[cfg(test)]
mod tests {
    use indoc::indoc;
    use super::*;
    use crate::test_helpers::*;
    use tokio::test;
    use crate::copy_data::{copy_data, CopyDataOptions};
    use crate::schema_reader::tests::introspect_schema;
    use crate::{default, storage};
    use crate::storage::postgres_instance::PostgresInstanceStorage;
    use crate::storage::tests::validate_copy_state;

    async fn export_to_string(source: &TestHelper, sql_file_options: SqlFileOptions) -> String {
        let mut result_file = Vec::<u8>::new();


        {
            let quoter = IdentifierQuoter::empty();

            let mut sql_file = SqlFile::new(&mut result_file, Arc::new(quoter), SqlFileOptions {
                chunk_separator: "test_chunk_separator".to_string(),
                max_commands_per_chunk: 5,
                ..sql_file_options
            }).await.unwrap();

            let source = PostgresInstanceStorage::new(source.get_conn()).await.unwrap();


            copy_data(&source, &mut sql_file, CopyDataOptions::default()).await.unwrap();
        }

        String::from_utf8(result_file).unwrap()
    }


    #[test]
    async fn exports_to_fake_file_15() {
        let source = get_test_helper_on_port("source", 5415).await;
        if source.get_conn().version() < 150 {
            panic!("This test is only for 15");
        }

        //language=postgresql
        source.execute_not_query(storage::tests::get_copy_source_database_create_script(source.get_conn().version())).await;


        let result_file = export_to_string(&source, default()).await;

        similar_asserts::assert_eq!(result_file, indoc! {r#"
            -- chunk-separator-test_chunk_separator --
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

            -- chunk-separator-test_chunk_separator --
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

            -- chunk-separator-test_chunk_separator --
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

            -- chunk-separator-test_chunk_separator --
            insert into public.array_test (name) values
            (E'{foo,bar}'),
            (E'{baz,qux}'),
            (E'{quux,corge}');

            -- chunk-separator-test_chunk_separator --
            insert into public.cats (id, name, color) values
            (2, E'Fluffy', E'white');

            -- chunk-separator-test_chunk_separator --
            insert into public.dogs (id, name, breed) values
            (1, E'Fido', E'beagle');

            -- chunk-separator-test_chunk_separator --
            insert into public.my_partitioned_table_1 (value) values
            (1),
            (9);

            -- chunk-separator-test_chunk_separator --
            insert into public.my_partitioned_table_2 (value) values
            (11),
            (19);

            -- chunk-separator-test_chunk_separator --
            insert into public.people (id, name, age) values
            (1, E'foo', 42),
            (2, E'bar', 89),
            (3, E'nice', 69),
            (4, E'str\nange', 420),
            (5, E't\t\tap', 421),
            (6, E'q''t', 12);

            -- chunk-separator-test_chunk_separator --
            insert into public.pets (id, name) values
            (3, E'Remy');


            -- chunk-separator-test_chunk_separator --
            create index ext_test_table_name_idx on public.ext_test_table using gin (id, search_vector);

            create index people_age_brin_idx on public.people using brin (age);

            create index people_age_idx on public.people using btree (age desc nulls first) include (name, id) where (age % 2) = 0;

            create unique index people_name_key on public.people using btree (name asc nulls last);

            create index people_name_lower_idx on public.people using btree (lower(name) asc nulls last);

            -- chunk-separator-test_chunk_separator --
            create unique index field_id_id_unique on public.tree_node using btree (field_id asc nulls last, id asc nulls last);

            create unique index unique_name_per_level on public.tree_node using btree (field_id asc nulls last, parent_id asc nulls last, name asc nulls last) nulls not distinct;

            create sequence public.ext_test_table_id_seq as int4 increment by 1 minvalue 1 maxvalue 2147483647 start 1 cache 1;

            create sequence public.field_id_seq as int4 increment by 1 minvalue 1 maxvalue 2147483647 start 1 cache 1;

            create sequence public.people_id_seq as int4 increment by 1 minvalue 1 maxvalue 2147483647 start 1 cache 1;

            -- chunk-separator-test_chunk_separator --
            create sequence public.pets_id_seq as int4 increment by 1 minvalue 1 maxvalue 2147483647 start 1 cache 1;

            create sequence public.tree_node_id_seq as int4 increment by 1 minvalue 1 maxvalue 2147483647 start 1 cache 1;

            select pg_catalog.setval('public.people_id_seq', 6, true);

            select pg_catalog.setval('public.pets_id_seq', 3, true);

            alter table public.cats alter column id set default nextval('pets_id_seq'::regclass);

            -- chunk-separator-test_chunk_separator --
            alter table public.dogs alter column id set default nextval('pets_id_seq'::regclass);

            alter table public.ext_test_table alter column id set default nextval('ext_test_table_id_seq'::regclass);

            alter table public.field alter column id set default nextval('field_id_seq'::regclass);

            alter table public.people alter column id set default nextval('people_id_seq'::regclass);

            alter table public.pets alter column id set default nextval('pets_id_seq'::regclass);

            -- chunk-separator-test_chunk_separator --
            alter table public.tree_node alter column id set default nextval('tree_node_id_seq'::regclass);

            create view public.people_who_cant_drink (id, name, age) as  SELECT people.id,
                people.name,
                people.age
               FROM people
              WHERE people.age < 18;

            alter table public.people add constraint people_name_key unique using index people_name_key;

            alter table public.tree_node add constraint tree_node_field_id_fkey foreign key (field_id) references public.field (id);

            alter table public.tree_node add constraint tree_node_field_id_parent_id_fkey foreign key (field_id, parent_id) references public.tree_node (field_id, id);

            -- chunk-separator-test_chunk_separator --
            alter table public.tree_node add constraint field_id_id_unique unique using index field_id_id_unique;

            alter table public.tree_node add constraint unique_name_per_level unique using index unique_name_per_level;"#});

        let destination = get_test_helper_on_port("destination", 5415).await;
        apply_sql_string(&result_file, destination.get_conn()).await.unwrap();

        let source_schema = introspect_schema(&source).await;
        let destination_schema = introspect_schema(&destination).await;

        assert_eq!(source_schema, destination_schema);

        validate_copy_state(&destination).await;
    }

    #[test]
    async fn exports_to_fake_file_14() {
        let source = get_test_helper_on_port("source", 5414).await;
        if source.get_conn().version() < 140 || source.get_conn().version() >= 150 {
            panic!("This test is only for 14");
        }

        //language=postgresql
        source.execute_not_query(storage::tests::get_copy_source_database_create_script(source.get_conn().version())).await;


        let result_file = export_to_string(&source, default()).await;

        similar_asserts::assert_eq!(result_file, indoc! {r#"
            -- chunk-separator-test_chunk_separator --
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

            -- chunk-separator-test_chunk_separator --
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

            -- chunk-separator-test_chunk_separator --
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

            -- chunk-separator-test_chunk_separator --
            insert into public.array_test (name) values
            (E'{foo,bar}'),
            (E'{baz,qux}'),
            (E'{quux,corge}');

            -- chunk-separator-test_chunk_separator --
            insert into public.cats (id, name, color) values
            (2, E'Fluffy', E'white');

            -- chunk-separator-test_chunk_separator --
            insert into public.dogs (id, name, breed) values
            (1, E'Fido', E'beagle');

            -- chunk-separator-test_chunk_separator --
            insert into public.my_partitioned_table_1 (value) values
            (1),
            (9);

            -- chunk-separator-test_chunk_separator --
            insert into public.my_partitioned_table_2 (value) values
            (11),
            (19);

            -- chunk-separator-test_chunk_separator --
            insert into public.people (id, name, age) values
            (1, E'foo', 42),
            (2, E'bar', 89),
            (3, E'nice', 69),
            (4, E'str\nange', 420),
            (5, E't\t\tap', 421),
            (6, E'q''t', 12);

            -- chunk-separator-test_chunk_separator --
            insert into public.pets (id, name) values
            (3, E'Remy');


            -- chunk-separator-test_chunk_separator --
            create index ext_test_table_name_idx on public.ext_test_table using gin (id, search_vector);

            create index people_age_brin_idx on public.people using brin (age);

            create index people_age_idx on public.people using btree (age desc nulls first) include (name, id) where (age % 2) = 0;

            create unique index people_name_key on public.people using btree (name asc nulls last);

            create index people_name_lower_idx on public.people using btree (lower(name) asc nulls last);

            -- chunk-separator-test_chunk_separator --
            create unique index field_id_id_unique on public.tree_node using btree (field_id asc nulls last, id asc nulls last);

            create unique index unique_name_per_level on public.tree_node using btree (field_id asc nulls last, parent_id asc nulls last, name asc nulls last);

            create sequence public.ext_test_table_id_seq as int4 increment by 1 minvalue 1 maxvalue 2147483647 start 1 cache 1;

            create sequence public.field_id_seq as int4 increment by 1 minvalue 1 maxvalue 2147483647 start 1 cache 1;

            create sequence public.people_id_seq as int4 increment by 1 minvalue 1 maxvalue 2147483647 start 1 cache 1;

            -- chunk-separator-test_chunk_separator --
            create sequence public.pets_id_seq as int4 increment by 1 minvalue 1 maxvalue 2147483647 start 1 cache 1;

            create sequence public.tree_node_id_seq as int4 increment by 1 minvalue 1 maxvalue 2147483647 start 1 cache 1;

            select pg_catalog.setval('public.people_id_seq', 6, true);

            select pg_catalog.setval('public.pets_id_seq', 3, true);

            alter table public.cats alter column id set default nextval('pets_id_seq'::regclass);

            -- chunk-separator-test_chunk_separator --
            alter table public.dogs alter column id set default nextval('pets_id_seq'::regclass);

            alter table public.ext_test_table alter column id set default nextval('ext_test_table_id_seq'::regclass);

            alter table public.field alter column id set default nextval('field_id_seq'::regclass);

            alter table public.people alter column id set default nextval('people_id_seq'::regclass);

            alter table public.pets alter column id set default nextval('pets_id_seq'::regclass);

            -- chunk-separator-test_chunk_separator --
            alter table public.tree_node alter column id set default nextval('tree_node_id_seq'::regclass);

            create view public.people_who_cant_drink (id, name, age) as  SELECT people.id,
                people.name,
                people.age
               FROM people
              WHERE people.age < 18;

            alter table public.people add constraint people_name_key unique using index people_name_key;

            alter table public.tree_node add constraint tree_node_field_id_fkey foreign key (field_id) references public.field (id);

            alter table public.tree_node add constraint tree_node_field_id_parent_id_fkey foreign key (field_id, parent_id) references public.tree_node (field_id, id);

            -- chunk-separator-test_chunk_separator --
            alter table public.tree_node add constraint field_id_id_unique unique using index field_id_id_unique;

            alter table public.tree_node add constraint unique_name_per_level unique using index unique_name_per_level;"#});

        let destination = get_test_helper_on_port("destination", 5414).await;
        apply_sql_string(&result_file, destination.get_conn()).await.unwrap();


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

        let result_file = export_to_string(&source, default()).await;

        similar_asserts::assert_eq!(result_file, indoc! {r#"
            -- chunk-separator-test_chunk_separator --
            create schema if not exists public;

            create table public.edge_case_values (
                r4 float4,
                r8 float8
            );

            -- chunk-separator-test_chunk_separator --
            insert into public.edge_case_values (r4, r8) values
            (1, 1),
            ('NaN', 'NaN'),
            ('Infinity', 'Infinity'),
            ('-Infinity', '-Infinity'),
            (null, null);
            "#});

        let destination = get_test_helper("destination").await;
        apply_sql_string(&result_file, destination.get_conn()).await.unwrap();


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

        let result_file = export_to_string(&source, default()).await;

        similar_asserts::assert_eq!(result_file, indoc! {r#"
            -- chunk-separator-test_chunk_separator --
            create schema if not exists public;

            create table public.array_values (
                values int4[]
            );

            -- chunk-separator-test_chunk_separator --
            insert into public.array_values (values) values
            (E'{1,2,3}'),
            (E'{4,5,6}');
            "#});

        let destination = get_test_helper("destination").await;
        apply_sql_string(&result_file, destination.get_conn()).await.unwrap();


        let items = destination.get_results::<(Vec<i32>, )>("select values from array_values;").await;

        assert_eq!(items.len(), 2);
        assert_eq!(items[0].0, vec![1, 2, 3]);
        assert_eq!(items[1].0, vec![4, 5, 6]);
    }

    #[test]
    async fn export_as_copy_statements() {
        let source = get_test_helper("source").await;

        //language=postgresql
        source.execute_not_query(r#"
        create table test_table(
            value int4 not null
        );

        create index test_table_value_idx on test_table using btree (value);

        insert into test_table(value)
        values (1),
               (2),
               (3);

        create table test_table_2(
            value int4 not null
        );

        create index test_table_2_value_idx on test_table_2 using btree (value);
        
        insert into test_table_2(value)
        values (4),
               (5),
               (6);
        "#).await;

        let result_file = export_to_string(&source, SqlFileOptions {
            data_mode: SqlDataMode::CopyStatements,
            ..default()
        }).await;

        similar_asserts::assert_eq!(result_file, indoc! {r#"
            -- chunk-separator-test_chunk_separator --
            create schema if not exists public;

            create table public.test_table (
                value int4 not null
            );
            
            create table public.test_table_2 (
                value int4 not null
            );

            -- chunk-separator-test_chunk_separator --
            copy public.test_table (value) from stdin with (format text, header false);
            -- chunk-separator-test_chunk_separator --
            1
            2
            3
            \.
            
            -- chunk-separator-test_chunk_separator --
            copy public.test_table_2 (value) from stdin with (format text, header false);
            -- chunk-separator-test_chunk_separator --
            4
            5
            6
            \.
            
            
            -- chunk-separator-test_chunk_separator --
            create index test_table_value_idx on public.test_table using btree (value asc nulls last);
            
            create index test_table_2_value_idx on public.test_table_2 using btree (value asc nulls last);"#});

        let destination = get_test_helper("destination").await;
        apply_sql_string(&result_file, destination.get_conn()).await.unwrap();


        let items = destination.get_single_results::<i32>("select value from test_table;").await;

        assert_eq!(items, vec![1,2,3]);
        let items = destination.get_single_results::<i32>("select value from test_table_2;").await;

        assert_eq!(items, vec![4,5,6]);
    }
}