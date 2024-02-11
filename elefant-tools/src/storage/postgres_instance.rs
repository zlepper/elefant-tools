use std::sync::Arc;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{pin_mut, SinkExt, Stream, StreamExt, TryStreamExt};
use futures::stream::MapErr;
use tokio_postgres::CopyOutStream;
use crate::models::PostgresDatabase;
use crate::postgres_client_wrapper::PostgresClientWrapper;
use crate::schema_reader::SchemaReader;
use crate::storage::{BaseCopyTarget, CopyDestination, CopySource, DataFormat, TableData};
use crate::*;
use crate::models::PostgresSchema;
use crate::models::PostgresTable;
use crate::quoting::IdentifierQuoter;

pub struct PostgresInstanceStorage<'a> {
    connection: &'a PostgresClientWrapper,
    postgres_version: String,
    identifier_quoter: Arc<IdentifierQuoter>,
}

impl<'a> PostgresInstanceStorage<'a> {
    pub async fn new(connection: &'a PostgresClientWrapper) -> Result<Self> {
        let postgres_version = connection.get_single_result("select version()").await?;

        let keywords = connection.get_single_results("select word from pg_get_keywords() where catcode <> 'U'").await?;

        let quoter = IdentifierQuoter::new(keywords.into_iter().collect());

        Ok(PostgresInstanceStorage {
            connection,
            postgres_version,
            identifier_quoter: Arc::new(quoter),
        })
    }
}

#[async_trait]
impl BaseCopyTarget for PostgresInstanceStorage<'_> {
    async fn supported_data_format(&self) -> Result<Vec<DataFormat>> {
        Ok(vec![
            DataFormat::Text,
            DataFormat::PostgresBinary {
                postgres_version: Some(self.postgres_version.clone()),
            },
        ])
    }
}

fn tokio_postgres_error_to_crate_error(e: tokio_postgres::Error) -> ElefantToolsError {
    e.into()
}

#[async_trait]
impl<'a> CopySource for PostgresInstanceStorage<'a> {
    type DataStream = MapErr<CopyOutStream, fn(tokio_postgres::Error) -> ElefantToolsError>;

    async fn get_introspection(&self) -> Result<PostgresDatabase> {
        let reader = SchemaReader::new(self.connection);
        reader.introspect_database().await
    }

    async fn get_data(&self, schema: &PostgresSchema, table: &PostgresTable, data_format: &DataFormat) -> Result<TableData<Self::DataStream>> {
        let copy_command = table.get_copy_out_command(schema, data_format, &self.identifier_quoter);
        let copy_out_stream = self.connection.copy_out(&copy_command).await?;

        let stream = copy_out_stream.map_err(tokio_postgres_error_to_crate_error as fn(tokio_postgres::Error) -> ElefantToolsError);

        match data_format {
            DataFormat::Text => {
                Ok(TableData::Text {
                    data: stream
                })
            }
            DataFormat::PostgresBinary { .. } => {
                Ok(TableData::PostgresBinary {
                    postgres_version: self.postgres_version.clone(),
                    data: stream,
                })
            }
        }
    }
}


#[async_trait]
impl<'a> CopyDestination for PostgresInstanceStorage<'a> {
    async fn apply_data<S: Stream<Item=Result<Bytes>> + Send>(&mut self, schema: &PostgresSchema, table: &PostgresTable, data: TableData<S>) -> Result<()> {
        let data_format = data.get_data_format();

        let copy_statement = table.get_copy_in_command(schema, &data_format, &self.identifier_quoter);

        let sink = self.connection.copy_in::<Bytes>(&copy_statement).await?;
        pin_mut!(sink);

        let stream = data.into_stream();

        pin_mut!(stream);

        while let Some(item) = stream.next().await {
            let item = item?;
            sink.send(item).await?;
        }

        sink.close().await?;

        Ok(())
    }

    async fn apply_ddl_statement(&mut self, statement: &str) -> Result<()> {
        self.connection.execute_non_query(statement).await?;
        Ok(())
    }

    fn get_identifier_quoter(&self) -> Arc<IdentifierQuoter> {
        self.identifier_quoter.clone()
    }
}

#[cfg(test)]
mod tests {
    use tokio::test;
    use tokio_postgres::error::SqlState;
    use crate::copy_data::{copy_data, CopyDataOptions};
    use crate::schema_reader::tests::introspect_schema;
    use super::*;
    use crate::test_helpers::*;


    async fn test_copy(data_format: DataFormat) {
        let source = get_test_helper("source").await;

        source.execute_not_query(storage::tests::SOURCE_DATABASE_CREATE_SCRIPT).await;

        let source_schema = introspect_schema(&source).await;
        let source = PostgresInstanceStorage::new(source.get_conn()).await.unwrap();

        let destination = get_test_helper("destination").await;
        let mut destination_worker = PostgresInstanceStorage::new(destination.get_conn()).await.unwrap();

        copy_data(&source, &mut destination_worker, CopyDataOptions {
            data_format: Some(data_format)
        }).await.expect("Failed to copy data");


        let items = destination.get_results::<(i32, String, i32)>("select id, name, age from people;").await;

        assert_eq!(items, storage::tests::get_expected_people_data());

        let destination_schema = introspect_schema(&destination).await;

        assert_eq!(source_schema, destination_schema);

        let result = destination.get_conn().execute_non_query("insert into people (name, age) values ('new-value', 10000)").await;
        assert_pg_error(result, SqlState::CHECK_VIOLATION);

        let result = destination.get_conn().execute_non_query("insert into people (name, age) values ('foo', 100)").await;
        assert_pg_error(result, SqlState::UNIQUE_VIOLATION);

        destination.execute_not_query("insert into field (id) values (1);").await;

        destination.execute_not_query("insert into tree_node(id, field_id, name, parent_id) values (1, 1, 'foo', null), (2, 1, 'bar', 1)").await;
        let result = destination.get_conn().execute_non_query("insert into tree_node(id, field_id, name, parent_id) values (3, 1, 'foo', null)").await;
        assert_pg_error(result, SqlState::UNIQUE_VIOLATION);

        let result = destination.get_conn().execute_non_query("insert into tree_node(id, field_id, name, parent_id) values (9999, 9999, 'foobarbaz', null)").await;
        assert_pg_error(result, SqlState::FOREIGN_KEY_VIOLATION);

        let people_who_cant_drink = destination.get_results::<(i32, String, i32)>("select id, name, age from people_who_cant_drink;").await;
        assert_eq!(people_who_cant_drink, vec![(6, "q't".to_string(), 12)]);

        let array_test_data = destination.get_results::<(Vec<String>,)>("select name from array_test;").await;

        assert_eq!(array_test_data, storage::tests::get_expected_array_test_data());
    }


    #[test]
    async fn copies_between_databases_binary_format() {
        test_copy(DataFormat::PostgresBinary {
            postgres_version: None
        }).await;
    }

    #[test]
    async fn copies_between_databases_text_format() {
        test_copy(DataFormat::Text).await;
    }

    async fn test_round_trip(sql: &str) {
        let source = get_test_helper("source").await;

        source.execute_not_query(sql).await;

        let source_schema = introspect_schema(&source).await;
        let source = PostgresInstanceStorage::new(source.get_conn()).await.unwrap();

        let destination = get_test_helper("destination").await;
        let mut destination_worker = PostgresInstanceStorage::new(destination.get_conn()).await.unwrap();

        copy_data(&source, &mut destination_worker, CopyDataOptions {
            data_format: None
        }).await.expect("Failed to copy data");

        let destination_schema = introspect_schema(&destination).await;

        assert_eq!(source_schema, destination_schema);
    }

    macro_rules! test_round_trip {
        ($name:ident, $sql:expr) => {
            #[test]
            async fn $name() {
                test_round_trip($sql).await;
            }
        };
    }

    test_round_trip!(foreign_key_actions_are_preserved, r#"
        CREATE TABLE products (
            product_no integer PRIMARY KEY,
            name text,
            price numeric
        );

        CREATE TABLE orders (
            order_id integer PRIMARY KEY,
            shipping_address text
        );

        CREATE TABLE order_items (
            product_no integer REFERENCES products ON DELETE RESTRICT ON UPDATE CASCADE,
            order_id integer REFERENCES orders ON DELETE CASCADE ON UPDATE RESTRICT,
            quantity integer,
            PRIMARY KEY (product_no, order_id)
        );
    "#);

    test_round_trip!(filtered_foreign_key_set_null, r#"
        CREATE TABLE tenants (
            tenant_id integer PRIMARY KEY
        );

        CREATE TABLE users (
            tenant_id integer REFERENCES tenants ON DELETE CASCADE,
            user_id integer NOT NULL,
            PRIMARY KEY (tenant_id, user_id)
        );

        CREATE TABLE posts (
            tenant_id integer REFERENCES tenants ON DELETE CASCADE,
            post_id integer NOT NULL,
            author_id integer,
            PRIMARY KEY (tenant_id, post_id),
            FOREIGN KEY (tenant_id, author_id) REFERENCES users ON DELETE SET NULL (author_id)
        );
    "#);

    test_round_trip!(generated_columns, r#"
    CREATE TABLE people (
        height_cm numeric,
        height_in numeric GENERATED ALWAYS AS (height_cm / 2.54) STORED
    );
    "#);

    test_round_trip!(functions, r#"

    create function add(a integer, b integer) returns integer as $$
        begin
            return a + b;
        end;
    $$ language plpgsql;

    create function filter_stuff(value text) returns table(id int, name text) as
        $$
        begin

        create temp table temp_table(id int, name text);

        insert into temp_table(id, name) values (1, 'foo'), (2, 'bar');

        return query select * from temp_table where name = value;

        end;

        $$ language plpgsql;
    "#);

    test_round_trip!(qouted_identifier_name, r#"
        create table "MyTable" (
            "MyColumn" int,
            "MyTextColumn" text
        );

        create index "MyIndex" on "MyTable" (lower("MyTextColumn"));
    "#);

    //language=postgresql
    test_round_trip!(ddl_dependencies_1, r#"
        create function a_is_odd(a integer) returns boolean as $$
        begin
            return a % 2 = 1;
        end;
        $$ language plpgsql;

        create function b_is_even(a integer) returns boolean as $$
        begin
            return a_is_odd(a) = false;
        end;
        $$ language plpgsql;
    "#);

    //language=postgresql
    test_round_trip!(ddl_dependencies_2, r#"
        create function b_is_odd(a integer) returns boolean as $$
        begin
            return a % 2 = 1;
        end;
        $$ language plpgsql;

        create function a_is_even(a integer) returns boolean as $$
        begin
            return b_is_odd(a) = false;
        end;
        $$ language plpgsql;
    "#);

    //language=postgresql
    test_round_trip!(ddl_dependencies_1_1, r#"
        create function b_is_even(a integer) returns boolean as $$
        begin
            return a_is_odd(a) = false;
        end;
        $$ language plpgsql;

        create function a_is_odd(a integer) returns boolean as $$
        begin
            return a % 2 = 1;
        end;
        $$ language plpgsql;
    "#);

    //language=postgresql
    test_round_trip!(ddl_dependencies_2_2, r#"
        create function a_is_even(a integer) returns boolean as $$
        begin
            return b_is_odd(a) = false;
        end;
        $$ language plpgsql;

        create function b_is_odd(a integer) returns boolean as $$
        begin
            return a % 2 = 1;
        end;
        $$ language plpgsql;
    "#);

    test_round_trip!(ddl_dependencies_3, r#"
        create function is_odd(a integer) returns boolean as $$
        begin
            return a % 2 = 1;
        end;
        $$ language plpgsql;

        create table tab(
            value int not null check (is_odd(value))
        );
    "#);

    test_round_trip!(comments_on_stuff, r#"
        create table my_table(
            value serial not null,
            another_value int not null unique
        );

        alter table my_table add constraint my_table_value_check check (value > 0);

        comment on table my_table is 'This is a ''table''';
        comment on column my_table.value is 'This is a column';
        comment on constraint my_table_value_check on my_table is 'This is a constraint';

        create function my_function() returns int as $$ begin return 1; end; $$ language plpgsql;
        create function my_function_2(a int, b int) returns int as $$ begin return a + b; end; $$ language plpgsql;

        comment on function my_function() is 'This is a function';
        comment on function my_function_2(int, int) is 'This is another function';

        create view my_view as select 1 as value;

        comment on view my_view is 'This is a view';

        comment on schema public is 'This is a schema';

        comment on sequence my_table_value_seq is 'This is a sequence';

        comment on index my_table_another_value_key is 'This is an index';
        comment on constraint my_table_another_value_key on my_table is 'This is a unique constraint';

    "#);

    test_round_trip!(array_columns, r#"
        create table my_table(
            id serial primary key,
            names text[]
        );
    "#);

    test_round_trip!(materialized_views, r#"
        create table my_table(
            id serial primary key,
            name text
        );

        insert into my_table(name) values ('foo'), ('bar');

        create materialized view my_materialized_view as select id, name from my_table;

        comment on materialized view my_materialized_view is 'This is a materialized view';
    "#);
}