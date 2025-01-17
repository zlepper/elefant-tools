use crate::chunk_reader::StringChunkReader;
use crate::copy_data::{copy_data, CopyDataOptions};
use crate::helpers::StringExt;
use crate::quoting::{AttemptedKeywordUsage, Quotable};
use crate::schema_reader::tests::introspect_schema;
use crate::schema_reader::SchemaReader;
use crate::storage::tests::validate_copy_state;
use crate::test_helpers;
use crate::test_helpers::*;
use crate::{
    apply_sql_string, default, storage, DataFormat, IdentifierQuoter, PostgresColumn,
    PostgresDatabase, PostgresIndex, PostgresIndexColumnDirection, PostgresIndexKeyColumn,
    PostgresIndexNullsOrder, PostgresIndexType, PostgresInstanceStorage, PostgresSchema,
    PostgresSequence, PostgresTable, SqlDataMode, SqlFile, SqlFileOptions,
};
use elefant_test_macros::pg_test;
use itertools::Itertools;
use std::num::NonZeroUsize;
use std::sync::Arc;

async fn test_copy(data_format: DataFormat, source: &TestHelper, destination: &TestHelper) {
    source
        .execute_not_query(storage::tests::get_copy_source_database_create_script(
            source.get_conn().version(),
        ))
        .await;

    let source_schema = introspect_schema(source).await;
    let source = PostgresInstanceStorage::new(source.get_conn())
        .await
        .unwrap();

    let mut destination_worker = PostgresInstanceStorage::new(destination.get_conn())
        .await
        .unwrap();

    copy_data(
        &source,
        &mut destination_worker,
        CopyDataOptions {
            data_format: Some(data_format),
            max_parallel: Some(NonZeroUsize::new(16).unwrap()),
            ..default()
        },
    )
    .await
    .expect("Failed to copy data");

    let destination_schema = introspect_schema(destination).await;

    assert_eq!(source_schema, destination_schema);

    validate_copy_state(destination).await;
}

#[pg_test(arg(postgres = 15), arg(postgres = 15))]
async fn copies_between_databases_binary_format(source: &TestHelper, destination: &TestHelper) {
    test_copy(
        DataFormat::PostgresBinary {
            postgres_version: None,
        },
        source,
        destination,
    )
    .await;
}

#[pg_test(arg(postgres = 15), arg(postgres = 15))]
async fn copies_between_databases_text_format(source: &TestHelper, destination: &TestHelper) {
    test_copy(DataFormat::Text, source, destination).await;
}

async fn test_round_trip(sql: &str, source: &TestHelper, destination: &TestHelper) {
    apply_sql_string(sql, source.get_conn()).await.unwrap();

    let source_schema = introspect_schema(source).await;
    let source = PostgresInstanceStorage::new(source.get_conn())
        .await
        .unwrap();

    let mut destination_worker = PostgresInstanceStorage::new(destination.get_conn())
        .await
        .unwrap();

    copy_data(
        &source,
        &mut destination_worker,
        CopyDataOptions {
            data_format: None,
            max_parallel: Some(NonZeroUsize::new(16).unwrap()),
            ..default()
        },
    )
    .await
    .expect("Failed to copy data");

    let destination_schema = introspect_schema(destination).await;

    assert_eq!(source_schema, destination_schema);
}

macro_rules! test_round_trip {
    ($name:ident, $sql:literal) => {
        mod $name {
            use super::*;

            const SQL: &str = $sql;

            #[pg_test(arg(postgres = 12), arg(postgres = 12))]
            #[pg_test(arg(postgres = 13), arg(postgres = 13))]
            #[pg_test(arg(postgres = 14), arg(postgres = 14))]
            #[pg_test(arg(postgres = 15), arg(postgres = 15))]
            #[pg_test(arg(postgres = 16), arg(postgres = 16))]
            async fn non_differential(source: &TestHelper, destination: &TestHelper) {
                test_round_trip(SQL, source, destination).await;
            }

            #[pg_test(arg(postgres = 12))]
            #[pg_test(arg(postgres = 13))]
            #[pg_test(arg(postgres = 14))]
            #[pg_test(arg(postgres = 15))]
            #[pg_test(arg(postgres = 16))]
            async fn differential(source: &TestHelper) {
                test_differential_copy_generic(source, SQL).await;
            }
        }
    };
}

test_round_trip!(
    foreign_key_actions_are_preserved,
    r#"
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
    "#
);

#[pg_test(arg(postgres = 15), arg(postgres = 15))]
#[pg_test(arg(postgres = 16), arg(postgres = 16))]
async fn filtered_foreign_key_set_null(source: &TestHelper, destination: &TestHelper) {
    test_round_trip(
        r#"
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
    "#,
        source,
        destination,
    )
    .await;
}

test_round_trip!(
    generated_columns,
    r#"
    CREATE TABLE people (
        height_cm numeric,
        height_in numeric GENERATED ALWAYS AS (height_cm / 2.54) STORED
    );
    "#
);

test_round_trip!(
    functions,
    r#"

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
    "#
);

test_round_trip!(
    qouted_identifier_name,
    r#"
        create table "MyTable" (
            "MyColumn" int,
            "MyTextColumn" text
        );

        create index "MyIndex" on "MyTable" (lower("MyTextColumn"));
    "#
);

//language=postgresql
test_round_trip!(
    ddl_dependencies_1,
    r#"
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
    "#
);

//language=postgresql
test_round_trip!(
    ddl_dependencies_2,
    r#"
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
    "#
);

//language=postgresql
test_round_trip!(
    ddl_dependencies_1_1,
    r#"
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
    "#
);

//language=postgresql
test_round_trip!(
    ddl_dependencies_2_2,
    r#"
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
    "#
);

test_round_trip!(
    ddl_dependencies_3,
    r#"
        create function is_odd(a integer) returns boolean as $$
        begin
            return a % 2 = 1;
        end;
        $$ language plpgsql;

        create table tab(
            value int not null check (is_odd(value))
        );
    "#
);

test_round_trip!(
    ddl_dependencies_4,
    r#"
        create view a_view as select 1 as value;

        create view b_view as select * from a_view;
    "#
);

test_round_trip!(
    ddl_dependencies_4_opposite,
    r#"
        create view b_view as select 1 as value;

        create view a_view as select * from b_view;
    "#
);

test_round_trip!(
    ddl_dependencies_5,
    r#"
        create materialized view a_view as select 1 as value;

        create materialized view b_view as select * from a_view;
    "#
);

test_round_trip!(
    ddl_dependencies_5_opposite,
    r#"
        create materialized view b_view as select 1 as value;

        create materialized view a_view as select * from b_view;
    "#
);

test_round_trip!(
    functions_reading_from_tables_in_pure_sql,
    r#"
create table my_table(
    value int not null
);

create function my_function() returns bigint as $$
    select sum(value) from my_table
$$ language sql;
"#
);

test_round_trip!(
    comments_on_stuff,
    r#"
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

    "#
);

test_round_trip!(
    array_columns,
    r#"
        create table my_table(
            id serial primary key,
            names text[]
        );
    "#
);

test_round_trip!(
    materialized_views,
    r#"
        create table my_table(
            id serial primary key,
            name text
        );

        insert into my_table(name) values ('foo'), ('bar');

        create materialized view my_materialized_view as select id, name from my_table;

        comment on materialized view my_materialized_view is 'This is a materialized view';
    "#
);

test_round_trip!(
    triggers,
    r#"

        create table my_table(
            value int
        );

        create function my_trigger_function() returns trigger as $$
        begin return new; end;
        $$ language plpgsql;
        
        create function my_parametised_trigger_function() returns trigger as $$
        begin return new; end;
        $$ language plpgsql;

        create trigger my_trigger after insert on my_table for each row execute function my_trigger_function();

        comment on trigger my_trigger on my_table is 'This is a trigger';

        create trigger scoped_trigger before update on my_table for each row when (OLD.value is distinct from NEW.value) execute procedure my_trigger_function();

        create trigger truncate_trigger after truncate on my_table for each statement execute procedure my_trigger_function();

        create trigger updt_insert_trigger before update or insert on my_table for each row execute procedure my_parametised_trigger_function(42, 'foo');
    "#
);

test_round_trip!(
    enumerations,
    r#"
    create type mood as enum ('sad', 'ok', 'happy');
    create table person (
        name text,
        current_mood mood
    );

    alter type mood add value 'mehh' before 'ok';
    "#
);

test_round_trip!(
    range_partitions,
    r#"
    CREATE TABLE sales (
                       sale_id INT,
                       sale_date DATE,
                       product_id INT,
                       quantity INT,
                       amount NUMERIC
) partition by range (sale_date);

CREATE TABLE sales_january PARTITION OF sales
    FOR VALUES FROM ('2023-01-01') TO ('2023-02-01');

CREATE TABLE sales_february PARTITION OF sales
    FOR VALUES FROM ('2023-02-01') TO ('2023-03-01');

CREATE TABLE sales_march PARTITION OF sales
    FOR VALUES FROM ('2023-03-01') TO ('2023-04-01');
    "#
);

test_round_trip!(
    list_partitions,
    r#"
CREATE TABLE products (
    product_id int,
    category TEXT,
    product_name TEXT,
    price NUMERIC
) partition by list(category);

CREATE TABLE electronics PARTITION OF products
    FOR VALUES IN ('Electronics');

CREATE TABLE clothing PARTITION OF products
    FOR VALUES IN ('Clothing');

CREATE TABLE furniture PARTITION OF products
    FOR VALUES IN ('Furniture');
    "#
);

test_round_trip!(
    hash_partitions,
    r#"
CREATE TABLE orders (
    order_id int,
    order_date DATE,
    customer_id INT,
    total_amount NUMERIC
) partition by hash(customer_id);

CREATE TABLE orders_1 PARTITION OF orders
    FOR VALUES WITH (MODULUS 3, REMAINDER 0);

CREATE TABLE orders_2 PARTITION OF orders
    FOR VALUES WITH (MODULUS 3, REMAINDER 1);

CREATE TABLE orders_3 PARTITION OF orders
    FOR VALUES WITH (MODULUS 3, REMAINDER 2);
    "#
);

test_round_trip!(
    inheritance,
    r#"
create table pets (
    id serial primary key,
    name text not null check(length(name) > 1)
);

create table dogs(
    breed text not null check(length(breed) > 1)
) inherits (pets);

create table cats(
    color text not null
) inherits (pets);
    "#
);

test_round_trip!(
    multiple_inheritance,
    r#"
create table animal(
    breed text not null
);

create table human(
    name text not null
);

create table animorph() inherits (animal, human);
"#
);

test_round_trip!(
    functions_returning_custom_table,
    r#"
create function my_function() returns table(id int, name text) as $$
begin
    return query select 1, 'foo';
end;
$$ language plpgsql;
"#
);

test_round_trip!(
    functions_returning_table_type,
    r#"

create table my_table(id int, name text);

create function my_function() returns setof my_table as $$
begin
    return query select 1, 'foo';
end;
$$ language plpgsql;
"#
);

#[pg_test(arg(postgres = 13), arg(postgres = 13))]
#[pg_test(arg(postgres = 14), arg(postgres = 14))]
#[pg_test(arg(postgres = 15), arg(postgres = 15))]
#[pg_test(arg(postgres = 16), arg(postgres = 16))]
async fn storage_parameters(source: &TestHelper, destination: &TestHelper) {
    test_round_trip(
        r#"
    create table my_table(name text not null) with (fillfactor=50);

    create index my_index on my_table(name) with (fillfactor = 20, deduplicate_items = off);
    "#,
        source,
        destination,
    )
    .await;
}

#[pg_test(arg(postgres = 12), arg(postgres = 12))]
async fn storage_parameters_pg_12(source: &TestHelper, destination: &TestHelper) {
    test_round_trip(
        r#"
    create table my_table(name text not null) with (fillfactor=50);

    create index my_index on my_table(name) with (fillfactor = 20);
    "#,
        source,
        destination,
    )
    .await;
}

#[pg_test(arg(timescale_db = 15), arg(timescale_db = 15))]
#[pg_test(arg(timescale_db = 16), arg(timescale_db = 16))]
async fn timescale_hypertable_time_single_dimension(source: &TestHelper, destination: &TestHelper) {
    test_round_trip(r#"

CREATE TABLE stocks_real_time (
  time TIMESTAMPTZ NOT NULL,
  symbol TEXT NOT NULL,
  price DOUBLE PRECISION NULL,
  day_volume INT NULL
);

SELECT create_hypertable('stocks_real_time', by_range('time', '7 days'::interval));

CREATE INDEX ix_symbol_time ON stocks_real_time (symbol, time DESC);

insert into stocks_real_time(time, symbol, price, day_volume) values ('2023-01-01', 'AAPL', 100.0, 1000);

        "#, source, destination).await;

    let items = destination
        .get_results::<(String, f64, i32)>(
            "select symbol, price, day_volume from stocks_real_time;",
        )
        .await;

    assert_eq!(items, vec![("AAPL".to_string(), 100.0, 1000)]);
}

#[pg_test(arg(timescale_db = 15), arg(timescale_db = 15))]
#[pg_test(arg(timescale_db = 16), arg(timescale_db = 16))]
async fn timescale_hypertable_time_multiple_dimensions(
    source: &TestHelper,
    destination: &TestHelper,
) {
    test_round_trip(
        r#"

CREATE TABLE stocks_real_time (
  time TIMESTAMPTZ NOT NULL,
  symbol TEXT NOT NULL,
  price DOUBLE PRECISION NULL,
  day_volume INT NULL,
  primary key (time, symbol, day_volume)
);

SELECT create_hypertable('stocks_real_time', by_range('time', '7 days'::interval));
SELECT add_dimension('stocks_real_time', by_hash('symbol', 4));
SELECT add_dimension('stocks_real_time', by_range('day_volume', 100));

CREATE INDEX ix_symbol_time ON stocks_real_time (symbol, time DESC);

        "#,
        source,
        destination,
    )
    .await;
}

#[pg_test(arg(timescale_db = 15), arg(timescale_db = 15))]
#[pg_test(arg(timescale_db = 16), arg(timescale_db = 16))]
async fn timescale_hypertable_compression(source: &TestHelper, destination: &TestHelper) {
    test_round_trip(
        r#"

CREATE TABLE stocks_real_time (
  time TIMESTAMPTZ NOT NULL,
  symbol TEXT NOT NULL,
  price DOUBLE PRECISION NULL,
  day_volume INT NOT NULL
);

SELECT create_hypertable('stocks_real_time', by_range('time', '7 days'::interval));

alter table stocks_real_time set(
    timescaledb.compress,
        timescaledb.compress_segmentby = 'symbol',
        timescaledb.compress_orderby = 'time,day_volume',
        timescaledb.compress_chunk_time_interval='14 days'
        );

select add_compression_policy('stocks_real_time', interval '7 days');

        "#,
        source,
        destination,
    )
    .await;
}

#[pg_test(arg(timescale_db = 15), arg(timescale_db = 15))]
#[pg_test(arg(timescale_db = 16), arg(timescale_db = 16))]
async fn timescale_continuous_aggregate(source: &TestHelper, destination: &TestHelper) {
    test_round_trip(r#"
CREATE TABLE stocks_real_time (
  time TIMESTAMPTZ NOT NULL,
  symbol TEXT NOT NULL,
  price DOUBLE PRECISION NULL,
  day_volume INT NOT NULL
);

SELECT create_hypertable('stocks_real_time', by_range('time', '7 days'::interval));

insert into stocks_real_time(time, symbol, price, day_volume) values ('2023-01-01', 'AAPL', 100.0, 1000);

CREATE MATERIALIZED VIEW stock_candlestick_daily
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 day', "time") AS day,
  symbol,
  max(price) AS high,
  first(price, time) AS open,
  last(price, time) AS close,
  min(price) AS low
FROM stocks_real_time srt
GROUP BY day, symbol
WITH NO DATA;

SELECT add_continuous_aggregate_policy('stock_candlestick_daily',
                                       start_offset => INTERVAL '6 month',
                                       end_offset => INTERVAL '1 day',
                                       schedule_interval => INTERVAL '1 hour');

alter materialized view stock_candlestick_daily set (timescaledb.compress = true);

SELECT add_compression_policy('stock_candlestick_daily', compress_after=>'360 days'::interval);
SELECT add_retention_policy('stock_candlestick_daily', INTERVAL '2 years');
       "#, source, destination).await;

    let items = destination
        .get_results::<(String, String, f64, f64, f64, f64)>(
            "select day::text, symbol, high, open, close, low from stock_candlestick_daily;",
        )
        .await;

    assert_eq!(
        items,
        vec![(
            "2023-01-01 00:00:00+00".to_string(),
            "AAPL".to_string(),
            100.0,
            100.0,
            100.0,
            100.0
        )]
    );
}

#[pg_test(arg(timescale_db = 15), arg(timescale_db = 15))]
#[pg_test(arg(timescale_db = 16), arg(timescale_db = 16))]
async fn timescale_retention_policy(source: &TestHelper, destination: &TestHelper) {
    test_round_trip(
        r#"
CREATE TABLE conditions (
  time TIMESTAMPTZ NOT NULL
);

SELECT create_hypertable('conditions', by_range('time', '1 hour'::interval));
SELECT add_retention_policy('conditions', INTERVAL '24 hours');
       "#,
        source,
        destination,
    )
    .await;
}

#[pg_test(arg(timescale_db = 15), arg(timescale_db = 15))]
#[pg_test(arg(timescale_db = 16), arg(timescale_db = 16))]
async fn timescale_user_defined_jobs(source: &TestHelper, destination: &TestHelper) {
    test_round_trip(
        r#"
CREATE PROCEDURE user_defined_action(job_id INT, config JSONB)
    LANGUAGE PLPGSQL AS
    $$
    BEGIN
        RAISE NOTICE 'Executing job % with config %', job_id, config;
    END
    $$;

SELECT add_job('user_defined_action', '1h', config => '{"hypertable":"metrics"}');
       "#,
        source,
        destination,
    )
    .await;
}

// This is quite slow, so we only test against 1 postgres instance
// We are not really testing postgres, but the internal parallel handling
// in this program.
#[pg_test(arg(postgres = 15), arg(postgres = 15))]
async fn ensure_survives_many_tables(source: &TestHelper, destination: &TestHelper) {
    let mut sql = String::new();

    for i in 0..50 {
        sql.push_str(&format!(
            "create table my_table_{}(id serial primary key, name text);\n",
            i
        ));
        sql.push_str(&format!(
            r#"
insert into my_table_{} (
    name
)
select
    md5(random()::text)
from generate_series(1, 1000) s(i);"#,
            i
        ))
    }

    test_round_trip(&sql, source, destination).await;

    for i in 0..50 {
        let items = destination
            .get_results::<(i32, String)>(&format!("select id, name from my_table_{};", i))
            .await;
        assert_eq!(items.len(), 1000);
    }
}

#[pg_test(arg(postgres = 15))]
async fn copies_between_schemas_in_same_db(helper: &TestHelper) {
    helper
        .execute_not_query("create schema source_schema; create schema destination_schema;")
        .await;

    let source = helper.get_schema_connection("source_schema").await;
    let destination = helper.get_schema_connection("destination_schema").await;

    source
        .execute_non_query(
            r#"
        create table my_table(id serial primary key, name text not null);
        insert into my_table (
    name
)
select
    md5(random()::text)
from generate_series(1, 1000) s(i);
        "#,
        )
        .await
        .unwrap();

    let source_storage = PostgresInstanceStorage::new(&source).await.unwrap();
    let mut destination_storage = PostgresInstanceStorage::new(&destination).await.unwrap();

    copy_data(
        &source_storage,
        &mut destination_storage,
        CopyDataOptions {
            target_schema: Some("source_schema".to_string()),
            rename_schema_to: Some("destination_schema".to_string()),
            ..default()
        },
    )
    .await
    .unwrap();

    let destination_schema = SchemaReader::new(&destination)
        .introspect_database()
        .await
        .unwrap()
        .filtered_to_schema("destination_schema");

    assert_eq!(
        destination_schema,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "destination_schema".to_string(),
                tables: vec![PostgresTable {
                    name: "my_table".to_string(),
                    columns: vec![
                        PostgresColumn {
                            name: "id".to_string(),
                            data_type: "int4".to_string(),
                            is_nullable: false,
                            ordinal_position: 1,
                            default_value: Some("nextval('my_table_id_seq'::regclass)".to_string()),
                            ..default()
                        },
                        PostgresColumn {
                            name: "name".to_string(),
                            data_type: "text".to_string(),
                            is_nullable: false,
                            ordinal_position: 2,
                            ..default()
                        },
                    ],
                    indices: vec![PostgresIndex {
                        name: "my_table_pkey".to_string(),
                        key_columns: vec![PostgresIndexKeyColumn {
                            ordinal_position: 1,
                            name: "id".to_string(),
                            direction: Some(PostgresIndexColumnDirection::Ascending),
                            nulls_order: Some(PostgresIndexNullsOrder::Last)
                        }],
                        index_constraint_type: PostgresIndexType::PrimaryKey,
                        index_type: "btree".to_string(),
                        ..default()
                    }],
                    ..default()
                }],
                sequences: vec![PostgresSequence {
                    name: "my_table_id_seq".to_string(),
                    data_type: "int4".to_string(),
                    max_value: 2147483647,
                    last_value: Some(1000),
                    ..default()
                }],
                ..default()
            }],
            ..default()
        }
    );

    let items = source
        .get_results::<(i32, String)>("select id, name from my_table;")
        .await
        .unwrap();
    assert_eq!(items.len(), 1000);

    let items = destination
        .get_results::<(i32, String)>("select id, name from my_table;")
        .await
        .unwrap();
    assert_eq!(items.len(), 1000);
}

test_round_trip!(
    two_way_references,
    r#"
create table assets(
    asset_id serial primary key,
    asset_digiupload_id int
);

create table asset_digiuploads(
    asset_digiupload_id serial primary key,
    asset_id int references assets(asset_id)
);

alter table assets add constraint fk_asset_digiupload_id foreign key (asset_digiupload_id) references asset_digiuploads(asset_digiupload_id);
"#
);

test_round_trip!(
    multiple_unique_constraints_on_same_table,
    r#"
create table users(
    id serial primary key,
    username text not null unique,
    email text not null unique
);
"#
);

test_round_trip!(
    domains,
    r#"
create domain public.year as integer
    constraint year_check check (((value >= 1901) and (value <= 2155)));

create domain public.twenties as year
    constraint twenties_check check (value >= 1920 and value <= 1929);

comment on domain public.year is 'year between 1901 and 2155';

create domain unix_year as integer default 1970;

create domain non_null_year as year not null;

create domain smol_text as varchar(10);

create table movie
(
    name text not null,
    year year not null
);
"#
);

test_round_trip!(
    limited_length_columns,
    r#"
create table my_table(
    name varchar(200) not null,
    var_char_array varchar(666)[] not null
);
"#
);

#[pg_test(arg(timescale_db = 15), arg(timescale_db = 15))]
#[pg_test(arg(timescale_db = 16), arg(timescale_db = 16))]
async fn timescale_foreign_keys_on_compressed_tables(
    source: &TestHelper,
    destination: &TestHelper,
) {
    test_round_trip(
        r#"
create table user_files(
    id serial primary key,
    file_name text not null
);

create table user_file_downloads(
    time timestamptz not null,
    user_file_id int not null references user_files(id)
);

select create_hypertable('user_file_downloads', by_range('time', '7 day'::interval));

alter table user_file_downloads set(
    timescaledb.compress,
        timescaledb.compress_segmentby = 'user_file_id'
    );

select add_compression_policy('user_file_downloads', interval '7 days');

       "#,
        source,
        destination,
    )
    .await;
}

async fn export_to_string(source: &TestHelper) -> String {
    let mut result_file = Vec::<u8>::new();

    {
        let quoter = IdentifierQuoter::empty();

        let mut sql_file = SqlFile::new(
            &mut result_file,
            Arc::new(quoter),
            SqlFileOptions {
                chunk_separator: "test_chunk_separator".to_string(),
                max_commands_per_chunk: 1,
                data_mode: SqlDataMode::InsertStatements,
                ..default()
            },
        )
        .await
        .unwrap();

        let source = PostgresInstanceStorage::new(source.get_conn())
            .await
            .unwrap();

        copy_data(&source, &mut sql_file, CopyDataOptions::default())
            .await
            .unwrap();
    }

    String::from_utf8(result_file).unwrap()
}
const SEPARATOR_LINE: &str = "-- chunk-separator-test_chunk_separator --\n";

pub async fn test_differential_copy_generic(source: &TestHelper, setup_query: &str) {
    source.execute_not_query(setup_query).await;

    let source_schema = introspect_schema(source).await;

    let sql = export_to_string(source).await;

    let source_storage = PostgresInstanceStorage::new(source.get_conn())
        .await
        .unwrap();

    let commands = sql
        .as_bytes()
        .read_lines_until_separator_line_to_vec(SEPARATOR_LINE)
        .await
        .unwrap();

    for i in 0..commands.len() {
        let to_execute = commands.iter().take(i);

        let destination = source.create_another_database(&format!("test_{i}")).await;

        for command in to_execute {
            destination.execute_not_query(command).await;
        }

        let mut destination_worker = PostgresInstanceStorage::new(destination.get_conn())
            .await
            .unwrap();

        copy_data(
            &source_storage,
            &mut destination_worker,
            CopyDataOptions {
                data_format: None,
                max_parallel: None,
                differential: true,
                ..default()
            },
        )
        .await
        .expect("Failed to copy data");

        let destination_schema = introspect_schema(&destination).await;

        assert_eq!(source_schema, destination_schema);

        let destination_raw_connection = destination.get_conn().underlying_connection();
        let source_raw_connection = source.get_conn().underlying_connection();

        for schema in &source_schema.schemas {
            for table in &schema.tables {
                let mut query = "select ".to_string();

                query.push_join(
                    ", ",
                    table
                        .columns
                        .iter()
                        .filter(|c| c.generated.is_none())
                        .map(|c| {
                            format!(
                                "{}::text",
                                c.name.quote(
                                    &source_storage.identifier_quoter,
                                    AttemptedKeywordUsage::ColumnName
                                )
                            )
                        })
                        .collect_vec(),
                );

                query.push_str(" from ");
                query.push_str(&schema.name.quote(
                    &source_storage.identifier_quoter,
                    AttemptedKeywordUsage::Other,
                ));
                query.push('.');
                query.push_str(&table.name.quote(
                    &source_storage.identifier_quoter,
                    AttemptedKeywordUsage::TypeOrFunctionName,
                ));

                let from_source = source_raw_connection.query(&query, &[]).await.unwrap();
                let from_destination = destination_raw_connection.query(&query, &[]).await.unwrap();

                assert_eq!(
                    from_source.len(),
                    from_destination.len(),
                    "Table: {}.{}. Expected {}, got {}",
                    schema.name,
                    table.name,
                    from_source.len(),
                    from_destination.len()
                );

                for (row_index, (source_row, destination_row)) in
                    from_source.iter().zip(from_destination).enumerate()
                {
                    for (idx, col) in source_row.columns().iter().enumerate() {
                        let source_value: String = source_row.get(idx);
                        let destination_value: String = destination_row.get(idx);
                        assert_eq!(
                            source_value,
                            destination_value,
                            "Table: {}.{}. Row: {}. Column: {}. Expected {:?}, got {:?}",
                            schema.name,
                            table.name,
                            row_index,
                            col.name(),
                            source_value,
                            destination_value
                        );
                    }
                }
            }
        }

        destination.stop().await;
    }
}

#[pg_test(arg(postgres = 15))]
async fn test_differential_copy(source: &TestHelper) {
    test_differential_copy_generic(source, r#"

        CREATE TABLE products (
            product_no integer PRIMARY KEY,
            name text,
            price numeric
        );

        insert into products(product_no, name, price) values (1, 'foo', 1.0), (2, 'bar', 2.0), (3, 'baz', 3.0);

        CREATE TABLE orders (
            order_id integer PRIMARY KEY,
            shipping_address text
        );

        insert into orders(order_id, shipping_address) values (1, 'foo'), (2, 'bar'), (3, 'baz');

        CREATE TABLE order_items (
            product_no integer REFERENCES products ON DELETE RESTRICT ON UPDATE CASCADE,
            order_id integer REFERENCES orders ON DELETE CASCADE ON UPDATE RESTRICT,
            quantity integer,
            PRIMARY KEY (product_no, order_id)
        );

        insert into order_items(product_no, order_id, quantity) values (1, 1, 1), (2, 2, 2), (3, 3, 3);
    "#).await;
}


test_round_trip!(identity_column_by_default, r#"
    create table my_table(
        id int generated by default as identity primary key,
        name text not null
    );

    insert into my_table(name) values ('foo'), ('bar');
"#);

test_round_trip!(identity_column_always, r#"
    create table my_table(
        id int generated always as identity primary key,
        name text not null
    );

    insert into my_table(name) values ('foo'), ('bar');
"#);

test_round_trip!(identity_column_by_default_custom_sequence, r#"
    create table my_table(
        id int generated by default as identity (START WITH 10 INCREMENT BY 10) primary key,
        name text not null
    );

    insert into my_table(name) values ('foo'), ('bar');
"#);

test_round_trip!(identity_column_by_default_custom_sequence_start_only, r#"
    create table my_table(
        id int generated by default as identity (START WITH 10) primary key,
        name text not null
    );

    insert into my_table(name) values ('foo'), ('bar');
"#);

test_round_trip!(identity_column_by_default_custom_sequence_increment_only, r#"
    create table my_table(
        id int generated by default as identity (INCREMENT BY 10) primary key,
        name text not null
    );

    insert into my_table(name) values ('foo'), ('bar');
"#);

#[pg_test(arg(postgres = 15), arg(postgres = 15))]
async fn identity_column_sequence_continues_correctly(source: &TestHelper, destination: &TestHelper) {
    test_round_trip(r#"
    create table my_table(
        id int generated by default as identity primary key,
        name text not null
    );

    insert into my_table(name) values ('foo'), ('bar');
"#, source, destination).await;

    destination.execute_not_query("insert into my_table(name) values ('baz'), ('qux')").await;

    let items = destination.get_results::<(i32, String)>("select id, name from my_table order by id").await;

    assert_eq!(items, vec![(1, "foo".to_string()), (2, "bar".to_string()), (3, "baz".to_string()), (4, "qux".to_string())]);

}

test_round_trip!(identity_columns_on_renamed_tables, r#"
    create table my_table(
        id int generated by default as identity primary key,
        name text not null
    );

    insert into my_table(name) values ('foo'), ('bar');

    alter table my_table rename to new_my_table;
"#);


test_round_trip!(identity_columns_on_renamed_tables_id_column_is_not_first_column, r#"
    create table my_table(
        name text not null,
        id int generated by default as identity primary key
    );

    insert into my_table(name) values ('foo'), ('bar');

    alter table my_table rename to new_my_table;
"#);


#[pg_test(arg(timescale_db = 15), arg(timescale_db = 15))]
#[pg_test(arg(timescale_db = 16), arg(timescale_db = 16))]
async fn timescale_constraints_on_indices(source: &TestHelper, destination: &TestHelper) {
    test_round_trip(r#"
    create table my_table(time timestamptz not null, event_id uuid not null, member_id int not null, web_site_url text not null);

    alter table my_table add constraint my_uniq unique (time, event_id);

    select create_hypertable('my_table', by_range('time', '7 day'::interval));
    "#, source, destination).await;
}