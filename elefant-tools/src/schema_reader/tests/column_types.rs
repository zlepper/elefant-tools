use elefant_test_macros::pg_test;
use crate::{default, PostgresColumn, PostgresDatabase, PostgresSchema, PostgresTable, TimescaleSupport};
use crate::schema_reader::tests;
use crate::test_helpers::TestHelper;
use crate::test_helpers;

#[pg_test(arg(postgres = 12))]
#[pg_test(arg(postgres = 13))]
#[pg_test(arg(postgres = 14))]
#[pg_test(arg(postgres = 15))]
#[pg_test(arg(postgres = 16))]
#[pg_test(arg(timescale_db = 15))]
#[pg_test(arg(timescale_db = 16))]
async fn array_columns(helper: &TestHelper) {
    tests::test_introspection(
        helper,
        r#"
        create table my_table(
            int_array int4[]
        );
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                object_id: 1.into(),
                tables: vec![PostgresTable {
                    name: "my_table".to_string(),
                    columns: vec![PostgresColumn {
                        name: "int_array".to_string(),
                        ordinal_position: 1,
                        is_nullable: true,
                        data_type: "int4".to_string(),
                        array_dimensions: 1,
                        ..default()
                    }],
                    object_id: 2.into(),
                    ..default()
                }],
                ..default()
            }],
            timescale_support: TimescaleSupport::from_test_helper(helper),
            ..default()
        },
    )
        .await;
}

#[pg_test(arg(postgres = 12))]
#[pg_test(arg(postgres = 13))]
#[pg_test(arg(postgres = 14))]
#[pg_test(arg(postgres = 15))]
#[pg_test(arg(postgres = 16))]
#[pg_test(arg(timescale_db = 15))]
#[pg_test(arg(timescale_db = 16))]
async fn column_types_of_limited_size(helper: &TestHelper) {

    tests::test_introspection(
        helper,
        r#"
        create table my_table(
            name varchar(200) not null,
            var_char_array varchar(666)[] not null
        );
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                object_id: 1.into(),
                tables: vec![PostgresTable {
                    name: "my_table".to_string(),
                    columns: vec![PostgresColumn {
                        name: "name".to_string(),
                        ordinal_position: 1,
                        is_nullable: false,
                        data_type: "varchar".to_string(),
                        array_dimensions: 0,
                        data_type_length: Some(200),
                        ..default()
                    }, PostgresColumn {
                        name: "var_char_array".to_string(),
                        ordinal_position: 2,
                        is_nullable: false,
                        data_type: "varchar".to_string(),
                        array_dimensions: 1,
                        data_type_length: Some(666),
                        ..default()
                    }],
                    object_id: 2.into(),
                    ..default()
                }],
                ..default()
            }],
            timescale_support: TimescaleSupport::from_test_helper(helper),
            ..default()
        },
    )
        .await;
}