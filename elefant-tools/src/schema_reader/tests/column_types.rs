use elefant_test_macros::pg_test;
use crate::{default, PostgresColumn, PostgresDatabase, PostgresSchema, PostgresTable, TimescaleSupport};
use crate::schema_reader::tests;
use crate::test_helpers::TestHelper;

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
