use crate::schema_reader::tests;
use crate::test_helpers;
use crate::test_helpers::TestHelper;
use crate::{default, PostgresDatabase, PostgresExtension, PostgresSchema, TimescaleSupport};
use elefant_test_macros::pg_test;

#[pg_test(arg(postgres = 12))]
#[pg_test(arg(postgres = 13))]
#[pg_test(arg(postgres = 14))]
#[pg_test(arg(postgres = 15))]
#[pg_test(arg(postgres = 16))]
#[pg_test(arg(timescale_db = 15))]
#[pg_test(arg(timescale_db = 16))]
async fn test_extensions(helper: &TestHelper) {
    tests::test_introspection(
        helper,
        r#"
        create extension "btree_gin";
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                ..default()
            }],
            enabled_extensions: vec![PostgresExtension {
                name: "btree_gin".to_string(),
                schema_name: "public".to_string(),
                version: "1.3".to_string(),
                relocatable: true,
                ..default()
            }],
            timescale_support: TimescaleSupport::from_test_helper(helper),
            ..default()
        },
    )
    .await;
}
