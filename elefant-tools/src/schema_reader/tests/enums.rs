use elefant_test_macros::pg_test;
use crate::{default, PostgresColumn, PostgresDatabase, PostgresEnum, PostgresSchema, PostgresTable, TimescaleSupport};
use crate::schema_reader::tests;
use crate::test_helpers::TestHelper;

#[pg_test(arg(postgres = 12))]
#[pg_test(arg(postgres = 13))]
#[pg_test(arg(postgres = 14))]
#[pg_test(arg(postgres = 15))]
#[pg_test(arg(postgres = 16))]
#[pg_test(arg(timescale_db = 15))]
#[pg_test(arg(timescale_db = 16))]
async fn enums(helper: &TestHelper) {
    tests::test_introspection(
        helper,
        r#"
    CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
    CREATE TABLE person (
        name text,
        current_mood mood
    );
    alter type mood add value 'mehh' before 'ok';

    comment on type mood is 'This is a mood';
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                tables: vec![PostgresTable {
                    name: "person".to_string(),
                    columns: vec![
                        PostgresColumn {
                            name: "name".to_string(),
                            is_nullable: true,
                            ordinal_position: 1,
                            data_type: "text".to_string(),
                            ..default()
                        },
                        PostgresColumn {
                            name: "current_mood".to_string(),
                            is_nullable: true,
                            ordinal_position: 2,
                            data_type: "mood".to_string(),
                            ..default()
                        },
                    ],
                    ..default()
                }],
                enums: vec![PostgresEnum {
                    name: "mood".to_string(),
                    values: vec![
                        "sad".to_string(),
                        "mehh".to_string(),
                        "ok".to_string(),
                        "happy".to_string(),
                    ],
                    comment: Some("This is a mood".to_string()),
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
