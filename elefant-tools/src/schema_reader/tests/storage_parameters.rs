use crate::schema_reader::tests::test_introspection;
use crate::test_helpers;
use crate::test_helpers::TestHelper;
use crate::{
    default, PostgresColumn, PostgresDatabase, PostgresIndex, PostgresIndexColumnDirection,
    PostgresIndexKeyColumn, PostgresIndexNullsOrder, PostgresIndexType, PostgresSchema,
    PostgresTable, TimescaleSupport,
};
use elefant_test_macros::pg_test;

#[pg_test(arg(postgres = 13))]
#[pg_test(arg(postgres = 14))]
#[pg_test(arg(postgres = 15))]
#[pg_test(arg(postgres = 16))]
#[pg_test(arg(timescale_db = 15))]
#[pg_test(arg(timescale_db = 16))]
async fn index_storage_parameters(helper: &TestHelper) {
    test_introspection(
        helper,
        r#"
    create table my_table(name text not null) with (fillfactor=50);

    create index my_index on my_table(name) with (fillfactor = 20, deduplicate_items = off);
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                tables: vec![PostgresTable {
                    name: "my_table".to_string(),
                    columns: vec![PostgresColumn {
                        name: "name".to_string(),
                        ordinal_position: 1,
                        is_nullable: false,
                        data_type: "text".to_string(),
                        ..default()
                    }],
                    indices: vec![PostgresIndex {
                        name: "my_index".to_string(),
                        key_columns: vec![PostgresIndexKeyColumn {
                            name: "name".to_string(),
                            ordinal_position: 1,
                            direction: Some(PostgresIndexColumnDirection::Ascending),
                            nulls_order: Some(PostgresIndexNullsOrder::Last),
                        }],
                        index_type: "btree".to_string(),
                        index_constraint_type: PostgresIndexType::Index,
                        storage_parameters: vec![
                            "fillfactor=20".to_string(),
                            "deduplicate_items=off".to_string(),
                        ],
                        ..default()
                    }],
                    storage_parameters: vec!["fillfactor=50".to_string()],
                    ..default()
                }],
                name: "public".to_string(),
                ..default()
            }],
            timescale_support: TimescaleSupport::from_test_helper(helper),
            ..default()
        },
    )
    .await;
}

#[pg_test(arg(postgres = 12))]
async fn index_storage_parameters_pg_12(helper: &TestHelper) {
    test_introspection(
        helper,
        r#"
    create table my_table(name text not null) with (fillfactor=50);

    create index my_index on my_table(name) with (fillfactor = 20);
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                tables: vec![PostgresTable {
                    name: "my_table".to_string(),
                    columns: vec![PostgresColumn {
                        name: "name".to_string(),
                        ordinal_position: 1,
                        is_nullable: false,
                        data_type: "text".to_string(),
                        ..default()
                    }],
                    indices: vec![PostgresIndex {
                        name: "my_index".to_string(),
                        key_columns: vec![PostgresIndexKeyColumn {
                            name: "name".to_string(),
                            ordinal_position: 1,
                            direction: Some(PostgresIndexColumnDirection::Ascending),
                            nulls_order: Some(PostgresIndexNullsOrder::Last),
                        }],
                        index_type: "btree".to_string(),
                        index_constraint_type: PostgresIndexType::Index,
                        storage_parameters: vec!["fillfactor=20".to_string()],
                        ..default()
                    }],
                    storage_parameters: vec!["fillfactor=50".to_string()],
                    ..default()
                }],
                name: "public".to_string(),
                ..default()
            }],
            ..default()
        },
    )
    .await;
}
