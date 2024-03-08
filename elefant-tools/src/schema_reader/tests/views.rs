use elefant_test_macros::pg_test;
use crate::{default, PostgresColumn, PostgresDatabase, PostgresSchema, PostgresTable, PostgresView, PostgresViewColumn, TimescaleSupport};
use crate::schema_reader::tests;
use crate::test_helpers::TestHelper;
use crate::test_helpers;

#[pg_test(arg(postgres = 12))]
#[pg_test(arg(postgres = 13))]
#[pg_test(arg(postgres = 14))]
#[pg_test(arg(postgres = 15))]
#[pg_test(arg(timescale_db = 15))]
async fn test_views(helper: &TestHelper) {
    tests::test_introspection(
        helper,
        r#"
    CREATE TABLE products (
        name text not null
    );

    create view products_view (product_name) as select name from products where name like 'a%';
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                tables: vec![PostgresTable {
                    name: "products".to_string(),
                    columns: vec![PostgresColumn {
                        name: "name".to_string(),
                        ordinal_position: 1,
                        is_nullable: false,
                        data_type: "text".to_string(),
                        ..default()
                    }],
                    ..default()
                }],
                views: vec![PostgresView {
                    name: "products_view".to_string(),
                    definition: " SELECT products.name AS product_name
   FROM products
  WHERE products.name ~~ 'a%'::text;"
                        .into(),
                    columns: vec![PostgresViewColumn {
                        name: "product_name".to_string(),
                        ordinal_position: 1,
                    }],
                    is_materialized: false,
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

#[pg_test(arg(postgres = 16))]
#[pg_test(arg(timescale_db = 16))]
async fn test_views_pg_16(helper: &TestHelper) {
    tests::test_introspection(
        helper,
        r#"
    CREATE TABLE products (
        name text not null
    );

    create view products_view (product_name) as select name from products where name like 'a%';
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                tables: vec![PostgresTable {
                    name: "products".to_string(),
                    columns: vec![PostgresColumn {
                        name: "name".to_string(),
                        ordinal_position: 1,
                        is_nullable: false,
                        data_type: "text".to_string(),
                        ..default()
                    }],
                    ..default()
                }],
                views: vec![PostgresView {
                    name: "products_view".to_string(),
                    definition: " SELECT name AS product_name
   FROM products
  WHERE name ~~ 'a%'::text;"
                        .into(),
                    columns: vec![PostgresViewColumn {
                        name: "product_name".to_string(),
                        ordinal_position: 1,
                    }],
                    is_materialized: false,
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
async fn materialized_view(helper: &TestHelper) {
    tests::test_introspection(
        helper,
        r#"
        create materialized view my_view as select 1 as value;
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                views: vec![PostgresView {
                    name: "my_view".to_string(),
                    definition: "SELECT 1 AS value;".into(),
                    columns: vec![PostgresViewColumn {
                        name: "value".to_string(),
                        ordinal_position: 1,
                    }],
                    is_materialized: true,
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
