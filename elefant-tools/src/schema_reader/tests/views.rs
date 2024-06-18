use crate::schema_reader::tests;
use crate::test_helpers;
use crate::test_helpers::TestHelper;
use crate::{
    default, PostgresColumn, PostgresDatabase, PostgresSchema, PostgresTable, PostgresView,
    PostgresViewColumn, TimescaleSupport,
};
use elefant_test_macros::pg_test;

#[pg_test(arg(postgres = 12))]
#[pg_test(arg(postgres = 13))]
#[pg_test(arg(postgres = 14))]
#[pg_test(arg(postgres = 15))]
#[pg_test(arg(timescale_db = 15))]
#[pg_test(arg(pg_bouncer = 15))]
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
                    object_id: 2.into(),
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
                    depends_on: vec![2.into()],
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
                    object_id: 2.into(),
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
                    depends_on: vec![2.into()],
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

#[pg_test(arg(postgres = 12))]
#[pg_test(arg(postgres = 13))]
#[pg_test(arg(postgres = 14))]
#[pg_test(arg(postgres = 15))]
#[pg_test(arg(timescale_db = 15))]
async fn view_depends_15_below(helper: &TestHelper) {
    tests::test_introspection(
        helper,
        r#"
        create materialized view a_view as select 1 as value;

        create materialized view b_view as select * from a_view;
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                object_id: 1.into(),
                views: vec![
                    PostgresView {
                        name: "a_view".to_string(),
                        object_id: 2.into(),
                        definition: "SELECT 1 AS value;".into(),
                        columns: vec![PostgresViewColumn {
                            name: "value".to_string(),
                            ordinal_position: 1,
                        }],
                        is_materialized: true,
                        ..default()
                    },
                    PostgresView {
                        name: "b_view".to_string(),
                        object_id: 3.into(),
                        definition: "SELECT a_view.value FROM a_view;".into(),
                        columns: vec![PostgresViewColumn {
                            name: "value".to_string(),
                            ordinal_position: 1,
                        }],
                        is_materialized: true,
                        depends_on: vec![2.into()],
                        ..default()
                    },
                ],
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
async fn view_depends_16(helper: &TestHelper) {
    tests::test_introspection(
        helper,
        r#"
        create materialized view a_view as select 1 as value;

        create materialized view b_view as select * from a_view;
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                object_id: 1.into(),
                views: vec![
                    PostgresView {
                        name: "a_view".to_string(),
                        object_id: 2.into(),
                        definition: "SELECT 1 AS value;".into(),
                        columns: vec![PostgresViewColumn {
                            name: "value".to_string(),
                            ordinal_position: 1,
                        }],
                        is_materialized: true,
                        ..default()
                    },
                    PostgresView {
                        name: "b_view".to_string(),
                        object_id: 3.into(),
                        definition: "SELECT value FROM a_view;".into(),
                        columns: vec![PostgresViewColumn {
                            name: "value".to_string(),
                            ordinal_position: 1,
                        }],
                        is_materialized: true,
                        depends_on: vec![2.into()],
                        ..default()
                    },
                ],
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
#[pg_test(arg(timescale_db = 15))]
async fn view_depends_15_below_opposite(helper: &TestHelper) {
    tests::test_introspection(
        helper,
        r#"
        create materialized view b_view as select 1 as value;

        create materialized view a_view as select * from b_view;
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                object_id: 1.into(),
                views: vec![
                    PostgresView {
                        name: "a_view".to_string(),
                        object_id: 2.into(),
                        definition: "SELECT b_view.value FROM b_view;".into(),
                        columns: vec![PostgresViewColumn {
                            name: "value".to_string(),
                            ordinal_position: 1,
                        }],
                        is_materialized: true,
                        depends_on: vec![3.into()],
                        ..default()
                    },
                    PostgresView {
                        name: "b_view".to_string(),
                        object_id: 3.into(),
                        definition: "SELECT 1 AS value;".into(),
                        columns: vec![PostgresViewColumn {
                            name: "value".to_string(),
                            ordinal_position: 1,
                        }],
                        is_materialized: true,
                        ..default()
                    },
                ],
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
async fn view_depends_16_opposite(helper: &TestHelper) {
    tests::test_introspection(
        helper,
        r#"
        create materialized view b_view as select 1 as value;

        create materialized view a_view as select * from b_view;
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                object_id: 1.into(),
                views: vec![
                    PostgresView {
                        name: "a_view".to_string(),
                        object_id: 2.into(),
                        definition: "SELECT value FROM b_view;".into(),
                        columns: vec![PostgresViewColumn {
                            name: "value".to_string(),
                            ordinal_position: 1,
                        }],
                        is_materialized: true,
                        depends_on: vec![3.into()],
                        ..default()
                    },
                    PostgresView {
                        name: "b_view".to_string(),
                        object_id: 3.into(),
                        definition: "SELECT 1 AS value;".into(),
                        columns: vec![PostgresViewColumn {
                            name: "value".to_string(),
                            ordinal_position: 1,
                        }],
                        is_materialized: true,
                        ..default()
                    },
                ],
                ..default()
            }],
            timescale_support: TimescaleSupport::from_test_helper(helper),
            ..default()
        },
    )
    .await;
}
