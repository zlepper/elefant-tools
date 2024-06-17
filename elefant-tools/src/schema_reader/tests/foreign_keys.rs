use crate::schema_reader::tests;
use crate::test_helpers;
use crate::test_helpers::TestHelper;
use crate::{
    default, PostgresColumn, PostgresConstraint, PostgresDatabase, PostgresForeignKey,
    PostgresForeignKeyColumn, PostgresForeignKeyReferencedColumn, PostgresIndex,
    PostgresIndexColumnDirection, PostgresIndexKeyColumn, PostgresIndexNullsOrder,
    PostgresIndexType, PostgresSchema, PostgresSequence, PostgresTable, ReferenceAction,
    TimescaleSupport,
};
use elefant_test_macros::pg_test;

#[pg_test(arg(postgres = 12))]
#[pg_test(arg(postgres = 13))]
#[pg_test(arg(postgres = 14))]
#[pg_test(arg(postgres = 15))]
#[pg_test(arg(postgres = 16))]
#[pg_test(arg(timescale_db = 15))]
#[pg_test(arg(timescale_db = 16))]
async fn foreign_keys(helper: &TestHelper) {
    tests::test_introspection(
        helper,
        r#"
    create table items(
        id serial primary key
    );

    create table users(
        id serial primary key,
        item_id int not null references items(id)
    );
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                tables: vec![
                    PostgresTable {
                        name: "items".to_string(),
                        columns: vec![PostgresColumn {
                            name: "id".to_string(),
                            ordinal_position: 1,
                            is_nullable: false,
                            data_type: "int4".to_string(),
                            default_value: Some("nextval('items_id_seq'::regclass)".to_string()),
                            ..default()
                        }],
                        indices: vec![PostgresIndex {
                            name: "items_pkey".to_string(),
                            key_columns: vec![PostgresIndexKeyColumn {
                                name: "id".to_string(),
                                ordinal_position: 1,
                                direction: Some(PostgresIndexColumnDirection::Ascending),
                                nulls_order: Some(PostgresIndexNullsOrder::Last),
                            }],
                            index_type: "btree".to_string(),
                            predicate: None,
                            included_columns: vec![],
                            index_constraint_type: PostgresIndexType::PrimaryKey,
                            ..default()
                        }],
                        ..default()
                    },
                    PostgresTable {
                        name: "users".to_string(),
                        columns: vec![
                            PostgresColumn {
                                name: "id".to_string(),
                                ordinal_position: 1,
                                is_nullable: false,
                                data_type: "int4".to_string(),
                                default_value: Some(
                                    "nextval('users_id_seq'::regclass)".to_string(),
                                ),
                                ..default()
                            },
                            PostgresColumn {
                                name: "item_id".to_string(),
                                ordinal_position: 2,
                                is_nullable: false,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                        ],
                        constraints: vec![PostgresConstraint::ForeignKey(PostgresForeignKey {
                            name: "users_item_id_fkey".to_string(),
                            columns: vec![PostgresForeignKeyColumn {
                                name: "item_id".to_string(),
                                ordinal_position: 1,
                                affected_by_delete_action: true,
                            }],
                            referenced_schema: None,
                            referenced_table: "items".to_string(),
                            referenced_columns: vec![PostgresForeignKeyReferencedColumn {
                                name: "id".to_string(),
                                ordinal_position: 1,
                            }],
                            ..default()
                        })],
                        indices: vec![PostgresIndex {
                            name: "users_pkey".to_string(),
                            key_columns: vec![PostgresIndexKeyColumn {
                                name: "id".to_string(),
                                ordinal_position: 1,
                                direction: Some(PostgresIndexColumnDirection::Ascending),
                                nulls_order: Some(PostgresIndexNullsOrder::Last),
                            }],
                            index_type: "btree".to_string(),
                            predicate: None,
                            included_columns: vec![],
                            index_constraint_type: PostgresIndexType::PrimaryKey,
                            ..default()
                        }],
                        ..default()
                    },
                ],
                sequences: vec![
                    PostgresSequence {
                        name: "items_id_seq".to_string(),
                        data_type: "int4".to_string(),
                        start_value: 1,
                        increment: 1,
                        min_value: 1,
                        max_value: 2147483647,
                        cache_size: 1,
                        cycle: false,
                        last_value: None,
                        ..default()
                    },
                    PostgresSequence {
                        name: "users_id_seq".to_string(),
                        data_type: "int4".to_string(),
                        start_value: 1,
                        increment: 1,
                        min_value: 1,
                        max_value: 2147483647,
                        cache_size: 1,
                        cycle: false,
                        last_value: None,
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
#[pg_test(arg(postgres = 16))]
#[pg_test(arg(timescale_db = 15))]
#[pg_test(arg(timescale_db = 16))]
async fn foreign_key_constraints(helper: &TestHelper) {
    tests::test_introspection(
        helper,
        r#"
    CREATE TABLE products (
        product_no int4 PRIMARY KEY
    );

    CREATE TABLE orders (
        order_id int4 PRIMARY KEY
    );

    CREATE TABLE order_items (
        product_no int4 REFERENCES products ON DELETE RESTRICT ON UPDATE CASCADE,
        order_id int4 REFERENCES orders ON DELETE CASCADE ON UPDATE RESTRICT,
        PRIMARY KEY (product_no, order_id)
    );
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                tables: vec![
                    PostgresTable {
                        name: "order_items".to_string(),
                        columns: vec![
                            PostgresColumn {
                                name: "product_no".to_string(),
                                ordinal_position: 1,
                                is_nullable: false,
                                data_type: "int4".to_string(),
                                default_value: None,
                                ..default()
                            },
                            PostgresColumn {
                                name: "order_id".to_string(),
                                ordinal_position: 2,
                                is_nullable: false,
                                data_type: "int4".to_string(),
                                default_value: None,
                                ..default()
                            },
                        ],
                        constraints: vec![
                            PostgresConstraint::ForeignKey(PostgresForeignKey {
                                name: "order_items_order_id_fkey".to_string(),
                                columns: vec![PostgresForeignKeyColumn {
                                    name: "order_id".to_string(),
                                    ordinal_position: 1,
                                    affected_by_delete_action: true,
                                }],
                                referenced_schema: None,
                                referenced_table: "orders".to_string(),
                                referenced_columns: vec![PostgresForeignKeyReferencedColumn {
                                    name: "order_id".to_string(),
                                    ordinal_position: 1,
                                }],
                                update_action: ReferenceAction::Restrict,
                                delete_action: ReferenceAction::Cascade,
                                ..default()
                            }),
                            PostgresConstraint::ForeignKey(PostgresForeignKey {
                                name: "order_items_product_no_fkey".to_string(),
                                columns: vec![PostgresForeignKeyColumn {
                                    name: "product_no".to_string(),
                                    ordinal_position: 1,
                                    affected_by_delete_action: true,
                                }],
                                referenced_schema: None,
                                referenced_table: "products".to_string(),
                                referenced_columns: vec![PostgresForeignKeyReferencedColumn {
                                    name: "product_no".to_string(),
                                    ordinal_position: 1,
                                }],
                                update_action: ReferenceAction::Cascade,
                                delete_action: ReferenceAction::Restrict,
                                ..default()
                            }),
                        ],
                        indices: vec![PostgresIndex {
                            name: "order_items_pkey".to_string(),
                            key_columns: vec![
                                PostgresIndexKeyColumn {
                                    name: "product_no".to_string(),
                                    ordinal_position: 1,
                                    direction: Some(PostgresIndexColumnDirection::Ascending),
                                    nulls_order: Some(PostgresIndexNullsOrder::Last),
                                },
                                PostgresIndexKeyColumn {
                                    name: "order_id".to_string(),
                                    ordinal_position: 2,
                                    direction: Some(PostgresIndexColumnDirection::Ascending),
                                    nulls_order: Some(PostgresIndexNullsOrder::Last),
                                },
                            ],
                            index_type: "btree".to_string(),
                            predicate: None,
                            included_columns: vec![],
                            index_constraint_type: PostgresIndexType::PrimaryKey,
                            ..default()
                        }],
                        ..default()
                    },
                    PostgresTable {
                        name: "orders".to_string(),
                        columns: vec![PostgresColumn {
                            name: "order_id".to_string(),
                            ordinal_position: 1,
                            is_nullable: false,
                            data_type: "int4".to_string(),
                            default_value: None,
                            ..default()
                        }],
                        indices: vec![PostgresIndex {
                            name: "orders_pkey".to_string(),
                            key_columns: vec![PostgresIndexKeyColumn {
                                name: "order_id".to_string(),
                                ordinal_position: 1,
                                direction: Some(PostgresIndexColumnDirection::Ascending),
                                nulls_order: Some(PostgresIndexNullsOrder::Last),
                            }],
                            index_type: "btree".to_string(),
                            predicate: None,
                            included_columns: vec![],
                            index_constraint_type: PostgresIndexType::PrimaryKey,
                            ..default()
                        }],
                        ..default()
                    },
                    PostgresTable {
                        name: "products".to_string(),
                        columns: vec![PostgresColumn {
                            name: "product_no".to_string(),
                            ordinal_position: 1,
                            is_nullable: false,
                            data_type: "int4".to_string(),
                            default_value: None,
                            ..default()
                        }],
                        indices: vec![PostgresIndex {
                            name: "products_pkey".to_string(),
                            key_columns: vec![PostgresIndexKeyColumn {
                                name: "product_no".to_string(),
                                ordinal_position: 1,
                                direction: Some(PostgresIndexColumnDirection::Ascending),
                                nulls_order: Some(PostgresIndexNullsOrder::Last),
                            }],
                            index_type: "btree".to_string(),
                            predicate: None,
                            included_columns: vec![],
                            index_constraint_type: PostgresIndexType::PrimaryKey,
                            ..default()
                        }],
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
