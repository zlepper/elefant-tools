use super::*;
use crate::default;
use crate::test_helpers::{get_test_helper, TestHelper};

pub async fn introspect_schema(test_helper: &TestHelper) -> PostgresDatabase {
    let conn = test_helper.get_conn();
    let reader = SchemaReader::new(conn);
    reader.introspect_database().await.unwrap()
}

fn test_introspection(create_table_statement: &str, expected: PostgresDatabase) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let helper = get_test_helper("helper").await;
            helper.execute_not_query(create_table_statement).await;

            let db = introspect_schema(&helper).await;

            assert_eq!(db, expected)
        });
}

#[test]
fn reads_simple_schema() {
    test_introspection(
        r#"
    create table my_table(
        id serial primary key,
        name text not null unique,
        age int not null check (age > 21),
        constraint my_multi_check check (age > 21 and age < 65 and name is not null)
    );

    create index lower_case_name_idx on my_table (lower(name));

    insert into my_table(name, age) values ('foo', 42), ('bar', 22);
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                tables: vec![PostgresTable {
                    name: "my_table".to_string(),
                    columns: vec![
                        PostgresColumn {
                            name: "id".to_string(),
                            ordinal_position: 1,
                            is_nullable: false,
                            data_type: "int4".to_string(),
                            default_value: Some("nextval('my_table_id_seq'::regclass)".to_string()),
                            ..default()
                        },
                        PostgresColumn {
                            name: "name".to_string(),
                            ordinal_position: 2,
                            is_nullable: false,
                            data_type: "text".to_string(),
                            ..default()
                        },
                        PostgresColumn {
                            name: "age".to_string(),
                            ordinal_position: 3,
                            is_nullable: false,
                            data_type: "int4".to_string(),
                            ..default()
                        },
                    ],
                    constraints: vec![
                        PostgresConstraint::Check(PostgresCheckConstraint {
                            name: "my_multi_check".to_string(),
                            check_clause: "(((age > 21) AND (age < 65) AND (name IS NOT NULL)))"
                                .to_string(),
                            ..default()
                        }),
                        PostgresConstraint::Check(PostgresCheckConstraint {
                            name: "my_table_age_check".to_string(),
                            check_clause: "((age > 21))".to_string(),
                            ..default()
                        }),
                        PostgresConstraint::Unique(PostgresUniqueConstraint {
                            name: "my_table_name_key".to_string(),
                            unique_index_name: "my_table_name_key".to_string(),
                            ..default()
                        }),
                    ],
                    indices: vec![
                        PostgresIndex {
                            name: "lower_case_name_idx".to_string(),
                            key_columns: vec![PostgresIndexKeyColumn {
                                name: "lower(name)".to_string(),
                                ordinal_position: 1,
                                direction: Some(PostgresIndexColumnDirection::Ascending),
                                nulls_order: Some(PostgresIndexNullsOrder::Last),
                            }],
                            index_type: "btree".to_string(),
                            predicate: None,
                            included_columns: vec![],
                            index_constraint_type: PostgresIndexType::Index,
                            ..default()
                        },
                        PostgresIndex {
                            name: "my_table_name_key".to_string(),
                            key_columns: vec![PostgresIndexKeyColumn {
                                name: "name".to_string(),
                                ordinal_position: 1,
                                direction: Some(PostgresIndexColumnDirection::Ascending),
                                nulls_order: Some(PostgresIndexNullsOrder::Last),
                            }],
                            index_type: "btree".to_string(),
                            predicate: None,
                            included_columns: vec![],
                            index_constraint_type: PostgresIndexType::Unique {
                                nulls_distinct: true,
                            },
                            ..default()
                        },
                        PostgresIndex {
                            name: "my_table_pkey".to_string(),
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
                        },
                    ],
                    ..default()
                }],
                sequences: vec![PostgresSequence {
                    name: "my_table_id_seq".to_string(),
                    data_type: "int4".to_string(),
                    start_value: 1,
                    increment: 1,
                    min_value: 1,
                    max_value: 2147483647,
                    cache_size: 1,
                    cycle: false,
                    last_value: Some(2),
                    ..default()
                }],
                ..default()
            }],
            ..default()
        },
    );
}

#[test]
fn table_without_columns() {
    test_introspection(
        r#"
    create table my_table();
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                tables: vec![PostgresTable {
                    name: "my_table".to_string(),
                    ..default()
                }],
                name: "public".to_string(),
                ..default()
            }],
            ..default()
        },
    );
}

#[test]
fn table_without_primary_key() {
    test_introspection(
        r#"
    create table my_table(
        name text not null,
        age int not null
    );
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                tables: vec![PostgresTable {
                    name: "my_table".to_string(),
                    columns: vec![
                        PostgresColumn {
                            name: "name".to_string(),
                            ordinal_position: 1,
                            is_nullable: false,
                            data_type: "text".to_string(),
                            ..default()
                        },
                        PostgresColumn {
                            name: "age".to_string(),
                            ordinal_position: 2,
                            is_nullable: false,
                            data_type: "int4".to_string(),
                            ..default()
                        },
                    ],
                    ..default()
                }],
                ..default()
            }],
            ..default()
        },
    )
}

#[test]
fn composite_primary_keys() {
    test_introspection(
        r#"
    create table my_table(
        id_part_1 int not null,
        id_part_2 int not null,
        name text,
        age int,
        constraint my_table_pk primary key (id_part_1, id_part_2)
    );
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                tables: vec![PostgresTable {
                    name: "my_table".to_string(),
                    columns: vec![
                        PostgresColumn {
                            name: "id_part_1".to_string(),
                            ordinal_position: 1,
                            is_nullable: false,
                            data_type: "int4".to_string(),
                            ..default()
                        },
                        PostgresColumn {
                            name: "id_part_2".to_string(),
                            ordinal_position: 2,
                            is_nullable: false,
                            data_type: "int4".to_string(),
                            ..default()
                        },
                        PostgresColumn {
                            name: "name".to_string(),
                            ordinal_position: 3,
                            is_nullable: true,
                            data_type: "text".to_string(),
                            ..default()
                        },
                        PostgresColumn {
                            name: "age".to_string(),
                            ordinal_position: 4,
                            is_nullable: true,
                            data_type: "int4".to_string(),
                            ..default()
                        },
                    ],
                    indices: vec![
                        PostgresIndex {
                            name: "my_table_pk".to_string(),
                            key_columns: vec![
                                PostgresIndexKeyColumn {
                                    name: "id_part_1".to_string(),
                                    ordinal_position: 1,
                                    direction: Some(PostgresIndexColumnDirection::Ascending),
                                    nulls_order: Some(PostgresIndexNullsOrder::Last),
                                },
                                PostgresIndexKeyColumn {
                                    name: "id_part_2".to_string(),
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
                        },
                    ],
                    ..default()
                }],
                ..default()
            }],
            ..default()
        },
    );
}

#[test]
fn indices() {
    test_introspection(
        r#"
    create table my_table(
        value int
    );

    create index my_table_value_asc_nulls_first on my_table(value asc nulls first);
    create index my_table_value_asc_nulls_last on my_table(value asc nulls last);
    create index my_table_value_desc_nulls_first on my_table(value desc nulls first);
    create index my_table_value_desc_nulls_last on my_table(value desc nulls last);

    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                tables: vec![PostgresTable {
                    name: "my_table".to_string(),
                    columns: vec![PostgresColumn {
                        name: "value".to_string(),
                        ordinal_position: 1,
                        is_nullable: true,
                        data_type: "int4".to_string(),
                        ..default()
                    }],
                    constraints: vec![],
                    indices: vec![
                        PostgresIndex {
                            name: "my_table_value_asc_nulls_first".to_string(),
                            key_columns: vec![PostgresIndexKeyColumn {
                                name: "value".to_string(),
                                ordinal_position: 1,
                                direction: Some(PostgresIndexColumnDirection::Ascending),
                                nulls_order: Some(PostgresIndexNullsOrder::First),
                            }],
                            index_type: "btree".to_string(),
                            predicate: None,
                            included_columns: vec![],
                            index_constraint_type: PostgresIndexType::Index,
                            ..default()
                        },
                        PostgresIndex {
                            name: "my_table_value_asc_nulls_last".to_string(),
                            key_columns: vec![PostgresIndexKeyColumn {
                                name: "value".to_string(),
                                ordinal_position: 1,
                                direction: Some(PostgresIndexColumnDirection::Ascending),
                                nulls_order: Some(PostgresIndexNullsOrder::Last),
                            }],
                            index_type: "btree".to_string(),
                            predicate: None,
                            included_columns: vec![],
                            index_constraint_type: PostgresIndexType::Index,
                            ..default()
                        },
                        PostgresIndex {
                            name: "my_table_value_desc_nulls_first".to_string(),
                            key_columns: vec![PostgresIndexKeyColumn {
                                name: "value".to_string(),
                                ordinal_position: 1,
                                direction: Some(PostgresIndexColumnDirection::Descending),
                                nulls_order: Some(PostgresIndexNullsOrder::First),
                            }],
                            index_type: "btree".to_string(),
                            predicate: None,
                            included_columns: vec![],
                            index_constraint_type: PostgresIndexType::Index,
                            ..default()
                        },
                        PostgresIndex {
                            name: "my_table_value_desc_nulls_last".to_string(),
                            key_columns: vec![PostgresIndexKeyColumn {
                                name: "value".to_string(),
                                ordinal_position: 1,
                                direction: Some(PostgresIndexColumnDirection::Descending),
                                nulls_order: Some(PostgresIndexNullsOrder::Last),
                            }],
                            index_type: "btree".to_string(),
                            predicate: None,
                            included_columns: vec![],
                            index_constraint_type: PostgresIndexType::Index,
                            ..default()
                        },
                    ],
                    ..default()
                }],
                ..default()
            }],
            ..default()
        },
    );
}

#[test]
fn index_types() {
    test_introspection(
        r#"
    create table my_table(
        free_text tsvector
    );

    create index my_table_gist on my_table using gist (free_text);
    create index my_table_gin on my_table using gin (free_text);
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                tables: vec![PostgresTable {
                    name: "my_table".to_string(),
                    columns: vec![PostgresColumn {
                        name: "free_text".to_string(),
                        ordinal_position: 1,
                        is_nullable: true,
                        data_type: "tsvector".to_string(),
                        ..default()
                    }],
                    indices: vec![
                        PostgresIndex {
                            name: "my_table_gin".to_string(),
                            key_columns: vec![PostgresIndexKeyColumn {
                                name: "free_text".to_string(),
                                ordinal_position: 1,
                                direction: None,
                                nulls_order: None,
                            }],
                            index_type: "gin".to_string(),
                            predicate: None,
                            included_columns: vec![],
                            index_constraint_type: PostgresIndexType::Index,
                            ..default()
                        },
                        PostgresIndex {
                            name: "my_table_gist".to_string(),
                            key_columns: vec![PostgresIndexKeyColumn {
                                name: "free_text".to_string(),
                                ordinal_position: 1,
                                direction: None,
                                nulls_order: None,
                            }],
                            index_type: "gist".to_string(),
                            predicate: None,
                            included_columns: vec![],
                            index_constraint_type: PostgresIndexType::Index,
                            ..default()
                        },
                    ],
                    ..default()
                }],
                ..default()
            }],
            ..default()
        },
    );
}

#[test]
fn filtered_index() {
    test_introspection(
        r#"
    create table my_table(
        value int
    );

    create index my_table_idx on my_table (value) where (value % 2 = 0);
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                tables: vec![PostgresTable {
                    name: "my_table".to_string(),
                    columns: vec![PostgresColumn {
                        name: "value".to_string(),
                        ordinal_position: 1,
                        is_nullable: true,
                        data_type: "int4".to_string(),
                        ..default()
                    }],
                    indices: vec![PostgresIndex {
                        name: "my_table_idx".to_string(),
                        key_columns: vec![PostgresIndexKeyColumn {
                            name: "value".to_string(),
                            ordinal_position: 1,
                            direction: Some(PostgresIndexColumnDirection::Ascending),
                            nulls_order: Some(PostgresIndexNullsOrder::Last),
                        }],
                        index_type: "btree".to_string(),
                        predicate: Some("(value % 2) = 0".to_string()),
                        included_columns: vec![],
                        index_constraint_type: PostgresIndexType::Index,
                        ..default()
                    }],
                    ..default()
                }],
                ..default()
            }],
            ..default()
        },
    );
}

#[test]
fn index_with_include() {
    test_introspection(
        r#"
    create table my_table(
        value int,
        another_value int
    );

    create index my_table_idx on my_table (value) include (another_value);
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                tables: vec![PostgresTable {
                    name: "my_table".to_string(),
                    columns: vec![
                        PostgresColumn {
                            name: "value".to_string(),
                            ordinal_position: 1,
                            is_nullable: true,
                            data_type: "int4".to_string(),
                            ..default()
                        },
                        PostgresColumn {
                            name: "another_value".to_string(),
                            ordinal_position: 2,
                            is_nullable: true,
                            data_type: "int4".to_string(),
                            ..default()
                        },
                    ],
                    indices: vec![PostgresIndex {
                        name: "my_table_idx".to_string(),
                        key_columns: vec![PostgresIndexKeyColumn {
                            name: "value".to_string(),
                            ordinal_position: 1,
                            direction: Some(PostgresIndexColumnDirection::Ascending),
                            nulls_order: Some(PostgresIndexNullsOrder::Last),
                        }],
                        index_type: "btree".to_string(),
                        predicate: None,
                        included_columns: vec![PostgresIndexIncludedColumn {
                            name: "another_value".to_string(),
                            ordinal_position: 2,
                        }],
                        index_constraint_type: PostgresIndexType::Index,
                        ..default()
                    }],
                    ..default()
                }],
                ..default()
            }],
            ..default()
        },
    );
}

#[test]
fn table_with_non_distinct_nulls() {
    test_introspection(
        r#"
    create table my_table(
        value int unique nulls not distinct
    );
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                tables: vec![PostgresTable {
                    name: "my_table".to_string(),
                    columns: vec![PostgresColumn {
                        name: "value".to_string(),
                        ordinal_position: 1,
                        is_nullable: true,
                        data_type: "int4".to_string(),
                        ..default()
                    }],
                    constraints: vec![
                        PostgresConstraint::Unique(PostgresUniqueConstraint {
                            name: "my_table_value_key".to_string(),
                            unique_index_name: "my_table_value_key".to_string(),
                            ..default()
                        }),
                    ],
                    indices: vec![
                        PostgresIndex {
                            name: "my_table_value_key".to_string(),
                            key_columns: vec![PostgresIndexKeyColumn {
                                name: "value".to_string(),
                                ordinal_position: 1,
                                direction: Some(PostgresIndexColumnDirection::Ascending),
                                nulls_order: Some(PostgresIndexNullsOrder::Last),
                            }],
                            index_type: "btree".to_string(),
                            predicate: None,
                            included_columns: vec![],
                            index_constraint_type: PostgresIndexType::Unique {
                                nulls_distinct: false,
                            },
                            ..default()
                        },
                    ],
                    ..default()
                }],
                ..default()
            }],
            ..default()
        },
    );
}

#[test]
fn foreign_keys() {
    test_introspection(
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
                        indices: vec![
                            PostgresIndex {
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
                            }
                        ],
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
                        constraints: vec![
                            PostgresConstraint::ForeignKey(PostgresForeignKey {
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
                            }),
                        ],
                        indices: vec![
                            PostgresIndex {
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
                            },
                        ],
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
            ..default()
        },
    );
}

#[test]
fn foreign_key_constraints() {
    test_introspection(
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
                        indices: vec![
                            PostgresIndex {
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
                            },
                        ],
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
                        indices: vec![
                            PostgresIndex {
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
                            },
                        ],
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
                        indices: vec![
                            PostgresIndex {
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
                            },
                        ],
                        ..default()
                    },
                ],
                ..default()
            }],
            ..default()
        },
    );
}

#[test]
fn generated_column() {
    test_introspection(
        r#"
    CREATE TABLE products (
        name text not null,
        search tsvector not null GENERATED ALWAYS AS (to_tsvector('english', name)) STORED
    );
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                sequences: vec![],
                tables: vec![PostgresTable {
                    name: "products".to_string(),
                    columns: vec![
                        PostgresColumn {
                            name: "name".to_string(),
                            ordinal_position: 1,
                            is_nullable: false,
                            data_type: "text".to_string(),
                            ..default()
                        },
                        PostgresColumn {
                            name: "search".to_string(),
                            ordinal_position: 2,
                            is_nullable: false,
                            data_type: "tsvector".to_string(),
                            generated: Some("to_tsvector('english'::regconfig, name)".to_string()),
                            ..default()
                        },
                    ],
                    ..default()
                }],
                ..default()
            }],
            ..default()
        },
    );
}

#[test]
fn test_views() {
    test_introspection(
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
                        .to_string(),
                    columns: vec![PostgresViewColumn {
                        name: "product_name".to_string(),
                        ordinal_position: 1,
                    }],
                    is_materialized: false,
                    ..default()
                }],
                ..default()
            }],
            ..default()
        },
    );
}

#[test]
fn test_functions() {
    test_introspection(
        r#"

    create function add(a int4, b int4) returns int4 as $$ begin return a + b; end; $$ language plpgsql;

    create function filter_stuff(value text) returns table(id int, name text) as
        $$
        begin

        create temp table temp_table(id int, name text);

        insert into temp_table(id, name) values (1, 'foo'), (2, 'bar');

        return query select * from temp_table where name = value;

        end;

        $$ language plpgsql;


    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                functions: vec![
                    PostgresFunction {
                        function_name: "add".to_string(),
                        language: "plpgsql".to_string(),
                        estimated_cost: NotNan::new(100.0).unwrap(),
                        estimated_rows: NotNan::new(0.0).unwrap(),
                        support_function: None,
                        kind: FunctionKind::Function,
                        security_definer: false,
                        leak_proof: false,
                        strict: false,
                        returns_set: false,
                        volatility: Volatility::Volatile,
                        parallel: Parallel::Unsafe,
                        sql_body: r#"begin return a + b; end;"#
                            .to_string(),
                        configuration: None,
                        arguments: "a integer, b integer".to_string(),
                        result: Some("integer".to_string()),
                        ..default()
                    },
                    PostgresFunction {
                        function_name: "filter_stuff".to_string(),
                        language: "plpgsql".to_string(),
                        estimated_cost: NotNan::new(100.0).unwrap(),
                        estimated_rows: NotNan::new(1000.0).unwrap(),
                        support_function: None,
                        kind: FunctionKind::Function,
                        security_definer: false,
                        leak_proof: false,
                        strict: false,
                        returns_set: true,
                        volatility: Volatility::Volatile,
                        parallel: Parallel::Unsafe,
                        sql_body: r#"begin

        create temp table temp_table(id int, name text);

        insert into temp_table(id, name) values (1, 'foo'), (2, 'bar');

        return query select * from temp_table where name = value;

        end;"#
                            .to_string(),
                        configuration: None,
                        arguments: "value text".to_string(),
                        result: Some("TABLE(id integer, name text)".to_string()),
                        ..default()
                    },
                ],
                ..default()
            }],
            ..default()
        },
    )
}

#[test]
fn test_quoted_identifier_names() {
    test_introspection(r#"
        create table "MyTable" (int serial primary key);
    "#, PostgresDatabase {
        schemas: vec![
            PostgresSchema {
                name: "public".to_string(),
                tables: vec![
                    PostgresTable {
                        name: "MyTable".to_string(),
                        columns: vec![
                            PostgresColumn {
                                name: "int".to_string(),
                                ordinal_position: 1,
                                is_nullable: false,
                                data_type: "int4".to_string(),
                                default_value: Some("nextval('\"MyTable_int_seq\"'::regclass)".to_string()),
                                ..default()
                            }
                        ],
                        indices: vec![
                            PostgresIndex {
                                name: "MyTable_pkey".to_string(),
                                key_columns: vec![
                                    PostgresIndexKeyColumn {
                                        name: "\"int\"".to_string(),
                                        ordinal_position: 1,
                                        direction: Some(PostgresIndexColumnDirection::Ascending),
                                        nulls_order: Some(PostgresIndexNullsOrder::Last),
                                    }
                                ],
                                index_type: "btree".to_string(),
                                predicate: None,
                                included_columns: vec![],
                                index_constraint_type: PostgresIndexType::PrimaryKey,
                                ..default()
                            }
                        ],
                        ..default()
                    }
                ],
                sequences: vec![
                    PostgresSequence {
                        name: "MyTable_int_seq".to_string(),
                        data_type: "int4".to_string(),
                        ..default()
                    }
                ],
                ..default()
            }
        ],
        ..default()
    })
}

#[test]
fn test_extensions() {
    test_introspection(r#"
        create extension "btree_gin";
    "#, PostgresDatabase {
        schemas: vec![
            PostgresSchema {
                name: "public".to_string(),
                ..default()
            }
        ],
        enabled_extensions: vec![
            PostgresExtension {
                name: "btree_gin".to_string(),
                schema_name: "public".to_string(),
                version: "1.3".to_string(),
                relocatable: true,
            }
        ],
        ..default()
    })
}

#[test]
fn comments_on_stuff() {
    test_introspection(r#"
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


    "#, PostgresDatabase {
        schemas: vec![
            PostgresSchema {
                name: "public".to_string(),
                comment: Some("This is a schema".to_string()),
                tables: vec![
                    PostgresTable {
                        name: "my_table".to_string(),
                        columns: vec![
                            PostgresColumn {
                                name: "value".to_string(),
                                ordinal_position: 1,
                                is_nullable: false,
                                data_type: "int4".to_string(),
                                comment: Some("This is a column".to_string()),
                                default_value: Some("nextval('my_table_value_seq'::regclass)".to_string()),
                                ..default()
                            },
                            PostgresColumn {
                                name: "another_value".to_string(),
                                ordinal_position: 2,
                                is_nullable: false,
                                data_type: "int4".to_string(),
                                comment: None,
                                ..default()
                            },
                        ],
                        constraints: vec![
                            PostgresConstraint::Check(PostgresCheckConstraint {
                                name: "my_table_value_check".to_string(),
                                check_clause: "((value > 0))".to_string(),
                                comment: Some("This is a constraint".to_string()),
                            }),
                            PostgresConstraint::Unique(PostgresUniqueConstraint {
                                name: "my_table_another_value_key".to_string(),
                                unique_index_name: "my_table_another_value_key".to_string(),
                                comment: Some("This is a unique constraint".to_string()),
                            }),
                        ],
                        indices: vec![
                            PostgresIndex {
                                name: "my_table_another_value_key".to_string(),
                                key_columns: vec![PostgresIndexKeyColumn {
                                    name: "another_value".to_string(),
                                    ordinal_position: 1,
                                    direction: Some(PostgresIndexColumnDirection::Ascending),
                                    nulls_order: Some(PostgresIndexNullsOrder::Last),
                                }],
                                index_type: "btree".to_string(),
                                predicate: None,
                                included_columns: vec![],
                                index_constraint_type: PostgresIndexType::Unique {
                                    nulls_distinct: true,
                                },
                                comment: Some("This is an index".to_string()),
                            },
                        ],
                        comment: Some("This is a 'table'".to_string()),
                        ..default()
                    }
                ],
                functions: vec![
                    PostgresFunction {
                        function_name: "my_function".to_string(),
                        language: "plpgsql".to_string(),
                        estimated_cost: NotNan::new(100.0).unwrap(),
                        estimated_rows: NotNan::new(0.0).unwrap(),
                        support_function: None,
                        kind: FunctionKind::Function,
                        security_definer: false,
                        leak_proof: false,
                        strict: false,
                        returns_set: false,
                        volatility: Volatility::Volatile,
                        parallel: Parallel::Unsafe,
                        sql_body: r#"begin return 1; end;"#
                            .to_string(),
                        configuration: None,
                        arguments: "".to_string(),
                        result: Some("integer".to_string()),
                        comment: Some("This is a function".to_string()),
                    },
                    PostgresFunction {
                        function_name: "my_function_2".to_string(),
                        language: "plpgsql".to_string(),
                        estimated_cost: NotNan::new(100.0).unwrap(),
                        estimated_rows: NotNan::new(0.0).unwrap(),
                        support_function: None,
                        kind: FunctionKind::Function,
                        security_definer: false,
                        leak_proof: false,
                        strict: false,
                        returns_set: false,
                        volatility: Volatility::Volatile,
                        parallel: Parallel::Unsafe,
                        sql_body: r#"begin return a + b; end;"#
                            .to_string(),
                        configuration: None,
                        arguments: "a integer, b integer".to_string(),
                        result: Some("integer".to_string()),
                        comment: Some("This is another function".to_string()),
                    },
                ],
                views: vec![
                    PostgresView {
                        name: "my_view".to_string(),
                        definition: " SELECT 1 AS value;".to_string(),
                        columns: vec![PostgresViewColumn {
                            name: "value".to_string(),
                            ordinal_position: 1,
                        }],
                        comment: Some("This is a view".to_string()),
                        ..default()
                    }
                ],
                sequences: vec![
                    PostgresSequence {
                        name: "my_table_value_seq".to_string(),
                        data_type: "int4".to_string(),
                        comment: Some("This is a sequence".to_string()),
                        ..default()
                    }
                ],
                ..default()
            }
        ],
        ..default()
    })
}

#[test]
fn array_columns() {
    test_introspection(r#"
        create table my_table(
            int_array int4[]
        );
    "#, PostgresDatabase {
        schemas: vec![
            PostgresSchema {
                name: "public".to_string(),
                tables: vec![
                    PostgresTable {
                        name: "my_table".to_string(),
                        columns: vec![
                            PostgresColumn {
                                name: "int_array".to_string(),
                                ordinal_position: 1,
                                is_nullable: true,
                                data_type: "int4".to_string(),
                                array_dimensions: 1,
                                ..default()
                            }
                        ],
                        ..default()
                    }
                ],
                ..default()
            }
        ],
        ..default()
    })
}

#[test]
fn materialized_view() {
    test_introspection(r#"
        create materialized view my_view as select 1 as value;
    "#, PostgresDatabase {
        schemas: vec![
            PostgresSchema {
                name: "public".to_string(),
                views: vec![
                    PostgresView {
                        name: "my_view".to_string(),
                        definition: " SELECT 1 AS value;".to_string(),
                        columns: vec![PostgresViewColumn {
                            name: "value".to_string(),
                            ordinal_position: 1,
                        }],
                        is_materialized: true,
                        ..default()
                    }
                ],
                ..default()
            }
        ],
        ..default()
    })
}

#[test]
fn triggers() {
    test_introspection(r#"
        create table my_table(
            value int
        );

        create function my_trigger_function() returns trigger as $$
        begin return new; end;
        $$ language plpgsql;

        create trigger my_trigger after insert on my_table for each row execute function my_trigger_function();

        comment on trigger my_trigger on my_table is 'This is a trigger';

        create trigger scoped_trigger before update on my_table for each row when (OLD.value is distinct from NEW.value) execute procedure my_trigger_function();

        create trigger truncate_trigger after truncate on my_table for each statement execute procedure my_trigger_function();

    "#, PostgresDatabase {
        schemas: vec![
            PostgresSchema {
                name: "public".to_string(),
                tables: vec![
                    PostgresTable {
                        name: "my_table".to_string(),
                        columns: vec![
                            PostgresColumn {
                                name: "value".to_string(),
                                ordinal_position: 1,
                                is_nullable: true,
                                data_type: "int4".to_string(),
                                ..default()
                            }
                        ],
                        ..default()
                    }
                ],
                functions: vec![
                    PostgresFunction {
                        function_name: "my_trigger_function".to_string(),
                        language: "plpgsql".to_string(),
                        estimated_cost: NotNan::new(100.0).unwrap(),
                        estimated_rows: NotNan::new(0.0).unwrap(),
                        support_function: None,
                        kind: FunctionKind::Function,
                        security_definer: false,
                        leak_proof: false,
                        strict: false,
                        returns_set: false,
                        volatility: Volatility::Volatile,
                        parallel: Parallel::Unsafe,
                        sql_body: "begin return new; end;".to_string(),
                        configuration: None,
                        arguments: "".to_string(),
                        result: Some("trigger".to_string()),
                        ..default()
                    }
                ],
                triggers: vec![
                    PostgresTrigger {
                        name: "my_trigger".to_string(),
                        table_name: "my_table".to_string(),
                        event: PostgresTriggerEvent::Insert,
                        timing: PostgresTriggerTiming::After,
                        level: PostgresTriggerLevel::Row,
                        function_name: "my_trigger_function".to_string(),
                        comment: Some("This is a trigger".to_string()),
                        ..default()
                    },
                    PostgresTrigger {
                        name: "scoped_trigger".to_string(),
                        table_name: "my_table".to_string(),
                        event: PostgresTriggerEvent::Update,
                        timing: PostgresTriggerTiming::Before,
                        level: PostgresTriggerLevel::Row,
                        function_name: "my_trigger_function".to_string(),
                        condition: Some("(old.value IS DISTINCT FROM new.value)".to_string()),
                        ..default()
                    },
                    PostgresTrigger {
                        name: "truncate_trigger".to_string(),
                        table_name: "my_table".to_string(),
                        event: PostgresTriggerEvent::Truncate,
                        timing: PostgresTriggerTiming::After,
                        level: PostgresTriggerLevel::Statement,
                        function_name: "my_trigger_function".to_string(),
                        ..default()
                    }
                ],
                ..default()
            }
        ],
        ..default()
    })
}

#[test]
fn enums() {
    test_introspection(r#"
    CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
    CREATE TABLE person (
        name text,
        current_mood mood
    );
    alter type mood add value 'mehh' before 'ok';

    comment on type mood is 'This is a mood';
    "#, PostgresDatabase {
        schemas: vec![
            PostgresSchema {
                name: "public".to_string(),
                tables: vec![
                    PostgresTable {
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
                            }
                        ],
                        ..default()
                    }
                ],
                enums: vec![
                    PostgresEnum {
                        name: "mood".to_string(),
                        values: vec!["sad".to_string(), "mehh".to_string(), "ok".to_string(), "happy".to_string()],
                        comment: Some("This is a mood".to_string()),
                    }
                ],
                ..default()
            }
        ],
        ..default()
    })
}

#[test]
fn range_partitions() {
    test_introspection(r#"
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
    "#, PostgresDatabase {
        schemas: vec![
            PostgresSchema {
                name: "public".to_string(),
                tables: vec![
                    PostgresTable {
                        name: "sales".to_string(),
                        columns: vec![
                            PostgresColumn {
                                name: "sale_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 1,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "sale_date".to_string(),
                                is_nullable: true,
                                ordinal_position: 2,
                                data_type: "date".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "product_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 3,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "quantity".to_string(),
                                is_nullable: true,
                                ordinal_position: 4,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "amount".to_string(),
                                is_nullable: true,
                                ordinal_position: 5,
                                data_type: "numeric".to_string(),
                                ..default()
                            }
                        ],
                        table_type: TableTypeDetails::PartitionedParentTable {
                            partition_columns: PartitionedTableColumns::Columns(vec!["sale_date".to_string()]),
                            default_partition_name: None,
                            partition_strategy: TablePartitionStrategy::Range,
                        },
                        ..default()
                    },
                    PostgresTable {
                        name: "sales_february".to_string(),
                        table_type: TableTypeDetails::PartitionedChildTable {
                            partition_expression: "FOR VALUES FROM ('2023-02-01') TO ('2023-03-01')".to_string(),
                            parent_table: "sales".to_string(),
                        },
                        columns: vec![
                            PostgresColumn {
                                name: "sale_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 1,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "sale_date".to_string(),
                                is_nullable: true,
                                ordinal_position: 2,
                                data_type: "date".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "product_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 3,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "quantity".to_string(),
                                is_nullable: true,
                                ordinal_position: 4,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "amount".to_string(),
                                is_nullable: true,
                                ordinal_position: 5,
                                data_type: "numeric".to_string(),
                                ..default()
                            }
                        ],
                        ..default()
                    },
                    PostgresTable {
                        name: "sales_january".to_string(),
                        table_type: TableTypeDetails::PartitionedChildTable {
                            partition_expression: "FOR VALUES FROM ('2023-01-01') TO ('2023-02-01')".to_string(),
                            parent_table: "sales".to_string(),
                        },
                        columns: vec![
                            PostgresColumn {
                                name: "sale_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 1,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "sale_date".to_string(),
                                is_nullable: true,
                                ordinal_position: 2,
                                data_type: "date".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "product_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 3,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "quantity".to_string(),
                                is_nullable: true,
                                ordinal_position: 4,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "amount".to_string(),
                                is_nullable: true,
                                ordinal_position: 5,
                                data_type: "numeric".to_string(),
                                ..default()
                            }
                        ],
                        ..default()
                    },
                    PostgresTable {
                        name: "sales_march".to_string(),
                        table_type: TableTypeDetails::PartitionedChildTable {
                            partition_expression: "FOR VALUES FROM ('2023-03-01') TO ('2023-04-01')".to_string(),
                            parent_table: "sales".to_string(),
                        },
                        columns: vec![
                            PostgresColumn {
                                name: "sale_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 1,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "sale_date".to_string(),
                                is_nullable: true,
                                ordinal_position: 2,
                                data_type: "date".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "product_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 3,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "quantity".to_string(),
                                is_nullable: true,
                                ordinal_position: 4,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "amount".to_string(),
                                is_nullable: true,
                                ordinal_position: 5,
                                data_type: "numeric".to_string(),
                                ..default()
                            }
                        ],
                        ..default()
                    }
                ],
                ..default()
            }
        ],
        ..default()
    })
}

#[test]
fn list_partitions() {
    test_introspection(r#"
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
    "#, PostgresDatabase {
        schemas: vec![
            PostgresSchema {
                name: "public".to_string(),
                tables: vec![
                    PostgresTable {
                        name: "clothing".to_string(),
                        table_type: TableTypeDetails::PartitionedChildTable {
                            partition_expression: "FOR VALUES IN ('Clothing')".to_string(),
                            parent_table: "products".to_string(),
                        },
                        columns: vec![
                            PostgresColumn {
                                name: "product_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 1,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "category".to_string(),
                                is_nullable: true,
                                ordinal_position: 2,
                                data_type: "text".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "product_name".to_string(),
                                is_nullable: true,
                                ordinal_position: 3,
                                data_type: "text".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "price".to_string(),
                                is_nullable: true,
                                ordinal_position: 4,
                                data_type: "numeric".to_string(),
                                ..default()
                            }
                        ],
                        ..default()
                    },
                    PostgresTable {
                        name: "electronics".to_string(),
                        table_type: TableTypeDetails::PartitionedChildTable {
                            partition_expression: "FOR VALUES IN ('Electronics')".to_string(),
                            parent_table: "products".to_string(),
                        },
                        columns: vec![
                            PostgresColumn {
                                name: "product_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 1,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "category".to_string(),
                                is_nullable: true,
                                ordinal_position: 2,
                                data_type: "text".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "product_name".to_string(),
                                is_nullable: true,
                                ordinal_position: 3,
                                data_type: "text".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "price".to_string(),
                                is_nullable: true,
                                ordinal_position: 4,
                                data_type: "numeric".to_string(),
                                ..default()
                            }
                        ],
                        ..default()
                    },
                    PostgresTable {
                        name: "furniture".to_string(),
                        table_type: TableTypeDetails::PartitionedChildTable {
                            partition_expression: "FOR VALUES IN ('Furniture')".to_string(),
                            parent_table: "products".to_string(),
                        },
                        columns: vec![
                            PostgresColumn {
                                name: "product_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 1,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "category".to_string(),
                                is_nullable: true,
                                ordinal_position: 2,
                                data_type: "text".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "product_name".to_string(),
                                is_nullable: true,
                                ordinal_position: 3,
                                data_type: "text".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "price".to_string(),
                                is_nullable: true,
                                ordinal_position: 4,
                                data_type: "numeric".to_string(),
                                ..default()
                            }
                        ],
                        ..default()
                    },
                    PostgresTable {
                        name: "products".to_string(),
                        columns: vec![
                            PostgresColumn {
                                name: "product_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 1,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "category".to_string(),
                                is_nullable: true,
                                ordinal_position: 2,
                                data_type: "text".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "product_name".to_string(),
                                is_nullable: true,
                                ordinal_position: 3,
                                data_type: "text".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "price".to_string(),
                                is_nullable: true,
                                ordinal_position: 4,
                                data_type: "numeric".to_string(),
                                ..default()
                            }
                        ],
                        table_type: TableTypeDetails::PartitionedParentTable {
                            partition_strategy: TablePartitionStrategy::List,
                            default_partition_name: None,
                            partition_columns: PartitionedTableColumns::Columns(vec!["category".to_string()]),
                        },
                        ..default()
                    },
                ],
                ..default()
            }
        ],
        ..default()
    })
}


#[test]
fn hash_partitions() {
    test_introspection(r#"
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
    "#, PostgresDatabase {
        schemas: vec![
            PostgresSchema {
                name: "public".to_string(),
                tables: vec![
                    PostgresTable {
                        name: "orders".to_string(),
                        columns: vec![
                            PostgresColumn {
                                name: "order_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 1,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "order_date".to_string(),
                                is_nullable: true,
                                ordinal_position: 2,
                                data_type: "date".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "customer_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 3,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "total_amount".to_string(),
                                is_nullable: true,
                                ordinal_position: 4,
                                data_type: "numeric".to_string(),
                                ..default()
                            }
                        ],
                        table_type: TableTypeDetails::PartitionedParentTable {
                            partition_strategy: TablePartitionStrategy::Hash,
                            default_partition_name: None,
                            partition_columns: PartitionedTableColumns::Columns(vec!["customer_id".to_string()]),
                        },
                        ..default()
                    },
                    PostgresTable {
                        name: "orders_1".to_string(),
                        table_type: TableTypeDetails::PartitionedChildTable {
                            partition_expression: "FOR VALUES WITH (modulus 3, remainder 0)".to_string(),
                            parent_table: "orders".to_string(),
                        },
                        columns: vec![
                            PostgresColumn {
                                name: "order_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 1,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "order_date".to_string(),
                                is_nullable: true,
                                ordinal_position: 2,
                                data_type: "date".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "customer_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 3,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "total_amount".to_string(),
                                is_nullable: true,
                                ordinal_position: 4,
                                data_type: "numeric".to_string(),
                                ..default()
                            }
                        ],
                        ..default()
                    },
                    PostgresTable {
                        name: "orders_2".to_string(),
                        table_type: TableTypeDetails::PartitionedChildTable {
                            partition_expression: "FOR VALUES WITH (modulus 3, remainder 1)".to_string(),
                            parent_table: "orders".to_string(),
                        },
                        columns: vec![
                            PostgresColumn {
                                name: "order_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 1,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "order_date".to_string(),
                                is_nullable: true,
                                ordinal_position: 2,
                                data_type: "date".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "customer_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 3,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "total_amount".to_string(),
                                is_nullable: true,
                                ordinal_position: 4,
                                data_type: "numeric".to_string(),
                                ..default()
                            }
                        ],
                        ..default()
                    },
                    PostgresTable {
                        name: "orders_3".to_string(),
                        table_type: TableTypeDetails::PartitionedChildTable {
                            partition_expression: "FOR VALUES WITH (modulus 3, remainder 2)".to_string(),
                            parent_table: "orders".to_string(),
                        },
                        columns: vec![
                            PostgresColumn {
                                name: "order_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 1,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "order_date".to_string(),
                                is_nullable: true,
                                ordinal_position: 2,
                                data_type: "date".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "customer_id".to_string(),
                                is_nullable: true,
                                ordinal_position: 3,
                                data_type: "int4".to_string(),
                                ..default()
                            },
                            PostgresColumn {
                                name: "total_amount".to_string(),
                                is_nullable: true,
                                ordinal_position: 4,
                                data_type: "numeric".to_string(),
                                ..default()
                            }
                        ],
                        ..default()
                    }
                ],
                ..default()
            }
        ],
        ..default()
    })
}





