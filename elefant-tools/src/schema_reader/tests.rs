use super::*;
use crate::default;
use crate::test_helpers::{get_test_helper, TestHelper};
use tokio::test;

pub async fn introspect_schema(test_helper: &TestHelper) -> PostgresDatabase {
    let conn = test_helper.get_conn();
    let reader = SchemaReader::new(conn);
    reader.introspect_database().await.unwrap()
}

#[test]
async fn reads_simple_schema() {
    let helper = get_test_helper("helper").await;
    helper
        .execute_not_query(
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
        )
        .await;

    let db = introspect_schema(&helper).await;

    assert_eq!(
        db,
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
                            data_type: "integer".to_string(),
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
                            data_type: "integer".to_string(),
                            ..default()
                        },
                    ],
                    constraints: vec![
                        PostgresConstraint::PrimaryKey(PostgresPrimaryKey {
                            name: "my_table_pkey".to_string(),
                            columns: vec![PostgresPrimaryKeyColumn {
                                column_name: "id".to_string(),
                                ordinal_position: 1,
                            }],
                        }),
                        PostgresConstraint::Unique(PostgresUniqueConstraint {
                            name: "my_table_name_key".to_string(),
                            columns: vec![PostgresUniqueConstraintColumn {
                                column_name: "name".to_string(),
                                ordinal_position: 1,
                            }],
                            distinct_nulls: true,
                        }),
                        PostgresConstraint::Check(PostgresCheckConstraint {
                            name: "my_multi_check".to_string(),
                            check_clause: "(((age > 21) AND (age < 65) AND (name IS NOT NULL)))"
                                .to_string(),
                        }),
                        PostgresConstraint::Check(PostgresCheckConstraint {
                            name: "my_table_age_check".to_string(),
                            check_clause: "((age > 21))".to_string(),
                        }),
                    ],
                    indices: vec![PostgresIndex {
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
                    }],
                }],
                sequences: vec![PostgresSequence {
                    name: "my_table_id_seq".to_string(),
                    data_type: "integer".to_string(),
                    start_value: 1,
                    increment: 1,
                    min_value: 1,
                    max_value: 2147483647,
                    cache_size: 1,
                    cycle: false,
                    last_value: Some(2),
                }],
            }]
        }
    )
}

#[test]
async fn table_without_columns() {
    let helper = get_test_helper("helper").await;
    helper
        .execute_not_query(
            r#"
    create table my_table();
    "#,
        )
        .await;

    let db = introspect_schema(&helper).await;

    assert_eq!(
        db,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                tables: vec![PostgresTable {
                    name: "my_table".to_string(),
                    columns: vec![],
                    constraints: vec![],
                    indices: vec![],
                }],
                name: "public".to_string(),
                sequences: vec![],
            }]
        }
    )
}

#[test]
async fn table_without_primary_key() {
    let helper = get_test_helper("helper").await;
    helper
        .execute_not_query(
            r#"
    create table my_table(
        name text not null,
        age int not null
    );
    "#,
        )
        .await;

    let db = introspect_schema(&helper).await;

    assert_eq!(
        db,
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
                            data_type: "integer".to_string(),
                            ..default()
                        },
                    ],
                    constraints: vec![],
                    indices: vec![],
                }],
                sequences: vec![],
            }]
        }
    )
}

#[test]
async fn composite_primary_keys() {
    let helper = get_test_helper("helper").await;
    helper
        .execute_not_query(
            r#"
    create table my_table(
        id_part_1 int not null,
        id_part_2 int not null,
        name text,
        age int,
        constraint my_table_pk primary key (id_part_1, id_part_2)
    );
    "#,
        )
        .await;

    let db = introspect_schema(&helper).await;

    assert_eq!(
        db,
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
                            data_type: "integer".to_string(),
                            ..default()
                        },
                        PostgresColumn {
                            name: "id_part_2".to_string(),
                            ordinal_position: 2,
                            is_nullable: false,
                            data_type: "integer".to_string(),
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
                            data_type: "integer".to_string(),
                            ..default()
                        },
                    ],
                    constraints: vec![PostgresConstraint::PrimaryKey(PostgresPrimaryKey {
                        name: "my_table_pk".to_string(),
                        columns: vec![
                            PostgresPrimaryKeyColumn {
                                column_name: "id_part_1".to_string(),
                                ordinal_position: 1,
                            },
                            PostgresPrimaryKeyColumn {
                                column_name: "id_part_2".to_string(),
                                ordinal_position: 2,
                            },
                        ],
                    }),],
                    indices: vec![],
                }],
                sequences: vec![],
            }]
        }
    )
}

#[test]
async fn indices() {
    let helper = get_test_helper("helper").await;
    helper
        .execute_not_query(
            r#"
    create table my_table(
        value int
    );

    create index my_table_value_asc_nulls_first on my_table(value asc nulls first);
    create index my_table_value_asc_nulls_last on my_table(value asc nulls last);
    create index my_table_value_desc_nulls_first on my_table(value desc nulls first);
    create index my_table_value_desc_nulls_last on my_table(value desc nulls last);

    "#,
        )
        .await;

    let db = introspect_schema(&helper).await;

    assert_eq!(
        db,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                tables: vec![PostgresTable {
                    name: "my_table".to_string(),
                    columns: vec![PostgresColumn {
                        name: "value".to_string(),
                        ordinal_position: 1,
                        is_nullable: true,
                        data_type: "integer".to_string(),
                        ..default()
                    },],
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
                        },
                    ],
                }],
                sequences: vec![],
            }]
        }
    )
}

#[test]
async fn index_types() {
    let helper = get_test_helper("helper").await;
    helper
        .execute_not_query(
            r#"
    create table my_table(
        free_text tsvector
    );

    create index my_table_gist on my_table using gist (free_text);
    create index my_table_gin on my_table using gin (free_text);
    "#,
        )
        .await;

    let db = introspect_schema(&helper).await;

    assert_eq!(
        db,
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
                    },],
                    constraints: vec![],
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
                        },
                    ],
                }],
                sequences: vec![],
            }]
        }
    )
}

#[test]
async fn filtered_index() {
    let helper = get_test_helper("helper").await;
    helper
        .execute_not_query(
            r#"
    create table my_table(
        value int
    );

    create index my_table_idx on my_table (value) where (value % 2 = 0);
    "#,
        )
        .await;

    let db = introspect_schema(&helper).await;

    assert_eq!(
        db,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                tables: vec![PostgresTable {
                    name: "my_table".to_string(),
                    columns: vec![PostgresColumn {
                        name: "value".to_string(),
                        ordinal_position: 1,
                        is_nullable: true,
                        data_type: "integer".to_string(),
                        ..default()
                    },],
                    constraints: vec![],
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
                    },],
                }],
                sequences: vec![],
            }]
        }
    )
}

#[test]
async fn index_with_include() {
    let helper = get_test_helper("helper").await;
    //language=postgresql
    helper
        .execute_not_query(
            r#"
    create table my_table(
        value int,
        another_value int
    );

    create index my_table_idx on my_table (value) include (another_value);
    "#,
        )
        .await;

    let db = introspect_schema(&helper).await;

    assert_eq!(
        db,
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
                            data_type: "integer".to_string(),
                            ..default()
                        },
                        PostgresColumn {
                            name: "another_value".to_string(),
                            ordinal_position: 2,
                            is_nullable: true,
                            data_type: "integer".to_string(),
                            ..default()
                        },
                    ],
                    constraints: vec![],
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
                    },],
                }],
                sequences: vec![],
            }]
        }
    )
}

#[test]
async fn table_with_non_distinct_nulls() {
    let helper = get_test_helper("helper").await;
    //language=postgresql
    helper
        .execute_not_query(
            r#"
    create table my_table(
        value int unique nulls not distinct
    );
    "#,
        )
        .await;

    let db = introspect_schema(&helper).await;

    assert_eq!(
        db,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                tables: vec![PostgresTable {
                    name: "my_table".to_string(),
                    columns: vec![PostgresColumn {
                        name: "value".to_string(),
                        ordinal_position: 1,
                        is_nullable: true,
                        data_type: "integer".to_string(),
                        ..default()
                    },],
                    constraints: vec![PostgresConstraint::Unique(PostgresUniqueConstraint {
                        name: "my_table_value_key".to_string(),
                        columns: vec![PostgresUniqueConstraintColumn {
                            column_name: "value".to_string(),
                            ordinal_position: 1,
                        }],
                        distinct_nulls: false,
                    })],
                    indices: vec![],
                }],
                sequences: vec![],
            }]
        }
    )
}

#[test]
async fn foreign_keys() {
    let helper = get_test_helper("helper").await;
    //language=postgresql
    helper
        .execute_not_query(
            r#"
    create table items(
        id serial primary key
    );

    create table users(
        id serial primary key,
        item_id int not null references items(id)
    );
    "#,
        )
        .await;

    let db = introspect_schema(&helper).await;

    assert_eq!(
        db,
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
                            data_type: "integer".to_string(),
                            default_value: Some("nextval('items_id_seq'::regclass)".to_string()),
                            ..default()
                        },],
                        constraints: vec![PostgresConstraint::PrimaryKey(PostgresPrimaryKey {
                            name: "items_pkey".to_string(),
                            columns: vec![PostgresPrimaryKeyColumn {
                                column_name: "id".to_string(),
                                ordinal_position: 1,
                            }],
                        }),],
                        ..default()
                    },
                    PostgresTable {
                        name: "users".to_string(),
                        columns: vec![
                            PostgresColumn {
                                name: "id".to_string(),
                                ordinal_position: 1,
                                is_nullable: false,
                                data_type: "integer".to_string(),
                                default_value: Some(
                                    "nextval('users_id_seq'::regclass)".to_string()
                                ),
                                ..default()
                            },
                            PostgresColumn {
                                name: "item_id".to_string(),
                                ordinal_position: 2,
                                is_nullable: false,
                                data_type: "integer".to_string(),
                                ..default()
                            },
                        ],
                        constraints: vec![
                            PostgresConstraint::PrimaryKey(PostgresPrimaryKey {
                                name: "users_pkey".to_string(),
                                columns: vec![PostgresPrimaryKeyColumn {
                                    column_name: "id".to_string(),
                                    ordinal_position: 1,
                                }],
                            }),
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
                        ..default()
                    },
                ],
                sequences: vec![
                    PostgresSequence {
                        name: "items_id_seq".to_string(),
                        data_type: "integer".to_string(),
                        start_value: 1,
                        increment: 1,
                        min_value: 1,
                        max_value: 2147483647,
                        cache_size: 1,
                        cycle: false,
                        last_value: None,
                    },
                    PostgresSequence {
                        name: "users_id_seq".to_string(),
                        data_type: "integer".to_string(),
                        start_value: 1,
                        increment: 1,
                        min_value: 1,
                        max_value: 2147483647,
                        cache_size: 1,
                        cycle: false,
                        last_value: None,
                    },
                ],
            }]
        }
    )
}

#[test]
async fn foreign_key_constraints() {
    let helper = get_test_helper("helper").await;
    //language=postgresql
    helper
        .execute_not_query(
            r#"
    CREATE TABLE products (
        product_no integer PRIMARY KEY
    );
    
    CREATE TABLE orders (
        order_id integer PRIMARY KEY
    );
    
    CREATE TABLE order_items (
        product_no integer REFERENCES products ON DELETE RESTRICT ON UPDATE CASCADE,
        order_id integer REFERENCES orders ON DELETE CASCADE ON UPDATE RESTRICT,
        PRIMARY KEY (product_no, order_id)
    );
    "#,
        )
        .await;

    let db = introspect_schema(&helper).await;

    assert_eq!(
        db,
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
                                data_type: "integer".to_string(),
                                default_value: None,
                                ..default()
                            },
                            PostgresColumn {
                                name: "order_id".to_string(),
                                ordinal_position: 2,
                                is_nullable: false,
                                data_type: "integer".to_string(),
                                default_value: None,
                                ..default()
                            },
                        ],
                        constraints: vec![
                            PostgresConstraint::PrimaryKey(PostgresPrimaryKey {
                                name: "order_items_pkey".to_string(),
                                columns: vec![
                                    PostgresPrimaryKeyColumn {
                                        column_name: "product_no".to_string(),
                                        ordinal_position: 1,
                                    },
                                    PostgresPrimaryKeyColumn {
                                        column_name: "order_id".to_string(),
                                        ordinal_position: 2,
                                    },
                                ],
                            }),
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
                            }),
                        ],
                        ..default()
                    },
                    PostgresTable {
                        name: "orders".to_string(),
                        columns: vec![PostgresColumn {
                            name: "order_id".to_string(),
                            ordinal_position: 1,
                            is_nullable: false,
                            data_type: "integer".to_string(),
                            default_value: None,
                            ..default()
                        }],
                        constraints: vec![PostgresConstraint::PrimaryKey(PostgresPrimaryKey {
                            name: "orders_pkey".to_string(),
                            columns: vec![PostgresPrimaryKeyColumn {
                                column_name: "order_id".to_string(),
                                ordinal_position: 1,
                            }],
                        }),],
                        ..default()
                    },
                    PostgresTable {
                        name: "products".to_string(),
                        columns: vec![PostgresColumn {
                            name: "product_no".to_string(),
                            ordinal_position: 1,
                            is_nullable: false,
                            data_type: "integer".to_string(),
                            default_value: None,
                            ..default()
                        },],
                        constraints: vec![PostgresConstraint::PrimaryKey(PostgresPrimaryKey {
                            name: "products_pkey".to_string(),
                            columns: vec![PostgresPrimaryKeyColumn {
                                column_name: "product_no".to_string(),
                                ordinal_position: 1,
                            }],
                        }),],
                        ..default()
                    }
                ],
                sequences: vec![],
            }]
        }
    )
}

#[test]
async fn generated_column() {

    let helper = get_test_helper("helper").await;
    //language=postgresql
    helper
        .execute_not_query(
            r#"
    CREATE TABLE products (
        name text not null,
        search tsvector not null GENERATED ALWAYS AS (to_tsvector('english', name)) STORED
    );
    "#,
        )
        .await;

    let db = introspect_schema(&helper).await;

    assert_eq!(db, PostgresDatabase {
        schemas: vec![
            PostgresSchema {
                name: "public".to_string(),
                sequences: vec![],
                tables: vec![
                    PostgresTable {
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
                    }
                ]
            }
        ]
    })
}