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
                ..default()
            }],
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
                            data_type: "integer".to_string(),
                            ..default()
                        },
                    ],
                    ..default()
                }],
                ..default()
            }],
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
                    })],
                    ..default()
                }],
                ..default()
            }],
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
                        data_type: "integer".to_string(),
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
                ..default()
            }],
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
                    ..default()
                }],
                ..default()
            }],
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
                        data_type: "integer".to_string(),
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
                    }],
                    ..default()
                }],
                ..default()
            }],
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
                    }],
                    ..default()
                }],
                ..default()
            }],
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
                        data_type: "integer".to_string(),
                        ..default()
                    }],
                    constraints: vec![PostgresConstraint::Unique(PostgresUniqueConstraint {
                        name: "my_table_value_key".to_string(),
                        columns: vec![PostgresUniqueConstraintColumn {
                            column_name: "value".to_string(),
                            ordinal_position: 1,
                        }],
                        distinct_nulls: false,
                    })],
                    ..default()
                }],
                ..default()
            }],
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
                            data_type: "integer".to_string(),
                            default_value: Some("nextval('items_id_seq'::regclass)".to_string()),
                            ..default()
                        }],
                        constraints: vec![PostgresConstraint::PrimaryKey(PostgresPrimaryKey {
                            name: "items_pkey".to_string(),
                            columns: vec![PostgresPrimaryKeyColumn {
                                column_name: "id".to_string(),
                                ordinal_position: 1,
                            }],
                        })],
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
                                    "nextval('users_id_seq'::regclass)".to_string(),
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
                ..default()
            }],
        },
    );
}

#[test]
fn foreign_key_constraints() {
    test_introspection(
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
                        })],
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
                        }],
                        constraints: vec![PostgresConstraint::PrimaryKey(PostgresPrimaryKey {
                            name: "products_pkey".to_string(),
                            columns: vec![PostgresPrimaryKeyColumn {
                                column_name: "product_no".to_string(),
                                ordinal_position: 1,
                            }],
                        })],
                        ..default()
                    },
                ],
                ..default()
            }],
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
                }],
                ..default()
            }],
        },
    );
}

#[test]
fn test_functions() {
    test_introspection(
        r#"

    create function add(a integer, b integer) returns integer as $$
        begin
            return a + b;
        end;
    $$ language plpgsql;

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
                        sql_body: r#"
        begin
            return a + b;
        end;
    "#
                        .to_string(),
                        configuration: None,
                        arguments: "a integer, b integer".to_string(),
                        result: Some("integer".to_string()),
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
                        sql_body: r#"
        begin

        create temp table temp_table(id int, name text);

        insert into temp_table(id, name) values (1, 'foo'), (2, 'bar');

        return query select * from temp_table where name = value;

        end;

        "#
                        .to_string(),
                        configuration: None,
                        arguments: "value text".to_string(),
                        result: Some("TABLE(id integer, name text)".to_string()),
                    },
                ],
                ..default()
            }],
        },
    )
}

#[test]
fn test_qouted_identifier_names() {
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
                                data_type: "integer".to_string(),
                                default_value: Some("nextval('\"MyTable_int_seq\"'::regclass)".to_string()),
                                ..default()
                            }
                        ],
                        constraints: vec![
                            PostgresConstraint::PrimaryKey(PostgresPrimaryKey {
                                name: "MyTable_pkey".to_string(),
                                columns: vec![
                                    PostgresPrimaryKeyColumn {
                                        column_name: "int".to_string(),
                                        ordinal_position: 1,
                                    }
                                ]
                            })
                        ],
                        ..default()
                    }
                ],
                sequences: vec![
                    PostgresSequence {
                        name: "MyTable_int_seq".to_string(),
                        data_type: "integer".to_string(),
                        ..default()
                    }
                ],
                ..default()
            }
        ]
    })
}