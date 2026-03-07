mod column_types;
mod comments;
mod custom_types;
mod extensions;
mod foreign_keys;
mod functions;
mod indices;
mod inheritance;
mod partitioning;
mod respects_permissions;
mod storage_parameters;
mod timescale;
mod triggers;
mod views;

use super::*;
use crate::default;
use crate::test_helpers;
use crate::test_helpers::TestHelper;
use elefant_test_macros::pg_test;

pub async fn introspect_schema(test_helper: &TestHelper) -> PostgresDatabase {
    let conn = test_helper.get_conn();
    let reader = SchemaReader::new(conn);
    reader.introspect_database().await.unwrap()
}

async fn test_introspection(
    helper: &TestHelper,
    create_table_statement: &str,
    expected: PostgresDatabase,
) {
    helper.execute_not_query(create_table_statement).await;

    let db = introspect_schema(helper).await;

    assert_eq!(db, expected)
}

#[pg_test(arg(postgres = 12))]
#[pg_test(arg(postgres = 13))]
#[pg_test(arg(postgres = 14))]
#[pg_test(arg(postgres = 15))]
#[pg_test(arg(postgres = 16))]
#[pg_test(arg(postgres = 17))]
#[pg_test(arg(postgres = 18))]
#[pg_test(arg(timescale_db = 15))]
#[pg_test(arg(timescale_db = 16))]
#[pg_test(arg(timescale_db = 17))]
#[pg_test(arg(timescale_db = 18))]
async fn reads_simple_schema(helper: &TestHelper) {
    test_introspection(
        helper,
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
                                .into(),
                            ..default()
                        }),
                        PostgresConstraint::Check(PostgresCheckConstraint {
                            name: "my_table_age_check".to_string(),
                            check_clause: "((age > 21))".into(),
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
#[pg_test(arg(postgres = 17))]
#[pg_test(arg(postgres = 18))]
#[pg_test(arg(timescale_db = 15))]
#[pg_test(arg(timescale_db = 16))]
#[pg_test(arg(timescale_db = 17))]
#[pg_test(arg(timescale_db = 18))]
async fn identity_column_always_generated(helper: &TestHelper) {
    test_introspection(
        helper,
        r#"
    create table my_table(
        id int generated always as identity primary key,
        name text not null
    );

    insert into my_table(name) values ('foo'), ('bar');
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
                            identity: Some(ColumnIdentity::GeneratedAlways),
                            ..default()
                        },
                        PostgresColumn {
                            name: "name".to_string(),
                            ordinal_position: 2,
                            is_nullable: false,
                            data_type: "text".to_string(),
                            ..default()
                        },
                    ],
                    indices: vec![PostgresIndex {
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
                    }],
                    ..default()
                }],
                sequences: vec![PostgresSequence {
                    name: "my_table_id_seq".to_string(),
                    data_type: "int4".to_string(),
                    start_value: 1,
                    increment: 1,
                    cycle: false,
                    last_value: Some(2),
                    is_internally_created: true,
                    author_table: Some("my_table".to_string()),
                    author_table_column_position: Some(1),
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
#[pg_test(arg(postgres = 17))]
#[pg_test(arg(postgres = 18))]
#[pg_test(arg(timescale_db = 15))]
#[pg_test(arg(timescale_db = 16))]
#[pg_test(arg(timescale_db = 17))]
#[pg_test(arg(timescale_db = 18))]
async fn identity_column_by_default(helper: &TestHelper) {
    test_introspection(
        helper,
        r#"
    create table my_table(
        id int generated by default as identity primary key,
        name text not null
    );

    insert into my_table(name) values ('foo'), ('bar');
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
                            identity: Some(ColumnIdentity::GeneratedByDefault),
                            ..default()
                        },
                        PostgresColumn {
                            name: "name".to_string(),
                            ordinal_position: 2,
                            is_nullable: false,
                            data_type: "text".to_string(),
                            ..default()
                        },
                    ],
                    indices: vec![PostgresIndex {
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
                    }],
                    ..default()
                }],
                sequences: vec![PostgresSequence {
                    name: "my_table_id_seq".to_string(),
                    data_type: "int4".to_string(),
                    start_value: 1,
                    increment: 1,
                    cycle: false,
                    last_value: Some(2),
                    is_internally_created: true,
                    author_table: Some("my_table".to_string()),
                    author_table_column_position: Some(1),
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
#[pg_test(arg(postgres = 17))]
#[pg_test(arg(postgres = 18))]
#[pg_test(arg(timescale_db = 15))]
#[pg_test(arg(timescale_db = 16))]
#[pg_test(arg(timescale_db = 17))]
#[pg_test(arg(timescale_db = 18))]
async fn identity_column_custom_sequence(helper: &TestHelper) {
    test_introspection(
        helper,
        r#"
    create table my_table(
        id int generated by default as identity (START WITH 10 INCREMENT BY 10) primary key,
        name text not null
    );

    insert into my_table(name) values ('foo'), ('bar');
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
                            identity: Some(ColumnIdentity::GeneratedByDefault),
                            ..default()
                        },
                        PostgresColumn {
                            name: "name".to_string(),
                            ordinal_position: 2,
                            is_nullable: false,
                            data_type: "text".to_string(),
                            ..default()
                        },
                    ],
                    indices: vec![PostgresIndex {
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
                    }],
                    ..default()
                }],
                sequences: vec![PostgresSequence {
                    name: "my_table_id_seq".to_string(),
                    data_type: "int4".to_string(),
                    start_value: 10,
                    increment: 10,
                    cycle: false,
                    last_value: Some(20),
                    is_internally_created: true,
                    author_table: Some("my_table".to_string()),
                    author_table_column_position: Some(1),
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
#[pg_test(arg(postgres = 17))]
#[pg_test(arg(postgres = 18))]
#[pg_test(arg(timescale_db = 15))]
#[pg_test(arg(timescale_db = 16))]
#[pg_test(arg(timescale_db = 17))]
#[pg_test(arg(timescale_db = 18))]
async fn table_without_columns(helper: &TestHelper) {
    test_introspection(
        helper,
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
#[pg_test(arg(postgres = 17))]
#[pg_test(arg(postgres = 18))]
#[pg_test(arg(timescale_db = 15))]
#[pg_test(arg(timescale_db = 16))]
#[pg_test(arg(timescale_db = 17))]
#[pg_test(arg(timescale_db = 18))]
async fn table_without_primary_key(helper: &TestHelper) {
    test_introspection(
        helper,
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
#[pg_test(arg(postgres = 17))]
#[pg_test(arg(postgres = 18))]
#[pg_test(arg(timescale_db = 15))]
#[pg_test(arg(timescale_db = 16))]
#[pg_test(arg(timescale_db = 17))]
#[pg_test(arg(timescale_db = 18))]
async fn composite_primary_keys(helper: &TestHelper) {
    test_introspection(
        helper,
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
                    indices: vec![PostgresIndex {
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

#[pg_test(arg(postgres = 12))]
#[pg_test(arg(postgres = 13))]
#[pg_test(arg(postgres = 14))]
#[pg_test(arg(postgres = 15))]
#[pg_test(arg(postgres = 16))]
#[pg_test(arg(postgres = 17))]
#[pg_test(arg(postgres = 18))]
#[pg_test(arg(timescale_db = 15))]
#[pg_test(arg(timescale_db = 16))]
#[pg_test(arg(timescale_db = 17))]
#[pg_test(arg(timescale_db = 18))]
async fn generated_column(helper: &TestHelper) {
    test_introspection(
        helper,
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
                            generated: Some(GeneratedColumn {
                                expression: "to_tsvector('english'::regconfig, name)".to_string(),
                                generation_type: GeneratedColumnType::Stored,
                            }),
                            ..default()
                        },
                    ],
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

#[pg_test(arg(postgres = 18))]
#[pg_test(arg(timescale_db = 18))]
async fn virtual_generated_column(helper: &TestHelper) {
    test_introspection(
        helper,
        r#"
    CREATE TABLE products (
        name text not null,
        name_upper text not null GENERATED ALWAYS AS (upper(name)) VIRTUAL
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
                            name: "name_upper".to_string(),
                            ordinal_position: 2,
                            is_nullable: false,
                            data_type: "text".to_string(),
                            generated: Some(GeneratedColumn {
                                expression: "upper(name)".to_string(),
                                generation_type: GeneratedColumnType::Virtual,
                            }),
                            ..default()
                        },
                    ],
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

#[pg_test(arg(postgres = 18))]
#[pg_test(arg(timescale_db = 18))]
async fn not_enforced_check_constraint(helper: &TestHelper) {
    test_introspection(
        helper,
        r#"
    CREATE TABLE products (
        id int not null,
        price numeric,
        constraint positive_price check (price > 0) not enforced
    );
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                tables: vec![PostgresTable {
                    name: "products".to_string(),
                    columns: vec![
                        PostgresColumn {
                            name: "id".to_string(),
                            ordinal_position: 1,
                            is_nullable: false,
                            data_type: "int4".to_string(),
                            ..default()
                        },
                        PostgresColumn {
                            name: "price".to_string(),
                            ordinal_position: 2,
                            is_nullable: true,
                            data_type: "numeric".to_string(),
                            ..default()
                        },
                    ],
                    constraints: vec![PostgresConstraint::Check(PostgresCheckConstraint {
                        name: "positive_price".to_string(),
                        check_clause: "((price > (0)::numeric))".into(),
                        is_enforced: false,
                        ..default()
                    })],
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
#[pg_test(arg(postgres = 17))]
#[pg_test(arg(postgres = 18))]
#[pg_test(arg(timescale_db = 15))]
#[pg_test(arg(timescale_db = 16))]
#[pg_test(arg(timescale_db = 17))]
#[pg_test(arg(timescale_db = 18))]
async fn test_quoted_identifier_names(helper: &TestHelper) {
    test_introspection(
        helper,
        r#"
        create table "MyTable" (int serial primary key);
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                tables: vec![PostgresTable {
                    name: "MyTable".to_string(),
                    columns: vec![PostgresColumn {
                        name: "int".to_string(),
                        ordinal_position: 1,
                        is_nullable: false,
                        data_type: "int4".to_string(),
                        default_value: Some("nextval('\"MyTable_int_seq\"'::regclass)".to_string()),
                        ..default()
                    }],
                    indices: vec![PostgresIndex {
                        name: "MyTable_pkey".to_string(),
                        key_columns: vec![PostgresIndexKeyColumn {
                            name: "\"int\"".to_string(),
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
                }],
                sequences: vec![PostgresSequence {
                    name: "MyTable_int_seq".to_string(),
                    data_type: "int4".to_string(),
                    ..default()
                }],
                ..default()
            }],
            timescale_support: TimescaleSupport::from_test_helper(helper),
            ..default()
        },
    )
    .await
}

#[pg_test(arg(postgres = 18))]
#[pg_test(arg(timescale_db = 18))]
async fn temporal_primary_key(helper: &TestHelper) {
    test_introspection(
        helper,
        r#"
    CREATE EXTENSION btree_gist;
    CREATE TABLE reservations (
        id int not null,
        valid_period tsrange not null,
        constraint reservations_pk primary key (id, valid_period without overlaps)
    );
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                tables: vec![PostgresTable {
                    name: "reservations".to_string(),
                    columns: vec![
                        PostgresColumn {
                            name: "id".to_string(),
                            ordinal_position: 1,
                            is_nullable: false,
                            data_type: "int4".to_string(),
                            ..default()
                        },
                        PostgresColumn {
                            name: "valid_period".to_string(),
                            ordinal_position: 2,
                            is_nullable: false,
                            data_type: "tsrange".to_string(),
                            ..default()
                        },
                    ],
                    indices: vec![PostgresIndex {
                        name: "reservations_pk".to_string(),
                        key_columns: vec![
                            PostgresIndexKeyColumn {
                                name: "id".to_string(),
                                ordinal_position: 1,
                                direction: None,
                                nulls_order: None,
                            },
                            PostgresIndexKeyColumn {
                                name: "valid_period".to_string(),
                                ordinal_position: 2,
                                direction: None,
                                nulls_order: None,
                            },
                        ],
                        index_type: "gist".to_string(),
                        index_constraint_type: PostgresIndexType::PrimaryKey,
                        constraint_definition: Some(
                            "PRIMARY KEY (id, valid_period WITHOUT OVERLAPS)".to_string(),
                        ),
                        ..default()
                    }],
                    ..default()
                }],
                ..default()
            }],
            enabled_extensions: vec![PostgresExtension {
                name: "btree_gist".to_string(),
                schema_name: "public".to_string(),
                version: "1.8".to_string(),
                relocatable: true,
                ..default()
            }],
            timescale_support: TimescaleSupport::from_test_helper(helper),
            ..default()
        },
    )
    .await
}
