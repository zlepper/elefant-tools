use elefant_test_macros::pg_test;
use crate::{default, PostgresColumn, PostgresConstraint, PostgresDatabase, PostgresIndex, PostgresIndexColumnDirection, PostgresIndexIncludedColumn, PostgresIndexKeyColumn, PostgresIndexNullsOrder, PostgresIndexType, PostgresSchema, PostgresTable, PostgresUniqueConstraint, TimescaleSupport};
use crate::schema_reader::tests;
use crate::test_helpers::TestHelper;

#[pg_test(arg(postgres = 12))]
#[pg_test(arg(postgres = 13))]
#[pg_test(arg(postgres = 14))]
#[pg_test(arg(postgres = 15))]
#[pg_test(arg(postgres = 16))]
#[pg_test(arg(timescale_db = 15))]
#[pg_test(arg(timescale_db = 16))]
async fn indices(helper: &TestHelper) {
    tests::test_introspection(
        helper,
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
async fn index_types(helper: &TestHelper) {
    tests::test_introspection(
        helper,
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
async fn filtered_index(helper: &TestHelper) {
    tests::test_introspection(
        helper,
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
async fn index_with_include(helper: &TestHelper) {
    tests::test_introspection(
        helper,
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
            timescale_support: TimescaleSupport::from_test_helper(helper),
            ..default()
        },
    )
        .await;
}

#[pg_test(arg(postgres = 15))]
#[pg_test(arg(postgres = 16))]
#[pg_test(arg(timescale_db = 15))]
#[pg_test(arg(timescale_db = 16))]
async fn table_with_non_distinct_nulls(helper: &TestHelper) {
    tests::test_introspection(
        helper,
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
                    constraints: vec![PostgresConstraint::Unique(PostgresUniqueConstraint {
                        name: "my_table_value_key".to_string(),
                        unique_index_name: "my_table_value_key".to_string(),
                        ..default()
                    })],
                    indices: vec![PostgresIndex {
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
