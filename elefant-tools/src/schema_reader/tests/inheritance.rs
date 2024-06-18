use crate::schema_reader::tests::test_introspection;
use crate::test_helpers;
use crate::test_helpers::TestHelper;
use crate::{
    default, PostgresCheckConstraint, PostgresColumn, PostgresConstraint, PostgresDatabase,
    PostgresIndex, PostgresIndexColumnDirection, PostgresIndexKeyColumn, PostgresIndexNullsOrder,
    PostgresIndexType, PostgresSchema, PostgresSequence, PostgresTable, TableTypeDetails,
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
#[pg_test(arg(pg_bouncer = 15))]
async fn inherited_tables(helper: &TestHelper) {
    test_introspection(
        helper,
        r#"
create table pets (
    id serial primary key,
    name text not null check(length(name) > 1)
);

create table dogs(
    breed text not null check(length(breed) > 1)
) inherits (pets);

create table cats(
    color text not null
) inherits (pets);
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                tables: vec![
                    PostgresTable {
                        name: "cats".to_string(),
                        columns: vec![
                            PostgresColumn {
                                name: "id".to_string(),
                                ordinal_position: 1,
                                is_nullable: false,
                                data_type: "int4".to_string(),
                                default_value: Some("nextval('pets_id_seq'::regclass)".to_string()),
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
                                name: "color".to_string(),
                                ordinal_position: 3,
                                is_nullable: false,
                                data_type: "text".to_string(),
                                ..default()
                            },
                        ],
                        constraints: vec![PostgresConstraint::Check(PostgresCheckConstraint {
                            name: "pets_name_check".to_string(),
                            check_clause: "((length(name) > 1))".into(),
                            ..default()
                        })],
                        table_type: TableTypeDetails::InheritedTable {
                            parent_tables: vec!["pets".to_string()],
                        },
                        depends_on: vec![9.into()],
                        ..default()
                    },
                    PostgresTable {
                        name: "dogs".to_string(),
                        columns: vec![
                            PostgresColumn {
                                name: "id".to_string(),
                                ordinal_position: 1,
                                is_nullable: false,
                                data_type: "int4".to_string(),
                                default_value: Some("nextval('pets_id_seq'::regclass)".to_string()),
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
                                name: "breed".to_string(),
                                ordinal_position: 3,
                                is_nullable: false,
                                data_type: "text".to_string(),
                                ..default()
                            },
                        ],
                        constraints: vec![
                            PostgresConstraint::Check(PostgresCheckConstraint {
                                name: "dogs_breed_check".to_string(),
                                check_clause: "((length(breed) > 1))".into(),
                                ..default()
                            }),
                            PostgresConstraint::Check(PostgresCheckConstraint {
                                name: "pets_name_check".to_string(),
                                check_clause: "((length(name) > 1))".into(),
                                ..default()
                            }),
                        ],
                        table_type: TableTypeDetails::InheritedTable {
                            parent_tables: vec!["pets".to_string()],
                        },
                        depends_on: vec![9.into()],
                        ..default()
                    },
                    PostgresTable {
                        name: "pets".to_string(),
                        object_id: 9.into(),
                        columns: vec![
                            PostgresColumn {
                                name: "id".to_string(),
                                ordinal_position: 1,
                                is_nullable: false,
                                data_type: "int4".to_string(),
                                default_value: Some("nextval('pets_id_seq'::regclass)".to_string()),
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
                        constraints: vec![PostgresConstraint::Check(PostgresCheckConstraint {
                            name: "pets_name_check".to_string(),
                            check_clause: "((length(name) > 1))".into(),
                            ..default()
                        })],
                        indices: vec![PostgresIndex {
                            name: "pets_pkey".to_string(),
                            key_columns: vec![PostgresIndexKeyColumn {
                                name: "id".to_string(),
                                ordinal_position: 1,
                                direction: Some(PostgresIndexColumnDirection::Ascending),
                                nulls_order: Some(PostgresIndexNullsOrder::Last),
                            }],
                            index_type: "btree".to_string(),
                            index_constraint_type: PostgresIndexType::PrimaryKey,
                            ..default()
                        }],
                        ..default()
                    },
                ],
                sequences: vec![PostgresSequence {
                    name: "pets_id_seq".to_string(),
                    data_type: "int4".to_string(),
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
#[pg_test(arg(timescale_db = 15))]
#[pg_test(arg(timescale_db = 16))]
#[pg_test(arg(pg_bouncer = 15))]
async fn multiple_inheritance(helper: &TestHelper) {
    test_introspection(
        helper,
        r#"
create table animal(
    breed text not null
);

create table human(
    name text not null
);

create table animorph() inherits (animal, human);
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                tables: vec![
                    PostgresTable {
                        name: "animal".to_string(),
                        columns: vec![PostgresColumn {
                            name: "breed".to_string(),
                            ordinal_position: 1,
                            is_nullable: false,
                            data_type: "text".to_string(),
                            ..default()
                        }],
                        object_id: 2.into(),
                        ..default()
                    },
                    PostgresTable {
                        name: "animorph".to_string(),
                        columns: vec![
                            PostgresColumn {
                                name: "breed".to_string(),
                                ordinal_position: 1,
                                is_nullable: false,
                                data_type: "text".to_string(),
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
                        table_type: TableTypeDetails::InheritedTable {
                            parent_tables: vec!["animal".to_string(), "human".to_string()],
                        },
                        depends_on: vec![2.into(), 4.into()],
                        ..default()
                    },
                    PostgresTable {
                        name: "human".to_string(),
                        object_id: 4.into(),
                        columns: vec![PostgresColumn {
                            name: "name".to_string(),
                            ordinal_position: 1,
                            is_nullable: false,
                            data_type: "text".to_string(),
                            ..default()
                        }],
                        ..default()
                    },
                ],
                name: "public".to_string(),
                ..default()
            }],
            timescale_support: TimescaleSupport::from_test_helper(helper),
            ..default()
        },
    )
    .await;
}
