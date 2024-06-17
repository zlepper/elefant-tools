use crate::schema_reader::tests;
use crate::test_helpers;
use crate::test_helpers::TestHelper;
use crate::{
    default, PostgresColumn, PostgresDatabase, PostgresDomain, PostgresDomainConstraint,
    PostgresEnum, PostgresSchema, PostgresTable, TimescaleSupport,
};
use elefant_test_macros::pg_test;

#[pg_test(arg(postgres = 12))]
#[pg_test(arg(postgres = 13))]
#[pg_test(arg(postgres = 14))]
#[pg_test(arg(postgres = 15))]
#[pg_test(arg(postgres = 16))]
#[pg_test(arg(timescale_db = 15))]
#[pg_test(arg(timescale_db = 16))]
async fn enums(helper: &TestHelper) {
    tests::test_introspection(
        helper,
        r#"
    CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
    CREATE TABLE person (
        name text,
        current_mood mood
    );
    alter type mood add value 'mehh' before 'ok';

    comment on type mood is 'This is a mood';
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                tables: vec![PostgresTable {
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
                        },
                    ],
                    ..default()
                }],
                enums: vec![PostgresEnum {
                    name: "mood".to_string(),
                    values: vec![
                        "sad".to_string(),
                        "mehh".to_string(),
                        "ok".to_string(),
                        "happy".to_string(),
                    ],
                    comment: Some("This is a mood".to_string()),
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
async fn domains(helper: &TestHelper) {
    tests::test_introspection(
        helper,
        r#"
create domain public.year as integer
    constraint year_check check (((value >= 1901) and (value <= 2155)));

create domain public.twenties as year
    constraint twenties_check check (value >= 1920 and value <= 1929);

comment on domain public.year is 'year between 1901 and 2155';

create domain unix_year as integer default 1970;

create domain non_null_year as year not null;

create domain smol_text as varchar(10);

create table movie
(
    name text not null,
    year year not null
);
    "#,
        PostgresDatabase {
            schemas: vec![PostgresSchema {
                name: "public".to_string(),
                tables: vec![PostgresTable {
                    name: "movie".to_string(),
                    object_id: 2.into(),
                    columns: vec![
                        PostgresColumn {
                            name: "name".to_string(),
                            is_nullable: false,
                            ordinal_position: 1,
                            data_type: "text".to_string(),
                            ..default()
                        },
                        PostgresColumn {
                            name: "year".to_string(),
                            is_nullable: false,
                            ordinal_position: 2,
                            data_type: "year".to_string(),
                            ..default()
                        },
                    ],
                    depends_on: vec![7.into()],
                    ..default()
                }],
                domains: vec![
                    PostgresDomain {
                        name: "non_null_year".to_string(),
                        base_type_name: "year".to_string(),
                        object_id: 3.into(),
                        not_null: true,
                        depends_on: vec![7.into()],
                        ..default()
                    },
                    PostgresDomain {
                        name: "smol_text".to_string(),
                        base_type_name: "varchar".to_string(),
                        object_id: 4.into(),
                        data_type_length: Some(10),
                        ..default()
                    },
                    PostgresDomain {
                        name: "twenties".to_string(),
                        base_type_name: "year".to_string(),
                        object_id: 5.into(),
                        constraint: Some(PostgresDomainConstraint {
                            name: "twenties_check".to_string(),
                            definition:
                                "((((VALUE)::integer >= 1920) AND ((VALUE)::integer <= 1929)))"
                                    .to_string(),
                        }),
                        depends_on: vec![7.into()],
                        ..default()
                    },
                    PostgresDomain {
                        name: "unix_year".to_string(),
                        base_type_name: "int4".to_string(),
                        object_id: 6.into(),
                        default_value: Some("1970".to_string()),
                        ..default()
                    },
                    PostgresDomain {
                        name: "year".to_string(),
                        base_type_name: "int4".to_string(),
                        object_id: 7.into(),
                        constraint: Some(PostgresDomainConstraint {
                            name: "year_check".to_string(),
                            definition: "(((VALUE >= 1901) AND (VALUE <= 2155)))".to_string(),
                        }),
                        description: Some("year between 1901 and 2155".to_string()),
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
