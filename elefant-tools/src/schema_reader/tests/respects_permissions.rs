use crate::pg_interval::Interval;
use crate::schema_reader::SchemaReader;
use crate::test_helpers::{get_test_connection_full, TestHelper};
use crate::TableTypeDetails::TimescaleHypertable;
use crate::ViewOptions::TimescaleContinuousAggregate;
use crate::{
    default, PostgresColumn, PostgresDatabase, PostgresSchema, PostgresTable, PostgresView,
    PostgresViewColumn, TimescaleSupport,
};
use crate::{
    test_helpers, HypertableDimension, ObjectId, PostgresIndex, PostgresIndexColumnDirection,
    PostgresIndexKeyColumn, PostgresIndexNullsOrder, PostgresIndexType,
};
use elefant_test_macros::pg_test;

#[pg_test(arg(postgres = 12))]
#[pg_test(arg(postgres = 13))]
#[pg_test(arg(postgres = 14))]
#[pg_test(arg(postgres = 15))]
#[pg_test(arg(timescale_db = 15))]
async fn respects_permissions(helper: &TestHelper) {
    //language=postgresql
    helper
        .execute_not_query(
            r#"
    create schema one;

    create schema two;

    drop user if exists one_user;
    drop user if exists two_user;

    create user one_user with password 'password' noinherit;
    create user two_user with password 'password' noinherit;

    grant all on schema one to one_user;
    grant all on schema two to two_user;
    grant all on schema public to one_user;
    grant all on schema public to two_user;
    "#,
        )
        .await;

    let schema_one_connection = get_test_connection_full(
        &helper.test_db_name,
        helper.port,
        "one_user",
        "password",
        Some("one"),
    )
    .await;

    schema_one_connection
        .execute_non_query(
            r#"
        create table my_table(id int);

        insert into my_table values (1);

        create view my_view as select * from my_table;
    "#,
        )
        .await
        .unwrap();

    let schema_two_connection = get_test_connection_full(
        &helper.test_db_name,
        helper.port,
        "two_user",
        "password",
        Some("two"),
    )
    .await;

    schema_two_connection
        .execute_non_query(
            r#"
        create table my_table(id int);

        insert into my_table values (2);

        create view my_view as select * from my_table;
    "#,
        )
        .await
        .unwrap();

    helper
        .execute_not_query(
            r#"
    grant usage on schema one to two_user;
    grant SELECT on table one.my_table to two_user;
    "#,
        )
        .await;

    _ = schema_one_connection
        .get_single_result::<i32>("select id from two.my_table;")
        .await
        .unwrap_err();

    let id = schema_two_connection
        .get_single_result::<i32>("select id from two.my_table;")
        .await
        .unwrap();
    assert_eq!(id, 2);
    let id = schema_two_connection
        .get_single_result::<i32>("select id from one.my_table;")
        .await
        .unwrap();
    assert_eq!(id, 1);

    let reader = SchemaReader::new(&schema_one_connection);
    let schema_one_introspection = reader.introspect_database().await.unwrap();

    assert_eq!(
        schema_one_introspection,
        PostgresDatabase {
            schemas: vec![
                PostgresSchema {
                    name: "one".to_string(),
                    tables: vec![PostgresTable {
                        name: "my_table".to_string(),
                        columns: vec![PostgresColumn {
                            name: "id".to_string(),
                            data_type: "int4".to_string(),
                            is_nullable: true,
                            ordinal_position: 1,
                            ..default()
                        }],
                        object_id: 3.into(),
                        ..default()
                    }],
                    views: vec![PostgresView {
                        name: "my_view".to_string(),
                        definition: "SELECT my_table.id FROM my_table;".into(),
                        columns: vec![PostgresViewColumn {
                            name: "id".to_string(),
                            ordinal_position: 1,
                        }],
                        depends_on: vec![3.into()],
                        ..default()
                    }],
                    ..default()
                },
                PostgresSchema {
                    name: "public".to_string(),
                    ..default()
                },
            ],
            timescale_support: TimescaleSupport::from_test_helper(helper),
            ..default()
        }
    )
}

#[pg_test(arg(timescale_db = 15))]
async fn hypertable_permissions(helper: &TestHelper) {
    //language=postgresql
    helper
        .execute_not_query(
            r#"
    create schema ht_one;

    create schema ht_two;

    drop user if exists ht_one_user;
    drop user if exists ht_two_user;

    create user ht_one_user with password 'password' noinherit;
    create user ht_two_user with password 'password' noinherit;

    grant all on schema ht_one to ht_one_user;
    grant all on schema ht_two to ht_two_user;
    grant all on schema public to ht_one_user;
    grant all on schema public to ht_two_user;
    "#,
        )
        .await;

    let schema_one_connection = get_test_connection_full(
        &helper.test_db_name,
        helper.port,
        "ht_one_user",
        "password",
        Some("ht_one"),
    )
    .await;

    schema_one_connection.execute_non_query(r#"
        create table my_table(time timestamptz not null, id int);
        
        SELECT public.create_hypertable('my_table', public.by_range('time'));

        insert into my_table values (now(), 1);

        create materialized view my_view with (timescaledb.continuous) as select public.time_bucket('1 day', time) as tb, count(id) from my_table group by tb with no data;
    "#).await.unwrap();

    let schema_two_connection = get_test_connection_full(
        &helper.test_db_name,
        helper.port,
        "ht_two_user",
        "password",
        Some("ht_two"),
    )
    .await;

    schema_two_connection.execute_non_query(r#"
        create table my_table(time timestamptz not null, id int);

        SELECT public.create_hypertable('my_table', public.by_range('time'));
        
        insert into my_table values (now(), 2);

        create materialized view my_view with (timescaledb.continuous) as select public.time_bucket('1 day', time) as tb, count(id) from my_table group by tb with no data;
    "#).await.unwrap();

    helper
        .execute_not_query(
            r#"
    grant usage on schema ht_one to ht_two_user;
    grant select on table ht_one.my_table to ht_two_user;
    "#,
        )
        .await;

    _ = schema_one_connection
        .get_single_result::<i32>("select id from ht_two.my_table;")
        .await
        .unwrap_err();

    let id = schema_two_connection
        .get_single_result::<i32>("select id from ht_two.my_table;")
        .await
        .unwrap();
    assert_eq!(id, 2);
    let id = schema_two_connection
        .get_single_result::<i32>("select id from ht_one.my_table;")
        .await
        .unwrap();
    assert_eq!(id, 1);

    let reader = SchemaReader::new(&schema_one_connection);
    let schema_one_introspection = reader.introspect_database().await.unwrap();

    assert_eq!(
        schema_one_introspection,
        PostgresDatabase {
            schemas: vec![
                PostgresSchema {
                    tables: vec![PostgresTable {
                        name: "my_table".to_string(),
                        columns: vec![
                            PostgresColumn {
                                name: "time".to_string(),
                                ordinal_position: 1,
                                is_nullable: false,
                                data_type: "timestamptz".to_string(),
                                default_value: None,
                                generated: None,
                                comment: None,
                                array_dimensions: 0,
                                data_type_length: None,
                            },
                            PostgresColumn {
                                name: "id".to_string(),
                                ordinal_position: 2,
                                is_nullable: true,
                                data_type: "int4".to_string(),
                                default_value: None,
                                generated: None,
                                comment: None,
                                array_dimensions: 0,
                                data_type_length: None,
                            }
                        ],
                        constraints: vec![],
                        indices: vec![PostgresIndex {
                            name: "my_table_time_idx".to_string(),
                            key_columns: vec![PostgresIndexKeyColumn {
                                name: "\"time\"".to_string(),
                                ordinal_position: 1,
                                direction: Some(PostgresIndexColumnDirection::Descending),
                                nulls_order: Some(PostgresIndexNullsOrder::First),
                            }],
                            index_type: "btree".to_string(),
                            predicate: None,
                            included_columns: vec![],
                            index_constraint_type: PostgresIndexType::Index,
                            storage_parameters: vec![],
                            comment: None,
                            object_id: ObjectId::new(3),
                        }],
                        comment: None,
                        storage_parameters: vec![],
                        table_type: TimescaleHypertable {
                            dimensions: vec![HypertableDimension::Time {
                                column_name: "time".to_string(),
                                time_interval: Interval {
                                    months: 0,
                                    days: 7,
                                    microseconds: 0
                                },
                            }],
                            compression: None,
                            retention: None,
                        },
                        object_id: ObjectId::new(4),
                        depends_on: vec![],
                    }],
                    sequences: vec![],
                    views: vec![PostgresView {
                        name: "my_view".to_string(),
                        definition:
                            r#" SELECT public.time_bucket('1 day'::interval, my_table."time") AS tb,
    count(my_table.id) AS count
   FROM my_table
  GROUP BY (public.time_bucket('1 day'::interval, my_table."time"));"#
                                .into(),
                        columns: vec![
                            PostgresViewColumn {
                                name: "tb".to_string(),
                                ordinal_position: 1,
                            },
                            PostgresViewColumn {
                                name: "count".to_string(),
                                ordinal_position: 2,
                            }
                        ],
                        comment: None,
                        is_materialized: true,
                        view_options: TimescaleContinuousAggregate {
                            refresh: None,
                            compression: None,
                            retention: None
                        },
                        object_id: ObjectId::new(5),
                        depends_on: vec![],
                    }],
                    name: "ht_one".to_string(),
                    object_id: ObjectId::new(1),
                    ..default()
                },
                PostgresSchema {
                    name: "public".to_string(),
                    object_id: ObjectId::new(2),
                    ..default()
                }
            ],
            timescale_support: TimescaleSupport {
                is_enabled: true,
                timescale_toolkit_is_enabled: true,
                ..default()
            },
            ..default()
        }
    )
}
