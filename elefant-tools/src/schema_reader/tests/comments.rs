use elefant_test_macros::pg_test;
use ordered_float::NotNan;
use crate::{default, FunctionKind, Parallel, PostgresCheckConstraint, PostgresColumn, PostgresConstraint, PostgresDatabase, PostgresFunction, PostgresIndex, PostgresIndexColumnDirection, PostgresIndexKeyColumn, PostgresIndexNullsOrder, PostgresIndexType, PostgresSchema, PostgresSequence, PostgresTable, PostgresUniqueConstraint, PostgresView, PostgresViewColumn, TimescaleSupport, Volatility};
use crate::schema_reader::tests;
use crate::test_helpers::TestHelper;
use crate::test_helpers;

#[pg_test(arg(postgres = 12))]
#[pg_test(arg(postgres = 13))]
#[pg_test(arg(postgres = 14))]
#[pg_test(arg(postgres = 15))]
#[pg_test(arg(postgres = 16))]
#[pg_test(arg(timescale_db = 15))]
#[pg_test(arg(timescale_db = 16))]
async fn comments_on_stuff(helper: &TestHelper) {
    tests::test_introspection(helper, r#"
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
                                check_clause: "((value > 0))".into(),
                                comment: Some("This is a constraint".to_string()),
                                ..default()
                            }),
                            PostgresConstraint::Unique(PostgresUniqueConstraint {
                                name: "my_table_another_value_key".to_string(),
                                unique_index_name: "my_table_another_value_key".to_string(),
                                comment: Some("This is a unique constraint".to_string()),
                                ..default()
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
                                index_constraint_type: PostgresIndexType::Unique {
                                    nulls_distinct: true,
                                },
                                comment: Some("This is an index".to_string()),
                                ..default()
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
                            .into(),
                        configuration: None,
                        arguments: "".to_string(),
                        result: Some("integer".to_string()),
                        comment: Some("This is a function".to_string()),
                        ..default()
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
                            .into(),
                        configuration: None,
                        arguments: "a integer, b integer".to_string(),
                        result: Some("integer".to_string()),
                        comment: Some("This is another function".to_string()),
                        ..default()
                    },
                ],
                views: vec![
                    PostgresView {
                        name: "my_view".to_string(),
                        definition: "SELECT 1 AS value;".into(),
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
        timescale_support: TimescaleSupport::from_test_helper(helper),
        ..default()
    }).await;
}
