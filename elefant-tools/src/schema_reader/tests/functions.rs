use elefant_test_macros::pg_test;
use ordered_float::NotNan;
use crate::{default, FunctionKind, Parallel, PostgresDatabase, PostgresFunction, PostgresSchema, TimescaleSupport, Volatility};
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
async fn test_functions(helper: &TestHelper) {
    tests::test_introspection(helper,
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
                                           .into(),
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
                                           .into(),
                                       configuration: None,
                                       arguments: "value text".to_string(),
                                       result: Some("TABLE(id integer, name text)".to_string()),
                                       ..default()
                                   },
                               ],
                               ..default()
                           }],
                           timescale_support: TimescaleSupport::from_test_helper(helper),
                           ..default()
                       },
    ).await;
}
