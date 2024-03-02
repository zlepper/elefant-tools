use elefant_test_macros::pg_test;
use ordered_float::NotNan;
use crate::{default, FunctionKind, Parallel, PostgresColumn, PostgresDatabase, PostgresFunction, PostgresSchema, PostgresTable, PostgresTrigger, PostgresTriggerEvent, PostgresTriggerLevel, PostgresTriggerTiming, TimescaleSupport, Volatility};
use crate::schema_reader::tests;
use crate::test_helpers::TestHelper;

#[pg_test(arg(postgres = 12))]
#[pg_test(arg(postgres = 13))]
#[pg_test(arg(postgres = 14))]
#[pg_test(arg(postgres = 15))]
#[pg_test(arg(postgres = 16))]
#[pg_test(arg(timescale_db = 15))]
#[pg_test(arg(timescale_db = 16))]
async fn triggers(helper: &TestHelper) {
    tests::test_introspection(helper, r#"
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
                        sql_body: "begin return new; end;".into(),
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
                    },
                ],
                ..default()
            }
        ],
        timescale_support: TimescaleSupport::from_test_helper(helper),
        ..default()
    }).await;
}
