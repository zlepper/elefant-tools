use crate::protocol::FieldDescription;
use crate::types::{FromSql, ToSql};
use crate::types::PostgresType;
use serde_json::Value;
use std::error::Error;

// PostgreSQL JSON type - stored as text and parsed/serialized as JSON
impl<'a> FromSql<'a> for Value {
    fn from_sql_binary(
        raw: &'a [u8],
        field: &FieldDescription,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        // JSON in binary format is stored as UTF-8 text - parse directly from bytes
        serde_json::from_slice(raw).map_err(|e| {
            format!(
                "Failed to parse JSON from binary data: {}. Error occurred when parsing field {:?}",
                e, field
            )
            .into()
        })
    }

    fn from_sql_text(
        raw: &'a str,
        field: &FieldDescription,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        // JSON text format is direct JSON string
        serde_json::from_str(raw).map_err(|e| {
            format!(
                "Failed to parse JSON from text '{}': {}. Error occurred when parsing field {:?}",
                raw, e, field
            )
            .into()
        })
    }

    fn accepts_postgres_type(oid: i32) -> bool {
        oid == PostgresType::JSON.oid
    }
}

impl ToSql for Value {
    fn to_sql_binary(
        &self,
        target_buffer: &mut Vec<u8>,
    ) -> Result<(), Box<dyn Error + Sync + Send>> {
        // Serialize JSON directly to the target buffer as UTF-8 bytes
        serde_json::to_writer(target_buffer, self)
            .map_err(|e| format!("Failed to serialize JSON to binary: {}", e).into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[cfg(feature = "tokio")]
    mod tokio_connection {
        use super::*;
        use crate::test_helpers::get_settings;
        use crate::tokio_connection::new_client;
        use tokio::test;

        #[test]
        async fn test_json_type() {
            let mut client = new_client(get_settings()).await.unwrap();

            // Test empty object
            let empty_object = json!({});
            let value: Value = client
                .read_single_value("select '{}'::json;", &[])
                .await
                .unwrap();
            assert_eq!(value, empty_object);

            // Test empty array
            let empty_array = json!([]);
            let value: Value = client
                .read_single_value("select '[]'::json;", &[])
                .await
                .unwrap();
            assert_eq!(value, empty_array);

            // Test complex JSON object
            let complex_json = json!({
                "name": "test",
                "age": 30,
                "active": true,
                "tags": ["rust", "postgresql"],
                "metadata": {
                    "created": "2024-01-15",
                    "version": 1
                }
            });
            let value: Value = client.read_single_value(
                r#"select '{"name":"test","age":30,"active":true,"tags":["rust","postgresql"],"metadata":{"created":"2024-01-15","version":1}}'::json;"#, 
                &[]
            ).await.unwrap();
            assert_eq!(value, complex_json);

            // Test round-trip with parameter binding
            client.execute_non_query("drop table if exists test_json_table; create table test_json_table(value json);", &[]).await.unwrap();
            client
                .execute_non_query("insert into test_json_table values ($1);", &[&complex_json])
                .await
                .unwrap();
            let retrieved: Value = client
                .read_single_value("select value from test_json_table;", &[])
                .await
                .unwrap();
            assert_eq!(retrieved, complex_json);

            // Test NULL handling
            let null_value: Option<Value> = client
                .read_single_value("select null::json;", &[])
                .await
                .unwrap();
            assert_eq!(null_value, None);
        }

        #[test]
        async fn test_json_multiple_values() {
            let mut client = new_client(get_settings()).await.unwrap();

            // Test multiple different JSON types in a table for comprehensive testing
            client.execute_non_query("drop table if exists test_json_multi; create table test_json_multi(id int, value json);", &[]).await.unwrap();

            // Insert various JSON types
            let test_values = vec![
                (1, json!({})),
                (2, json!([1, 2, 3])),
                (3, json!({"test": "value", "number": 42})),
                (4, json!(null)),
                (5, json!("simple string")),
                (6, json!(true)),
                (7, json!(123.456)),
            ];

            for (id, json_val) in &test_values {
                client
                    .execute_non_query(
                        "insert into test_json_multi values ($1, $2);",
                        &[id, json_val],
                    )
                    .await
                    .unwrap();
            }

            // Retrieve and verify each value
            for (expected_id, expected_json) in &test_values {
                let retrieved: Value = client
                    .read_single_value(
                        "select value from test_json_multi where id = $1;",
                        &[expected_id],
                    )
                    .await
                    .unwrap();
                assert_eq!(&retrieved, expected_json, "Failed for ID {}", expected_id);
            }
        }

        #[test]
        async fn test_json_escaping_roundtrip() {
            let mut client = new_client(get_settings()).await.unwrap();

            // Test JSON values that require escaping at the JSON level
            client.execute_non_query("drop table if exists test_json_escaping; create table test_json_escaping(id int, value json);", &[]).await.unwrap();

            let escaping_test_cases = vec![
                (1, json!({"quote": "He said \"hello\" to me"})),
                (2, json!({"backslash": "C:\\Users\\name\\file.txt"})),
                (3, json!({"newline": "line1\nline2\nline3"})),
                (4, json!({"tab": "col1\tcol2\tcol3"})),
                (5, json!({"unicode": "emoji: 😊 and math: ∑"})),
                (
                    6,
                    json!({"mixed": "Quote: \"text\", Path: C:\\temp\\file\nNext line"}),
                ),
                (
                    7,
                    json!({"nested_object": {"inner_quote": "nested \"value\" here"}}),
                ),
                (8, json!(["array", "with \"quotes\"", "and\nnewlines"])),
                (9, json!({"control_chars": "\u{0001}\u{0002}\u{0003}"})),
                (10, json!({"empty_and_quotes": "", "quotes": "\"\""})),
            ];

            // Insert all test cases using parameter binding (avoids SQL escaping)
            for (id, json_val) in &escaping_test_cases {
                client
                    .execute_non_query(
                        "insert into test_json_escaping values ($1, $2);",
                        &[id, json_val],
                    )
                    .await
                    .unwrap();
            }

            // Retrieve and verify each value maintains proper JSON escaping
            for (expected_id, expected_json) in &escaping_test_cases {
                let retrieved: Value = client
                    .read_single_value(
                        "select value from test_json_escaping where id = $1;",
                        &[expected_id],
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    &retrieved, expected_json,
                    "JSON escaping failed for test case ID {}",
                    expected_id
                );
            }

            // Additional test: Verify that the JSON is properly serialized/deserialized by checking a specific complex case
            let complex_case = json!({
                "message": "Error: \"file not found\" at C:\\temp\\data.json",
                "details": {
                    "path": "C:\\Users\\john\\Documents\\file with spaces.txt",
                    "error_code": 404,
                    "trace": "line1\nline2\nline3"
                },
                "tags": ["error", "\"critical\"", "needs\tescaping"]
            });

            client
                .execute_non_query(
                    "insert into test_json_escaping values ($1, $2);",
                    &[&99, &complex_case],
                )
                .await
                .unwrap();
            let retrieved_complex: Value = client
                .read_single_value(
                    "select value from test_json_escaping where id = $1;",
                    &[&99],
                )
                .await
                .unwrap();
            assert_eq!(
                retrieved_complex, complex_case,
                "Complex JSON escaping case failed"
            );
        }

        #[test]
        async fn test_json_error_handling() {
            let mut client = new_client(get_settings()).await.unwrap();

            // Test that PostgreSQL validates JSON syntax - invalid JSON should cause database error
            let result = client
                .read_single_value::<Value>("select '{invalid json'::json;", &[])
                .await;
            assert!(
                result.is_err(),
                "Expected PostgreSQL to reject invalid JSON syntax"
            );
        }
    }
}
