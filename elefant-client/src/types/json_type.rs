use crate::protocol::FieldDescription;
use crate::types::{FromSql, ToSql};
use crate::types::PostgresType;
use serde_json::Value;
use std::error::Error;

/// Wrapper type for PostgreSQL JSON values
/// Use this when binding parameters to JSON columns specifically.
/// Note: `serde_json::Value` now defaults to JSONB format for best performance.
#[derive(Debug, Clone, PartialEq)]
pub struct Json(pub Value);

/// Wrapper type for PostgreSQL JSONB values  
/// Use this when you want to be explicit about JSONB format.
/// Note: `serde_json::Value` now defaults to JSONB format for best performance.
#[derive(Debug, Clone, PartialEq)]
pub struct Jsonb(pub Value);

// PostgreSQL JSON type - stored as text and parsed/serialized as JSON
impl<'a> FromSql<'a> for Value {
    fn from_sql_binary(
        raw: &'a [u8],
        field: &FieldDescription,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if field.data_type_oid == PostgresType::JSONB.oid {
            // JSONB binary format: version byte (0x01) + UTF-8 JSON text
            if raw.is_empty() {
                return Err("JSONB data cannot be empty".into());
            }
            
            let version = raw[0];
            if version != 1 {
                return Err(format!("Unsupported JSONB version number: {version}").into());
            }
            
            let json_text = &raw[1..];
            serde_json::from_slice(json_text).map_err(|e| {
                format!(
                    "Failed to parse JSONB from binary data: {e}. Error occurred when parsing field {field:?}"
                )
                .into()
            })
        } else {
            // JSON in binary format is stored as UTF-8 text - parse directly from bytes
            serde_json::from_slice(raw).map_err(|e| {
                format!(
                    "Failed to parse JSON from binary data: {e}. Error occurred when parsing field {field:?}"
                )
                .into()
            })
        }
    }

    fn from_sql_text(
        raw: &'a str,
        field: &FieldDescription,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        // Both JSON and JSONB text format is direct JSON string
        serde_json::from_str(raw).map_err(|e| {
            format!(
                "Failed to parse JSON/JSONB from text '{raw}': {e}. Error occurred when parsing field {field:?}"
            )
            .into()
        })
    }

    fn accepts_postgres_type(oid: i32) -> bool {
        oid == PostgresType::JSON.oid || oid == PostgresType::JSONB.oid
    }
}

// Default implementation uses JSONB format (recommended best practice)
impl ToSql for Value {
    fn to_sql_binary(
        &self,
        target_buffer: &mut Vec<u8>,
    ) -> Result<(), Box<dyn Error + Sync + Send>> {
        // Default to JSONB format (version byte + JSON text) as it's more efficient
        // Use explicit Json wrapper for JSON columns if needed
        target_buffer.push(1); // JSONB version byte
        serde_json::to_writer(target_buffer, self)
            .map_err(|e| format!("Failed to serialize JSON/JSONB to binary: {e}").into())
    }
}

// Specific implementation for JSON columns
impl ToSql for Json {
    fn to_sql_binary(
        &self,
        target_buffer: &mut Vec<u8>,
    ) -> Result<(), Box<dyn Error + Sync + Send>> {
        // JSON columns expect plain UTF-8 JSON text
        serde_json::to_writer(target_buffer, &self.0)
            .map_err(|e| format!("Failed to serialize JSON to binary: {e}").into())
    }
}

// Specific implementation for JSONB columns
impl ToSql for Jsonb {
    fn to_sql_binary(
        &self,
        target_buffer: &mut Vec<u8>,
    ) -> Result<(), Box<dyn Error + Sync + Send>> {
        // JSONB columns expect version byte (0x01) + UTF-8 JSON text
        target_buffer.push(1); // JSONB version byte
        serde_json::to_writer(target_buffer, &self.0)
            .map_err(|e| format!("Failed to serialize JSONB to binary: {e}").into())
    }
}

// FromSql implementations for wrapper types
impl<'a> FromSql<'a> for Json {
    fn from_sql_binary(
        raw: &'a [u8],
        field: &FieldDescription,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let value = Value::from_sql_binary(raw, field)?;
        Ok(Json(value))
    }

    fn from_sql_text(
        raw: &'a str,
        field: &FieldDescription,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let value = Value::from_sql_text(raw, field)?;
        Ok(Json(value))
    }

    fn accepts_postgres_type(oid: i32) -> bool {
        oid == PostgresType::JSON.oid
    }
}

impl<'a> FromSql<'a> for Jsonb {
    fn from_sql_binary(
        raw: &'a [u8],
        field: &FieldDescription,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let value = Value::from_sql_binary(raw, field)?;
        Ok(Jsonb(value))
    }

    fn from_sql_text(
        raw: &'a str,
        field: &FieldDescription,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let value = Value::from_sql_text(raw, field)?;
        Ok(Jsonb(value))
    }

    fn accepts_postgres_type(oid: i32) -> bool {
        oid == PostgresType::JSONB.oid
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
            let json_param = Json(complex_json.clone());
            client
                .execute_non_query("insert into test_json_table values ($1);", &[&json_param])
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
                let json_param = Json(json_val.clone());
                client
                    .execute_non_query(
                        "insert into test_json_multi values ($1, $2);",
                        &[id, &json_param],
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
                assert_eq!(&retrieved, expected_json, "Failed for ID {expected_id}");
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
                let json_param = Json(json_val.clone());
                client
                    .execute_non_query(
                        "insert into test_json_escaping values ($1, $2);",
                        &[id, &json_param],
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
                    "JSON escaping failed for test case ID {expected_id}"
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

            let complex_json_param = Json(complex_case.clone());
            client
                .execute_non_query(
                    "insert into test_json_escaping values ($1, $2);",
                    &[&99, &complex_json_param],
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

        #[test]
        async fn test_jsonb_type() {
            let mut client = new_client(get_settings()).await.unwrap();

            // Test empty object
            let empty_object = json!({});
            let value: Value = client
                .read_single_value("select '{}'::jsonb;", &[])
                .await
                .unwrap();
            assert_eq!(value, empty_object);

            // Test empty array
            let empty_array = json!([]);
            let value: Value = client
                .read_single_value("select '[]'::jsonb;", &[])
                .await
                .unwrap();
            assert_eq!(value, empty_array);

            // Test complex JSONB object
            let complex_jsonb = json!({
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
                r#"select '{"name":"test","age":30,"active":true,"tags":["rust","postgresql"],"metadata":{"created":"2024-01-15","version":1}}'::jsonb;"#, 
                &[]
            ).await.unwrap();
            assert_eq!(value, complex_jsonb);

            // Test round-trip with parameter binding
            client.execute_non_query("drop table if exists test_jsonb_table; create table test_jsonb_table(value jsonb);", &[]).await.unwrap();
            let jsonb_param = Jsonb(complex_jsonb.clone());
            client
                .execute_non_query("insert into test_jsonb_table values ($1);", &[&jsonb_param])
                .await
                .unwrap();
            let retrieved: Value = client
                .read_single_value("select value from test_jsonb_table;", &[])
                .await
                .unwrap();
            assert_eq!(retrieved, complex_jsonb);

            // Test NULL handling
            let null_value: Option<Value> = client
                .read_single_value("select null::jsonb;", &[])
                .await
                .unwrap();
            assert_eq!(null_value, None);
        }

        #[test]
        async fn test_jsonb_vs_json_differences() {
            let mut client = new_client(get_settings()).await.unwrap();

            // Test that JSONB normalizes data (removes whitespace, reorders keys)
            client.execute_non_query("drop table if exists test_jsonb_vs_json; create table test_jsonb_vs_json(id int, json_val json, jsonb_val jsonb);", &[]).await.unwrap();

            // Insert the same JSON with extra whitespace and different key order
            let json_with_spaces = r#"{ "z_last": 3 , "a_first":   1,  "middle": 2 }"#;
            let json_value: Value = serde_json::from_str(json_with_spaces).unwrap();
            
            // Use wrapper types for proper parameter binding
            let json_param = Json(json_value.clone());
            let jsonb_param = Jsonb(json_value.clone());
            
            client
                .execute_non_query(
                    "insert into test_jsonb_vs_json values (1, $1, $2);",
                    &[&json_param, &jsonb_param],
                )
                .await
                .unwrap();

            // Retrieve both values
            let json_val: Value = client
                .read_single_value("select json_val from test_jsonb_vs_json where id = 1;", &[])
                .await
                .unwrap();
            let jsonb_val: Value = client
                .read_single_value("select jsonb_val from test_jsonb_vs_json where id = 1;", &[])
                .await
                .unwrap();

            // Both should have the same logical content
            let expected = json!({"z_last": 3, "a_first": 1, "middle": 2});
            assert_eq!(json_val, expected);
            assert_eq!(jsonb_val, expected);
        }

        #[test]
        async fn test_jsonb_array_support() {
            let mut client = new_client(get_settings()).await.unwrap();

            // Test JSONB value that contains an array (not PostgreSQL array of JSONB)
            client.execute_non_query("drop table if exists test_jsonb_arrays; create table test_jsonb_arrays(value jsonb);", &[]).await.unwrap();

            let json_array_value = json!([
                {"type": "user", "id": 1},
                {"type": "admin", "id": 2},
                [1, 2, 3],
                "simple string",
                null
            ]);

            let jsonb_param = Jsonb(json_array_value.clone());
            client
                .execute_non_query("insert into test_jsonb_arrays values ($1);", &[&jsonb_param])
                .await
                .unwrap();

            let retrieved_array: Value = client
                .read_single_value("select value from test_jsonb_arrays;", &[])
                .await
                .unwrap();
            assert_eq!(retrieved_array, json_array_value);
        }

        #[test]
        async fn test_jsonb_error_handling() {
            let mut client = new_client(get_settings()).await.unwrap();

            // Test that PostgreSQL validates JSONB syntax - invalid JSON should cause database error
            let result = client
                .read_single_value::<Value>("select '{invalid json'::jsonb;", &[])
                .await;
            assert!(
                result.is_err(),
                "Expected PostgreSQL to reject invalid JSONB syntax"
            );

            // Test JSONB with binary format version handling
            // This test verifies our implementation handles the version byte correctly
            client.execute_non_query("drop table if exists test_jsonb_version; create table test_jsonb_version(value jsonb);", &[]).await.unwrap();

            let test_json = json!({"version_test": true, "data": [1, 2, 3]});
            let jsonb_param = Jsonb(test_json.clone());
            client
                .execute_non_query("insert into test_jsonb_version values ($1);", &[&jsonb_param])
                .await
                .unwrap();

            let retrieved: Value = client
                .read_single_value("select value from test_jsonb_version;", &[])
                .await
                .unwrap();
            assert_eq!(retrieved, test_json);
        }

        #[test]
        async fn test_jsonb_parameter_binding_types() {
            let mut client = new_client(get_settings()).await.unwrap();

            // Test that parameter binding works correctly for both JSON and JSONB columns
            client.execute_non_query("drop table if exists test_json_jsonb_params; create table test_json_jsonb_params(id int, json_col json, jsonb_col jsonb);", &[]).await.unwrap();

            let test_value = json!({
                "test": "parameter binding",
                "numbers": [1, 2, 3],
                "nested": {
                    "key": "value"
                }
            });

            // Use wrapper types for proper parameter binding
            let json_param = Json(test_value.clone());
            let jsonb_param = Jsonb(test_value.clone());

            client
                .execute_non_query(
                    "insert into test_json_jsonb_params values ($1, $2, $3);",
                    &[&1, &json_param, &jsonb_param],
                )
                .await
                .unwrap();

            // Retrieve both and verify they work correctly
            let json_result: Value = client
                .read_single_value("select json_col from test_json_jsonb_params where id = 1;", &[])
                .await
                .unwrap();
            let jsonb_result: Value = client
                .read_single_value("select jsonb_col from test_json_jsonb_params where id = 1;", &[])
                .await
                .unwrap();

            assert_eq!(json_result, test_value);
            assert_eq!(jsonb_result, test_value);
        }

        #[test]
        async fn test_jsonb_escaping_roundtrip() {
            let mut client = new_client(get_settings()).await.unwrap();

            // Test JSONB values that require escaping at the JSON level - same cases as JSON test
            client.execute_non_query("drop table if exists test_jsonb_escaping; create table test_jsonb_escaping(id int, value jsonb);", &[]).await.unwrap();

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
                let jsonb_param = Jsonb(json_val.clone());
                client
                    .execute_non_query(
                        "insert into test_jsonb_escaping values ($1, $2);",
                        &[id, &jsonb_param],
                    )
                    .await
                    .unwrap();
            }

            // Retrieve and verify each value maintains proper JSON escaping
            for (expected_id, expected_json) in &escaping_test_cases {
                let retrieved: Value = client
                    .read_single_value(
                        "select value from test_jsonb_escaping where id = $1;",
                        &[expected_id],
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    &retrieved, expected_json,
                    "JSONB escaping failed for test case ID {expected_id}"
                );
            }

            // Additional test: Verify that the JSONB is properly serialized/deserialized by checking a specific complex case
            let complex_case = json!({
                "message": "Error: \"file not found\" at C:\\temp\\data.json",
                "details": {
                    "path": "C:\\Users\\john\\Documents\\file with spaces.txt",
                    "error_code": 404,
                    "trace": "line1\nline2\nline3"
                },
                "tags": ["error", "\"critical\"", "needs\tescaping"]
            });

            let complex_jsonb_param = Jsonb(complex_case.clone());
            client
                .execute_non_query(
                    "insert into test_jsonb_escaping values ($1, $2);",
                    &[&99, &complex_jsonb_param],
                )
                .await
                .unwrap();
            let retrieved_complex: Value = client
                .read_single_value(
                    "select value from test_jsonb_escaping where id = $1;",
                    &[&99],
                )
                .await
                .unwrap();
            assert_eq!(
                retrieved_complex, complex_case,
                "Complex JSONB escaping case failed"
            );
        }

        #[test]
        async fn test_default_value_behavior() {
            let mut client = new_client(get_settings()).await.unwrap();

            // Test that serde_json::Value now defaults to JSONB format
            client.execute_non_query("drop table if exists test_default_behavior; create table test_default_behavior(id int, jsonb_col jsonb);", &[]).await.unwrap();

            let test_value = json!({
                "default_test": true,
                "message": "serde_json::Value should default to JSONB format",
                "data": [1, 2, 3]
            });

            // Use raw serde_json::Value - should work with JSONB columns now
            client
                .execute_non_query(
                    "insert into test_default_behavior values ($1, $2);",
                    &[&1, &test_value],
                )
                .await
                .unwrap();

            let retrieved: Value = client
                .read_single_value("select jsonb_col from test_default_behavior where id = 1;", &[])
                .await
                .unwrap();
            assert_eq!(retrieved, test_value);
        }
    }
}
