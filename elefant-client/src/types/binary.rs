use std::error::Error;
use crate::PostgresType;
use crate::protocol::FieldDescription;
use crate::types::{FromSql, ToSql, PostgresNamedType};

impl<'a> FromSql<'a> for Vec<u8> {
    fn from_sql_binary(raw: &'a [u8], _field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(raw.to_vec())
    }

    fn from_sql_text(raw: &'a str, _field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        // PostgreSQL BYTEA text format uses \x prefix for hex encoding
        // Handle both direct \x format and escaped \\x format
        let hex_str = if let Some(stripped) = raw.strip_prefix("\\x") {
            stripped
        } else if raw.starts_with("\"\\\\x") && raw.ends_with("\"") {
            // Handle escaped format in quotes: "\\x48656C6C6F" 
            &raw[4..raw.len()-1]
        } else if let Some(stripped) = raw.strip_prefix("\\\\x") {
            // Handle escaped format: \\x48656C6C6F
            stripped
        } else {
            // For array elements, PostgreSQL might return the raw hex without escapes
            // Let's try direct hex parsing
            let mut result = Vec::with_capacity(raw.len() / 2);
            for chunk in raw.as_bytes().chunks(2) {
                if chunk.len() == 2 {
                    let hex_byte = std::str::from_utf8(chunk)?;
                    if let Ok(byte) = u8::from_str_radix(hex_byte, 16) {
                        result.push(byte);
                    } else {
                        // If it's not valid hex, treat it as raw bytes
                        return Ok(raw.as_bytes().to_vec());
                    }
                } else {
                    // Odd length, treat as raw bytes
                    return Ok(raw.as_bytes().to_vec());
                }
            }
            return Ok(result);
        };

        let mut result = Vec::with_capacity(hex_str.len() / 2);
        
        for chunk in hex_str.as_bytes().chunks(2) {
            if chunk.len() == 2 {
                let hex_byte = std::str::from_utf8(chunk)?;
                let byte = u8::from_str_radix(hex_byte, 16)
                    .map_err(|e| format!("Invalid hex byte '{hex_byte}': {e}"))?;
                result.push(byte);
            }
        }
        Ok(result)
    }

    fn accepts_postgres_type(oid: i32) -> bool {
        oid == PostgresType::BYTEA.oid
    }
}

impl ToSql for Vec<u8> {
    fn to_sql_binary(&self, target_buffer: &mut Vec<u8>) -> Result<(), Box<dyn Error + Sync + Send>> {
        target_buffer.extend_from_slice(self);
        Ok(())
    }
}

impl PostgresNamedType for Vec<u8> {
    const PG_NAME: &'static str = PostgresType::BYTEA.name;
}

impl<'a> FromSql<'a> for &'a [u8] {
    fn from_sql_binary(raw: &'a [u8], _field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(raw)
    }

    fn from_sql_text(raw: &'a str, field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        // We can't return a borrowed slice from parsed hex data since it would need to be owned
        // So for text format, we'll return an error suggesting to use Vec<u8> instead
        Err(format!("Cannot create &[u8] from BYTEA text format '{raw}'. Use Vec<u8> instead for text format parsing. Field: {field:?}").into())
    }

    fn accepts_postgres_type(oid: i32) -> bool {
        oid == PostgresType::BYTEA.oid
    }
}

impl ToSql for &[u8] {
    fn to_sql_binary(&self, target_buffer: &mut Vec<u8>) -> Result<(), Box<dyn Error + Sync + Send>> {
        target_buffer.extend_from_slice(self);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "tokio")]
    mod tokio_connection {
        use crate::test_helpers::get_settings;
        use crate::tokio_connection::new_client;
        use tokio::test;

        #[test]
        async fn test_bytea_vec_u8() {
            let mut client = new_client(get_settings()).await.unwrap();

            // Test BYTEA (Vec<u8>)
            let empty_bytes: Vec<u8> = client.read_single_value("select ''::bytea;", &[]).await.unwrap();
            assert_eq!(empty_bytes, Vec::<u8>::new());

            let test_bytes: Vec<u8> = client.read_single_value("select '\\x48656C6C6F'::bytea;", &[]).await.unwrap();
            assert_eq!(test_bytes, b"Hello".to_vec());

            let binary_data: Vec<u8> = client.read_single_value("select '\\x00010203FF'::bytea;", &[]).await.unwrap();
            assert_eq!(binary_data, vec![0, 1, 2, 3, 255]);

            // Test round-trip for BYTEA (manual test since Vec<u8> doesn't implement Display)
            let test_data = vec![0u8, 255u8, 42u8];
            let round_trip_result: Vec<u8> = client.read_single_value("select $1::bytea;", &[&test_data]).await.unwrap();
            assert_eq!(round_trip_result, test_data);

            let empty_data = Vec::<u8>::new();
            let round_trip_empty: Vec<u8> = client.read_single_value("select $1::bytea;", &[&empty_data]).await.unwrap();
            assert_eq!(round_trip_empty, empty_data);

            let large_data = vec![1u8; 1000];
            let round_trip_large: Vec<u8> = client.read_single_value("select $1::bytea;", &[&large_data]).await.unwrap();
            assert_eq!(round_trip_large, large_data);

            // Test with parameter
            let param_bytes: Vec<u8> = client.read_single_value("select $1::bytea;", &[&vec![72u8, 101u8, 108u8, 108u8, 111u8]]).await.unwrap();
            assert_eq!(param_bytes, b"Hello".to_vec());
        }

        #[test]
        async fn test_bytea_slice() {
            let mut client = new_client(get_settings()).await.unwrap();

            // Test ToSql for &[u8] with parameter binding (this uses binary format internally)
            let test_slice: &[u8] = b"World";
            let received_from_slice: Vec<u8> = client.read_single_value("select $1::bytea;", &[&test_slice]).await.unwrap();
            assert_eq!(received_from_slice, b"World".to_vec());

            // Note: &[u8] FromSql only works with binary format since we can't create borrowed slices 
            // from parsed hex text. For text format queries, use Vec<u8> instead.
        }

        #[test]
        async fn test_bytea_nullable() {
            let mut client = new_client(get_settings()).await.unwrap();

            client.execute_non_query(r#"
                drop table if exists test_bytea_table;
                create table test_bytea_table(data bytea);
                insert into test_bytea_table values ('\x48656C6C6F');
                "#, &[]).await.unwrap();

            let bytea_value: Option<Vec<u8>> = client.read_single_value("select data from test_bytea_table;", &[]).await.unwrap();
            assert_eq!(bytea_value, Some(b"Hello".to_vec()));

            client.execute_non_query("update test_bytea_table set data = null;", &[]).await.unwrap();
            let null_bytea: Option<Vec<u8>> = client.read_single_value("select data from test_bytea_table;", &[]).await.unwrap();
            assert_eq!(null_bytea, None);

            // Test inserting NULL BYTEA via parameter
            client.execute_non_query("delete from test_bytea_table;", &[]).await.unwrap();
            client.execute_non_query("insert into test_bytea_table values ($1);", &[&None::<Vec<u8>>]).await.unwrap();
            let value: Option<Vec<u8>> = client.read_single_value("select data from test_bytea_table;", &[]).await.unwrap();
            assert_eq!(value, None);

            // Test inserting Some(Vec<u8>) via parameter
            client.execute_non_query("delete from test_bytea_table;", &[]).await.unwrap();
            client.execute_non_query("insert into test_bytea_table values ($1);", &[&Some(vec![1u8, 2u8, 3u8])]).await.unwrap();
            let value: Option<Vec<u8>> = client.read_single_value("select data from test_bytea_table;", &[]).await.unwrap();
            assert_eq!(value, Some(vec![1, 2, 3]));

            // Test &[u8] parameter binding (works fine since it uses binary format)
            client.execute_non_query("delete from test_bytea_table;", &[]).await.unwrap();
            let slice_data: &[u8] = b"SliceTest";
            client.execute_non_query("insert into test_bytea_table values ($1);", &[&slice_data]).await.unwrap();
            let value: Option<Vec<u8>> = client.read_single_value("select data from test_bytea_table;", &[]).await.unwrap();
            assert_eq!(value, Some(b"SliceTest".to_vec()));

            // Test Option<&[u8]> parameter binding
            client.execute_non_query("delete from test_bytea_table;", &[]).await.unwrap();
            let some_slice: Option<&[u8]> = Some(b"OptionSlice");
            client.execute_non_query("insert into test_bytea_table values ($1);", &[&some_slice]).await.unwrap();
            let value: Option<Vec<u8>> = client.read_single_value("select data from test_bytea_table;", &[]).await.unwrap();
            assert_eq!(value, Some(b"OptionSlice".to_vec()));

            // Test None for Option<&[u8]>
            client.execute_non_query("delete from test_bytea_table;", &[]).await.unwrap();
            let none_slice: Option<&[u8]> = None;
            client.execute_non_query("insert into test_bytea_table values ($1);", &[&none_slice]).await.unwrap();
            let value: Option<Vec<u8>> = client.read_single_value("select data from test_bytea_table;", &[]).await.unwrap();
            assert_eq!(value, None);

            // Note: &[u8] FromSql only works with binary format since we can't create borrowed slices 
            // from text format hex parsing. For reading from text queries, use Vec<u8> instead.
            // Parameter binding works fine since it uses binary format via ToSql.
        }

        #[test]
        async fn test_bytea_arrays() {
            let mut client = new_client(get_settings()).await.unwrap();

            // Test BYTEA arrays
            client.execute_non_query(r#"
                drop table if exists test_bytea_array_table;
                create table test_bytea_array_table(data bytea[]);
                "#, &[]).await.unwrap();

            client.execute_non_query("insert into test_bytea_array_table values (array['\\x48656C6C6F'::bytea, '\\x576F726C64'::bytea]);", &[]).await.unwrap();
            let bytea_array: Vec<Vec<u8>> = client.read_single_value("select data from test_bytea_array_table;", &[]).await.unwrap();
            assert_eq!(bytea_array, vec![b"Hello".to_vec(), b"World".to_vec()]);

            client.execute_non_query("update test_bytea_array_table set data = array[]::bytea[]", &[]).await.unwrap();
            let empty_bytea_array: Vec<Vec<u8>> = client.read_single_value("select data from test_bytea_array_table;", &[]).await.unwrap();
            assert_eq!(empty_bytea_array, Vec::<Vec<u8>>::new());
        }
    }
}