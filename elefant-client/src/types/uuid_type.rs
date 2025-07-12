use std::error::Error;
use crate::protocol::FieldDescription;
use crate::types::{FromSql, ToSql};
use crate::types::PostgresType;
use uuid::Uuid;

// PostgreSQL UUID type - 16 bytes in big-endian byte order
impl<'a> FromSql<'a> for Uuid {
    fn from_sql_binary(raw: &'a [u8], field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if raw.len() != 16 {
            return Err(format!(
                "Invalid length for UUID. Expected 16 bytes, got {} bytes instead. Error occurred when parsing field {:?}", 
                raw.len(), field
            ).into());
        }
        
        // UUID crate expects bytes in big-endian order, which matches PostgreSQL's format
        let uuid_bytes: [u8; 16] = raw.try_into().unwrap();
        Ok(Uuid::from_bytes(uuid_bytes))
    }

    fn from_sql_text(raw: &'a str, field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        // PostgreSQL UUID text format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        Uuid::parse_str(raw)
            .map_err(|e| format!(
                "Failed to parse UUID from text '{}': {}. Error occurred when parsing field {:?}", 
                raw, e, field
            ).into())
    }

    fn accepts_postgres_type(oid: i32) -> bool {
        oid == PostgresType::UUID.oid
    }
}

impl ToSql for Uuid {
    fn to_sql_binary(&self, target_buffer: &mut Vec<u8>) -> Result<(), Box<dyn Error + Sync + Send>> {
        // UUID bytes are already in the correct big-endian format
        target_buffer.extend_from_slice(self.as_bytes());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[cfg(feature = "tokio")]
    mod tokio_connection {
        use super::*;
        use crate::test_helpers::get_settings;
        use crate::tokio_connection::new_client;
        use tokio::test;

        #[test]
        async fn test_uuid_type() {
            let mut client = new_client(get_settings()).await.unwrap();

            // Test nil UUID (all zeros)
            let nil_uuid = Uuid::nil();
            let value: Uuid = client.read_single_value("select '00000000-0000-0000-0000-000000000000'::uuid;", &[]).await.unwrap();
            assert_eq!(value, nil_uuid);

            // Test a specific UUID
            let test_uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
            let value: Uuid = client.read_single_value("select '550e8400-e29b-41d4-a716-446655440000'::uuid;", &[]).await.unwrap();
            assert_eq!(value, test_uuid);

            // Test round-trip with parameter binding
            client.execute_non_query("drop table if exists test_uuid_table; create table test_uuid_table(value uuid);", &[]).await.unwrap();
            client.execute_non_query("insert into test_uuid_table values ($1);", &[&test_uuid]).await.unwrap();
            let retrieved: Uuid = client.read_single_value("select value from test_uuid_table;", &[]).await.unwrap();
            assert_eq!(retrieved, test_uuid);

            // Test NULL handling
            let null_value: Option<Uuid> = client.read_single_value("select null::uuid;", &[]).await.unwrap();
            assert_eq!(null_value, None);

            // Test another specific UUID for round-trip
            let another_uuid = Uuid::parse_str("6ba7b810-9dad-11d1-80b4-00c04fd430c8").unwrap();
            client.execute_non_query("insert into test_uuid_table values ($1);", &[&another_uuid]).await.unwrap();
            let retrieved_another: Uuid = client.read_single_value("select value from test_uuid_table order by value desc limit 1;", &[]).await.unwrap();
            assert_eq!(retrieved_another, another_uuid);
        }

        #[test]
        async fn test_uuid_arrays() {
            let mut client = new_client(get_settings()).await.unwrap();

            // Test UUID array
            let uuids = vec![
                Uuid::nil(),
                Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
                Uuid::parse_str("6ba7b810-9dad-11d1-80b4-00c04fd430c8").unwrap()
            ];
            let value: Vec<Uuid> = client.read_single_value(
                "select '{00000000-0000-0000-0000-000000000000,550e8400-e29b-41d4-a716-446655440000,6ba7b810-9dad-11d1-80b4-00c04fd430c8}'::uuid[];", 
                &[]
            ).await.unwrap();
            assert_eq!(value, uuids);

            // Test UUID array with NULLs
            let uuids_with_nulls = vec![
                Some(Uuid::nil()),
                None,
                Some(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap())
            ];
            let value: Vec<Option<Uuid>> = client.read_single_value(
                "select '{00000000-0000-0000-0000-000000000000,null,550e8400-e29b-41d4-a716-446655440000}'::uuid[];", 
                &[]
            ).await.unwrap();
            assert_eq!(value, uuids_with_nulls);
        }
    }
}