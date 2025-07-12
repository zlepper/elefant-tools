use std::error::Error;
use crate::PostgresType;
use crate::protocol::FieldDescription;
use crate::types::{FromSql, ToSql};

impl<'a> FromSql<'a> for &'a str {
    fn from_sql_binary(raw: &'a [u8], _field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(std::str::from_utf8(raw)?)
    }

    fn from_sql_text(raw: &'a str, _field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(raw)
    }

    fn accepts_postgres_type(oid: i32) -> bool {
        oid == PostgresType::TEXT.oid
    }
}

impl<'a> FromSql<'a> for String {
    fn from_sql_binary(raw: &'a [u8], _field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(std::str::from_utf8(raw)?.to_string())
    }

    fn from_sql_text(raw: &'a str, _field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(raw.to_string())
    }

    fn accepts_postgres_type(oid: i32) -> bool {
        oid == PostgresType::TEXT.oid
    }
}

impl ToSql for String {
    fn to_sql_binary(&self, target_buffer: &mut Vec<u8>) -> Result<(), Box<dyn Error + Sync + Send>> {
        target_buffer.extend_from_slice(self.as_bytes());
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
        async fn test_text_types() {
            let mut client = new_client(get_settings()).await.unwrap();

            let s: &str = client.read_single_value("select 'hello'::text;", &[]).await.unwrap();
            assert_eq!(s, "hello");

            let s: String = client.read_single_value("select 'hello'::text;", &[]).await.unwrap();
            assert_eq!(s, "hello");
        }
    }
}