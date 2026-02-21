use crate::protocol::FieldDescription;
use crate::types::{FromSqlBase, FromSqlBinary, FromSqlText, ToSql};
use crate::PostgresType;
use std::error::Error;

impl<'a> FromSqlBase<'a> for char {
    fn accepts_postgres_type(oid: i32) -> bool {
        oid == PostgresType::CHAR.oid || oid == PostgresType::BPCHAR.oid
    }
}

impl<'a> FromSqlBinary<'a> for char {
    fn from_sql_binary(
        raw: &'a [u8],
        field: &FieldDescription,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if raw.len() != 1 {
            return Err(format!("Invalid length for char. Expected 1 byte, got {} bytes instead. Error occurred when parsing field {:?}", raw.len(), field).into());
        }

        Ok(raw[0] as char)
    }
}

impl<'a> FromSqlText<'a> for char {
    fn from_sql_text(
        raw: &'a str,
        field: &FieldDescription,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if raw.len() != 1 {
            return Err(format!("Invalid length for char. Expected 1 byte, got {} bytes instead. Error occurred when parsing field {:?}", raw.len(), field).into());
        }

        Ok(raw.chars().next().unwrap())
    }
}

impl ToSql for char {
    fn to_sql_binary(
        &self,
        target_buffer: &mut Vec<u8>,
    ) -> Result<(), Box<dyn Error + Sync + Send>> {
        target_buffer.push(*self as u8);
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
        async fn test_char_type() {
            let mut client = new_client(get_settings()).await.unwrap();

            let c: char = client
                .read_single_value_dual_mode("select 'a'::\"char\"").await;
            assert_eq!(c, 'a');

            let c: char = client
                .read_single_value("select $1::\"char\";", &[&'A']).await;
            assert_eq!(c, 'A');
        }
    }
}
