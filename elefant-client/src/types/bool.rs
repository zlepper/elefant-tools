use crate::protocol::FieldDescription;
use crate::types::{FromSqlBase, FromSqlBinary, FromSqlText, ToSql};
use crate::PostgresType;
use std::error::Error;

impl<'a> FromSqlBase<'a> for bool {
    fn accepts_postgres_type(oid: i32) -> bool {
        oid == PostgresType::BOOL.oid
    }
}

impl<'a> FromSqlBinary<'a> for bool {
    fn from_sql_binary(
        raw: &'a [u8],
        field: &FieldDescription,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if raw.len() != 1 {
            return Err(format!("Invalid length for boolean. Expected 1 byte, got {} bytes instead. Error occurred when parsing field {:?}", raw.len(), field).into());
        }

        Ok(raw[0] == 1)
    }
}

impl<'a> FromSqlText<'a> for bool {
    fn from_sql_text(
        raw: &'a str,
        field: &FieldDescription,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        match raw {
            "t" => Ok(true),
            "f" => Ok(false),
            _ => Err(format!(
                "Invalid value for boolean: {raw}. Error occurred when parsing field {field:?}"
            )
            .into()),
        }
    }
}

impl ToSql for bool {
    fn to_sql_binary(
        &self,
        target_buffer: &mut Vec<u8>,
    ) -> Result<(), Box<dyn Error + Sync + Send>> {
        target_buffer.push(if *self { 1 } else { 0 });
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
        async fn test_bool_type() {
            let mut client = new_client(get_settings()).await.unwrap();

            let b: bool = client.read_single_value("select 't'::bool;", &[]).await;
            assert!(b);

            let b: bool = client.read_single_value("select 'f'::bool;", &[]).await;
            assert!(!b);

            let b: bool = client.read_single_value("select $1::bool;", &[&true]).await;
            assert!(b);

            let b: bool = client
                .read_single_value("select $1::bool;", &[&false])
                .await;
            assert!(!b);
        }

        #[test]
        async fn test_bool_dual_mode() {
            let mut client = new_client(get_settings()).await.unwrap();

            let value: bool = client.read_single_value_dual_mode("select 't'::bool").await;
            assert!(value);

            let value: bool = client
                .read_single_value_dual_mode("select false::bool")
                .await;
            assert!(!value);

            let value: bool = client
                .read_single_value_dual_mode("select true::bool")
                .await;
            assert!(value);

            let value: bool = client.read_single_value_dual_mode("select 'f'::bool").await;
            assert!(!value);
        }
    }
}
