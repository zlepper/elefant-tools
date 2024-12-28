use crate::protocol::FieldDescription;
use std::error::Error;

pub trait FromSql<'a>: Sized {
    fn from_sql_binary(
        raw: &'a [u8],
        field: &FieldDescription,
    ) -> Result<Self, Box<dyn Error + Sync + Send>>;

    fn from_sql_text(
        raw: &'a str,
        field: &FieldDescription,
    ) -> Result<Self, Box<dyn Error + Sync + Send>>;

    fn accepts(field: &FieldDescription) -> bool;
}
/// A trait for types which can be created from a Postgres value without borrowing any data.
///
/// This is primarily useful for trait bounds on functions.
pub trait FromSqlOwned: for<'a> FromSql<'a> {}

impl<T> FromSqlOwned for T where T: for<'a> FromSql<'a> {}

pub trait ToSql {
    fn to_sql_binary(&self, target_buffer: &mut Vec<u8>);
}

impl<'a> FromSql<'a> for i16 {
    fn from_sql_binary(
        raw: &'a [u8],
        field: &FieldDescription,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if raw.len() != 2 {
            return Err(format!("Invalid length for i16. Expected 2 bytes, got {} bytes instead. Error occurred when parsing field {:?}", raw.len(), field).into());
        }
        
        Ok(i16::from_be_bytes([raw[0], raw[1]]))
    }

    fn from_sql_text(
        raw: &'a str,
        field: &FieldDescription,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(raw.parse()?)
    }

    fn accepts(field: &FieldDescription) -> bool {
        field.name.as_str() == "int2"
    }
}

#[cfg(test)]
mod tests {

    #[cfg(feature = "tokio")]
    mod tokio_connection {
        use std::fmt::{Debug, Display};
        use crate::postgres_client::FromSqlOwned;
        use crate::test_helpers::get_settings;
        use crate::tokio_connection::{new_client, TokioPostgresClient};
        use tokio::test;

        struct DataReaderTest {
            client: TokioPostgresClient,
        }

        impl DataReaderTest {
            async fn new() -> Self {
                let client = new_client(get_settings()).await.unwrap();
                Self { client }
            }



            pub async fn test_read<T>(&mut self, value: T, cast_to: &str)
            where
                T: FromSqlOwned + Display + Eq + Debug,
            {
                let sql = format!("select {}::{};", value, cast_to);

                let received_value: T = self.client.read_single_column_and_row(sql.as_str()).await;

                assert_eq!(received_value, value);
                
                let prepared_query = self.client.prepare_query(&sql).await.unwrap();
                
                let received_value: T = self.client.read_single_column_and_row(&prepared_query).await;
                
                assert_eq!(received_value, value);
            }
        }
        
        #[test]
        async fn reads_data() {

            let mut helper = DataReaderTest::new().await;

            helper.test_read(1i16, "int2").await;
            helper.test_read(1i16, "smallint").await;
            helper.test_read(i16::MAX, "int2").await;
        }
        
    }
}
