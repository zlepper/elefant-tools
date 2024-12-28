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

pub trait PostgresNamedType {
    const PG_NAME: &'static str;
}

impl<'a> FromSql<'a> for i16 {
    fn from_sql_binary(
        raw: &'a [u8],
        field: &FieldDescription,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if raw.len() != 2 {
            return Err(format!("Invalid length for i16. Expected 2 bytes, got {} bytes instead. Error occurred when parsing field {:?}", raw.len(), field).into());
        }
        
        Ok(i16::from_be_bytes(raw.try_into().unwrap()))
    }

    fn from_sql_text(
        raw: &'a str,
        _field: &FieldDescription,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(raw.parse()?)
    }

    fn accepts(field: &FieldDescription) -> bool {
        field.data_type_oid == 21
    }
}

impl PostgresNamedType for i16 {
    const PG_NAME: &'static str = "int2";
}

impl ToSql for i16 {
    fn to_sql_binary(&self, target_buffer: &mut Vec<u8>) {
        target_buffer.extend_from_slice(&self.to_be_bytes());
    }
}



#[cfg(test)]
mod tests {

    #[cfg(feature = "tokio")]
    mod tokio_connection {
        use std::fmt::{Debug, Display};
        use crate::postgres_client::{FromSqlOwned, ToSql};
        use crate::test_helpers::get_settings;
        use crate::tokio_connection::{new_client, TokioPostgresClient};
        use tokio::test;
        use crate::postgres_client::data_types::PostgresNamedType;

        struct DataReaderTest {
            client: TokioPostgresClient,
        }

        impl DataReaderTest {
            async fn new() -> Self {
                let client = new_client(get_settings()).await.unwrap();
                Self { client }
            }



            pub async fn test_read_special_cast<T>(&mut self, value: T, cast_to: &str)
            where
                T: FromSqlOwned + Display + Eq + Debug,
            {
                let sql = format!("select '{}'::{};", value, cast_to);

                let received_value: T = self.client.read_single_column_and_row(sql.as_str(), &[]).await;

                assert_eq!(received_value, value);

                let prepared_query = self.client.prepare_query(&sql).await.unwrap();

                let received_value: T = self.client.read_single_column_and_row(&prepared_query, &[]).await;

                assert_eq!(received_value, value);
            }

            pub async fn test_read<T>(&mut self, value: T)
            where
                T: FromSqlOwned + Display + Eq + Debug + PostgresNamedType,
            {
                self.test_read_special_cast(value, T::PG_NAME).await
            }

            pub async fn test_round_trip<T>(&mut self, value: T)
                where T: FromSqlOwned + Display + Eq + Debug + ToSql + PostgresNamedType
            {
                let sql = format!("select t.f::{0} from (select b.f::text from (select $1::{0} as f) as b) as t", T::PG_NAME);

                let received_value: T = self.client.read_single_column_and_row(sql.as_str(), &[&value]).await;

                assert_eq!(received_value, value);
            }
        }
        
        #[test]
        async fn reads_data() {

            let mut helper = DataReaderTest::new().await;

            helper.test_read(1i16).await;
            helper.test_read(i16::MAX).await;
            helper.test_read(i16::MIN).await;
            helper.test_read(-1i16).await;
            helper.test_read(0i16).await;
            helper.test_read_special_cast(1i16, "smallint").await;

            helper.test_round_trip(1i16).await;
            helper.test_round_trip(i16::MAX).await;
            helper.test_round_trip(i16::MIN).await;
            helper.test_round_trip(-1i16).await;
            helper.test_round_trip(0i16).await;
        }
        
    }
}
