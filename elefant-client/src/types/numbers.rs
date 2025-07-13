use crate::protocol::FieldDescription;
use crate::types::{FromSql, PostgresNamedType, ToSql};
use crate::PostgresType;
use std::error::Error;

macro_rules! impl_number {
    ($typ: ty, $standard_type: expr) => {
        impl<'a> FromSql<'a> for $typ {
            fn from_sql_binary(
                raw: &'a [u8],
                field: &FieldDescription,
            ) -> Result<Self, Box<dyn Error + Sync + Send>> {

                const BYTE_SIZE: usize = std::mem::size_of::<$typ>();

                if raw.len() != BYTE_SIZE {
                    return Err(format!("Invalid length for {}. Expected {} bytes, got {} bytes instead. Error occurred when parsing field {:?}", std::any::type_name::<$typ>(), BYTE_SIZE, raw.len(), field).into());
                }

                Ok(<$typ>::from_be_bytes(raw.try_into().unwrap()))
            }

            fn from_sql_text(
                raw: &'a str,
                _field: &FieldDescription,
            ) -> Result<Self, Box<dyn Error + Sync + Send>> {
                Ok(raw.parse()?)
            }

            fn accepts_postgres_type(oid: i32) -> bool {
                oid == $standard_type.oid
            }
        }

        impl PostgresNamedType for $typ {
            const PG_NAME: &'static str = $standard_type.name;
        }

        impl ToSql for $typ {
            fn to_sql_binary(&self, target_buffer: &mut Vec<u8>) -> Result<(), Box<dyn Error + Sync + Send>> {
                target_buffer.extend_from_slice(&self.to_be_bytes());
                Ok(())
            }
        }
    };
}

impl_number!(i16, PostgresType::INT2);
impl_number!(i32, PostgresType::INT4);
impl_number!(i64, PostgresType::INT8);
impl_number!(f32, PostgresType::FLOAT4);
impl_number!(f64, PostgresType::FLOAT8);

#[cfg(test)]
mod tests {
    #[cfg(feature = "tokio")]
    mod tokio_connection {
        use crate::test_helpers::get_settings;
        use crate::tokio_connection::{new_client, TokioPostgresClient};
        use crate::types::*;
        use std::fmt::{Debug, Display};
        use tokio::test;

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
                T: FromSqlOwned + Display + PartialEq + Debug,
            {
                let sql = format!("select '{value}'::{cast_to}; ");

                let received_value: T = self
                    .client
                    .read_single_column_and_row_exactly(sql.as_str(), &[])
                    .await;

                assert_eq!(received_value, value);

                let prepared_query = self.client.prepare_query(&sql).await.unwrap();

                let received_value: T = self
                    .client
                    .read_single_column_and_row_exactly(&prepared_query, &[])
                    .await;

                assert_eq!(received_value, value);
            }

            pub async fn test_read<T>(&mut self, value: T)
            where
                T: FromSqlOwned + Display + PartialEq + Debug + PostgresNamedType,
            {
                self.test_read_special_cast(value, T::PG_NAME).await
            }

            pub async fn test_round_trip<T>(&mut self, value: T)
            where
                T: FromSqlOwned + Display + PartialEq + Debug + ToSql + PostgresNamedType,
            {
                let sql = format!(
                    "select t.f::{0} from (select b.f::text from (select $1::{0} as f) as b) as t ",
                    T::PG_NAME
                );

                let received_value: T = self
                    .client
                    .read_single_column_and_row_exactly(sql.as_str(), &[&value])
                    .await;

                assert_eq!(received_value, value);
            }
        }

        #[test]
        async fn test_integer_types() {
            let mut helper = DataReaderTest::new().await;

            macro_rules! test_integer_values {
                ($typ: ty) => {
                    helper.test_read::<$typ>(1).await;
                    helper.test_read(<$typ>::MAX).await;
                    helper.test_read(<$typ>::MIN).await;
                    helper.test_read::<$typ>(-1).await;
                    helper.test_read::<$typ>(0).await;

                    helper.test_round_trip::<$typ>(1).await;
                    helper.test_round_trip::<$typ>(<$typ>::MAX).await;
                    helper.test_round_trip::<$typ>(<$typ>::MIN).await;
                    helper.test_round_trip::<$typ>(-1).await;
                    helper.test_round_trip::<$typ>(0).await;
                };
            }

            test_integer_values!(i16);
            test_integer_values!(i32);
            test_integer_values!(i64);

            helper.test_read_special_cast(1i16, "smallint").await;
        }

        #[test]
        async fn test_float_types() {
            let mut helper = DataReaderTest::new().await;

            macro_rules! test_float_values {
                ($typ: ty) => {
                    helper.test_read::<$typ>(1.0).await;
                    helper.test_read::<$typ>(-1.0).await;
                    helper.test_read::<$typ>(0.5).await;
                    helper.test_read::<$typ>(-0.5).await;
                    helper.test_read::<$typ>(0.0).await;
                    helper.test_read::<$typ>(<$typ>::INFINITY).await;
                    helper.test_read::<$typ>(<$typ>::NEG_INFINITY).await;

                    helper.test_round_trip::<$typ>(1.0).await;
                    helper.test_round_trip::<$typ>(-1.0).await;
                    helper.test_round_trip::<$typ>(0.5).await;
                    helper.test_round_trip::<$typ>(-0.5).await;
                    helper.test_round_trip::<$typ>(0.0).await;
                    helper.test_round_trip::<$typ>(<$typ>::INFINITY).await;
                    helper.test_round_trip::<$typ>(<$typ>::NEG_INFINITY).await;

                    let should_be_nan: $typ = helper
                        .client
                        .read_single_value(&format!("select 'NaN'::{} ", <$typ>::PG_NAME), &[])
                        .await
                        .unwrap();
                    assert!(should_be_nan.is_nan());

                    let should_be_nan: $typ = helper
                        .client
                        .read_single_value(
                            &format!("select $1::{} ", <$typ>::PG_NAME),
                            &[&<$typ>::NAN],
                        )
                        .await
                        .unwrap();
                    assert!(should_be_nan.is_nan());
                };
            }

            test_float_values!(f32);
            test_float_values!(f64);
        }
    }
}
