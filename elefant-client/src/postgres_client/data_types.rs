use crate::protocol::FieldDescription;
use std::error::Error;
use crate::ElefantClientError;

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

    fn from_null(field: &FieldDescription) -> Result<Self, ElefantClientError> {
        Err(ElefantClientError::UnexpectedNullValue {
            postgres_field: field.clone(),
        })
    }
}
/// A trait for types which can be created from a Postgres value without borrowing any data.
///
/// This is primarily useful for trait bounds on functions.
pub trait FromSqlOwned: for<'a> FromSql<'a> {}

impl<T> FromSqlOwned for T where T: for<'a> FromSql<'a> {}

pub trait ToSql {
    fn to_sql_binary(&self, target_buffer: &mut Vec<u8>) -> Result<(), Box<dyn Error + Sync + Send>>;
    fn is_null(&self) -> bool {
        false
    }
}

pub trait PostgresNamedType {
    const PG_NAME: &'static str;
}

macro_rules! impl_number {
    ($typ: ty, $pg_name: expr, $oid: expr) => {
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

            fn accepts(field: &FieldDescription) -> bool {
                field.data_type_oid == $oid
            }
        }

        impl PostgresNamedType for $typ {
            const PG_NAME: &'static str = $pg_name;
        }

        impl ToSql for $typ {
            fn to_sql_binary(&self, target_buffer: &mut Vec<u8>) -> Result<(), Box<dyn Error + Sync + Send>> {
                target_buffer.extend_from_slice(&self.to_be_bytes());
                Ok(())
            }
        }
    };
}

impl_number!(i16, "int2", 21);
impl_number!(i32, "int4", 23);
impl_number!(i64, "int8", 20);
impl_number!(f32, "float4", 700);
impl_number!(f64, "float8", 701);


impl<'a, T> FromSql<'a> for Option<T>
where T : FromSql<'a>
{
    fn from_sql_binary(raw: &'a [u8], field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        T::from_sql_binary(raw, field).map(Some)
    }

    fn from_sql_text(raw: &'a str, field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        T::from_sql_text(raw, field).map(Some)
    }

    fn accepts(field: &FieldDescription) -> bool {
        T::accepts(field)
    }

    fn from_null(_field: &FieldDescription) -> Result<Self, ElefantClientError> {
        Ok(None)
    }
}

impl<T> ToSql for Option<T>
    where T: ToSql
{
    fn to_sql_binary(&self, target_buffer: &mut Vec<u8>) -> Result<(), Box<dyn Error + Sync + Send>> {
        match self {
            Some(value) => value.to_sql_binary(target_buffer),
            None => Err("Cannot convert None to binary representation. This case should never happens and should be considered a bug in the ElefantClient library. Please create an issue on GitHub.".into())
        }
    }

    fn is_null(&self) -> bool {
        self.is_none()
    }
}

impl<'a> FromSql<'a> for char {
    fn from_sql_binary(raw: &'a [u8], field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if raw.len() != 1 {
            return Err(format!("Invalid length for char. Expected 1 byte, got {} bytes instead. Error occurred when parsing field {:?}", raw.len(), field).into());
        }
        
        Ok(raw[0] as char)
    }

    fn from_sql_text(raw: &'a str, field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if raw.len() != 1 {
            return Err(format!("Invalid length for char. Expected 1 byte, got {} bytes instead. Error occurred when parsing field {:?}", raw.len(), field).into());
        }

        Ok(raw.chars().next().unwrap())
    }

    fn accepts(field: &FieldDescription) -> bool {
        field.data_type_oid == 18
    }
}

impl ToSql for char {
    fn to_sql_binary(&self, target_buffer: &mut Vec<u8>) -> Result<(), Box<dyn Error + Sync + Send>> {
        target_buffer.push(*self as u8);
        Ok(())
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
        use crate::ElefantClientError;
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
                T: FromSqlOwned + Display + PartialEq + Debug,
            {
                let sql = format!("select '{}'::{};", value, cast_to);

                let received_value: T = self.client.read_single_column_and_row_exactly(sql.as_str(), &[]).await;

                assert_eq!(received_value, value);

                let prepared_query = self.client.prepare_query(&sql).await.unwrap();

                let received_value: T = self.client.read_single_column_and_row_exactly(&prepared_query, &[]).await;

                assert_eq!(received_value, value);
            }

            pub async fn test_read<T>(&mut self, value: T)
            where
                T: FromSqlOwned + Display + PartialEq + Debug + PostgresNamedType,
            {
                self.test_read_special_cast(value, T::PG_NAME).await
            }

            pub async fn test_round_trip<T>(&mut self, value: T)
                where T: FromSqlOwned + Display + PartialEq + Debug + ToSql + PostgresNamedType
            {
                let sql = format!("select t.f::{0} from (select b.f::text from (select $1::{0} as f) as b) as t", T::PG_NAME);

                let received_value: T = self.client.read_single_column_and_row_exactly(sql.as_str(), &[&value]).await;

                assert_eq!(received_value, value);
            }

            pub async fn exec(&mut self, sql: &str) {
                self.client.execute_non_query(sql, &[]).await.unwrap();
            }
        }
        
        #[test]
        async fn reads_data() {

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

                    let should_be_nan: $typ = helper.client.read_single_value(&format!("select 'NaN'::{};", <$typ>::PG_NAME), &[]).await.unwrap();
                    assert!(should_be_nan.is_nan());

                    let should_be_nan: $typ = helper.client.read_single_value(&format!("select $1::{};", <$typ>::PG_NAME), &[&<$typ>::NAN]).await.unwrap();
                    assert!(should_be_nan.is_nan());
                };
            }

            test_float_values!(f32);
            test_float_values!(f64);

            let c: char = helper.client.read_single_value("select 'a'::\"char\";", &[]).await.unwrap();
            assert_eq!(c, 'a');

            let c: char = helper.client.read_single_value("select $1::\"char\";", &[&'A']).await.unwrap();
            assert_eq!(c, 'A');
        }

        #[test]
        async fn nullable_types() {
            let mut helper = DataReaderTest::new().await;
            helper.exec(r#"
                    drop table if exists test_table;
                    create table test_table(value int2);
                    insert into test_table values (42);
                    "#).await;


            let value: Option<i16> = helper.client.read_single_value("select value from test_table;", &[]).await.unwrap();

            assert_eq!(value, Some(42));

            helper.exec("delete from test_table; insert into test_table values (null);").await;

            let value: Option<i16> = helper.client.read_single_value("select value from test_table;", &[]).await.unwrap();

            assert_eq!(value, None);

            let result = helper.client.read_single_value::<i16>("select value from test_table;", &[]).await;

            if let Err(ElefantClientError::UnexpectedNullValue {postgres_field}) = result {
                assert_eq!(postgres_field.column_attribute_number, 1);
            } else {
                panic!("Expected UnexpectedNullValue error, got {:?}", result);
            }

            helper.exec("delete from test_table;").await;

            helper.client.execute_non_query("insert into test_table values ($1);", &[&None::<i16>]).await.unwrap();
            let value: Option<i16> = helper.client.read_single_value("select value from test_table;", &[]).await.unwrap();
            assert_eq!(value, None);

            helper.exec("delete from test_table;").await;

            helper.client.execute_non_query("insert into test_table values ($1);", &[&Some(42i16)]).await.unwrap();
            let value: Option<i16> = helper.client.read_single_value("select value from test_table;", &[]).await.unwrap();
            assert_eq!(value, Some(42));
        }
        
    }
}
