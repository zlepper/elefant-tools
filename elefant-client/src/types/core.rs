use std::error::Error;
use crate::{ElefantClientError, PostgresType};
use crate::protocol::FieldDescription;
use crate::types::{FromSql, ToSql, PostgresNamedType};

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

impl<'a> FromSql<'a> for bool {
    fn from_sql_binary(raw: &'a [u8], field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if raw.len() != 1 {
            return Err(format!("Invalid length for boolean. Expected 1 byte, got {} bytes instead. Error occurred when parsing field {:?}", raw.len(), field).into());
        }

        Ok(raw[0] == 1)
    }

    fn from_sql_text(raw: &'a str, field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        match raw {
            "t" => Ok(true),
            "f" => Ok(false),
            _ => Err(format!("Invalid value for boolean: {}. Error occurred when parsing field {:?}", raw, field).into())
        }
    }

    fn accepts_postgres_type(oid: i32) -> bool {
        oid == PostgresType::BOOL.oid
    }
}

impl ToSql for bool {
    fn to_sql_binary(&self, target_buffer: &mut Vec<u8>) -> Result<(), Box<dyn Error + Sync + Send>> {
        target_buffer.push(if *self { 1 } else { 0 });
        Ok(())
    }
}

impl<'a, T> FromSql<'a> for Option<T>
where T : FromSql<'a>
{
    fn from_sql_binary(raw: &'a [u8], field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        T::from_sql_binary(raw, field).map(Some)
    }

    fn from_sql_text(raw: &'a str, field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        T::from_sql_text(raw, field).map(Some)
    }

    fn accepts_postgres_type(oid: i32) -> bool {
        T::accepts_postgres_type(oid)
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

    fn accepts_postgres_type(oid: i32) -> bool {
        oid == PostgresType::CHAR.oid
    }
}

impl ToSql for char {
    fn to_sql_binary(&self, target_buffer: &mut Vec<u8>) -> Result<(), Box<dyn Error + Sync + Send>> {
        target_buffer.push(*self as u8);
        Ok(())
    }
}

impl<'a, T> FromSql<'a> for Vec<T>
where T: FromSql<'a>
{
    fn from_sql_binary(raw: &'a [u8], field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {

        if raw.len() < 12 {
            return Err(format!("Invalid length for array. Expected at least 12 bytes, got {} bytes instead. Error occurred when parsing field {:?}", raw.len(), field).into());
        }
        let dimensions = i32::from_be_bytes(raw[0..4].try_into().unwrap());

        let has_null_bit_map = i32::from_be_bytes(raw[4..8].try_into().unwrap()) == 1;
        let element_oid = i32::from_be_bytes(raw[8..12].try_into().unwrap());

        if dimensions == 0 {
            return Ok(Vec::new());
        }

        if raw.len() < 20 {
            return Err(format!("Invalid length for non-empty array. Expected at least 20 bytes, got {} bytes instead. Error occurred when parsing field {:?}", raw.len(), field).into());
        }

        let size_of_first_dimension = i32::from_be_bytes(raw[12..16].try_into().unwrap());
        // let start_index_of_first_dimension = i32::from_be_bytes(raw[16..20].try_into().unwrap());
        let raw_data = &raw[20..];

        if dimensions != 1 {
            return Err(format!("Only one-dimensional arrays are supported. Error occurred when parsing field {:?}", field).into());
        }


        if !T::accepts_postgres_type(element_oid) {
            return Err(format!("Element type of the array is not supported. Error occurred when parsing field {:?}", field).into());
        }

        let mut result: Vec<T> = Vec::with_capacity(size_of_first_dimension as usize);

        let mut cursor = 0;
        for _ in 0..size_of_first_dimension {
            let element_size = i32::from_be_bytes(raw_data[cursor..cursor + 4].try_into().unwrap());
            cursor += 4;
            if has_null_bit_map && element_size == -1 {
                result.push(T::from_null(field)?);
            } else {
                let element_raw = &raw_data[cursor..cursor + element_size as usize];
                cursor += element_size as usize;
                result.push(T::from_sql_binary(element_raw, field)?);
            }
        }

        Ok(result)
    }

    fn from_sql_text(raw: &'a str, field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let typ = PostgresType::get_by_oid(field.data_type_oid).ok_or_else(|| format!("Unknown type oid: {}", field.data_type_oid))?;

        let mut result = Vec::new();

        let narrowed = &raw[1..raw.len() - 1];

        if narrowed.is_empty() {
            return Ok(result);
        }

        let items = narrowed.split(typ.array_delimiter);

        for item in items {

            if item == "NULL" {
                result.push(T::from_null(field)?);
                continue;
            }

            result.push(T::from_sql_text(item, field)?);
        }

        Ok(result)
    }

    fn accepts_postgres_type(oid: i32) -> bool {
        match PostgresType::get_by_oid(oid) {
            None => false,
            Some(t) => {
                if !t.is_array {
                    return false;
                }
                
                match t.element {
                    None => false,
                    Some(element_type) => T::accepts_postgres_type(element_type.oid)
                }
            }
        }
    }
}

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


#[cfg(test)]
mod tests {

    #[cfg(feature = "tokio")]
    mod tokio_connection {
        use std::fmt::{Debug, Display};
        use crate::test_helpers::get_settings;
        use crate::tokio_connection::{new_client, TokioPostgresClient};
        use tokio::test;
        use crate::ElefantClientError;
        use crate::types::*;

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


            let s: &str = helper.client.read_single_value("select 'hello'::text;", &[]).await.unwrap();
            assert_eq!(s, "hello");

            let s: String = helper.client.read_single_value("select 'hello'::text;", &[]).await.unwrap();
            assert_eq!(s, "hello");

            let b: bool = helper.client.read_single_value("select 't'::bool;", &[]).await.unwrap();
            assert_eq!(b, true);

            let b: bool = helper.client.read_single_value("select 'f'::bool;", &[]).await.unwrap();
            assert_eq!(b, false);

            let b: bool = helper.client.read_single_value("select $1::bool;", &[&true]).await.unwrap();
            assert_eq!(b, true);

            let b: bool = helper.client.read_single_value("select $1::bool;", &[&false]).await.unwrap();
            assert_eq!(b, false);
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

        #[test]
        async fn array_types() {
            let mut helper = DataReaderTest::new().await;

            helper.exec(r#"
                    drop table if exists test_array_table;
                    create table test_array_table(value int2[]);
                    "#).await;

            let prepared = helper.client.prepare_query("select value from test_array_table;").await.unwrap();

            helper.exec("insert into test_array_table values ('{1,2,3}');").await;

            let mut value: Vec<i16> = helper.client.read_single_value("select value from test_array_table;", &[]).await.unwrap();
            assert_eq!(value, vec![1, 2, 3]);
            value = helper.client.read_single_value(&prepared, &[]).await.unwrap();
            assert_eq!(value, vec![1, 2, 3]);

            helper.exec("update test_array_table set value = '{}'").await;

            value = helper.client.read_single_value("select value from test_array_table;", &[]).await.unwrap();
            assert_eq!(value, Vec::<i16>::new());
            value = helper.client.read_single_value(&prepared, &[]).await.unwrap();
            assert_eq!(value, Vec::<i16>::new());


            helper.exec("update test_array_table set value = '{1,null,3}'").await;

            let mut value: Vec<Option<i16>> = helper.client.read_single_value("select value from test_array_table;", &[]).await.unwrap();
            assert_eq!(value, vec![Some(1), None, Some(3)]);
            value = helper.client.read_single_value(&prepared, &[]).await.unwrap();
            assert_eq!(value, vec![Some(1), None, Some(3)]);

            helper.exec("update test_array_table set value = '{null}'").await;
            let mut value: Vec<Option<i16>> = helper.client.read_single_value("select value from test_array_table;", &[]).await.unwrap();
            assert_eq!(value, vec![None]);
            value = helper.client.read_single_value(&prepared, &[]).await.unwrap();
            assert_eq!(value, vec![None]);
        }
    }
}