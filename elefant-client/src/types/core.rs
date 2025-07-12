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

impl ToSql for String {
    fn to_sql_binary(&self, target_buffer: &mut Vec<u8>) -> Result<(), Box<dyn Error + Sync + Send>> {
        target_buffer.extend_from_slice(self.as_bytes());
        Ok(())
    }
}

impl<'a> FromSql<'a> for Vec<u8> {
    fn from_sql_binary(raw: &'a [u8], _field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(raw.to_vec())
    }

    fn from_sql_text(raw: &'a str, _field: &FieldDescription) -> Result<Self, Box<dyn Error + Sync + Send>> {
        // PostgreSQL BYTEA text format uses \x prefix for hex encoding
        // Handle both direct \x format and escaped \\x format
        let hex_str = if raw.starts_with("\\x") {
            &raw[2..]
        } else if raw.starts_with("\"\\\\x") && raw.ends_with("\"") {
            // Handle escaped format in quotes: "\\x48656C6C6F" 
            &raw[4..raw.len()-1]
        } else if raw.starts_with("\\\\x") {
            // Handle escaped format: \\x48656C6C6F
            &raw[3..]
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
                    .map_err(|e| format!("Invalid hex byte '{}': {}", hex_byte, e))?;
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
        Err(format!("Cannot create &[u8] from BYTEA text format '{}'. Use Vec<u8> instead for text format parsing. Field: {:?}", raw, field).into())
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

            // Test BYTEA (Vec<u8>)
            let empty_bytes: Vec<u8> = helper.client.read_single_value("select ''::bytea;", &[]).await.unwrap();
            assert_eq!(empty_bytes, Vec::<u8>::new());

            let test_bytes: Vec<u8> = helper.client.read_single_value("select '\\x48656C6C6F'::bytea;", &[]).await.unwrap();
            assert_eq!(test_bytes, b"Hello".to_vec());

            let binary_data: Vec<u8> = helper.client.read_single_value("select '\\x00010203FF'::bytea;", &[]).await.unwrap();
            assert_eq!(binary_data, vec![0, 1, 2, 3, 255]);

            // Test round-trip for BYTEA (manual test since Vec<u8> doesn't implement Display)
            let test_data = vec![0u8, 255u8, 42u8];
            let round_trip_result: Vec<u8> = helper.client.read_single_value("select $1::bytea;", &[&test_data]).await.unwrap();
            assert_eq!(round_trip_result, test_data);

            let empty_data = Vec::<u8>::new();
            let round_trip_empty: Vec<u8> = helper.client.read_single_value("select $1::bytea;", &[&empty_data]).await.unwrap();
            assert_eq!(round_trip_empty, empty_data);

            let large_data = vec![1u8; 1000];
            let round_trip_large: Vec<u8> = helper.client.read_single_value("select $1::bytea;", &[&large_data]).await.unwrap();
            assert_eq!(round_trip_large, large_data);

            // Test with parameter
            let param_bytes: Vec<u8> = helper.client.read_single_value("select $1::bytea;", &[&vec![72u8, 101u8, 108u8, 108u8, 111u8]]).await.unwrap();
            assert_eq!(param_bytes, b"Hello".to_vec());

            // Test ToSql for &[u8] with parameter binding (this uses binary format internally)
            let test_slice: &[u8] = b"World";
            let received_from_slice: Vec<u8> = helper.client.read_single_value("select $1::bytea;", &[&test_slice]).await.unwrap();
            assert_eq!(received_from_slice, b"World".to_vec());

            // Note: &[u8] FromSql only works with binary format since we can't create borrowed slices 
            // from parsed hex text. For text format queries, use Vec<u8> instead.
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

            // Test nullable BYTEA
            helper.exec(r#"
                    drop table if exists test_bytea_table;
                    create table test_bytea_table(data bytea);
                    insert into test_bytea_table values ('\x48656C6C6F');
                    "#).await;

            let bytea_value: Option<Vec<u8>> = helper.client.read_single_value("select data from test_bytea_table;", &[]).await.unwrap();
            assert_eq!(bytea_value, Some(b"Hello".to_vec()));

            helper.exec("update test_bytea_table set data = null;").await;
            let null_bytea: Option<Vec<u8>> = helper.client.read_single_value("select data from test_bytea_table;", &[]).await.unwrap();
            assert_eq!(null_bytea, None);

            // Test inserting NULL BYTEA via parameter
            helper.exec("delete from test_bytea_table;").await;
            helper.client.execute_non_query("insert into test_bytea_table values ($1);", &[&None::<Vec<u8>>]).await.unwrap();
            let value: Option<Vec<u8>> = helper.client.read_single_value("select data from test_bytea_table;", &[]).await.unwrap();
            assert_eq!(value, None);

            // Test inserting Some(Vec<u8>) via parameter
            helper.exec("delete from test_bytea_table;").await;
            helper.client.execute_non_query("insert into test_bytea_table values ($1);", &[&Some(vec![1u8, 2u8, 3u8])]).await.unwrap();
            let value: Option<Vec<u8>> = helper.client.read_single_value("select data from test_bytea_table;", &[]).await.unwrap();
            assert_eq!(value, Some(vec![1, 2, 3]));

            // Test &[u8] parameter binding (works fine since it uses binary format)
            helper.exec("delete from test_bytea_table;").await;
            let slice_data: &[u8] = b"SliceTest";
            helper.client.execute_non_query("insert into test_bytea_table values ($1);", &[&slice_data]).await.unwrap();
            let value: Option<Vec<u8>> = helper.client.read_single_value("select data from test_bytea_table;", &[]).await.unwrap();
            assert_eq!(value, Some(b"SliceTest".to_vec()));

            // Test Option<&[u8]> parameter binding
            helper.exec("delete from test_bytea_table;").await;
            let some_slice: Option<&[u8]> = Some(b"OptionSlice");
            helper.client.execute_non_query("insert into test_bytea_table values ($1);", &[&some_slice]).await.unwrap();
            let value: Option<Vec<u8>> = helper.client.read_single_value("select data from test_bytea_table;", &[]).await.unwrap();
            assert_eq!(value, Some(b"OptionSlice".to_vec()));

            // Test None for Option<&[u8]>
            helper.exec("delete from test_bytea_table;").await;
            let none_slice: Option<&[u8]> = None;
            helper.client.execute_non_query("insert into test_bytea_table values ($1);", &[&none_slice]).await.unwrap();
            let value: Option<Vec<u8>> = helper.client.read_single_value("select data from test_bytea_table;", &[]).await.unwrap();
            assert_eq!(value, None);

            // Note: &[u8] FromSql only works with binary format since we can't create borrowed slices 
            // from text format hex parsing. For reading from text queries, use Vec<u8> instead.
            // Parameter binding works fine since it uses binary format via ToSql.
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

            // Test BYTEA arrays
            helper.exec(r#"
                    drop table if exists test_bytea_array_table;
                    create table test_bytea_array_table(data bytea[]);
                    "#).await;

            helper.exec("insert into test_bytea_array_table values (array['\\x48656C6C6F'::bytea, '\\x576F726C64'::bytea]);").await;
            let bytea_array: Vec<Vec<u8>> = helper.client.read_single_value("select data from test_bytea_array_table;", &[]).await.unwrap();
            assert_eq!(bytea_array, vec![b"Hello".to_vec(), b"World".to_vec()]);

            helper.exec("update test_bytea_array_table set data = array[]::bytea[]").await;
            let empty_bytea_array: Vec<Vec<u8>> = helper.client.read_single_value("select data from test_bytea_array_table;", &[]).await.unwrap();
            assert_eq!(empty_bytea_array, Vec::<Vec<u8>>::new());
        }
    }
}