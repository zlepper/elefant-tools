use std::error::Error;
use crate::PostgresType;
use crate::protocol::FieldDescription;
use crate::types::FromSql;

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
            return Err(format!("Only one-dimensional arrays are supported. Error occurred when parsing field {field:?}").into());
        }


        if !T::accepts_postgres_type(element_oid) {
            return Err(format!("Element type of the array is not supported. Error occurred when parsing field {field:?}").into());
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

        // Parse array elements while respecting quoted boundaries
        let mut element_start = 0;
        let mut in_quotes = false;
        let delimiter_char = typ.array_delimiter;
        let bytes = narrowed.as_bytes();
        
        for (i, &byte) in bytes.iter().enumerate() {
            let ch = byte as char;
            match ch {
                '"' => {
                    in_quotes = !in_quotes;
                }
                c if c == delimiter_char && !in_quotes => {
                    // End of current element
                    if i > element_start {
                        let element = &narrowed[element_start..i];
                        // Remove quotes if present
                        let clean_element = if element.starts_with('"') && element.ends_with('"') && element.len() >= 2 {
                            &element[1..element.len()-1]
                        } else {
                            element
                        };
                        
                        if clean_element == "NULL" {
                            result.push(T::from_null(field)?);
                        } else {
                            result.push(T::from_sql_text(clean_element, field)?);
                        }
                    }
                    element_start = i + 1;
                }
                _ => {}
            }
        }
        
        // Handle the last element
        if element_start < narrowed.len() {
            let element = &narrowed[element_start..];
            // Remove quotes if present
            let clean_element = if element.starts_with('"') && element.ends_with('"') && element.len() >= 2 {
                &element[1..element.len()-1]
            } else {
                element
            };
            
            if clean_element == "NULL" {
                result.push(T::from_null(field)?);
            } else {
                result.push(T::from_sql_text(clean_element, field)?);
            }
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

#[cfg(test)]
mod tests {
    #[cfg(feature = "tokio")]
    mod tokio_connection {
        use crate::test_helpers::get_settings;
        use crate::tokio_connection::new_client;
        use tokio::test;

        #[test]
        async fn test_array_types() {
            let mut client = new_client(get_settings()).await.unwrap();

            client.execute_non_query(r#"
                drop table if exists test_array_table;
                create table test_array_table(value int2[]);
                "#, &[]).await.unwrap();

            let prepared = client.prepare_query("select value from test_array_table;").await.unwrap();

            client.execute_non_query("insert into test_array_table values ('{1,2,3}');", &[]).await.unwrap();

            let mut value: Vec<i16> = client.read_single_value("select value from test_array_table;", &[]).await.unwrap();
            assert_eq!(value, vec![1, 2, 3]);
            value = client.read_single_value(&prepared, &[]).await.unwrap();
            assert_eq!(value, vec![1, 2, 3]);

            client.execute_non_query("update test_array_table set value = '{}'", &[]).await.unwrap();

            value = client.read_single_value("select value from test_array_table;", &[]).await.unwrap();
            assert_eq!(value, Vec::<i16>::new());
            value = client.read_single_value(&prepared, &[]).await.unwrap();
            assert_eq!(value, Vec::<i16>::new());


            client.execute_non_query("update test_array_table set value = '{1,null,3}'", &[]).await.unwrap();

            let mut value: Vec<Option<i16>> = client.read_single_value("select value from test_array_table;", &[]).await.unwrap();
            assert_eq!(value, vec![Some(1), None, Some(3)]);
            value = client.read_single_value(&prepared, &[]).await.unwrap();
            assert_eq!(value, vec![Some(1), None, Some(3)]);

            client.execute_non_query("update test_array_table set value = '{null}'", &[]).await.unwrap();
            let mut value: Vec<Option<i16>> = client.read_single_value("select value from test_array_table;", &[]).await.unwrap();
            assert_eq!(value, vec![None]);
            value = client.read_single_value(&prepared, &[]).await.unwrap();
            assert_eq!(value, vec![None]);
        }
    }
}