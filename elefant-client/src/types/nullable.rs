use crate::protocol::FieldDescription;
use crate::types::{FromSqlBase, FromSqlBinary, FromSqlText, ToSql};
use crate::ElefantClientError;
use std::error::Error;

impl<'a, T> FromSqlBase<'a> for Option<T>
where
    T: FromSqlBase<'a>,
{
    fn accepts_postgres_type(oid: i32) -> bool {
        T::accepts_postgres_type(oid)
    }

    fn from_null(_field: &FieldDescription) -> Result<Self, ElefantClientError> {
        Ok(None)
    }
}

impl<'a, T> FromSqlBinary<'a> for Option<T>
where
    T: FromSqlBinary<'a>,
{
    fn from_sql_binary(
        raw: &'a [u8],
        field: &FieldDescription,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        T::from_sql_binary(raw, field).map(Some)
    }
}

impl<'a, T> FromSqlText<'a> for Option<T>
where
    T: FromSqlText<'a>,
{
    fn from_sql_text(
        raw: &'a str,
        field: &FieldDescription,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        T::from_sql_text(raw, field).map(Some)
    }
}

impl<T> ToSql for Option<T>
where
    T: ToSql,
{
    fn to_sql_binary(
        &self,
        target_buffer: &mut Vec<u8>,
    ) -> Result<(), Box<dyn Error + Sync + Send>> {
        match self {
            Some(value) => value.to_sql_binary(target_buffer),
            None => Err("Cannot convert None to binary representation. This case should never happens and should be considered a bug in the ElefantClient library. Please create an issue on GitHub.".into())
        }
    }

    fn is_null(&self) -> bool {
        self.is_none()
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "tokio")]
    mod tokio_connection {
        use crate::test_helpers::get_settings;
        use crate::tokio_connection::new_client;
        use crate::ElefantClientError;
        use tokio::test;

        #[test]
        async fn test_nullable_types() {
            let mut client = new_client(get_settings()).await.unwrap();

            client
                .execute_non_query_simple(
                    r#"
                drop table if exists test_table;
                create table test_table(value int2);
                insert into test_table values (42);
                "#,
                )
                .await
                .unwrap();

            let value: Option<i16> = client
                .read_single_value("select value from test_table;", &[])
                .await;
            assert_eq!(value, Some(42));

            client
                .execute_non_query_simple(
                    "delete from test_table; insert into test_table values (null);",
                )
                .await
                .unwrap();

            let value: Option<i16> = client
                .read_single_value("select value from test_table;", &[])
                .await;
            assert_eq!(value, None);

            let result = client
                .try_read_single_value::<i16>("select value from test_table;", &[])
                .await;

            if let Err(ElefantClientError::UnexpectedNullValue { postgres_field }) = result {
                assert_eq!(postgres_field.column_attribute_number, 1);
            } else {
                panic!("Expected UnexpectedNullValue error, got {result:?}");
            }

            client
                .execute_non_query("delete from test_table;", &[])
                .await
                .unwrap();

            client
                .execute_non_query("insert into test_table values ($1);", &[&None::<i16>])
                .await
                .unwrap();
            let value: Option<i16> = client
                .read_single_value("select value from test_table;", &[])
                .await;
            assert_eq!(value, None);

            client
                .execute_non_query("delete from test_table;", &[])
                .await
                .unwrap();

            client
                .execute_non_query("insert into test_table values ($1);", &[&Some(42i16)])
                .await
                .unwrap();
            let value: Option<i16> = client
                .read_single_value("select value from test_table;", &[])
                .await;
            assert_eq!(value, Some(42));
        }
    }
}
