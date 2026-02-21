use crate::postgres_client::{PostgresClient, QueryResultSet};
use crate::protocol::async_io::ElefantAsyncReadWrite;
use crate::{ElefantClientError, FromSql, FromSqlBinary, FromSqlText, PostgresConnectionSettings, Statement, ToSql};

pub(crate) fn get_settings() -> PostgresConnectionSettings {
    PostgresConnectionSettings {
        password: "passw0rd".to_string(),
        port: 5415,
        ..Default::default()
    }
}

#[cfg(feature = "tokio")]
pub(crate) async fn get_tokio_test_client() -> crate::tokio_connection::TokioPostgresClient {
    crate::tokio_connection::new_client(get_settings())
        .await
        .unwrap()
}

impl<C: ElefantAsyncReadWrite> PostgresClient<C> {
    /// Read a single value using binary mode (with prepared statements)
    /// Test helper method - enforces at compile time that T supports binary format parsing
    pub async fn read_single_value<'postgres_client, T>(
        &'postgres_client mut self,
        query: &(impl Statement + ?Sized),
        parameters: &[&(dyn ToSql)],
    ) -> Result<T, ElefantClientError>
    where
        T: FromSqlBinary<'postgres_client>,
    {
        let mut query_result = self.query(query, parameters).await?;
        let result_set = query_result.next_result_set().await?;

        match result_set {
            QueryResultSet::QueryProcessingComplete => Err(ElefantClientError::NoResultsReturned),
            QueryResultSet::RowDescriptionReceived(mut row_reader) => {
                match row_reader.next_row().await? {
                    None => Err(ElefantClientError::NoResultsReturned),
                    Some(row) => {
                        let value: T = row.get_binary(0)?;
                        Ok(value)
                    }
                }
            }
        }
    }

    /// Read a single value using simple query mode (text format, no parameters)
    /// Test helper method - enforces at compile time that T supports text format parsing
    pub async fn read_single_value_simple<'postgres_client, T>(
        &'postgres_client mut self,
        query: &str,
    ) -> Result<T, ElefantClientError>
    where
        T: FromSqlText<'postgres_client>,
    {
        let mut query_result = self.query_simple(query).await?;
        let result_set = query_result.next_result_set().await?;

        match result_set {
            QueryResultSet::QueryProcessingComplete => Err(ElefantClientError::NoResultsReturned),
            QueryResultSet::RowDescriptionReceived(mut row_reader) => {
                match row_reader.next_row().await? {
                    None => Err(ElefantClientError::NoResultsReturned),
                    Some(row) => {
                        let value: T = row.get_text(0)?;
                        Ok(value)
                    }
                }
            }
        }
    }

    pub async fn read_single_column_and_row_exactly<'a, S, T>(
        &'a mut self,
        sql: &S,
        parameters: &[&(dyn ToSql)],
    ) -> T
    where
        T: FromSql<'a>,
        S: Statement + ?Sized,
    {
        let mut query_result = self.query(sql, parameters).await.unwrap();

        let result_set = query_result.next_result_set().await.unwrap();

        let value: T;
        match result_set {
            QueryResultSet::QueryProcessingComplete => {
                panic!("Exact 1 result set was expected. Got 0");
            }
            QueryResultSet::RowDescriptionReceived(mut row_reader) => {
                match row_reader.next_row().await.unwrap() {
                    None => {
                        panic!("Exactly 1 row was expected. Got 0");
                    }
                    Some(row) => {
                        value = row.get::<T>(0).unwrap();
                    }
                }

                if row_reader.next_row().await.unwrap().is_some() {
                    panic!("Exactly 1 row was expected. Got more than 1");
                }
            }
        }

        match query_result.next_result_set().await.unwrap() {
            QueryResultSet::QueryProcessingComplete => {}
            QueryResultSet::RowDescriptionReceived(_) => {
                panic!("Exactly 1 result set was expected. Got more than 1");
            }
        }

        value
    }
}
