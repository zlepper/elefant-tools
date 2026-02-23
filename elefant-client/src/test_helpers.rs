use crate::pool::ConnectionFactory;
use crate::postgres_client::{PostgresClient, QueryResultSet};
use crate::{
    ElefantClientError, FromSql, FromSqlBinary, FromSqlOwned, FromSqlText,
    PostgresConnectionSettings, Statement, ToSql,
};
use std::fmt::Debug;

pub(crate) fn get_settings() -> PostgresConnectionSettings {
    PostgresConnectionSettings {
        password: "passw0rd".to_string(),
        port: 5415,
        ..Default::default()
    }
}

#[cfg(feature = "tokio")]
pub(crate) async fn get_tokio_test_client() -> crate::tokio_connection::TokioPoolableClient {
    crate::tokio_connection::new_client(get_settings())
        .await
        .unwrap()
}

impl<F: ConnectionFactory> PostgresClient<F> {
    /// Read a single value using binary mode (with prepared statements), returning Result
    /// Test helper method - enforces at compile time that T supports binary format parsing
    pub async fn try_read_single_value<'postgres_client, T>(
        &'postgres_client mut self,
        query: &(impl Statement + ?Sized),
        parameters: &[&dyn ToSql],
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

    /// Read a single value using binary mode (with prepared statements)
    /// Test helper method - panics on error
    pub async fn read_single_value<'postgres_client, T>(
        &'postgres_client mut self,
        query: &(impl Statement + ?Sized),
        parameters: &[&dyn ToSql],
    ) -> T
    where
        T: FromSqlBinary<'postgres_client>,
    {
        self.try_read_single_value(query, parameters).await.unwrap()
    }

    /// Read a single value using simple query mode (text format, no parameters), returning Result
    /// Test helper method - enforces at compile time that T supports text format parsing
    pub async fn try_read_single_value_simple<'postgres_client, T>(
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

    /// Read a single value using simple query mode (text format, no parameters)
    /// Test helper method - panics on error
    pub async fn read_single_value_simple<'postgres_client, T>(
        &'postgres_client mut self,
        query: &str,
    ) -> T
    where
        T: FromSqlText<'postgres_client>,
    {
        self.try_read_single_value_simple(query).await.unwrap()
    }

    /// Read a single value using both binary and text protocol modes,
    /// validating that both modes produce identical results. Returns Result.
    ///
    /// This test helper executes the query twice:
    /// 1. As a simple query (text protocol)
    /// 2. As a prepared statement (binary protocol)
    ///
    /// Returns error if the two modes produce different values.
    ///
    /// # Limitations
    /// - Only supports literal SQL values (no $1, $2 parameters)
    /// - Type T must implement FromSqlOwned (both binary and text modes, without borrowing)
    /// - Type T must implement PartialEq + Debug + Clone for comparison
    pub async fn try_read_single_value_dual_mode<T>(
        &mut self,
        query: &str,
    ) -> Result<T, ElefantClientError>
    where
        T: FromSqlOwned + PartialEq + Debug + Clone,
    {
        // Execute both modes and clone the first value to avoid borrow conflicts
        let text_value_original: T = self.try_read_single_value_simple(query).await?;
        let text_value = text_value_original.clone();
        drop(text_value_original); // Explicitly drop to end the borrow

        // Execute as prepared statement (binary mode)
        let binary_value: T = self.try_read_single_value(query, &[]).await?;

        // Assert they match
        assert_eq!(
            binary_value, text_value,
            "Binary and text protocol results differ for query: {query}\nBinary: {binary_value:?}\nText: {text_value:?}"
        );

        Ok(binary_value)
    }

    /// Read a single value using both binary and text protocol modes,
    /// validating that both modes produce identical results.
    ///
    /// This test helper executes the query twice:
    /// 1. As a simple query (text protocol)
    /// 2. As a prepared statement (binary protocol)
    ///
    /// Panics if the two modes produce different values.
    ///
    /// # Limitations
    /// - Only supports literal SQL values (no $1, $2 parameters)
    /// - Type T must implement FromSqlOwned (both binary and text modes, without borrowing)
    /// - Type T must implement PartialEq + Debug + Clone for comparison
    pub async fn read_single_value_dual_mode<T>(&mut self, query: &str) -> T
    where
        T: FromSqlOwned + PartialEq + Debug + Clone,
    {
        self.try_read_single_value_dual_mode(query).await.unwrap()
    }

    pub async fn read_single_column_and_row_exactly<'a, S, T>(
        &'a mut self,
        sql: &S,
        parameters: &[&dyn ToSql],
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
