use crate::Result;
use bytes::Buf;
use futures::{pin_mut, TryStreamExt};
use std::fmt::Display;
use std::ops::Deref;
use tokio::task::JoinHandle;
use tokio_postgres::row::RowIndex;
use tokio_postgres::types::FromSqlOwned;
use tokio_postgres::{Client, CopyInSink, CopyOutStream, NoTls, Row};
use tracing::instrument;

/// A wrapper around tokio_postgres::Client, which provides a more convenient interface for working with the client.
pub struct PostgresClientWrapper {
    /// The actual client
    client: PostgresClient,
    /// The version of the postgres server, reduced by 1000. For example, version 15.0 is represented as 150.
    version: i32,
    /// The connection string used to connect to the server
    connection_string: String,
}

impl PostgresClientWrapper {
    /// Create a new PostgresClientWrapper.
    ///
    /// This will connect to the postgres server to figure out the version of the server.
    /// If the version is less than 12, an error is returned.
    #[instrument(skip_all)]
    pub async fn new(connection_string: &str) -> Result<Self> {
        let client = PostgresClient::new(connection_string).await?;

        let version = match &client
            .client
            .simple_query("SHOW server_version_num;")
            .await?
            .get(1)
        {
            Some(tokio_postgres::SimpleQueryMessage::Row(row)) => {
                let version: i32 = row
                    .get(0)
                    .expect("failed to get version from row")
                    .parse()
                    .expect("failed to parse version");
                if version < 120000 {
                    return Err(crate::ElefantToolsError::UnsupportedPostgresVersion(
                        version,
                    ));
                }
                version / 1000
            }
            _ => return Err(crate::ElefantToolsError::InvalidPostgresVersionResponse),
        };

        Ok(PostgresClientWrapper {
            client,
            version,
            connection_string: connection_string.to_string(),
        })
    }

    /// Get the version of the postgres server
    pub fn version(&self) -> i32 {
        self.version
    }

    /// Create another connection to the same server
    pub async fn create_another_connection(&self) -> Result<Self> {
        let client = PostgresClient::new(&self.connection_string).await?;
        Ok(PostgresClientWrapper {
            client,
            version: self.version,
            connection_string: self.connection_string.clone(),
        })
    }

    #[cfg(test)]
    pub(crate) fn underlying_connection(&self) -> &Client {
        &self.client.client
    }
}

impl Deref for PostgresClientWrapper {
    type Target = PostgresClient;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

/// A wrapper around tokio_postgres::Client, which provides a more convenient interface for working with the client.
pub struct PostgresClient {
    client: Client,
    join_handle: JoinHandle<Result<()>>,
}

impl PostgresClient {
    /// Create a new PostgresClient.
    ///
    /// This will establish a connection to the postgres server.
    pub async fn new(connection_string: &str) -> Result<Self> {
        let (client, connection) = tokio_postgres::connect(connection_string, NoTls).await?;

        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        let join_handle = tokio::spawn(async move {
            match connection.await {
                Err(e) => Err(crate::ElefantToolsError::PostgresError(e)),
                Ok(_) => Ok(()),
            }
        });

        Ok(PostgresClient {
            client,
            join_handle,
        })
    }

    /// Execute a query that does not return any results.
    pub async fn execute_non_query(&self, sql: &str) -> Result {
        self.client.batch_execute(sql).await.map_err(|e| {
            crate::ElefantToolsError::PostgresErrorWithQuery {
                source: e,
                query: sql.to_string(),
            }
        })?;

        Ok(())
    }

    /// Execute a query that returns results.
    pub async fn get_results<T: FromRow>(&self, sql: &str) -> Result<Vec<T>> {
        let query_results = self
            .client
            .query_raw(sql, Vec::<i32>::new())
            .await
            .map_err(|e| crate::ElefantToolsError::PostgresErrorWithQuery {
                source: e,
                query: sql.to_string(),
            })?;

        pin_mut!(query_results);

        let mut output = Vec::new();

        while let Some(row) = query_results.try_next().await? {
            output.push(T::from_row(row)?);
        }

        Ok(output)
    }

    /// Execute a query that returns a single result.
    pub async fn get_result<T: FromRow>(&self, sql: &str) -> Result<T> {
        let results = self.get_results(sql).await?;
        if results.len() != 1 {
            return Err(crate::ElefantToolsError::InvalidNumberOfResults {
                actual: results.len(),
                expected: 1,
            });
        }

        // Safe, we have just checked the length of the vector
        let r = results.into_iter().next().unwrap();

        Ok(r)
    }

    /// Execute a query that returns a single column of results.
    pub async fn get_single_results<T: FromSqlOwned>(&self, sql: &str) -> Result<Vec<T>> {
        let r = self
            .get_results::<(T,)>(sql)
            .await?
            .into_iter()
            .map(|t| t.0)
            .collect();

        Ok(r)
    }

    /// Execute a query that returns a single column of a single row of results.
    pub async fn get_single_result<T: FromSqlOwned>(&self, sql: &str) -> Result<T> {
        let result = self.get_result::<(T,)>(sql).await?;
        Ok(result.0)
    }

    /// Starts a COPY IN operation.
    pub async fn copy_in<U>(&self, sql: &str) -> Result<CopyInSink<U>>
    where
        U: Buf + Send + 'static,
    {
        let sink = self.client.copy_in(sql).await?;
        Ok(sink)
    }

    /// Starts a COPY OUT operation.
    pub async fn copy_out(&self, sql: &str) -> Result<CopyOutStream> {
        let stream = self.client.copy_out(sql).await?;
        Ok(stream)
    }
}

impl Drop for PostgresClient {
    fn drop(&mut self) {
        self.join_handle.abort();
    }
}

/// Provides a more convenient way of reading an
/// entire row from a tokio_postgres::Row into a type.
pub trait FromRow: Sized {
    fn from_row(row: Row) -> Result<Self>;
}

impl<T1: FromSqlOwned> FromRow for (T1,) {
    fn from_row(row: Row) -> Result<Self> {
        Ok((row.try_get(0)?,))
    }
}

impl<T1: FromSqlOwned, T2: FromSqlOwned> FromRow for (T1, T2) {
    fn from_row(row: Row) -> Result<Self> {
        Ok((row.try_get(0)?, row.try_get(1)?))
    }
}

impl<T1: FromSqlOwned, T2: FromSqlOwned, T3: FromSqlOwned> FromRow for (T1, T2, T3) {
    fn from_row(row: Row) -> Result<Self> {
        Ok((row.try_get(0)?, row.try_get(1)?, row.try_get(2)?))
    }
}

impl<T1: FromSqlOwned, T2: FromSqlOwned, T3: FromSqlOwned, T4: FromSqlOwned> FromRow
    for (T1, T2, T3, T4)
{
    fn from_row(row: Row) -> Result<Self> {
        Ok((
            row.try_get(0)?,
            row.try_get(1)?,
            row.try_get(2)?,
            row.try_get(3)?,
        ))
    }
}

impl<T1: FromSqlOwned, T2: FromSqlOwned, T3: FromSqlOwned, T4: FromSqlOwned, T5: FromSqlOwned>
    FromRow for (T1, T2, T3, T4, T5)
{
    fn from_row(row: Row) -> Result<Self> {
        Ok((
            row.try_get(0)?,
            row.try_get(1)?,
            row.try_get(2)?,
            row.try_get(3)?,
            row.try_get(4)?,
        ))
    }
}

impl<
        T1: FromSqlOwned,
        T2: FromSqlOwned,
        T3: FromSqlOwned,
        T4: FromSqlOwned,
        T5: FromSqlOwned,
        T6: FromSqlOwned,
    > FromRow for (T1, T2, T3, T4, T5, T6)
{
    fn from_row(row: Row) -> Result<Self> {
        Ok((
            row.try_get(0)?,
            row.try_get(1)?,
            row.try_get(2)?,
            row.try_get(3)?,
            row.try_get(4)?,
            row.try_get(5)?,
        ))
    }
}

/// A trait for converting a postgres char to a Rust type.
pub(crate) trait FromPgChar: Sized {
    fn from_pg_char(c: char) -> std::result::Result<Self, crate::ElefantToolsError>;
}

/// Provides extension methods on tokio_postgres::Row for working with enums that implements FromPgChar.
pub(crate) trait RowEnumExt {
    /// Get an enum value from a row.
    fn try_get_enum_value<T: FromPgChar, I: RowIndex + Display>(&self, idx: I) -> Result<T>;
    /// Get an optional enum value from a row, aka `Option<T>`.
    fn try_get_opt_enum_value<T: FromPgChar, I: RowIndex + Display>(
        &self,
        idx: I,
    ) -> Result<Option<T>>;
}

impl RowEnumExt for Row {
    fn try_get_enum_value<T: FromPgChar, I: RowIndex + Display>(&self, idx: I) -> Result<T> {
        let value: i8 = self.try_get(idx)?;
        let c = value as u8 as char;
        T::from_pg_char(c)
    }

    fn try_get_opt_enum_value<T: FromPgChar, I: RowIndex + Display>(
        &self,
        idx: I,
    ) -> Result<Option<T>> {
        let value: Option<i8> = self.try_get(idx)?;
        match value {
            Some(0) => Ok(None),
            Some(value) => {
                let c = value as u8 as char;
                Ok(Some(T::from_pg_char(c)?))
            }
            None => Ok(None),
        }
    }
}
