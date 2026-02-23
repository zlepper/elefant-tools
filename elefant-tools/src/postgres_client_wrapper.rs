use crate::Result;
use elefant_client::tokio_connection::TokioPostgresPool;
use elefant_client::{FromSqlOwned, PostgresConnectionSettings, PostgresDataRow};
use tracing::instrument;

/// A wrapper around the elefant-client connection pool, providing a convenient interface.
pub struct PostgresClientWrapper {
    pool: TokioPostgresPool,
    /// The version of the postgres server, reduced by 1000. For example, version 15.0 is represented as 150.
    version: i32,
}

impl PostgresClientWrapper {
    /// Create a new PostgresClientWrapper.
    ///
    /// This will connect to the postgres server to figure out the version of the server.
    /// If the version is less than 12, an error is returned.
    #[instrument(skip_all)]
    pub async fn new(settings: PostgresConnectionSettings) -> Result<Self> {
        let pool = TokioPostgresPool::new(
            elefant_client::tokio_connection::TokioConnectionFactory,
            settings,
        );

        let mut client = pool.get_client().await?;

        let version_str: String = client
            .query_simple("SHOW server_version_num;")
            .await?
            .collect_single_column_to_vec::<String>()
            .await?
            .into_iter()
            .next()
            .ok_or(crate::ElefantToolsError::InvalidPostgresVersionResponse)?;

        let version: i32 = version_str
            .parse()
            .map_err(|_| crate::ElefantToolsError::InvalidPostgresVersionResponse)?;

        if version < 120000 {
            return Err(crate::ElefantToolsError::UnsupportedPostgresVersion(
                version,
            ));
        }

        let version = version / 1000;

        Ok(PostgresClientWrapper { pool, version })
    }

    /// Get the version of the postgres server
    pub fn version(&self) -> i32 {
        self.version
    }

    /// Get a reference to the underlying pool
    pub fn pool(&self) -> &TokioPostgresPool {
        &self.pool
    }

    /// Execute a query that does not return any results.
    pub async fn execute_non_query(&self, sql: &str) -> Result {
        let mut client = self.pool.get_client().await?;
        client.execute_non_query_simple(sql).await.map_err(|e| {
            crate::ElefantToolsError::PostgresErrorWithQuery {
                source: e,
                query: sql.to_string(),
            }
        })?;

        Ok(())
    }

    /// Execute a query that returns results.
    pub async fn get_results<T: FromRow>(&self, sql: &str) -> Result<Vec<T>> {
        let mut client = self.pool.get_client().await?;
        let query_result = client.query_simple(sql).await.map_err(|e| {
            crate::ElefantToolsError::PostgresErrorWithQuery {
                source: e,
                query: sql.to_string(),
            }
        })?;

        let rows = query_result
            .collect_to_vec::<RowAdapter<T>>()
            .await
            .map_err(|e| crate::ElefantToolsError::PostgresErrorWithQuery {
                source: e,
                query: sql.to_string(),
            })?;

        Ok(rows.into_iter().map(|r| r.0).collect())
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
}

/// Provides a way of reading an entire row from a PostgresDataRow into a type.
pub trait FromRow: Sized {
    fn from_row(row: &PostgresDataRow<'_, '_>) -> Result<Self>;
}

// Adapter to bridge FromRow to FromSqlRowOwned so we can use collect_to_vec
struct RowAdapter<T: FromRow>(T);

impl<'a, T: FromRow> elefant_client::FromSqlRow<'a> for RowAdapter<T> {
    fn from_sql_row(
        row: &'a PostgresDataRow<'_, '_>,
    ) -> std::result::Result<Self, elefant_client::ElefantClientError> {
        T::from_row(row).map(RowAdapter).map_err(|e| match e {
            crate::ElefantToolsError::PostgresError(inner) => inner,
            other => elefant_client::ElefantClientError::PostgresError(other.to_string()),
        })
    }
}

impl<T1: FromSqlOwned> FromRow for (T1,) {
    fn from_row(row: &PostgresDataRow<'_, '_>) -> Result<Self> {
        Ok((row.get(0)?,))
    }
}

impl<T1: FromSqlOwned, T2: FromSqlOwned> FromRow for (T1, T2) {
    fn from_row(row: &PostgresDataRow<'_, '_>) -> Result<Self> {
        Ok((row.get(0)?, row.get(1)?))
    }
}

impl<T1: FromSqlOwned, T2: FromSqlOwned, T3: FromSqlOwned> FromRow for (T1, T2, T3) {
    fn from_row(row: &PostgresDataRow<'_, '_>) -> Result<Self> {
        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
    }
}

impl<T1: FromSqlOwned, T2: FromSqlOwned, T3: FromSqlOwned, T4: FromSqlOwned> FromRow
    for (T1, T2, T3, T4)
{
    fn from_row(row: &PostgresDataRow<'_, '_>) -> Result<Self> {
        Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
    }
}

impl<T1: FromSqlOwned, T2: FromSqlOwned, T3: FromSqlOwned, T4: FromSqlOwned, T5: FromSqlOwned>
    FromRow for (T1, T2, T3, T4, T5)
{
    fn from_row(row: &PostgresDataRow<'_, '_>) -> Result<Self> {
        Ok((
            row.get(0)?,
            row.get(1)?,
            row.get(2)?,
            row.get(3)?,
            row.get(4)?,
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
    fn from_row(row: &PostgresDataRow<'_, '_>) -> Result<Self> {
        Ok((
            row.get(0)?,
            row.get(1)?,
            row.get(2)?,
            row.get(3)?,
            row.get(4)?,
            row.get(5)?,
        ))
    }
}

/// A trait for converting a postgres char to a Rust type.
pub(crate) trait FromPgChar: Sized {
    fn from_pg_char(c: char) -> std::result::Result<Self, crate::ElefantToolsError>;
}

/// Provides extension methods on PostgresDataRow for working with enums that implements FromPgChar.
pub(crate) trait RowEnumExt {
    /// Get an enum value from a row.
    fn try_get_enum_value<T: FromPgChar>(&self, idx: usize) -> Result<T>;
    /// Get an optional enum value from a row, aka `Option<T>`.
    fn try_get_opt_enum_value<T: FromPgChar>(&self, idx: usize) -> Result<Option<T>>;
}

impl RowEnumExt for PostgresDataRow<'_, '_> {
    fn try_get_enum_value<T: FromPgChar>(&self, idx: usize) -> Result<T> {
        let c: char = self.get(idx)?;
        T::from_pg_char(c)
    }

    fn try_get_opt_enum_value<T: FromPgChar>(&self, idx: usize) -> Result<Option<T>> {
        let c: Option<char> = self.get(idx)?;
        match c {
            Some('\0') => Ok(None),
            Some(c) => Ok(Some(T::from_pg_char(c)?)),
            None => Ok(None),
        }
    }
}
