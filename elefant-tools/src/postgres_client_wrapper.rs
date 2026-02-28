use crate::Result;
use elefant_client::tokio_connection::TokioPostgresPool;
use elefant_client::{
    CollectBatch, ElefantClientError, FlattenTuple, FromSqlOwned, FromSqlRowOwned,
    PostgresConnectionSettings, PostgresDataRow,
};
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
        )
        .await?;

        let client = pool.get_client().await?;

        let version_str = client
            .get_parameter("server_version")
            .ok_or(crate::ElefantToolsError::InvalidPostgresVersionResponse)?;

        // server_version is e.g. "15.3" or "15.3 (Debian 15.3-1.pgdg120+1)"
        let major_version: i32 = version_str
            .split('.')
            .next()
            .and_then(|s| s.parse().ok())
            .ok_or(crate::ElefantToolsError::InvalidPostgresVersionResponse)?;

        if major_version < 12 {
            return Err(crate::ElefantToolsError::UnsupportedPostgresVersion(
                version_str.to_string(),
            ));
        }

        let version = major_version * 10;

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
    pub async fn get_results<T: FromSqlRowOwned>(&self, sql: &str) -> Result<Vec<T>> {
        let mut client = self.pool.get_client().await?;
        let query_result = client.query_simple(sql).await.map_err(|e| {
            crate::ElefantToolsError::PostgresErrorWithQuery {
                source: e,
                query: sql.to_string(),
            }
        })?;

        let rows = query_result
            .collect_to_vec::<T>()
            .await
            .map_err(|e| crate::ElefantToolsError::PostgresErrorWithQuery {
                source: e,
                query: sql.to_string(),
            })?;

        Ok(rows)
    }

    /// Execute a query that returns a single result.
    pub async fn get_result<T: FromSqlRowOwned>(&self, sql: &str) -> Result<T> {
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

/// A trait for converting a postgres char to a Rust type.
pub(crate) trait FromPgChar: Sized {
    fn from_pg_char(c: char) -> std::result::Result<Self, crate::ElefantToolsError>;
}

/// Provides extension methods on PostgresDataRow for working with enums that implements FromPgChar.
pub(crate) trait RowEnumExt {
    /// Get an enum value from a row.
    fn try_get_enum_value<T: FromPgChar>(
        &self,
        idx: usize,
    ) -> std::result::Result<T, ElefantClientError>;
    /// Get an optional enum value from a row, aka `Option<T>`.
    fn try_get_opt_enum_value<T: FromPgChar>(
        &self,
        idx: usize,
    ) -> std::result::Result<Option<T>, ElefantClientError>;
}

impl RowEnumExt for PostgresDataRow<'_, '_> {
    fn try_get_enum_value<T: FromPgChar>(
        &self,
        idx: usize,
    ) -> std::result::Result<T, ElefantClientError> {
        let c: char = self.get(idx)?;
        T::from_pg_char(c).map_err(|e| ElefantClientError::PostgresError(e.to_string()))
    }

    fn try_get_opt_enum_value<T: FromPgChar>(
        &self,
        idx: usize,
    ) -> std::result::Result<Option<T>, ElefantClientError> {
        let c: Option<char> = self.get(idx)?;
        match c {
            Some('\0') => Ok(None),
            Some(c) => Ok(Some(
                T::from_pg_char(c)
                    .map_err(|e| ElefantClientError::PostgresError(e.to_string()))?,
            )),
            None => Ok(None),
        }
    }
}

/// A result type that knows its own SQL query. Implemented by each schema reader
/// result struct so the batch builder can tie query selection to result collection.
pub(crate) trait QueryResult: FromSqlRowOwned {
    fn query(version: i32) -> &'static str;
}

/// Type-safe batch query builder. Each `.add::<T>()` appends a query (from the
/// `QueryResult` trait) and its corresponding result type, ensuring the query
/// order and collect order are always in sync.
pub(crate) struct BatchQueryBuilder<Batch> {
    query: String,
    version: i32,
    _batch: std::marker::PhantomData<Batch>,
}

impl BatchQueryBuilder<()> {
    pub(crate) fn new(connection: &PostgresClientWrapper) -> Self {
        Self {
            query: String::new(),
            version: connection.version(),
            _batch: std::marker::PhantomData,
        }
    }
}

impl<Batch> BatchQueryBuilder<Batch> {
    pub(crate) fn add<T: QueryResult>(mut self) -> BatchQueryBuilder<(Batch, Vec<T>)> {
        self.query.push_str(T::query(self.version));
        BatchQueryBuilder {
            query: self.query,
            version: self.version,
            _batch: std::marker::PhantomData,
        }
    }
}

impl<Batch: CollectBatch + FlattenTuple> BatchQueryBuilder<Batch> {
    pub(crate) async fn execute(
        self,
        connection: &PostgresClientWrapper,
    ) -> Result<Batch::Output> {
        let mut client = connection.pool().get_client().await?;
        let mut result = client.query_simple(&self.query).await?;
        Ok(Batch::collect(&mut result).await?.flatten())
    }
}
