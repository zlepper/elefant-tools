use crate::Result;
use elefant_client::tokio_connection::TokioPostgresPool;
use elefant_client::{
    ConnectionFactory, ElefantClientError, FromSqlOwned, FromSqlRowOwned, PostgresConnectionSettings,
    PostgresDataRow, QueryResultSet, SimpleQueryResult,
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

/// Collect the next result set from a batched simple query into a `Vec<T>`.
///
/// Expects the next result set to contain rows. Returns an error if
/// the query has already completed (no more result sets).
async fn collect_next_result_set<T: FromSqlRowOwned, F: ConnectionFactory>(
    result: &mut SimpleQueryResult<'_, F>,
) -> Result<Vec<T>> {
    match result.next_result_set().await? {
        QueryResultSet::RowDescriptionReceived(reader) => Ok(reader.collect_to_vec().await?),
        QueryResultSet::QueryProcessingComplete => {
            Err(crate::ElefantToolsError::BatchQueryUnexpectedEnd)
        }
    }
}

/// A result type that knows its own SQL query. Implemented by each schema reader
/// result struct so the batch builder can tie query selection to result collection.
pub(crate) trait QueryResult: FromSqlRowOwned {
    fn query(version: i32) -> &'static str;
}

/// Recursive trait for collecting results from a batched simple query.
/// The base case is `()`, and each `.add::<T>()` wraps the previous batch
/// in a `(Prev, Vec<T>)` tuple.
pub(crate) trait CollectBatch: Sized {
    fn append_queries(query: &mut String, version: i32);
    async fn collect<F: ConnectionFactory>(
        result: &mut SimpleQueryResult<'_, F>,
    ) -> Result<Self>;
}

impl CollectBatch for () {
    fn append_queries(_query: &mut String, _version: i32) {}
    async fn collect<F: ConnectionFactory>(
        _result: &mut SimpleQueryResult<'_, F>,
    ) -> Result<Self> {
        Ok(())
    }
}

impl<Prev: CollectBatch, T: QueryResult> CollectBatch for (Prev, Vec<T>) {
    fn append_queries(query: &mut String, version: i32) {
        Prev::append_queries(query, version);
        query.push_str(T::query(version));
    }
    async fn collect<F: ConnectionFactory>(
        result: &mut SimpleQueryResult<'_, F>,
    ) -> Result<Self> {
        let prev = Prev::collect(result).await?;
        let current = collect_next_result_set::<T, F>(result).await?;
        Ok((prev, current))
    }
}

/// Appends an element to a flat tuple, producing a tuple one element larger.
pub(crate) trait TupleAppend<T> {
    type Output;
    fn append(self, item: T) -> Self::Output;
}

macro_rules! impl_tuple_append {
    (@emit $($idx:tt: $T:ident),* $(,)?) => {
        impl<$($T,)* New> TupleAppend<New> for ($($T,)*) {
            type Output = ($($T,)* New,);
            #[inline]
            fn append(self, item: New) -> Self::Output {
                ($(self.$idx,)* item,)
            }
        }
    };
    (@step [$($done:tt)*]) => {
        impl_tuple_append!(@emit $($done)*);
    };
    (@step [$($done:tt)*] $idx:tt: $T:ident $(, $($rest:tt)*)?) => {
        impl_tuple_append!(@emit $($done)*);
        impl_tuple_append!(@step [$($done)* $idx: $T,] $($($rest)*)?);
    };
    ($($all:tt)*) => {
        impl_tuple_append!(@step [] $($all)*);
    };
}

impl_tuple_append!(0: T0, 1: T1, 2: T2, 3: T3, 4: T4, 5: T5, 6: T6, 7: T7,
    8: T8, 9: T9, 10: T10, 11: T11, 12: T12, 13: T13, 14: T14, 15: T15);

/// Flattens a nested left-associated tuple like `((((), A), B), C)` into `(A, B, C)`.
pub(crate) trait FlattenTuple {
    type Output;
    fn flatten(self) -> Self::Output;
}

impl FlattenTuple for () {
    type Output = ();
    fn flatten(self) {}
}

impl<Prev: FlattenTuple, T> FlattenTuple for (Prev, T)
where
    Prev::Output: TupleAppend<T>,
{
    type Output = <Prev::Output as TupleAppend<T>>::Output;
    fn flatten(self) -> Self::Output {
        self.0.flatten().append(self.1)
    }
}

/// Type-safe batch query builder. Each `.add::<T>()` appends a query (from the
/// `QueryResult` trait) and its corresponding result type, ensuring the query
/// order and collect order are always in sync.
pub(crate) struct BatchQueryBuilder<Batch>(std::marker::PhantomData<Batch>);

impl BatchQueryBuilder<()> {
    pub(crate) fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<Batch> BatchQueryBuilder<Batch> {
    pub(crate) fn add<T: QueryResult>(self) -> BatchQueryBuilder<(Batch, Vec<T>)> {
        BatchQueryBuilder(std::marker::PhantomData)
    }
}

impl<Batch: CollectBatch + FlattenTuple> BatchQueryBuilder<Batch> {
    pub(crate) async fn execute(
        self,
        connection: &PostgresClientWrapper,
    ) -> Result<Batch::Output> {
        let mut query = String::new();
        Batch::append_queries(&mut query, connection.version());
        let mut client = connection.pool().get_client().await?;
        let mut result = client.query_simple(&query).await?;
        Ok(Batch::collect(&mut result).await?.flatten())
    }
}
