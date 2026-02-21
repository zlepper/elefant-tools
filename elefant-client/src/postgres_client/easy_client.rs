use crate::postgres_client::statements::Statement;
use crate::postgres_client::{PostgresClient, QueryResultSet};
use crate::protocol::async_io::ElefantAsyncReadWrite;
use crate::{ElefantClientError, ToSql};

impl<C: ElefantAsyncReadWrite> PostgresClient<C> {
    /// Execute a non-query statement using binary mode (with prepared statements)
    pub async fn execute_non_query<S>(
        &mut self,
        query: &S,
        parameters: &[&dyn ToSql],
    ) -> Result<(), ElefantClientError>
    where
        S: Statement + ?Sized,
    {
        let mut query_result = self.query(query, parameters).await?;

        loop {
            let result_set = query_result.next_result_set().await?;

            match result_set {
                QueryResultSet::QueryProcessingComplete => return Ok(()),
                QueryResultSet::RowDescriptionReceived(_) => {}
            }
        }
    }

    /// Execute a non-query statement using simple query mode (text format, no parameters)
    pub async fn execute_non_query_simple(
        &mut self,
        query: &str,
    ) -> Result<(), ElefantClientError> {
        let mut query_result = self.query_simple(query).await?;

        loop {
            let result_set = query_result.next_result_set().await?;

            match result_set {
                QueryResultSet::QueryProcessingComplete => return Ok(()),
                QueryResultSet::RowDescriptionReceived(_) => {}
            }
        }
    }
}
