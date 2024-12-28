use crate::postgres_client::{FromSqlOwned, PostgresClient, QueryResultSet, Statement, ToSql};
use crate::ElefantClientError;
use futures::{AsyncBufRead, AsyncRead, AsyncWrite};

impl<C: AsyncRead + AsyncBufRead + AsyncWrite + Unpin> PostgresClient<C> {
    pub async fn read_single_value<S, T>(
        &mut self,
        query: &S,
        parameters: &[&(dyn ToSql)],
    ) -> Result<T, ElefantClientError>
    where
        S: Statement + ?Sized,
        T: FromSqlOwned,
    {
        let mut query_result = self.query(query, parameters).await?;

        let result_set = query_result.next_result_set().await?;

        match result_set {
            QueryResultSet::QueryProcessingComplete => Err(ElefantClientError::NoResultsReturned),
            QueryResultSet::RowDescriptionReceived(mut row_reader) => {
                match row_reader.next_row().await? {
                    None => Err(ElefantClientError::NoResultsReturned),
                    Some(row) => {
                        let value: T = row.get(0)?;
                        Ok(value)
                    }
                }
            }
        }
    }

    pub async fn execute_non_query<S>(
        &mut self,
        query: &S,
        parameters: &[&(dyn ToSql)],
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
}
