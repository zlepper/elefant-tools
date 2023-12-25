use tokio::task::JoinHandle;
use tokio_postgres::{Client, NoTls, Row};
use tokio_postgres::types::{FromSqlOwned};
use crate::Result;

pub struct PostgresClientWrapper {
    client: Client,
    join_handle: JoinHandle<Result<()>>
}

impl PostgresClientWrapper {
    pub async fn new(connection_string: &str) -> Result<Self> {
        let (client, connection) =
            tokio_postgres::connect(connection_string, NoTls).await?;

        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        let join_handle = tokio::spawn(async move {
            match connection.await {
                Err(e) => Err(crate::ElefantToolsError::PostgresError(e)),
                Ok(_) => Ok(())
            }
        });

        Ok(PostgresClientWrapper {
            client,
            join_handle
        })
    }

    pub async fn execute_non_query(&self, sql: &str) -> Result {
        self.client.execute(sql, &[]).await.map_err(|e| crate::ElefantToolsError::PostgresErrorWithQuery {
            source: e,
            query: sql.to_string(),
        })?;

        Ok(())
    }

    pub async fn get_results<T: FromRow>(&self, sql: &str) -> Result<Vec<T>> {

        let query_results = self.client.query(sql, &[]).await.map_err(|e| crate::ElefantToolsError::PostgresErrorWithQuery {
            source: e,
            query: sql.to_string(),
        })?;

        let mut output = Vec::with_capacity(query_results.len());

        for row in query_results.into_iter() {
            output.push(T::from_row(row)?);
        }

        Ok(output)
    }

    pub async fn get_result<T: FromRow>(&self, sql: &str) -> Result<T> {
        let results = self.get_results(sql).await?;
        if results.len() != 1 {
            return Err(crate::ElefantToolsError::InvalidNumberOfResults{
                actual: results.len(),
                expected: 1,
            });
        }

        // Safe, we have just checked the length of the vector
        let r = results.into_iter().next().unwrap();

        Ok(r)
    }

    pub async  fn get_single_results<T: FromSqlOwned>(&self, sql: &str) -> Result<Vec<T>> {
        let r = self.get_results::<(T,)>(sql).await?.into_iter()
            .map(|t| t.0)
            .collect();

        Ok(r)
    }

    pub async fn get_single_result<T: FromSqlOwned>(&self, sql: &str) -> Result<T> {
        let result = self.get_result::<(T,)>(sql).await?;
        Ok(result.0)
    }
}

impl Drop for PostgresClientWrapper {
    fn drop(&mut self) {
        self.join_handle.abort();
    }
}

pub trait FromRow: Sized {
    fn from_row(row: Row) -> Result<Self>;
}

impl<T1: FromSqlOwned> FromRow for (T1,) {
    fn from_row(row: Row) -> Result<Self> {
        Ok((
            row.try_get(0)?,
        ))
    }
}

impl<T1: FromSqlOwned, T2: FromSqlOwned> FromRow for (T1, T2) {
    fn from_row(row: Row) -> Result<Self> {
        Ok((
            row.try_get(0)?,
            row.try_get(1)?,
        ))
    }
}



#[cfg(test)]
mod tests {

}