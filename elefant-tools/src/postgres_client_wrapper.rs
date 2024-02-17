use std::fmt::Display;
use tokio::task::JoinHandle;
use tokio_postgres::{Client, CopyInSink, CopyOutStream, NoTls, Row};
use tokio_postgres::types::{FromSqlOwned};
use crate::{Result};
use bytes::Buf;
use tokio_postgres::row::RowIndex;

pub struct PostgresClientWrapper {
    client: Client,
    join_handle: JoinHandle<Result<()>>,
    version: i32,
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

        let version = match &client.simple_query("SHOW server_version_num;").await?[0] {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                let version: i32 = row.get(0).expect("failed to get version from row").parse().expect("failed to parse version");
                if version < 120000 {
                    return Err(crate::ElefantToolsError::UnsupportedPostgresVersion(version));
                }
                version / 1000
            }
            _ => return Err(crate::ElefantToolsError::InvalidPostgresVersionResponse)
        };

        Ok(PostgresClientWrapper {
            client,
            join_handle,
            version
        })
    }

    pub async fn execute_non_query(&self, sql: &str) -> Result {
        self.client.batch_execute(sql).await.map_err(|e| crate::ElefantToolsError::PostgresErrorWithQuery {
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

    pub async fn copy_in<U>(&self, sql: &str) -> Result<CopyInSink<U>>
        where U: Buf + Send + 'static
    {
        let sink = self.client.copy_in(sql).await?;
        Ok(sink)
    }

    pub async fn copy_out(&self, sql: &str) -> Result<CopyOutStream> {
        let stream = self.client.copy_out(sql).await?;
        Ok(stream)
    }

    pub fn version(&self) -> i32 {
        self.version
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

impl<T1: FromSqlOwned, T2: FromSqlOwned, T3: FromSqlOwned> FromRow for (T1, T2, T3) {
    fn from_row(row: Row) -> Result<Self> {
        Ok((
            row.try_get(0)?,
            row.try_get(1)?,
            row.try_get(2)?,
        ))
    }
}


pub(crate) trait FromPgChar: Sized {
    fn from_pg_char(c: char) -> std::result::Result<Self, crate::ElefantToolsError>;
}

pub(crate) trait RowEnumExt {
    fn try_get_enum_value<T: FromPgChar, I: RowIndex + Display>(&self, idx: I) -> Result<T>;
    fn try_get_opt_enum_value<T: FromPgChar, I: RowIndex + Display>(&self, idx: I) -> Result<Option<T>>;
}

impl RowEnumExt for Row {
    fn try_get_enum_value<T: FromPgChar, I: RowIndex + Display>(&self, idx: I) -> Result<T> {
        let value: i8 = self.try_get(idx)?;
        let c = value as u8 as char;
        T::from_pg_char(c)
    }

    fn try_get_opt_enum_value<T: FromPgChar, I: RowIndex + Display>(&self, idx: I) -> Result<Option<T>> {
        let value: Option<i8> = self.try_get(idx)?;
        match value {
            Some(value) => {
                let c = value as u8 as char;
                Ok(Some(T::from_pg_char(c)?))
            }
            None => Ok(None)
        }
    }
}
