use futures::{AsyncBufRead, AsyncRead, AsyncWrite};
use crate::postgres_client::{FromSql, PostgresClient, QueryResultSet, Statement, ToSql};
use crate::PostgresConnectionSettings;
use crate::protocol::PostgresConnection;

pub(crate) fn get_settings() -> PostgresConnectionSettings {
    PostgresConnectionSettings {
        password: "passw0rd".to_string(),
        port: 5415,
        ..Default::default()
    }
}

impl<C: AsyncRead + AsyncBufRead + AsyncWrite + Unpin> PostgresClient<C> {
    pub async fn read_single_column_and_row_exactly<'a, S, T>(&'a mut self, sql: &S, parameters: &[&(dyn ToSql)]) -> T
    where T: FromSql<'a>,
        S: Statement + ?Sized
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
                    },
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
            QueryResultSet::QueryProcessingComplete => {
            }
            QueryResultSet::RowDescriptionReceived(_) => {
                panic!("Exactly 1 result set was expected. Got more than 1");
            }
        }


        value
    }
    
}