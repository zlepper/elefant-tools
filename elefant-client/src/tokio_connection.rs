use crate::postgres_client::PostgresClient;
use crate::protocol::PostgresConnection;
use crate::protocol::async_io::{ElefantAsyncRead, ElefantAsyncWrite};
use crate::{ElefantClientError, PostgresConnectionSettings};
use tokio::io::{AsyncReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use std::io;

pub struct TokioWrapper<T>(T);

impl<T: AsyncRead + Unpin> ElefantAsyncRead for TokioWrapper<T> {
    async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        AsyncReadExt::read(&mut self.0, buf).await
    }
}

impl<T: AsyncWrite + Unpin> ElefantAsyncWrite for TokioWrapper<T> {
    async fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        AsyncWriteExt::write_all(&mut self.0, buf).await
    }

    async fn flush(&mut self) -> io::Result<()> {
        AsyncWriteExt::flush(&mut self.0).await
    }
}

pub type TokioPostgresConnection = PostgresConnection<TokioWrapper<BufWriter<TcpStream>>>;
pub type TokioPostgresClient = PostgresClient<TokioWrapper<BufWriter<TcpStream>>>;

async fn new_connection(
    settings: &PostgresConnectionSettings,
) -> Result<TokioPostgresConnection, ElefantClientError> {
    let stream = TcpStream::connect(format!("{}:{}", settings.host, settings.port)).await?;
    stream.set_nodelay(true)?;

    let stream = BufWriter::new(stream);

    Ok(PostgresConnection::new(TokioWrapper(stream)))
}

pub async fn new_client(
    settings: PostgresConnectionSettings,
) -> Result<TokioPostgresClient, ElefantClientError> {
    let connection = new_connection(&settings).await?;
    
    let client = PostgresClient::new(connection, settings).await?;

    Ok(client)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres_client::{QueryResultSet};
    use tokio::test;
    use crate::test_helpers::get_settings;

    #[test]
    pub async fn hello_world() {
        let mut client = new_client(get_settings()).await.unwrap();

        let mut query_result = client.query("select 2147483647::int4; select 1::int4", &[]).await.unwrap();
        {
            let query_result_set = query_result.next_result_set().await.unwrap();
            match query_result_set {
                QueryResultSet::QueryProcessingComplete => {
                    panic!("At least two result sets should be returned");
                }
                QueryResultSet::RowDescriptionReceived(mut row_result_reader) => {

                    let row = row_result_reader.next_row().await.unwrap();
                    assert!(row.is_some());
                    let content = row.unwrap();
                    let stuff = content.get_some_bytes();
                    assert_eq!(stuff.len(), 1);
                    let bytes = stuff[0].unwrap();
                    assert_eq!(bytes, b"2147483647");


                    let row = row_result_reader.next_row().await.unwrap();
                    assert!(row.is_none());

                }
            }
        }

        {
            let query_result_set = query_result.next_result_set().await.unwrap();
            match query_result_set {
                QueryResultSet::QueryProcessingComplete => {
                    panic!("At least two result sets should be returned");
                }
                QueryResultSet::RowDescriptionReceived(mut row_result_reader) => {
                    let row = row_result_reader.next_row().await.unwrap();
                    assert!(row.is_some());
                    let content = row.unwrap();
                    let stuff = content.get_some_bytes();
                    assert_eq!(stuff.len(), 1);
                    let bytes = stuff[0].unwrap();
                    assert_eq!(bytes, b"1");
                }
            }
        }

        {
            let query_result_set = query_result.next_result_set().await.unwrap();
            match query_result_set {
                QueryResultSet::QueryProcessingComplete => {}
                QueryResultSet::RowDescriptionReceived(_) => {
                    panic!("Only two result sets should be returned");
                }
            }
        }
        
        let mut another_query_result = client.query("select 42::int4", &[]).await.unwrap();
        {
            let query_result_set = another_query_result.next_result_set().await.unwrap();
            match query_result_set {
                QueryResultSet::QueryProcessingComplete => {
                    panic!("At least one result set should be returned");
                }
                QueryResultSet::RowDescriptionReceived(mut row_result_reader) => {
                    let row = row_result_reader.next_row().await.unwrap();
                    assert!(row.is_some());
                    let content = row.unwrap();
                    let stuff = content.get_some_bytes();
                    assert_eq!(stuff.len(), 1);
                    let bytes = stuff[0].unwrap();
                    assert_eq!(bytes, b"42");
                }
            }
        }

        {
            let query_result_set = another_query_result.next_result_set().await.unwrap();
            match query_result_set {
                QueryResultSet::QueryProcessingComplete => {}
                QueryResultSet::RowDescriptionReceived(_) => {
                    panic!("Only one result set should be returned");
                }
            }
        }
    }

    #[test]
    pub async fn connect_to_all_the_postgres() {
        let pg_ports = vec![5412, 5413, 5414, 5415, 5416, 5515, 5516];

        for port in pg_ports {
            let _client = new_client(PostgresConnectionSettings {
                user: "postgres".to_string(),
                host: "localhost".to_string(),
                database: "postgres".to_string(),
                port,
                password: "passw0rd".to_string(),
            }).await.unwrap_or_else(|_| panic!("Failed to connect to port {}", port));
        }
    }
}
