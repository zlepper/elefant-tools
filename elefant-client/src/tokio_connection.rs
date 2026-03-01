use crate::pool::{ConnectionFactory, PoolableClient, PostgresPool};
use crate::postgres_client::PostgresClient;
use crate::protocol::async_io::{ElefantAsyncRead, ElefantAsyncWrite};
use crate::{ElefantClientError, PostgresConnectionSettings};
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

pub struct TokioWrapper<T>(T);

impl<T> TokioWrapper<T> {
    pub(crate) fn new(inner: T) -> Self {
        Self(inner)
    }
}

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

pub struct TokioConnectionFactory;

impl ConnectionFactory for TokioConnectionFactory {
    type Connection = TokioWrapper<TcpStream>;

    async fn create_connection(
        &self,
        settings: &PostgresConnectionSettings,
    ) -> Result<Self::Connection, ElefantClientError> {
        let stream = TcpStream::connect(format!("{}:{}", settings.host, settings.port)).await?;
        stream.set_nodelay(true)?;
        Ok(TokioWrapper::new(stream))
    }
}

pub type TokioPostgresClient = PostgresClient<TokioConnectionFactory>;
pub type TokioPostgresPool = PostgresPool<TokioConnectionFactory>;

pub type TokioPoolableClient = PoolableClient<TokioConnectionFactory>;

pub async fn new_client(
    settings: PostgresConnectionSettings,
) -> Result<TokioPoolableClient, ElefantClientError> {
    let pool = PostgresPool::new(TokioConnectionFactory, settings).await?;
    pool.get_client().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres_client::QueryResultSet;
    use crate::test_helpers::get_settings;
    use tokio::test;

    #[test]
    pub async fn hello_world() {
        let mut client = new_client(get_settings()).await.unwrap();

        let mut query_result = client
            .query_simple("select 2147483647::int4; select 1::int4")
            .await
            .unwrap();
        {
            let query_result_set = query_result.next_result_set().await.unwrap();
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
                    assert_eq!(bytes, &[0, 0, 0, 42]); // Binary format for int4
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
    pub async fn connect_with_options_sets_search_path() {
        let mut settings = get_settings();
        settings.options = Some("-c search_path=pg_catalog".to_string());

        let mut client = new_client(settings).await.unwrap();
        let search_path: String = client.read_single_value_simple("SHOW search_path").await;
        assert_eq!(search_path, "pg_catalog");
    }

    #[test]
    pub async fn connect_to_all_the_postgres() {
        let pg_ports = vec![5412, 5413, 5414, 5415, 5416, 5417, 5418, 5515, 5516, 5517, 5518];

        for port in pg_ports {
            let _client = new_client(
                PostgresConnectionSettings::new("localhost")
                    .port(port)
                    .password("passw0rd"),
            )
            .await
            .unwrap_or_else(|_| panic!("Failed to connect to port {port}"));
        }
    }
}
