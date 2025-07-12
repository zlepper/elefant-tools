use crate::postgres_client::PostgresClient;
use crate::protocol::PostgresConnection;
use crate::protocol::async_io::{ElefantAsyncRead, ElefantAsyncWrite};
use crate::{ElefantClientError, PostgresConnectionSettings};
use monoio::net::TcpStream;
use std::io;
use monoio::io::{AsyncReadRent, AsyncWriteRent};

/// Wrapper that implements ElefantAsyncReadWrite for monoio types
pub struct MonoioWrapper<T>(T);

impl<T: AsyncReadRent + Unpin> ElefantAsyncRead for MonoioWrapper<T> {
    async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // Create a Vec from the slice for monoio's ownership requirements
        let owned_buf = vec![0u8; buf.len()];
        let (result, returned_buf) = self.0.read(owned_buf).await;
        match result {
            Ok(n) => {
                buf[..n].copy_from_slice(&returned_buf[..n]);
                Ok(n)
            }
            Err(e) => Err(e),
        }
    }
}

impl<T: AsyncWriteRent + Unpin> ElefantAsyncWrite for MonoioWrapper<T> {
    async fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        use monoio::io::AsyncWriteRentExt;
        // Convert slice to owned Vec for monoio's ownership requirements
        let owned_buf = buf.to_vec();
        let (result, _) = self.0.write_all(owned_buf).await;
        result.map(|_| ())
    }

    async fn flush(&mut self) -> io::Result<()> {
        self.0.flush().await
    }
}

pub type MonoioPostgresConnection = PostgresConnection<MonoioWrapper<TcpStream>>;
pub type MonoioPostgresClient = PostgresClient<MonoioWrapper<TcpStream>>;

async fn new_connection(
    settings: &PostgresConnectionSettings,
) -> Result<MonoioPostgresConnection, ElefantClientError> {
    let stream = TcpStream::connect(format!("{}:{}", settings.host, settings.port)).await?;
    stream.set_nodelay(true)?;

    Ok(PostgresConnection::new(MonoioWrapper(stream)))
}


pub async fn new_client(
    settings: PostgresConnectionSettings,
) -> Result<MonoioPostgresClient, ElefantClientError> {
    let connection = new_connection(&settings).await?;
    
    let client = PostgresClient::new(connection, settings).await?;

    Ok(client)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres_client::{QueryResultSet};
    use crate::test_helpers::{get_settings, get_monoio_test_client};

    #[monoio::test]
    pub async fn basic_monoio_test() {
        // Simple test to verify monoio runtime works
        let result = 1 + 1;
        assert_eq!(result, 2);
    }

    #[monoio::test]
    pub async fn test_tcp_connection() {
        // Test if we can create a basic TCP connection
        let stream = TcpStream::connect("localhost:5415").await.unwrap();
        stream.set_nodelay(true).unwrap();
        // If we get here, TCP connection worked
    }

    #[monoio::test]
    pub async fn test_postgres_connection_only() {
        // Test creating just the PostgresConnection
        let _connection = new_connection(&get_settings()).await.unwrap();
        // If we get here, connection establishment worked
    }


    #[monoio::test]
    pub async fn test_connection_only() {
        // Test just creating a connection without queries
        let _client = new_client(get_settings()).await.unwrap();
        // If we get here, connection establishment worked
    }

    #[monoio::test] 
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

    #[monoio::test]
    pub async fn connect_to_all_the_postgres() {
        let pg_ports = vec![5412, 5413, 5414, 5415, 5416, 5515, 5516];

        for port in pg_ports {
            let _client = new_client(PostgresConnectionSettings {
                user: "postgres".to_string(),
                host: "localhost".to_string(),
                database: "postgres".to_string(),
                port,
                password: "passw0rd".to_string(),
            }).await.unwrap_or_else(|_| panic!("Failed to connect to port {port}"));
        }
    }

    #[monoio::test]
    pub async fn test_monoio_basic_query_operations() {
        let mut client = get_monoio_test_client().await;

        // Create test table
        client.query("DROP TABLE IF EXISTS monoio_query_test", &[]).await.unwrap();
        client.query("CREATE TABLE monoio_query_test (id INTEGER, data TEXT)", &[]).await.unwrap();

        // Insert test data using regular query
        client.query("INSERT INTO monoio_query_test VALUES (1, 'test data 1'), (2, 'test data 2')", &[]).await.unwrap();

        // Test SELECT query
        let mut query_result = client.query("SELECT id, data FROM monoio_query_test ORDER BY id", &[]).await.unwrap();
        let result_set = query_result.next_result_set().await.unwrap();
        
        match result_set {
            QueryResultSet::RowDescriptionReceived(mut row_reader) => {
                // Read first row
                let row1 = row_reader.next_row().await.unwrap().unwrap();
                let row1_data = row1.get_some_bytes();
                assert_eq!(String::from_utf8(row1_data[0].unwrap().to_vec()).unwrap(), "1");
                assert_eq!(String::from_utf8(row1_data[1].unwrap().to_vec()).unwrap(), "test data 1");

                // Read second row
                let row2 = row_reader.next_row().await.unwrap().unwrap();
                let row2_data = row2.get_some_bytes();
                assert_eq!(String::from_utf8(row2_data[0].unwrap().to_vec()).unwrap(), "2");
                assert_eq!(String::from_utf8(row2_data[1].unwrap().to_vec()).unwrap(), "test data 2");

                // Verify no more rows
                assert!(row_reader.next_row().await.unwrap().is_none());
            }
            _ => panic!("Expected row data"),
        }

        // Clean up
        client.query("DROP TABLE monoio_query_test", &[]).await.unwrap();
    }

    #[monoio::test]
    pub async fn test_monoio_multiple_queries() {
        let mut client = new_client(get_settings()).await.unwrap();

        // Test multiple sequential queries
        for i in 1..=5 {
            let mut query_result = client.query(&format!("SELECT {i}::int4"), &[]).await.unwrap();
            let result_set = query_result.next_result_set().await.unwrap();
            
            match result_set {
                QueryResultSet::RowDescriptionReceived(mut row_reader) => {
                    let row = row_reader.next_row().await.unwrap().unwrap();
                    let value_bytes = row.get_some_bytes()[0].unwrap();
                    let value_str = String::from_utf8(value_bytes.to_vec()).unwrap();
                    assert_eq!(value_str, i.to_string());
                }
                _ => panic!("Expected row data"),
            }
        }
    }

    #[monoio::test]
    pub async fn test_monoio_connection_reuse() {
        let mut client = new_client(get_settings()).await.unwrap();

        // Test that the same connection can be used multiple times
        for _ in 0..3 {
            let mut query_result = client.query("SELECT 'connection_test'::text", &[]).await.unwrap();
            let result_set = query_result.next_result_set().await.unwrap();
            
            match result_set {
                QueryResultSet::RowDescriptionReceived(mut row_reader) => {
                    let row = row_reader.next_row().await.unwrap().unwrap();
                    let value_bytes = row.get_some_bytes()[0].unwrap();
                    let value_str = String::from_utf8(value_bytes.to_vec()).unwrap();
                    assert_eq!(value_str, "connection_test");
                }
                _ => panic!("Expected row data"),
            }
        }
    }
}