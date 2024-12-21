use crate::postgres_client::PostgresClient;
use crate::protocol::PostgresConnection;
use crate::{ElefantClientError, PostgresConnectionSettings};
use tokio::io::BufStream;
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

pub type TokioPostgresConnection = PostgresConnection<Compat<BufStream<TcpStream>>>;
pub type TokioPostgresClient = PostgresClient<Compat<BufStream<TcpStream>>>;

async fn new_connection(
    settings: &PostgresConnectionSettings,
) -> Result<TokioPostgresConnection, ElefantClientError> {
    let stream = TcpStream::connect(format!("{}:{}", settings.host, settings.port)).await?;

    let stream = BufStream::new(stream);

    Ok(PostgresConnection::new(stream.compat()))
}

pub(crate) async fn new_client(
    settings: PostgresConnectionSettings,
) -> Result<PostgresClient<Compat<BufStream<TcpStream>>>, ElefantClientError> {
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
}
