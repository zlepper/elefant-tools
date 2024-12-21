use tokio::io::BufStream;
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};
use crate::{ElefantClientError, PostgresConnectionSettings};
use crate::postgres_client::PostgresClient;
use crate::protocol::PostgresConnection;

pub type TokioPostgresConnection = PostgresConnection<Compat<BufStream<TcpStream>>>;

pub(crate) async fn new_connection(
    settings: &PostgresConnectionSettings,
) -> Result<TokioPostgresConnection, ElefantClientError> {
    let stream = TcpStream::connect(format!("{}:{}", settings.host, settings.port)).await?;

    let stream = BufStream::new(stream);

    Ok(PostgresConnection::new(stream.compat()))
}

pub(crate) async fn new_client(settings: PostgresConnectionSettings) -> Result<PostgresClient<Compat<BufStream<TcpStream>>>, ElefantClientError> {
    let connection = new_connection(&settings).await?;
    let client = PostgresClient::new(connection, settings).await?;
    
    Ok(client)
}


#[cfg(test)]
mod tests {
    use super::*;
    use tokio::test;
    use crate::postgres_client::QueryResultSet;

    #[test]
    pub async fn hello_world() {
        let settings = PostgresConnectionSettings {
            password: "passw0rd".to_string(),
            port: 5415,
            ..Default::default()
        };


        let mut client = new_client(settings).await.unwrap();


        let mut query_result = client.query("select 2147483647::int4", &[]).await.unwrap();
        {
            let query_result_set = query_result.next_result_set().await.unwrap();
            match query_result_set {
                QueryResultSet::QueryProcessingComplete => {
                    panic!("At least one result set should be returned");
                }
                QueryResultSet::RowDescriptionReceived(mut row_result_reader) => {
                    {
                        let row = row_result_reader.next_row().await.unwrap();
                        assert!(row.is_some());
                        let content = row.unwrap();
                        let stuff = content.get_some_bytes();
                        assert_eq!(stuff.len(), 1);
                        let bytes = stuff[0].unwrap();
                        assert_eq!(bytes, b"2147483647");
                    }

                    // {
                    //     let row = row_result_reader.next_row().await.unwrap();
                    //     assert!(row.is_none());
                    // }
                }
            }
        }

    }

}