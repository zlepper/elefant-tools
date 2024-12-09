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
    let mut client = PostgresClient { connection, settings };
    
    client.establish().await?;
    
    Ok(client)
}


#[cfg(test)]
mod tests {
    use super::*;
    use tokio::test;

    #[test]
    pub async fn hello_world() {
        let settings = PostgresConnectionSettings {
            password: "passw0rd".to_string(),
            port: 5415,
            ..Default::default()
        };


        let mut client = new_client(settings).await.unwrap();



    }

}