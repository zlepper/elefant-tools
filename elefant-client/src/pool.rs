use std::sync::Arc;

use crate::protocol::async_io::ElefantAsyncReadWrite;
use crate::protocol::PostgresConnection;
use crate::{ElefantClientError, PostgresConnectionSettings};

pub trait ConnectionFactory {
    type Connection: ElefantAsyncReadWrite;

    fn create_connection(
        &self,
        settings: &PostgresConnectionSettings,
    ) -> impl std::future::Future<Output = Result<PostgresConnection<Self::Connection>, ElefantClientError>>;
}

pub struct PostgresPool<F: ConnectionFactory>(Arc<PostgresPoolInner<F>>);

struct PostgresPoolInner<F: ConnectionFactory> {
    factory: F,
    settings: PostgresConnectionSettings,
}

impl<F: ConnectionFactory> Clone for PostgresPool<F> {
    fn clone(&self) -> Self {
        PostgresPool(Arc::clone(&self.0))
    }
}

impl<F: ConnectionFactory> PostgresPool<F> {
    pub fn new(factory: F, settings: PostgresConnectionSettings) -> Self {
        PostgresPool(Arc::new(PostgresPoolInner { factory, settings }))
    }

    pub fn settings(&self) -> &PostgresConnectionSettings {
        &self.0.settings
    }

    pub async fn get_client(
        &self,
    ) -> Result<crate::postgres_client::PostgresClient<F>, ElefantClientError> {
        let connection = self.0.factory.create_connection(&self.0.settings).await?;
        crate::postgres_client::PostgresClient::new(connection, self.clone()).await
    }
}
