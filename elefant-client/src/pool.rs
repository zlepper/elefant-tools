use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use crate::protocol::async_io::ElefantAsyncReadWrite;
use crate::protocol::PostgresConnection;
use crate::{ElefantClientError, PostgresConnectionSettings};
use tracing::debug;

pub trait ConnectionFactory {
    type Connection: ElefantAsyncReadWrite;

    fn create_connection(
        &self,
        settings: &PostgresConnectionSettings,
    ) -> impl std::future::Future<
        Output = Result<PostgresConnection<Self::Connection>, ElefantClientError>,
    >;
}

pub struct PostgresPool<F: ConnectionFactory>(Arc<PostgresPoolInner<F>>);

struct PostgresPoolInner<F: ConnectionFactory> {
    factory: F,
    settings: PostgresConnectionSettings,
    idle_connections: Mutex<Vec<crate::postgres_client::PostgresClient<F>>>,
}

impl<F: ConnectionFactory> Clone for PostgresPool<F> {
    fn clone(&self) -> Self {
        PostgresPool(Arc::clone(&self.0))
    }
}

impl<F: ConnectionFactory> PostgresPool<F> {
    pub fn new(factory: F, settings: PostgresConnectionSettings) -> Self {
        PostgresPool(Arc::new(PostgresPoolInner {
            factory,
            settings,
            idle_connections: Mutex::new(Vec::new()),
        }))
    }

    pub fn settings(&self) -> &PostgresConnectionSettings {
        &self.0.settings
    }

    pub(crate) fn return_connection(&self, mut client: crate::postgres_client::PostgresClient<F>) {
        client.pool = None;
        self.0.idle_connections.lock().unwrap().push(client);
    }

    pub async fn get_client(&self) -> Result<PoolableClient<F>, ElefantClientError> {
        // Try to reuse an idle connection
        loop {
            let idle_client = { self.0.idle_connections.lock().unwrap().pop() };
            match idle_client {
                Some(mut client) => match client.reset().await {
                    Ok(()) => {
                        client.pool = Some(self.clone());
                        return Ok(PoolableClient {
                            client: Some(client),
                            pool: self.clone(),
                        });
                    }
                    Err(e) => {
                        debug!("Idle connection reset failed, discarding: {:?}", e);
                        continue;
                    }
                },
                None => break,
            }
        }

        // No idle connection available, create a new one
        let connection = self.0.factory.create_connection(&self.0.settings).await?;
        let mut client =
            crate::postgres_client::PostgresClient::new(connection, &self.0.settings).await?;
        client.pool = Some(self.clone());
        Ok(PoolableClient {
            client: Some(client),
            pool: self.clone(),
        })
    }
}

pub struct PoolableClient<F: ConnectionFactory> {
    client: Option<crate::postgres_client::PostgresClient<F>>,
    pool: PostgresPool<F>,
}

impl<F: ConnectionFactory> Deref for PoolableClient<F> {
    type Target = crate::postgres_client::PostgresClient<F>;

    fn deref(&self) -> &Self::Target {
        self.client
            .as_ref()
            .expect("client already returned to pool")
    }
}

impl<F: ConnectionFactory> DerefMut for PoolableClient<F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.client
            .as_mut()
            .expect("client already returned to pool")
    }
}

impl<F: ConnectionFactory> Drop for PoolableClient<F> {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            self.pool.return_connection(client);
        }
    }
}
