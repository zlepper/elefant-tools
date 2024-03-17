use std::sync::Arc;
use tokio::sync::Mutex;
use crate::{AsyncCleanup, PostgresClientWrapper};

/// A very simple connection pool for Postgres connections.
#[derive(Clone, Default)]
pub struct ConnectionPool {
    connection_pool: Arc<Mutex<Vec<PostgresClientWrapper>>>,
}

impl ConnectionPool {
    pub fn new() -> Self {
        Self {
            connection_pool: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Get a connection from the pool. If the pool is empty, this will return `None`.
    pub async fn get_connection(&self) -> Option<PostgresClientWrapper> {
        let mut pool = self.connection_pool.lock().await;
        pool.pop()
    }

    /// Release a connection back to the pool.
    pub async fn release_connection(&self, connection: PostgresClientWrapper) {
        let mut pool = self.connection_pool.lock().await;
        pool.push(connection);
    }
}

/// A wrapper around a connection that will release the connection back to the pool when `cleanup`
/// is called. 
pub struct ReleaseConnection {
    pool: ConnectionPool,
    connection: PostgresClientWrapper,
}

impl ReleaseConnection {
    /// Create a new `ReleaseConnection` instance.
    pub fn new(pool: ConnectionPool, connection: PostgresClientWrapper) -> Self {
        Self { pool, connection }
    }
}


impl AsyncCleanup for ReleaseConnection {
    async fn cleanup(self) -> crate::Result<()> {
        self.pool.release_connection(self.connection).await;
        Ok(())
    }
}
