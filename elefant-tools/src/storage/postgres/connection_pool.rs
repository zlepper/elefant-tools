use std::sync::Arc;
use tokio::sync::Mutex;
use crate::{AsyncCleanup, PostgresClientWrapper};

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

    pub async fn get_connection(&self) -> Option<PostgresClientWrapper> {
        let mut pool = self.connection_pool.lock().await;
        pool.pop()
    }

    pub async fn release_connection(&self, connection: PostgresClientWrapper) {
        let mut pool = self.connection_pool.lock().await;
        pool.push(connection);
    }
}


pub struct ReleaseConnection {
    pool: ConnectionPool,
    connection: PostgresClientWrapper,
}

impl ReleaseConnection {
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
