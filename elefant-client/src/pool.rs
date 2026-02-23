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
    ) -> impl std::future::Future<Output = Result<PostgresConnection<Self::Connection>, ElefantClientError>>;
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

    /// Gracefully close all idle connections by sending Terminate to each.
    /// Currently checked-out clients are not affected — close them individually
    /// via [`PoolableClient::close`] or let them return to the pool first.
    pub async fn close(&self) -> Result<(), ElefantClientError> {
        let clients: Vec<_> = self.0.idle_connections.lock().unwrap().drain(..).collect();
        for client in clients {
            client.close().await?;
        }
        Ok(())
    }

    pub async fn get_client(
        &self,
    ) -> Result<PoolableClient<F>, ElefantClientError> {
        // Try to reuse an idle connection
        loop {
            let idle_client = { self.0.idle_connections.lock().unwrap().pop() };
            match idle_client {
                Some(mut client) => {
                    match client.reset().await {
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
                    }
                }
                None => break,
            }
        }

        // No idle connection available, create a new one
        let connection = self.0.factory.create_connection(&self.0.settings).await?;
        let mut client = crate::postgres_client::PostgresClient::new(connection, &self.0.settings).await?;
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
        self.client.as_ref().expect("client already returned to pool")
    }
}

impl<F: ConnectionFactory> DerefMut for PoolableClient<F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.client.as_mut().expect("client already returned to pool")
    }
}

impl<F: ConnectionFactory> PoolableClient<F> {
    /// Gracefully close this connection by sending Terminate to the backend.
    /// The connection is not returned to the pool. Consumes the client.
    pub async fn close(mut self) -> Result<(), ElefantClientError> {
        match self.client.take() {
            Some(client) => client.close().await,
            None => Ok(()),
        }
    }
}

impl<F: ConnectionFactory> Drop for PoolableClient<F> {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            self.pool.return_connection(client);
        }
    }
}

#[cfg(test)]
impl<F: ConnectionFactory> PostgresPool<F> {
    pub(crate) fn idle_connection_count(&self) -> usize {
        self.0.idle_connections.lock().unwrap().len()
    }
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use crate::test_helpers::get_settings;
    use crate::tokio_connection::{new_client, TokioConnectionFactory, TokioPostgresPool};

    #[tokio::test]
    async fn pool_reuses_connection_after_drop() {
        let pool = TokioPostgresPool::new(TokioConnectionFactory, get_settings());

        let pid1: i32;
        {
            let mut client = pool.get_client().await.unwrap();
            pid1 = client.read_single_value_simple("select pg_backend_pid()").await;
        }

        let mut client2 = pool.get_client().await.unwrap();
        let pid2: i32 = client2.read_single_value_simple("select pg_backend_pid()").await;

        assert_eq!(pid1, pid2, "Pool should reuse the same connection");
    }

    #[tokio::test]
    async fn pool_creates_new_connection_when_all_checked_out() {
        let pool = TokioPostgresPool::new(TokioConnectionFactory, get_settings());

        let mut client1 = pool.get_client().await.unwrap();
        let mut client2 = pool.get_client().await.unwrap();

        let pid1: i32 = client1.read_single_value_simple("select pg_backend_pid()").await;
        let pid2: i32 = client2.read_single_value_simple("select pg_backend_pid()").await;

        assert_ne!(pid1, pid2, "Simultaneously checked-out clients must be different connections");
    }

    #[tokio::test]
    async fn pool_idle_count_grows_on_drop() {
        let pool = TokioPostgresPool::new(TokioConnectionFactory, get_settings());

        assert_eq!(pool.idle_connection_count(), 0);

        let client1 = pool.get_client().await.unwrap();
        let client2 = pool.get_client().await.unwrap();
        assert_eq!(pool.idle_connection_count(), 0);

        drop(client1);
        assert_eq!(pool.idle_connection_count(), 1);

        drop(client2);
        assert_eq!(pool.idle_connection_count(), 2);
    }

    #[tokio::test]
    async fn pool_reset_rolls_back_uncommitted_transaction() {
        let pool = TokioPostgresPool::new(TokioConnectionFactory, get_settings());

        {
            let mut client = pool.get_client().await.unwrap();
            client.execute_non_query_simple("BEGIN").await.unwrap();
            client
                .execute_non_query_simple("CREATE TEMP TABLE pool_txn_test (id int)")
                .await
                .unwrap();
            // Intentionally do NOT commit -- drop returns to pool
        }

        // get_client() calls reset() which rolls back the transaction
        let mut client = pool.get_client().await.unwrap();

        // The temp table should not exist because the transaction was rolled back
        let result = client
            .execute_non_query_simple("SELECT 1 FROM pool_txn_test")
            .await;
        assert!(result.is_err(), "Temp table should not exist after rollback");
    }

    #[tokio::test]
    async fn pool_reset_recovers_from_failed_transaction() {
        let pool = TokioPostgresPool::new(TokioConnectionFactory, get_settings());

        {
            let mut client = pool.get_client().await.unwrap();
            client.execute_non_query_simple("BEGIN").await.unwrap();
            // This will fail and put the connection into InFailedTransaction state
            let _ = client
                .execute_non_query_simple("SELECT * FROM nonexistent_table_that_does_not_exist")
                .await;
            // Drop without ROLLBACK or COMMIT
        }

        // reset() should detect InFailedTransaction and issue ROLLBACK
        let mut client = pool.get_client().await.unwrap();
        let value: i32 = client.read_single_value_simple("select 42").await;
        assert_eq!(value, 42);
    }

    #[tokio::test]
    async fn pool_clone_shares_state() {
        let pool1 = TokioPostgresPool::new(TokioConnectionFactory, get_settings());
        let pool2 = pool1.clone();

        let pid1: i32;
        {
            let mut client = pool1.get_client().await.unwrap();
            pid1 = client.read_single_value_simple("select pg_backend_pid()").await;
        }

        let mut client2 = pool2.get_client().await.unwrap();
        let pid2: i32 = client2.read_single_value_simple("select pg_backend_pid()").await;

        assert_eq!(pid1, pid2, "Cloned pool should share the idle connection vec");
    }

    #[tokio::test]
    async fn pool_client_works_across_multiple_reuse_cycles() {
        let pool = TokioPostgresPool::new(TokioConnectionFactory, get_settings());

        for i in 0..5 {
            let mut client = pool.get_client().await.unwrap();
            let value: i32 = client
                .read_single_value_simple(&format!("select {}", i + 1))
                .await;
            assert_eq!(value, i + 1);
        }
    }

    #[tokio::test]
    async fn pool_close_terminates_connections() {
        let pool = TokioPostgresPool::new(TokioConnectionFactory, get_settings());

        let mut client1 = pool.get_client().await.unwrap();
        let mut client2 = pool.get_client().await.unwrap();

        let pid1: i32 = client1.read_single_value_simple("select pg_backend_pid()").await;
        let pid2: i32 = client2.read_single_value_simple("select pg_backend_pid()").await;

        // Close one client directly (bypasses pool return)
        client1.close().await.unwrap();

        // Drop the other so it returns to the pool's idle vec
        drop(client2);
        assert_eq!(pool.idle_connection_count(), 1);

        // Close idle connections in the pool
        pool.close().await.unwrap();
        assert_eq!(pool.idle_connection_count(), 0);

        // No sleep needed — Terminate triggers immediate backend cleanup
        let mut checker = new_client(get_settings()).await.unwrap();
        let count: i64 = checker
            .read_single_value_simple(&format!(
                "select count(*) from pg_stat_activity where pid in ({}, {})",
                pid1, pid2
            ))
            .await;
        assert_eq!(count, 0, "All connections should be closed after graceful shutdown");
    }

    #[tokio::test]
    async fn pool_drop_closes_all_connections() {
        let pid1: i32;
        let pid2: i32;

        {
            let pool = TokioPostgresPool::new(TokioConnectionFactory, get_settings());

            let mut client1 = pool.get_client().await.unwrap();
            let mut client2 = pool.get_client().await.unwrap();

            pid1 = client1.read_single_value_simple("select pg_backend_pid()").await;
            pid2 = client2.read_single_value_simple("select pg_backend_pid()").await;

            // Drop clients first (returns to idle vec), then drop pool (closes connections)
            drop(client1);
            drop(client2);
        }
        // pool is dropped here -- Arc refcount hits 0, idle connections are dropped, TCP streams close

        // Small delay to let PostgreSQL clean up the backend processes
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Use a separate connection to verify the old connections are gone
        let mut checker = new_client(get_settings()).await.unwrap();
        let count: i64 = checker
            .read_single_value_simple(&format!(
                "select count(*) from pg_stat_activity where pid in ({}, {})",
                pid1, pid2
            ))
            .await;
        assert_eq!(count, 0, "All pool connections should be closed after pool drop");
    }
}
