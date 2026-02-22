#![allow(dead_code)]

mod error;
pub mod pool;
mod postgres_client;
pub mod profiler;
mod protocol;
#[cfg(test)]
mod test_helpers;
#[cfg(feature = "tokio")]
pub mod tokio_connection;
mod types;

pub use error::ElefantClientError;
pub use pool::{ConnectionFactory, PoolableClient, PostgresPool};
pub use postgres_client::*;
pub use protocol::FieldDescription;
pub use types::*;

#[derive(Clone)]
pub struct PostgresConnectionSettings {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub options: Option<String>,
}

impl Default for PostgresConnectionSettings {
    fn default() -> Self {
        Self {
            database: "postgres".to_string(),
            port: 5432,
            password: "".to_string(),
            host: "localhost".to_string(),
            user: "postgres".to_string(),
            options: None,
        }
    }
}
