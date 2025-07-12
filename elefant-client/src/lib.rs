#![allow(dead_code)]

mod protocol;
#[cfg(feature = "tokio")]
pub mod tokio_connection;
#[cfg(feature = "monoio")]
pub mod monoio_connection;
mod postgres_client;
#[cfg(test)]
mod test_helpers;
mod error;
mod types;
pub mod profiler;

pub use error::ElefantClientError;
pub use postgres_client::*;
pub use types::*;

#[derive(Clone)]
pub struct PostgresConnectionSettings {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
}

impl Default for PostgresConnectionSettings {
    fn default() -> Self {
        Self {
            database: "postgres".to_string(),
            port: 5432,
            password: "".to_string(),
            host: "localhost".to_string(),
            user: "postgres".to_string(),
        }
    }
}


