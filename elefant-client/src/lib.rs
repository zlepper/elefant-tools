#![allow(dead_code)]

mod error;
#[cfg(feature = "pg_interval")]
pub mod pg_interval;
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
#[cfg(feature = "pg_interval")]
pub use pg_interval::Interval;
pub use pool::{ConnectionFactory, PoolableClient, PostgresPool};
pub use postgres_client::*;
pub use protocol::FieldDescription;
pub use types::*;

#[cfg(feature = "derive")]
pub use elefant_client_macros::PostgresEnum;

#[derive(Clone)]
pub struct PostgresConnectionSettings {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub options: Option<String>,
    enum_type_names: Vec<&'static str>,
}

impl PostgresConnectionSettings {
    pub fn new(host: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            ..Self::default()
        }
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn user(mut self, user: impl Into<String>) -> Self {
        self.user = user.into();
        self
    }

    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = password.into();
        self
    }

    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.database = database.into();
        self
    }

    pub fn options(mut self, options: impl Into<String>) -> Self {
        self.options = Some(options.into());
        self
    }

    /// Register a PostgreSQL enum type for OID resolution at pool creation time.
    pub fn register_enum<T: PostgresEnum>(mut self) -> Self {
        if !self.enum_type_names.contains(&T::PG_TYPE_NAME) {
            self.enum_type_names.push(T::PG_TYPE_NAME);
        }

        self
    }

    /// Returns the registered enum type names.
    pub fn enum_type_names(&self) -> &[&'static str] {
        &self.enum_type_names
    }
}

impl Default for PostgresConnectionSettings {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password: String::new(),
            database: "postgres".to_string(),
            options: None,
            enum_type_names: Vec::new(),
        }
    }
}
