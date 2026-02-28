#![allow(dead_code)]

pub mod batch;
mod error;
#[cfg(feature = "pg_interval")]
pub mod pg_interval;
pub mod pool;
mod postgres_client;
pub mod profiler;
mod protocol;
#[cfg(test)]
mod test_helpers;
#[cfg(feature = "rustls")]
pub mod tls;
#[cfg(feature = "tokio")]
pub mod tokio_connection;
mod types;

pub use batch::{CollectBatch, FlattenTuple, TupleAppend};
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
    #[cfg(feature = "rustls")]
    pub tls: TlsSettings,
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

    #[cfg(feature = "rustls")]
    pub fn tls(mut self, tls: TlsSettings) -> Self {
        self.tls = tls;
        self
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
            #[cfg(feature = "rustls")]
            tls: TlsSettings::disable(),
        }
    }
}

#[cfg(feature = "rustls")]
#[derive(Clone)]
pub enum TlsSettings {
    /// TLS is disabled — connect without encryption.
    Disabled,
    /// Try TLS, but fall back to plaintext if the server declines.
    Prefer(std::sync::Arc<rustls::ClientConfig>),
    /// Require TLS — fail if the server does not support it.
    Require(std::sync::Arc<rustls::ClientConfig>),
}

#[cfg(feature = "rustls")]
impl std::fmt::Debug for TlsSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Disabled => write!(f, "Disabled"),
            Self::Prefer(_) => write!(f, "Prefer(...)"),
            Self::Require(_) => write!(f, "Require(...)"),
        }
    }
}

#[cfg(feature = "rustls")]
impl TlsSettings {
    pub fn disable() -> Self {
        Self::Disabled
    }

    pub fn prefer(config: std::sync::Arc<rustls::ClientConfig>) -> Self {
        Self::Prefer(config)
    }

    pub fn require(config: std::sync::Arc<rustls::ClientConfig>) -> Self {
        Self::Require(config)
    }

    pub fn config(&self) -> Option<&std::sync::Arc<rustls::ClientConfig>> {
        match self {
            Self::Disabled => None,
            Self::Prefer(c) | Self::Require(c) => Some(c),
        }
    }

    pub fn is_required(&self) -> bool {
        matches!(self, Self::Require(_))
    }
}
