#![allow(dead_code)]

mod error;
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
    #[cfg(feature = "rustls")]
    pub tls: TlsSettings,
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
