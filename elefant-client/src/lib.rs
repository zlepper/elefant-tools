use std::io::Error;
use crate::protocol::{BackendMessage, PostgresConnection};

mod protocol;
#[cfg(feature = "tokio")]
mod tokio_connection;
mod postgres_client;

#[derive(Debug)]
pub enum ElefantClientError {
    IoError(std::io::Error),
    PostgresMessageParseError(protocol::PostgresMessageParseError),
    UnexpectedBackendMessage(String),
    PostgresError(String),
}



impl From<std::io::Error> for ElefantClientError {
    fn from(value: Error) -> Self {
        ElefantClientError::IoError(value)
    }
}

impl From<protocol::PostgresMessageParseError> for ElefantClientError {
    fn from(value: protocol::PostgresMessageParseError) -> Self {
        ElefantClientError::PostgresMessageParseError(value)
    }
}


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


