use std::error::Error;
use std::fmt::{Display, Formatter};
use crate::protocol::{BackendMessage, FieldDescription, PostgresConnection};

mod protocol;
#[cfg(feature = "tokio")]
mod tokio_connection;
mod postgres_client;
#[cfg(test)]
mod test_helpers;

#[derive(Debug)]
pub enum ElefantClientError {
    IoError(std::io::Error),
    PostgresMessageParseError(protocol::PostgresMessageParseError),
    UnexpectedBackendMessage(String),
    PostgresError(String),
    UnsupportedFieldType {
        desired_rust_type: &'static str,
        postgres_field: FieldDescription,
    },
    DataTypeParseError {
        original_error: Box<dyn Error>,
        column_index: usize,
    }
}



impl From<std::io::Error> for ElefantClientError {
    fn from(value: std::io::Error) -> Self {
        ElefantClientError::IoError(value)
    }
}

impl From<protocol::PostgresMessageParseError> for ElefantClientError {
    fn from(value: protocol::PostgresMessageParseError) -> Self {
        ElefantClientError::PostgresMessageParseError(value)
    }
}

impl Display for ElefantClientError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ElefantClientError::IoError(e) => {
                write!(f, "IO error: {}", e)
            }
            ElefantClientError::PostgresMessageParseError(e) => {
                write!(f, "Postgres message parse error: {}", e)
            }
            ElefantClientError::UnexpectedBackendMessage(e) => {
                write!(f, "Unexpected backend message: {}", e)
            }
            ElefantClientError::PostgresError(e) => {
                write!(f, "Postgres error: {}", e)
            }
            ElefantClientError::UnsupportedFieldType { postgres_field, desired_rust_type } => {
                write!(f, "Unsupported field type: {:?} for desired rust type: {}", postgres_field, desired_rust_type)
            }
            ElefantClientError::DataTypeParseError{column_index, original_error} => {
                write!(f, "Error while parsing response data: {:?} as index {}", original_error, column_index)
            }
        }
    }
}

impl Error for ElefantClientError {
    
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


