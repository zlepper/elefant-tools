use crate::protocol;
use crate::protocol::FieldDescription;
use std::error::Error;
use std::fmt::{Display, Formatter};

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
        original_error: Box<dyn Error + Sync + Send>,
        column_index: usize,
    },
    NoResultsReturned,
    UnexpectedNullValue {
        postgres_field: FieldDescription,
    },
    NotEnoughColumns {
        desired: usize,
        actual: usize,
    },
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
                write!(f, "IO error: {e}")
            }
            ElefantClientError::PostgresMessageParseError(e) => {
                write!(f, "Postgres message parse error: {e}")
            }
            ElefantClientError::UnexpectedBackendMessage(e) => {
                write!(f, "Unexpected backend message: {e}")
            }
            ElefantClientError::PostgresError(e) => {
                write!(f, "Postgres error: {e}")
            }
            ElefantClientError::UnsupportedFieldType {
                postgres_field,
                desired_rust_type,
            } => {
                write!(f, "Unsupported field type: {postgres_field:?} for desired rust type: {desired_rust_type}")
            }
            ElefantClientError::DataTypeParseError {
                column_index,
                original_error,
            } => {
                write!(
                    f,
                    "Error while parsing response data: {original_error:?} as index {column_index}"
                )
            }
            ElefantClientError::NoResultsReturned => {
                write!(f, "No results returned from query.")
            }
            ElefantClientError::UnexpectedNullValue { postgres_field } => {
                write!(
                    f,
                    "Unexpected null value when processing field: {postgres_field:?}."
                )
            }
            ElefantClientError::NotEnoughColumns { desired, actual } => {
                write!(
                    f,
                    "Not enough columns returned. Desired: {desired}, Actual: {actual}"
                )
            }
        }
    }
}

impl Error for ElefantClientError {}
