use thiserror::Error;
use crate::storage::DataFormat;

#[derive(Error, Debug)]
pub enum ElefantToolsError {
    #[error("Error from postgres: `{0}`")]
    PostgresError(#[from] tokio_postgres::Error),

    #[error("Error from postgres: `{query}` when executing query: `{source}`")]
    PostgresErrorWithQuery {
        query: String,
        #[source]
        source: tokio_postgres::Error,
    },

    #[error("Invalid number of results returned from query. Expected `{expected}`, got `{actual}`")]
    InvalidNumberOfResults {
        actual: usize,
        expected: usize,
    },

    #[error("Unknown constraint type '{0}'")]
    UnknownConstraintType(String),

    #[error("io error: `{0}`")]
    IoError(#[from] std::io::Error),

    #[error("Data formats are not compatible between source and target. Supported by target: {supported_by_target:?}, supported by source: {supported_by_source:?}, required format: {required_format:?}")]
    DataFormatsNotCompatible {
        supported_by_target: Vec<DataFormat>,
        supported_by_source: Vec<DataFormat>,
        required_format: Option<DataFormat>,
    },
}

pub type Result<T = ()> = std::result::Result<T, ElefantToolsError>;
