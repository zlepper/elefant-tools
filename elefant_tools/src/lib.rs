use thiserror::Error;

mod postgres_client_wrapper;
#[cfg(test)]
mod test_helpers;
mod schema_reader;
mod models;
mod schema_importer;
mod ddl_query_builder;

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
}

pub type Result<T = ()> = std::result::Result<T, ElefantToolsError>;