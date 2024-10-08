use crate::storage::DataFormat;
use thiserror::Error;

/// All the errors that can occur in the elefant-tools library
#[non_exhaustive]
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

    #[error(
        "Invalid number of results returned from query. Expected `{expected}`, got `{actual}`"
    )]
    InvalidNumberOfResults { actual: usize, expected: usize },

    #[error("Unknown constraint type '{0}'")]
    UnknownConstraintType(String),

    #[error("Unknown foreign key action '{0}'")]
    UnknownForeignKeyAction(String),

    #[error("Unknown function kind '{0}'")]
    UnknownFunctionKind(String),

    #[error("Unknown volatility '{0}'")]
    UnknownVolatility(String),

    #[error("Unknown parallel '{0}'")]
    UnknownParallel(String),

    #[error("Unknown aggregate function final modify '{0}'")]
    UnknownAggregateFinalFunctionModify(String),

    #[error("Unknown trigger level '{0}'")]
    UnknownTriggerLevel(String),

    #[error("Unknown trigger timing '{0}'")]
    UnknownTriggerTiming(String),

    #[error("Unknown trigger event '{0}'")]
    UnknownTriggerEvent(String),

    #[error("Unknown column identity '{0}'")]
    UnknownColumnIdentity(String),

    #[error("Unknown table type '{0}'")]
    InvalidTableType(String),

    #[error("Unknown keyword type '{0}'")]
    InvalidKeywordType(String),

    #[error("Unknown table partitioning strategy '{0}'")]
    InvalidTablePartitioningStrategy(String),

    #[error("The table '{0}' is a partitioned table and does not have a parent table")]
    PartitionedTableWithoutParent(String),

    #[error(
        "The table '{table}' is a partitioned table and has multiple parent tables: {parents:?}"
    )]
    PartitionedTableHasMultipleParent { table: String, parents: Vec<String> },

    #[error("The table '{0}' is a partitioned table and does not have a partition expression")]
    PartitionedTableWithoutExpression(String),

    #[error("The table '{0}' is a partitioned table and does not have partition columns")]
    PartitionedTableWithoutPartitionColumns(String),

    #[error("The table '{0}' is a partitioned table and has both partition columns and a partition expression")]
    PartitionedTableWithBothPartitionColumnsAndExpression(String),

    #[error("Unsupported postgres version: {0}. Minimum supported version is 12")]
    UnsupportedPostgresVersion(i32),

    #[error("Invalid response from postgres when checking version")]
    InvalidPostgresVersionResponse,

    #[error("Hypertable '{table_name}' dimension '{dimension_number}' does not have an interval")]
    HypertableDimensionWithoutInterval {
        table_name: String,
        dimension_number: i64,
    },

    #[error("io error: `{0}`")]
    IoError(#[from] std::io::Error),

    #[error("Data formats are not compatible between source and target. Supported by target: {supported_by_target:?}, supported by source: {supported_by_source:?}, required format: {required_format:?}")]
    DataFormatsNotCompatible {
        supported_by_target: Vec<DataFormat>,
        supported_by_source: Vec<DataFormat>,
        required_format: Option<DataFormat>,
    },

    #[error("join error: `{0}`")]
    JoinError(#[from] tokio::task::JoinError),

    #[error("Aggregate function '{0}' is missing transition type")]
    AggregateFunctionMissingTransitionType(String),

    #[error("Aggregate function '{0}' is missing transition function")]
    AggregateFunctionMissingTransitionFunction(String),
}

/// A result type that uses the ElefantToolsError as the error type
pub type Result<T = ()> = std::result::Result<T, ElefantToolsError>;
