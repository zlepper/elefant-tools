use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use crate::models::PostgresDatabase;
use crate::*;

mod elefant_file;
mod sql_file;
mod postgres_instance;

// pub use elefant_file::ElefantFileDestinationStorage;
pub use sql_file::SqlFile;
use crate::models::PostgresSchema;
use crate::models::PostgresTable;

#[async_trait]
pub trait BaseCopyTarget {
    /// Which data format is supported by this destination.
    async fn supported_data_format(&self) -> Result<Vec<DataFormat>>;
}

#[async_trait]
pub trait CopySource: BaseCopyTarget {
    type DataStream: Stream<Item=Result<Bytes>> + Send;

    async fn get_introspection(&self) -> Result<PostgresDatabase>;

    async fn get_data(&self, schema: &PostgresSchema, table: &PostgresTable, data_format: &DataFormat) -> Result<TableData<Self::DataStream>>;
}

#[async_trait]
pub trait CopyDestination: BaseCopyTarget {
    /// This should apply the very basic structure, meaning schemas and tables with their
    /// columns and primary key. It should not apply any constraints or indexes.
    async fn apply_structure(&mut self, db: &PostgresDatabase) -> Result<()>;

    /// This should apply the data to the destination. The data is expected to be in the
    /// format returned by `supported_data_format`, if possible.
    async fn apply_data<S: Stream<Item=Result<Bytes>> + Send>(&mut self, schema: &PostgresSchema, table: &PostgresTable, data: TableData<S>) -> Result<()>;

    /// This should apply the constraints and indexes to the destination.
    async fn apply_post_structure(&mut self, db: &PostgresDatabase) -> Result<()>;
}

#[derive(Debug, Clone)]
pub enum DataFormat {
    /// Slightly slower, but works across postgres versions, is human readable and can be
    /// outputted in text files.
    Text,

    /// Faster, but has strict requirements to the postgres version and is not human readable.
    PostgresBinary {
        postgres_version: Option<String>,
    },
}

impl PartialEq for DataFormat {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (DataFormat::Text, DataFormat::Text) => true,
            (DataFormat::PostgresBinary { postgres_version: left_pg_version }, DataFormat::PostgresBinary { postgres_version: right_pg_version }) => match (left_pg_version, right_pg_version) {
                (None, _) => true,
                (_, None) => true,
                (Some(left), Some(right)) => left == right,
            },
            _ => false,
        }
    }
}

pub enum TableData<S: Stream<Item=Result<Bytes>> + Send> {
    /// Data is provided as a stream in the Postgres "Text" format
    Text {
        data: S,
    },

    /// Data is provided as a stream in the Postgres "Binary" format
    PostgresBinary {
        postgres_version: String,
        data: S,
    },
}

impl<S: Stream<Item=Result<Bytes>> + Send> TableData<S> {
    pub fn into_stream(self) -> S {
        match self {
            TableData::Text { data } => data,
            TableData::PostgresBinary { data, .. } => data,
        }
    }

    pub fn get_data_format(&self) -> DataFormat {
        match self {
            TableData::Text { .. } => DataFormat::Text,
            TableData::PostgresBinary { postgres_version, .. } => DataFormat::PostgresBinary {
                postgres_version: Some(postgres_version.to_string()),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    pub static SOURCE_DATABASE_CREATE_SCRIPT: &str = r#"
        create table people(
            id serial primary key,
            name text not null,
            age int not null check (age > 0),
            constraint multi_check check (name != 'fsgsdfgsdf' and age < 9999)
        );

        create index people_age_idx on people (age desc) include (name, id) where (age % 2 = 0);
        create index people_age_brin_idx on people using brin (age);
        create index people_name_lower_idx on people (lower(name));

        insert into people(name, age)
        values
            ('foo', 42),
            ('bar', 89),
            ('nice', 69),
            (E'str\nange', 420),
            (E't\t\tap', 421),
            (E'q''t', 12)
            ;

        create table tree_node(
            id serial primary key,
            name text not null,
            -- parent_id int references tree_node(id),
            parent_id int,
            constraint unique_name_per_level unique nulls not distinct (parent_id, name)
        );
    "#;

    pub fn get_expected_data() -> Vec<(i32, String, i32)> {
        vec![
            (1, "foo".to_string(), 42),
            (2, "bar".to_string(), 89),
            (3, "nice".to_string(), 69),
            (4, "str\nange".to_string(), 420),
            (5, "t\t\tap".to_string(), 421),
            (6, "q't".to_string(), 12),
        ]
    }


}