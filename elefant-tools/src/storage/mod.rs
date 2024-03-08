use std::sync::Arc;
use bytes::Bytes;
use futures::Stream;
use crate::models::PostgresDatabase;
use crate::*;

mod elefant_file;
mod sql_file;
mod postgres_instance;
mod table_data;
mod data_format;

// pub use elefant_file::ElefantFileDestinationStorage;
pub use sql_file::{SqlFile, SqlFileOptions, apply_sql_file, apply_sql_string};
pub use postgres_instance::PostgresInstanceStorage;
pub use data_format::*;
pub use table_data::*;
use crate::models::PostgresSchema;
use crate::models::PostgresTable;
use crate::quoting::IdentifierQuoter;

pub trait BaseCopyTarget {
    /// Which data format is supported by this destination.
    fn supported_data_format(&self) -> impl std::future::Future<Output = Result<Vec<DataFormat>>> + Send;
}

pub trait CopySourceFactory: BaseCopyTarget {
    type SequentialSource: CopySource;
    type ParallelSource: CopySource + Clone + Sync;

    fn create_source(&self) -> impl std::future::Future<Output = Result<SequentialOrParallel<Self::SequentialSource, Self::ParallelSource>>> + Send;
}

pub trait CopySource: Send {
    type DataStream: Stream<Item=Result<Bytes>> + Send;
    type Cleanup: AsyncCleanup;

    fn get_introspection(&self) -> impl std::future::Future<Output = Result<PostgresDatabase>> + Send;

    fn get_data(&self, schema: &PostgresSchema, table: &PostgresTable, data_format: &DataFormat) -> impl std::future::Future<Output = Result<TableData<Self::DataStream, Self::Cleanup>>> + Send;
}


pub trait CopyDestinationFactory<'a>: BaseCopyTarget {
    type SequentialDestination: CopyDestination;
    type ParallelDestination: CopyDestination + Clone + Sync;

    fn create_destination(&'a mut self) -> impl std::future::Future<Output = Result<SequentialOrParallel<Self::SequentialDestination, Self::ParallelDestination>>> + Send;
}

pub trait CopyDestination: Send {
    /// This should apply the data to the destination. The data is expected to be in the
    /// format returned by `supported_data_format`, if possible.
    fn apply_data<S: Stream<Item=Result<Bytes>> + Send, C: AsyncCleanup>(&mut self, schema: &PostgresSchema, table: &PostgresTable, data: TableData<S, C>) -> impl std::future::Future<Output = Result<()>> + Send;

    /// This should apply the DDL statements to the destination.
    fn apply_transactional_statement(&mut self, statement: &str) -> impl std::future::Future<Output = Result<()>> + Send;

    /// This should apply the DDL statements to the destination.
    fn apply_non_transactional_statement(&mut self, statement: &str) -> impl std::future::Future<Output = Result<()>> + Send;

    fn begin_transaction(&mut self) ->impl std::future::Future<Output = Result<()>> + Send;
    
    fn commit_transaction(&mut self) -> impl std::future::Future<Output = Result<()>> + Send;
    
    fn get_identifier_quoter(&self) -> Arc<IdentifierQuoter>;
}

pub enum SequentialOrParallel<S: Send, P: Send + Clone + Sync> {
    Sequential(S),
    Parallel(P),
}

impl< S: CopySource, P: CopySource + Clone + Sync> SequentialOrParallel<S, P> 
{
    pub async fn get_introspection(&self) -> Result<PostgresDatabase> {
        match self {
            SequentialOrParallel::Sequential(s) => s.get_introspection().await,
            SequentialOrParallel::Parallel(p) => p.get_introspection().await,
        }
    }
    
} 

impl< S: CopyDestination, P: CopyDestination + Clone + Sync> SequentialOrParallel<S, P> 
{
    pub async fn begin_transaction(&mut self) -> Result<()> {
        match self {
            SequentialOrParallel::Sequential(s) => s.begin_transaction().await,
            SequentialOrParallel::Parallel(p) => p.begin_transaction().await,
        }
    }
    
    pub async fn commit_transaction(&mut self) -> Result<()> {
        match self {
            SequentialOrParallel::Sequential(s) => s.commit_transaction().await,
            SequentialOrParallel::Parallel(p) => p.commit_transaction().await,
        }
    }
    
} 

/// A CopyDestination that panics when used.
/// Cannot be constructed outside this module, but is available for type reference
/// to indicate Parallel copy is not supported. 
#[derive(Copy, Clone)]
pub struct ParallelCopyDestinationNotAvailable {
    _private: (),
}

impl CopyDestination for ParallelCopyDestinationNotAvailable {
    async fn apply_data<S: Stream<Item=Result<Bytes>> + Send, C: AsyncCleanup>(&mut self, _schema: &PostgresSchema, _table: &PostgresTable, _data: TableData<S, C>) -> Result<()> {
        unreachable!("Parallel copy destination not available")
    }

    async fn apply_transactional_statement(&mut self, _statement: &str) -> Result<()> {
        unreachable!("Parallel copy destination not available")
    }

    async fn apply_non_transactional_statement(&mut self, _statement: &str) -> Result<()> {
        unreachable!("Parallel copy destination not available")
    }

    async fn begin_transaction(&mut self) -> Result<()> {
        unreachable!("Parallel copy destination not available")
    }

    async fn commit_transaction(&mut self) -> Result<()> {
        unreachable!("Parallel copy destination not available")
    }

    fn get_identifier_quoter(&self) -> Arc<IdentifierQuoter> {
        unreachable!("Parallel copy destination not available")
    }
}

#[cfg(test)]
mod tests {
    use tokio_postgres::error::SqlState;
    use crate::test_helpers::{assert_pg_error, TestHelper};

    pub fn get_copy_source_database_create_script(version: i32) -> &'static str {
        if version >= 150 {
            r#"
        create extension btree_gin;

        create table people(
            id serial primary key,
            name text not null unique,
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

        create table field(
            id serial primary key
        );

        create table tree_node(
            id serial primary key,
            field_id int not null references field(id),
            name text not null,
            parent_id int,
            constraint field_id_id_unique unique (field_id, id),
            foreign key (field_id, parent_id) references tree_node(field_id, id),
            constraint unique_name_per_level unique nulls not distinct (field_id, parent_id, name)
        );

        create view people_who_cant_drink as select * from people where age < 18;

        create table ext_test_table(
            id serial primary key,
            name text not null,
            search_vector tsvector generated always as (to_tsvector('english', name)) stored
        );

        create index ext_test_table_name_idx on ext_test_table using gin (id, search_vector);

        create table array_test(
            name text[] not null
        );

        insert into array_test(name)
        values
            ('{"foo", "bar"}'),
            ('{"baz", "qux"}'),
            ('{"quux", "corge"}');

        create table my_partitioned_table(
            value int not null
        ) partition by range (value);

        create table my_partitioned_table_1 partition of my_partitioned_table for values from (1) to (10);
        create table my_partitioned_table_2 partition of my_partitioned_table for values from (10) to (20);

        insert into my_partitioned_table(value)
        values (1), (9), (11), (19);

        create table pets (
            id serial primary key,
            name text not null check(length(name) > 1)
        );

        create table dogs(
            breed text not null check(length(breed) > 1)
        ) inherits (pets);

        create table cats(
            color text not null
        ) inherits (pets);

        insert into dogs(name, breed) values('Fido', 'beagle');
        insert into cats(name, color) values('Fluffy', 'white');
        insert into pets(name) values('Remy');
            "#
        } else {
            r#"
        create extension btree_gin;

        create table people(
            id serial primary key,
            name text not null unique,
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

        create table field(
            id serial primary key
        );

        create table tree_node(
            id serial primary key,
            field_id int not null references field(id),
            name text not null,
            parent_id int,
            constraint field_id_id_unique unique (field_id, id),
            foreign key (field_id, parent_id) references tree_node(field_id, id),
            constraint unique_name_per_level unique (field_id, parent_id, name)
        );

        create view people_who_cant_drink as select * from people where age < 18;

        create table ext_test_table(
            id serial primary key,
            name text not null,
            search_vector tsvector generated always as (to_tsvector('english', name)) stored
        );

        create index ext_test_table_name_idx on ext_test_table using gin (id, search_vector);

        create table array_test(
            name text[] not null
        );

        insert into array_test(name)
        values
            ('{"foo", "bar"}'),
            ('{"baz", "qux"}'),
            ('{"quux", "corge"}');

        create table my_partitioned_table(
            value int not null
        ) partition by range (value);

        create table my_partitioned_table_1 partition of my_partitioned_table for values from (1) to (10);
        create table my_partitioned_table_2 partition of my_partitioned_table for values from (10) to (20);

        insert into my_partitioned_table(value)
        values (1), (9), (11), (19);

        create table pets (
            id serial primary key,
            name text not null check(length(name) > 1)
        );

        create table dogs(
            breed text not null check(length(breed) > 1)
        ) inherits (pets);

        create table cats(
            color text not null
        ) inherits (pets);

        insert into dogs(name, breed) values('Fido', 'beagle');
        insert into cats(name, color) values('Fluffy', 'white');
        insert into pets(name) values('Remy');
            "#
        }
    }

    pub fn get_expected_people_data() -> Vec<(i32, String, i32)> {
        vec![
            (1, "foo".to_string(), 42),
            (2, "bar".to_string(), 89),
            (3, "nice".to_string(), 69),
            (4, "str\nange".to_string(), 420),
            (5, "t\t\tap".to_string(), 421),
            (6, "q't".to_string(), 12),
        ]
    }

    pub fn get_expected_array_test_data() -> Vec<(Vec<String>, )> {
        vec![
            (vec!["foo".to_string(), "bar".to_string()], ),
            (vec!["baz".to_string(), "qux".to_string()], ),
            (vec!["quux".to_string(), "corge".to_string()], ),
        ]
    }

    pub async fn validate_pets(connection: &TestHelper) {
        let pets = connection.get_results::<(i32, String)>("select id, name from pets order by id").await;
        assert_eq!(pets, vec![
            (1, "Fido".to_string()),
            (2, "Fluffy".to_string()),
            (3, "Remy".to_string()),
        ]);

        let dogs = connection.get_results::<(i32, String, String)>("select id, name, breed from dogs order by id").await;
        assert_eq!(dogs, vec![
            (1, "Fido".to_string(), "beagle".to_string()),
        ]);

        let cats = connection.get_results::<(i32, String, String)>("select id, name, color from cats order by id").await;
        assert_eq!(cats, vec![
            (2, "Fluffy".to_string(), "white".to_string()),
        ]);
    }

    pub async fn validate_copy_state(destination: &TestHelper) {
        let items = destination.get_results::<(i32, String, i32)>("select id, name, age from people;").await;

        assert_eq!(items, get_expected_people_data());

        let result = destination.get_conn().execute_non_query("insert into people (name, age) values ('new-value', 10000)").await;
        assert_pg_error(result, SqlState::CHECK_VIOLATION);

        let result = destination.get_conn().execute_non_query("insert into people (name, age) values ('foo', 100)").await;
        assert_pg_error(result, SqlState::UNIQUE_VIOLATION);

        destination.execute_not_query("insert into field (id) values (1);").await;

        destination.execute_not_query("insert into tree_node(id, field_id, name, parent_id) values (1, 1, 'foo', null), (2, 1, 'bar', 1)").await;
        if destination.get_conn().version() >= 150 {
            let result = destination.get_conn().execute_non_query("insert into tree_node(id, field_id, name, parent_id) values (3, 1, 'foo', null)").await;
            assert_pg_error(result, SqlState::UNIQUE_VIOLATION);
        }

        let result = destination.get_conn().execute_non_query("insert into tree_node(id, field_id, name, parent_id) values (9999, 9999, 'foobarbaz', null)").await;
        assert_pg_error(result, SqlState::FOREIGN_KEY_VIOLATION);

        let people_who_cant_drink = destination.get_results::<(i32, String, i32)>("select id, name, age from people_who_cant_drink;").await;
        assert_eq!(people_who_cant_drink, vec![(6, "q't".to_string(), 12)]);

        let array_test_data = destination.get_results::<(Vec<String>, )>("select name from array_test;").await;

        assert_eq!(array_test_data, get_expected_array_test_data());

        let partition_test_data = destination.get_results::<(i32, )>("select value from my_partitioned_table order by value;").await;

        assert_eq!(partition_test_data, vec![(1, ), (9, ), (11, ), (19, )]);

        validate_pets(destination).await;
    }
}
