use crate::models::PostgresDatabase;
use crate::*;
use std::sync::Arc;

mod data_format;
mod elefant_file;
mod postgres;
mod sql_file;
mod table_data;

// pub use elefant_file::ElefantFileDestinationStorage;
use crate::models::PostgresSchema;
use crate::models::PostgresTable;
use crate::quoting::IdentifierQuoter;
pub use data_format::*;
pub use postgres::PostgresInstanceStorage;
pub use sql_file::{apply_sql_file, apply_sql_string, SqlDataMode, SqlFile, SqlFileOptions};
pub use table_data::*;

/// A trait for thing that are either a CopyDestination or CopySource.
pub trait BaseCopyTarget {
    /// Which data format is supported by this destination/source.
    fn supported_data_format(&self) -> impl std::future::Future<Output = Result<Vec<DataFormat>>>;
}

/// A factory for providing copy sources. This is used to create a source that can be used to read data from.
pub trait CopySourceFactory: BaseCopyTarget {
    /// A type that can be used to read data from the source. This type has to support
    /// single threaded reading, but can support multiple threads reading at the same time.
    type SequentialSource: CopySource;

    /// A type that can be used to read data from the source. This type has to support
    /// multiple threads reading at the same time.
    type ParallelSource: CopySource + Clone;

    /// Should provide introspection data of the source. This means poking the `pg_catalog` tables when
    /// working with Postgres, for example.
    fn get_introspection(&self) -> impl std::future::Future<Output = Result<PostgresDatabase>>;

    /// Should create whatever type is needed to be able to read data from the source.
    fn create_source(
        &self,
    ) -> impl std::future::Future<
        Output = Result<SequentialOrParallel<Self::SequentialSource, Self::ParallelSource>>,
    >;

    /// Should create a datasource that works with single threaded reading.
    fn create_sequential_source(
        &self,
    ) -> impl std::future::Future<Output = Result<Self::SequentialSource>>;

    /// Should return what kind of parallelism is supported by the source. This is used
    /// for negotiation with the destination.
    fn supported_parallelism(&self) -> SupportedParallelism;
}

/// A copy source is something that can be used to read data from a source.
pub trait CopySource: Send {
    /// The type of the specific data reader provided when reading data
    type DataReader<'a>: TableDataReader + 'a
    where
        Self: 'a;

    /// The type of the cleanup that is returned when reading data. Can be `()` if no cleanup is needed.
    type Cleanup: AsyncCleanup;

    /// Should return a data-reader for the specified type in the specified format.
    fn get_data<'a>(
        &'a mut self,
        schema: &'a PostgresSchema,
        table: &'a PostgresTable,
        data_format: &'a DataFormat,
    ) -> impl std::future::Future<Output = Result<TableData<Self::DataReader<'a>, Self::Cleanup>>> + 'a;
}

/// A factory for providing copy destinations. This is used to create a destination that can be used to write data to.
pub trait CopyDestinationFactory<'a>: BaseCopyTarget {
    /// The implementation type when dealing with single-threaded workloads. The can optionally
    /// support multi-threading, but it is not needed.
    type SequentialDestination: CopyDestination;

    /// The implementation type when dealing with multithreaded workloads. This type has to support
    /// multi-threading.
    type ParallelDestination: CopyDestination + Clone;

    /// Should create whatever type is needed to be able to write data to the destination.
    fn create_destination(
        &'a mut self,
    ) -> impl std::future::Future<
        Output = Result<
            SequentialOrParallel<Self::SequentialDestination, Self::ParallelDestination>,
        >,
    >;

    /// Should create a destination that works with single threaded writing.
    fn create_sequential_destination(
        &'a mut self,
    ) -> impl std::future::Future<Output = Result<Self::SequentialDestination>>;

    /// Should return what kind of parallelism is supported by the destination. This is used
    /// for negotiation with the source.
    fn supported_parallelism(&self) -> SupportedParallelism;
}

/// A transaction on a copy destination. Returned by `CopyDestination::begin_transaction`.
/// All transactional DDL statements should be applied through this type, and the transaction
/// must be committed when done.
pub trait CopyTransaction: Send {
    /// Apply a DDL statement within this transaction.
    fn apply_statement(&mut self, statement: &str)
        -> impl std::future::Future<Output = Result<()>>;

    /// Commit the transaction.
    fn commit(self) -> impl std::future::Future<Output = Result<()>>;
}

pub trait CopyDestination: Send {
    /// The transaction type returned by `begin_transaction`.
    type Transaction<'a>: CopyTransaction + 'a
    where
        Self: 'a;

    /// This should apply the data to the destination. The data is expected to be in the
    /// format returned by `supported_data_format`, if possible.
    fn apply_data<R: TableDataReader, C: AsyncCleanup>(
        &mut self,
        schema: &PostgresSchema,
        table: &PostgresTable,
        data: TableData<R, C>,
    ) -> impl std::future::Future<Output = Result<()>>;

    /// This should apply the DDL statements to the destination.
    /// These commands has to be run outside a transaction, as they might fail otherwise.
    fn apply_non_transactional_statement(
        &mut self,
        statement: &str,
    ) -> impl std::future::Future<Output = Result<()>>;

    /// Should begin a new transaction and return a handle to it.
    fn begin_transaction(
        &mut self,
    ) -> impl std::future::Future<Output = Result<Self::Transaction<'_>>>;

    /// Should get the identifier quoter that works with this destination. This ensures
    /// quoting respects the rules of the destination, not the source.
    fn get_identifier_quoter(&self) -> Arc<IdentifierQuoter>;

    fn finish(&mut self) -> impl std::future::Future<Output = Result<()>> {
        async { Ok(()) }
    }

    /// Should try to introspect the destination. If introspection is not supported, this should return `Ok(None)`,
    /// not an error. Errors should only be returned if introspection is supported, but failed.
    fn try_introspect(
        &self,
    ) -> impl std::future::Future<Output = Result<Option<PostgresDatabase>>> {
        async { Ok(None) }
    }

    fn has_data_in_table(
        &self,
        _schema: &PostgresSchema,
        _table: &PostgresTable,
    ) -> impl std::future::Future<Output = Result<bool>> {
        async { Ok(false) }
    }
}

/// A type that can be either a sequential or parallel source or destination.
pub enum SequentialOrParallel<S: Send, P: Send + Clone> {
    Sequential(S),
    Parallel(P),
}

/// Indicates if parallelism is supported.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SupportedParallelism {
    /// Only sequential single-threaded operations are available.
    Sequential,
    /// Parallel multithreaded operations are available.
    Parallel,
}

impl SupportedParallelism {
    /// Negotiate the parallelism between two sources or destinations.
    pub fn negotiate_parallelism(&self, other: SupportedParallelism) -> SupportedParallelism {
        match (self, other) {
            (SupportedParallelism::Parallel, SupportedParallelism::Parallel) => {
                SupportedParallelism::Parallel
            }
            _ => SupportedParallelism::Sequential,
        }
    }
}

impl<S: CopyDestination, P: CopyDestination + Clone> SequentialOrParallel<S, P> {
    pub(crate) async fn finish(&mut self) -> Result<()> {
        with_both!(self, |d| d.finish().await)
    }

    pub(crate) async fn try_get_introspeciton(&self) -> Result<Option<PostgresDatabase>> {
        with_both!(self, |d| d.try_introspect().await)
    }
}

/// A CopyDestination that panics when used.
/// Cannot be constructed outside this module, but is available for type reference
/// to indicate Parallel copy is not supported.
#[derive(Copy, Clone)]
pub struct ParallelCopyDestinationNotAvailable {
    _private: (),
}

/// A CopyTransaction that panics when used.
pub struct ParallelCopyTransactionNotAvailable {
    _private: (),
}

impl CopyTransaction for ParallelCopyTransactionNotAvailable {
    async fn apply_statement(&mut self, _statement: &str) -> Result<()> {
        unreachable!("Parallel copy destination not available")
    }

    async fn commit(self) -> Result<()> {
        unreachable!("Parallel copy destination not available")
    }
}

impl CopyDestination for ParallelCopyDestinationNotAvailable {
    type Transaction<'a> = ParallelCopyTransactionNotAvailable;

    async fn apply_data<R: TableDataReader, C: AsyncCleanup>(
        &mut self,
        _schema: &PostgresSchema,
        _table: &PostgresTable,
        _data: TableData<R, C>,
    ) -> Result<()> {
        unreachable!("Parallel copy destination not available")
    }

    async fn apply_non_transactional_statement(&mut self, _statement: &str) -> Result<()> {
        unreachable!("Parallel copy destination not available")
    }

    async fn begin_transaction(&mut self) -> Result<ParallelCopyTransactionNotAvailable> {
        unreachable!("Parallel copy destination not available")
    }

    fn get_identifier_quoter(&self) -> Arc<IdentifierQuoter> {
        unreachable!("Parallel copy destination not available")
    }
}

#[cfg(test)]
mod tests {
    use crate::test_helpers::TestHelper;

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

    pub fn get_expected_array_test_data() -> Vec<(Vec<String>,)> {
        vec![
            (vec!["foo".to_string(), "bar".to_string()],),
            (vec!["baz".to_string(), "qux".to_string()],),
            (vec!["quux".to_string(), "corge".to_string()],),
        ]
    }

    pub async fn validate_pets(connection: &TestHelper) {
        let pets = connection
            .get_results::<(i32, String)>("select id, name from pets order by id")
            .await;
        assert_eq!(
            pets,
            vec![
                (1, "Fido".to_string()),
                (2, "Fluffy".to_string()),
                (3, "Remy".to_string()),
            ]
        );

        let dogs = connection
            .get_results::<(i32, String, String)>("select id, name, breed from dogs order by id")
            .await;
        assert_eq!(dogs, vec![(1, "Fido".to_string(), "beagle".to_string()),]);

        let cats = connection
            .get_results::<(i32, String, String)>("select id, name, color from cats order by id")
            .await;
        assert_eq!(cats, vec![(2, "Fluffy".to_string(), "white".to_string()),]);
    }

    pub async fn validate_copy_state(destination: &TestHelper) {
        let items = destination
            .get_results::<(i32, String, i32)>("select id, name, age from people;")
            .await;

        assert_eq!(items, get_expected_people_data());

        let result = destination
            .get_conn()
            .execute_non_query("insert into people (name, age) values ('new-value', 10000)")
            .await;
        assert!(result.is_err(), "Expected CHECK_VIOLATION error");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("check") || err_msg.contains("CHECK") || err_msg.contains("violates"),
            "Expected check violation, got: {err_msg}"
        );

        let result = destination
            .get_conn()
            .execute_non_query("insert into people (name, age) values ('foo', 100)")
            .await;
        assert!(result.is_err(), "Expected UNIQUE_VIOLATION error");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("unique")
                || err_msg.contains("UNIQUE")
                || err_msg.contains("duplicate"),
            "Expected unique violation, got: {err_msg}"
        );

        destination
            .execute_not_query("insert into field (id) values (1);")
            .await;

        destination.execute_not_query("insert into tree_node(id, field_id, name, parent_id) values (1, 1, 'foo', null), (2, 1, 'bar', 1)").await;
        if destination.get_conn().version() >= 150 {
            let result = destination.get_conn().execute_non_query("insert into tree_node(id, field_id, name, parent_id) values (3, 1, 'foo', null)").await;
            assert!(result.is_err(), "Expected UNIQUE_VIOLATION error");
        }

        let result = destination.get_conn().execute_non_query("insert into tree_node(id, field_id, name, parent_id) values (9999, 9999, 'foobarbaz', null)").await;
        assert!(result.is_err(), "Expected FOREIGN_KEY_VIOLATION error");

        let people_who_cant_drink = destination
            .get_results::<(i32, String, i32)>("select id, name, age from people_who_cant_drink;")
            .await;
        assert_eq!(people_who_cant_drink, vec![(6, "q't".to_string(), 12)]);

        let array_test_data = destination
            .get_results::<(Vec<String>,)>("select name from array_test;")
            .await;

        assert_eq!(array_test_data, get_expected_array_test_data());

        let partition_test_data = destination
            .get_results::<(i32,)>("select value from my_partitioned_table order by value;")
            .await;

        assert_eq!(partition_test_data, vec![(1,), (9,), (11,), (19,)]);

        validate_pets(destination).await;
    }
}
