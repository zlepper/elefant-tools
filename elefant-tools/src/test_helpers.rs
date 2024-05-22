use std::panic::{RefUnwindSafe, UnwindSafe};
use tokio_postgres::error::SqlState;
use tokio_postgres::types::FromSqlOwned;
use uuid::Uuid;
use crate::ElefantToolsError;
use crate::postgres_client_wrapper::{FromRow, PostgresClientWrapper};

#[allow(dead_code)]

/// A helper for running tests that require a database.
/// 
/// This will automatically create a new database for each test, 
/// and drop it when the test is done, if the test succeeded.
/// 
/// All the methods on this struct unwraps errors directly to make it easier to write tests.
pub struct TestHelper {
    /// The name of the test database
    pub test_db_name: String,
    /// The main connected used against the database
    main_connection: PostgresClientWrapper,
    /// An identifier for the test helper
    helper_name: String,
    /// The port of the Postgres instance that was connected to.
    pub port: u16,
    /// If the database was cleaned up nicely
    cleaned_up_nicely: bool,
    /// If the database is a timescale database
    is_timescale_db: bool,
}

impl Drop for TestHelper {
    /// Drops the test helper, cleaning up the database if the test succeeded.
    fn drop(&mut self) {
        if self.cleaned_up_nicely {
            return;
        }

        if std::thread::panicking() {
            eprintln!("Thread is panicking when dropping test helper. Leaving database '{}' ({}) around to be inspected", self.test_db_name, self.helper_name);
        } else {
            let db_name = self.test_db_name.clone();
            let port = self.port;
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                runtime.block_on(cleanup(&db_name, port));
            })
                .join()
                .expect("Failed to run test helper cleanup from drop");
        }
    }
}

impl RefUnwindSafe for TestHelper {}

impl UnwindSafe for TestHelper {}

/// Creates a new test helper, using a random database name.
/// This will connect to Postgres 15 on port 5415.
pub async fn get_test_helper(name: &str) -> TestHelper {
    get_test_helper_on_port(name, 5415).await
}

/// Creates a new test helper, using a random database name and a specific port.
pub async fn get_test_helper_on_port(name: &str, port: u16) -> TestHelper {
    let id = Uuid::new_v4().simple().to_string();

    let test_db_name = format!("test_db_{}", id);
    {
        let conn = get_test_connection_on_port("postgres", port).await;

        conn.execute_non_query(&format!("create database {}", test_db_name)).await.expect("Failed to create test database");
    }


    let conn = get_test_connection_on_port(&test_db_name, port).await;

    TestHelper {
        test_db_name,
        main_connection: conn,
        helper_name: name.to_string(),
        port,
        cleaned_up_nicely: false,
        is_timescale_db: (5500..5600).contains(&port),
    }
}

impl TestHelper {
    /// Executes a query that does not return any results.
    pub async fn execute_not_query(&self, sql: &str) {
        self.get_conn().execute_non_query(sql).await.unwrap_or_else(|e| panic!("Failed to execute non query: {:?}\n{}", e, sql));
    }

    /// Executes a query that returns results.
    pub async fn get_results<T: FromRow>(&self, sql: &str) -> Vec<T> {
        self.get_conn().get_results(sql).await.unwrap_or_else(|e| panic!("Failed to get results for query: {:?}\n{}", e, sql))
    }

    /// Executes a query that returns a single column.
    pub async fn get_single_results<T: FromSqlOwned>(&self, sql: &str) -> Vec<T> {
        self.get_results::<(T, )>(sql).await.into_iter()
            .map(|t| t.0)
            .collect()
    }

    /// Executes a query that returns a single row result.
    pub async fn get_result<T: FromRow>(&self, sql: &str) -> T {
        let results = self.get_results(sql).await;
        assert_eq!(results.len(), 1, "Expected one result, got {}", results.len());
        results.into_iter().next().unwrap()
    }

    /// Executes a query that returns a single column of a single row result.
    pub async fn get_single_result<T: FromSqlOwned>(&self, sql: &str) -> T {
        let result = self.get_result::<(T, )>(sql).await;
        result.0
    }

    /// Gets the underlying connection to the database.
    pub fn get_conn(&self) -> &PostgresClientWrapper {
        &self.main_connection
    }
    
    /// Gets a connection to a specific schema in the database.
    pub async fn get_schema_connection(&self, schema: &str) -> PostgresClientWrapper {
        let connection_string = format!("host=localhost port={} user=postgres password=passw0rd dbname={} options=--search_path={},public", self.port, self.test_db_name, schema);
        PostgresClientWrapper::new(&connection_string).await.expect("Connection to test database failed. Is postgres running?")
    } 

    /// Stops the test helper, cleaning up the database.
    pub async fn stop(mut self) {
        cleanup(&self.test_db_name, self.port).await;
        self.cleaned_up_nicely = true;
    }
    
    pub async fn create_another_database(&self, name: &str) -> TestHelper {
        get_test_helper_on_port(name, self.port).await
    }
}

/// Gets a connection to the specified database on the specified port.
async fn get_test_connection_on_port(database_name: &str, port: u16) -> PostgresClientWrapper {
    get_test_connection_full(database_name, port, "postgres", "passw0rd", None).await
}

/// Gets a connection to the specified database on the specified port.
pub(crate) async fn get_test_connection_full(database_name: &str, port: u16, user: &str, password: &str, schema: Option<&str>) -> PostgresClientWrapper {
    let mut connection_string = format!("host=localhost port={port} user={user} password={password} dbname={database_name}");
    
    if let Some(schema) = schema {
        connection_string.push_str(&format!(" options=--search_path={}", schema));
    }
    
    PostgresClientWrapper::new(&connection_string).await.expect("Connection to test database failed. Is postgres running?")
}

async fn cleanup(db_name: &str, port: u16) {
    let conn = get_test_connection_on_port("postgres", port).await;
    let version: i32 = conn.get_single_result::<String>("show server_version_num;").await.unwrap().parse().unwrap();
    if version >= 130000 {
        conn.execute_non_query(&format!("drop database {} with (force);", db_name)).await.expect("Failed to drop test database");
    } else {
        conn.execute_non_query(&format!("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{}' AND pid != pg_backend_pid()", db_name)).await.expect("Failed to drop test database");
        conn.execute_non_query(&format!("drop database {};", db_name)).await.expect("Failed to drop test database");
    }
}

impl crate::models::TimescaleSupport {
    #[allow(dead_code)]
    pub(crate) fn from_test_helper(helper: &TestHelper) -> Self {
        Self {
            is_enabled: helper.is_timescale_db,
            timescale_toolkit_is_enabled: helper.is_timescale_db,
            user_defined_jobs: vec![],
        }
    }
}

/// Asset that the specified Postgres error occurred.
pub fn assert_pg_error(result: crate::Result, code: SqlState) {
    match result {
        Err(ElefantToolsError::PostgresErrorWithQuery {
                source,
                ..
            }) => {
            assert_eq!(*source.as_db_error().unwrap().code(), code);
        }
        _ => {
            panic!("Expected PostgresErrorWithQuery, got {:?}", result);
        }
    }
}


#[cfg(test)]
mod tests {
    use std::hint::black_box;
    use std::panic::catch_unwind;
    use super::*;
    use tokio::test;
    use elefant_test_macros::pg_test;
    use crate::test_helpers;


    #[test]
    async fn creates_and_drops_database() {
        let (test_database_name, port) = {
            let helper = get_test_helper("helper").await;
            let port = helper.port;
            let test_database_name = helper.test_db_name.clone();
            let db_name: String = helper.get_single_result("select current_database();").await;
            assert_eq!(db_name, helper.test_db_name);
            let databases = helper.get_single_results("select datname from pg_database where datistemplate = false;").await;

            assert!(databases.contains(&helper.test_db_name));

            drop(helper);

            (test_database_name, port)
        };

        let conn = get_test_connection_on_port("postgres", port).await;
        let databases = conn.get_single_results("select datname from pg_database where datistemplate = false;").await.unwrap();
        assert!(!databases.contains(&test_database_name));
    }

    #[test]
    async fn database_if_left_around_on_panic() {
        let helper = get_test_helper("helper").await;
        let port = helper.port;
        let test_database_name = helper.test_db_name.clone();

        catch_unwind(move || {
            black_box(&helper);
            panic!("Panic in test");
        }).unwrap_err();

        let conn = get_test_connection_on_port("postgres", port).await;
        let databases = conn.get_single_results("select datname from pg_database where datistemplate = false;").await.unwrap();
        assert!(databases.contains(&test_database_name));

        cleanup(&test_database_name, port).await;
    }

    #[pg_test(arg(postgres = 14), arg(postgres = 15))]
    async fn injects_multiple_expected_versions(pg14: &TestHelper, pg15: &TestHelper) {
        assert_eq!(pg14.get_conn().version(), 140);
        assert_eq!(pg15.get_conn().version(), 150);
    }

    #[pg_test(arg(postgres = 14))]
    #[pg_test(arg(postgres = 15))]
    async fn tested_multiple_times_async(helper: &TestHelper) {
        let version = helper.get_conn().version();
        assert!((140..160).contains(&version));
    }

    #[pg_test(arg(postgres = 14))]
    #[pg_test(arg(postgres = 15))]
    fn tested_multiple_times_sync(helper: &TestHelper) {
        let version = helper.get_conn().version();
        assert!((140..160).contains(&version));
    }

    macro_rules! test_injected_version {
        ($name:ident, $version:expr) => {
            #[pg_test(arg(postgres = $version))]
            async fn $name(helper: &TestHelper) {
                assert_eq!(helper.get_conn().version(), $version * 10);
            }
        };
    }


    test_injected_version!(test_injected_version_12, 12);
    test_injected_version!(test_injected_version_13, 13);
    test_injected_version!(test_injected_version_14, 14);
    test_injected_version!(test_injected_version_15, 15);
}


