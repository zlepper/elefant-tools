use std::panic::{RefUnwindSafe, UnwindSafe};
use tokio_postgres::error::SqlState;
use tokio_postgres::types::FromSqlOwned;
use uuid::Uuid;
use crate::ElefantToolsError;
use crate::postgres_client_wrapper::{FromRow, PostgresClientWrapper};


pub struct TestHelper {
    test_db_name: String,
    main_connection: PostgresClientWrapper,
    helper_name: String,
    port: u16,
    cleaned_up_nicely: bool,
    is_timescale_db: bool,
}

impl Drop for TestHelper {
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

pub async fn get_test_helper(name: &str) -> TestHelper {
    get_test_helper_on_port(name, 5415).await
}

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
    pub async fn execute_not_query(&self, sql: &str) {
        self.get_conn().execute_non_query(sql).await.unwrap_or_else(|e| panic!("Failed to execute non query: {:?}\n{}", e, sql));
    }

    pub async fn get_results<T: FromRow>(&self, sql: &str) -> Vec<T> {
        self.get_conn().get_results(sql).await.unwrap_or_else(|e| panic!("Failed to get results for query: {:?}\n{}", e, sql))
    }

    pub async fn get_single_results<T: FromSqlOwned>(&self, sql: &str) -> Vec<T> {
        self.get_results::<(T, )>(sql).await.into_iter()
            .map(|t| t.0)
            .collect()
    }

    pub async fn get_result<T: FromRow>(&self, sql: &str) -> T {
        let results = self.get_results(sql).await;
        assert_eq!(results.len(), 1, "Expected one result, got {}", results.len());
        results.into_iter().next().unwrap()
    }

    pub async fn get_single_result<T: FromSqlOwned>(&self, sql: &str) -> T {
        let result = self.get_result::<(T, )>(sql).await;
        result.0
    }

    pub fn get_conn(&self) -> &PostgresClientWrapper {
        &self.main_connection
    }

    pub async fn stop(mut self) {
        cleanup(&self.test_db_name, self.port).await;
        self.cleaned_up_nicely = true;
    }
}

async fn get_test_connection_on_port(database_name: &str, port: u16) -> PostgresClientWrapper {
    PostgresClientWrapper::new(&format!("host=localhost port={port} user=postgres password=passw0rd dbname={database_name}")).await.expect("Connection to test database failed. Is postgres running?")
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
    pub(crate) fn from_test_helper(helper: &TestHelper) -> Self {
        Self {
            is_enabled: helper.is_timescale_db,
            timescale_toolkit_is_enabled: helper.is_timescale_db,
            user_defined_jobs: vec![],
        }
    }
}

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


