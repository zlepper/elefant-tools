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
}

impl Drop for TestHelper {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("Thread is panicking when dropping test helper. Leaving database '{}' ({}) around to be inspected", self.test_db_name, self.helper_name);
        } else {
            let db_name = self.test_db_name.clone();
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                runtime.block_on(cleanup(&db_name));
            })
                .join()
                .expect("Failed to run test helper cleanup from drop");
        }
    }
}

impl RefUnwindSafe for TestHelper {}

impl UnwindSafe for TestHelper {}

pub async fn get_test_helper(name: &str) -> TestHelper {

    let id = Uuid::new_v4().simple().to_string();

    let test_db_name = format!("test_db_{}", id);
    {
        let conn = get_test_connection("postgres").await;

        conn.execute_non_query(&format!("create database {}", test_db_name)).await.expect("Failed to create test database");
    }


    let conn = get_test_connection(&test_db_name).await;

    TestHelper {
        test_db_name,
        main_connection: conn,
        helper_name: name.to_string(),
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
}

async fn get_test_connection(database_name: &str) -> PostgresClientWrapper {
    PostgresClientWrapper::new(&format!("host=localhost user=postgres password=passw0rd dbname={}", database_name)).await.expect("Connection to test database failed. Is postgres running?")
}

async fn cleanup(db_name: &str) {
    let conn = get_test_connection("postgres").await;
    conn.execute_non_query(&format!("drop database {} with (force);", db_name)).await.expect("Failed to drop test database");
}


pub fn assert_pg_error(result: crate::Result, code: SqlState) {
    match result {
        Err(ElefantToolsError::PostgresErrorWithQuery {
                source,
                ..
            }) => {


            assert_eq!(*source.as_db_error().unwrap().code(), code);

        },
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

    #[test]
    async fn creates_and_drops_database() {
        let test_database_name = {
            let helper = get_test_helper("helper").await;
            let test_database_name = helper.test_db_name.clone();
            let db_name: String = helper.get_single_result("select current_database();").await;
            assert_eq!(db_name, helper.test_db_name);
            let databases = helper.get_single_results("select datname from pg_database where datistemplate = false;").await;

            assert!(databases.contains(&helper.test_db_name));

            drop(helper);

            test_database_name
        };

        let conn = get_test_connection("postgres").await;
        let databases = conn.get_single_results("select datname from pg_database where datistemplate = false;").await.unwrap();
        assert!(!databases.contains(&test_database_name));
    }

    #[test]
    async fn database_if_left_around_on_panic() {
        let helper = get_test_helper("helper").await;
        let test_database_name = helper.test_db_name.clone();

        catch_unwind(move || {
            black_box(&helper);
            panic!("Panic in test");
        }).unwrap_err();

        let conn = get_test_connection("postgres").await;
        let databases = conn.get_single_results("select datname from pg_database where datistemplate = false;").await.unwrap();
        assert!(databases.contains(&test_database_name));

        cleanup(&test_database_name).await;
    }
}