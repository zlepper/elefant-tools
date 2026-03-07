pub const DB_HOST: &str = "localhost";
pub const DB_USER: &str = "postgres";
pub const DB_PASSWORD: &str = "passw0rd";
pub const DB_PORT: u16 = 5418; // PostgreSQL 18

pub const BENCHMARK_DB: &str = "copy_benchmark_db";

pub fn elefant_settings(db: &str) -> elefant_client::PostgresConnectionSettings {
    elefant_client::PostgresConnectionSettings::new(DB_HOST)
        .port(DB_PORT)
        .user(DB_USER)
        .password(DB_PASSWORD)
        .database(db)
}

pub fn tokio_pg_connstr(db: &str) -> String {
    format!("host={DB_HOST} port={DB_PORT} user={DB_USER} password={DB_PASSWORD} dbname={db}")
}

pub async fn tokio_pg_connect(db: &str) -> tokio_postgres::Client {
    let (client, connection) =
        tokio_postgres::connect(&tokio_pg_connstr(db), tokio_postgres::NoTls)
            .await
            .unwrap();
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    client
}

pub fn run_block<F: std::future::Future>(fut: F) -> F::Output {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(fut)
}

/// Ensure a database exists, creating it if needed.
pub async fn ensure_database(db: &str) {
    let client = tokio_pg_connect("postgres").await;
    let _ = client.execute(&format!("CREATE DATABASE {db}"), &[]).await;
}
