use anyhow::Result;
use elefant_tools::{PostgresClientWrapper, PostgresConnectionSettings};

#[tokio::main]
async fn main() -> Result<()> {
    let conn = PostgresClientWrapper::new(PostgresConnectionSettings {
        host: "localhost".to_string(),
        port: 5432,
        user: "postgres".to_string(),
        password: "passw0rd".to_string(),
        database: "postgres".to_string(),
        options: None,
    })
    .await?;

    conn.execute_non_query("drop database if exists dvdrental_import")
        .await?;
    conn.execute_non_query("create database dvdrental_import;")
        .await?;

    conn.execute_non_query("drop database if exists bench_narrow_import")
        .await?;
    conn.execute_non_query("create database bench_narrow_import;")
        .await?;

    conn.execute_non_query("drop database if exists bench_wide_import")
        .await?;
    conn.execute_non_query("create database bench_wide_import;")
        .await?;

    conn.execute_non_query("checkpoint;").await?;

    Ok(())
}
