use anyhow::Result;
use elefant_tools::{PostgresClientWrapper, PostgresConnectionSettings};

#[tokio::main]
async fn main() -> Result<()> {
    let conn = PostgresClientWrapper::new(
        PostgresConnectionSettings::new("localhost").password("passw0rd"),
    )
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
