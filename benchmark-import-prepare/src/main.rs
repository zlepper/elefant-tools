use anyhow::Result;
use elefant_tools::PostgresClientWrapper;

#[tokio::main]
async fn main() -> Result<()> {
    let conn = PostgresClientWrapper::new(
        "host=localhost port=5432 user=postgres password=passw0rd dbname=postgres",
    )
    .await?;

    conn.execute_non_query("drop database if exists dvdrental_import")
        .await?;
    conn.execute_non_query("create database dvdrental_import;")
        .await?;

    Ok(())
}
