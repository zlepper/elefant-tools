// Simple test to verify database connectivity before running benchmarks

use tokio_postgres::NoTls;
use elefant_client::{tokio_connection, PostgresConnectionSettings};

const DB_HOST: &str = "localhost";
const DB_USER: &str = "postgres";
const DB_PASSWORD: &str = "passw0rd";
const DB_PORT: u16 = 5416; // PostgreSQL 16 (latest available)

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing database connectivity...");
    
    // Test tokio-postgres connection
    println!("Testing tokio-postgres connection...");
    let (client, connection) = tokio_postgres::connect(
        &format!("host={} port={} user={} password={} dbname=postgres", 
                DB_HOST, DB_PORT, DB_USER, DB_PASSWORD),
        NoTls,
    ).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    let result = client.query_one("SELECT 1 as test", &[]).await?;
    let test_val: i32 = result.get(0);
    println!("tokio-postgres test query result: {}", test_val);

    // Test elefant-client connection
    println!("Testing elefant-client connection...");
    let settings = PostgresConnectionSettings {
        host: DB_HOST.to_string(),
        port: DB_PORT,
        user: DB_USER.to_string(),
        password: DB_PASSWORD.to_string(),
        database: "postgres".to_string(),
    };

    let _elefant_client = tokio_connection::new_client(settings).await?;
    
    // For elefant-client, we need to use its query API
    // This is a simplified test just to verify connectivity
    println!("elefant-client connected successfully!");

    println!("✅ Both database clients can connect successfully!");
    println!("You can now run: cargo bench");
    
    Ok(())
}