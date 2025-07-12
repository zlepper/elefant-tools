use anyhow::Result;
use elefant_client::PostgresConnectionSettings;

#[tokio::main]
async fn main() -> Result<()> {
    let pg_ports = vec![5412, 5413, 5414, 5415, 5416, 5515, 5516];

    for port in pg_ports {
        let mut client = elefant_client::tokio_connection::new_client(PostgresConnectionSettings {
            user: "postgres".to_string(),
            host: "localhost".to_string(),
            database: "postgres".to_string(),
            port,
            password: "passw0rd".to_string(),
        }).await?;

        let databases = client.query("select datname from pg_database where datname like 'test_db_%'", &[]).await?.collect_single_column_to_vec::<String>().await?;

        let version: i32 = client
            .read_single_value::<String>("show server_version_num;", &[])
            .await?
            .parse()?;

        for db_name in databases {
            println!("Dropping database {db_name}");

            if version >= 130000 {
                client.execute_non_query(&format!("drop database {db_name} with (force);"), &[]).await?;
            } else {
                client.execute_non_query(&format!("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{db_name}' AND pid != pg_backend_pid()"), &[]).await?;
                client.execute_non_query(&format!("drop database {db_name};"), &[]).await?;
            }
        }

        println!("Finished port {port}");
    }

    Ok(())
}
