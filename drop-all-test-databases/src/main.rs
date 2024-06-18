use anyhow::Result;
use elefant_tools::PostgresClientWrapper;

#[tokio::main]
async fn main() -> Result<()> {
    let pg_ports = vec![5412, 5413, 5414, 5415, 5416, 5515, 5516, 6415];

    for port in pg_ports {
        let conn_str = format!(
            "host=localhost port={} user=postgres password=passw0rd dbname=postgres",
            port
        );
        let conn = PostgresClientWrapper::new(&conn_str).await?;

        let databases = conn
            .get_single_results::<String>(
                "select datname from pg_database
where datname like 'test_db_%'
",
            )
            .await?;

        let version: i32 = conn
            .get_single_result::<String>("show server_version_num;")
            .await
            .unwrap()
            .parse()
            .unwrap();

        for db_name in databases {
            println!("Dropping database {}", db_name);

            if version >= 130000 {
                conn.execute_non_query(&format!("drop database {} with (force);", db_name))
                    .await?;
            } else {
                conn.execute_non_query(&format!("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{}' AND pid != pg_backend_pid()", db_name)).await?;
                conn.execute_non_query(&format!("drop database {};", db_name))
                    .await?;
            }
        }

        println!("Finished port {}", port);
    }

    Ok(())
}
