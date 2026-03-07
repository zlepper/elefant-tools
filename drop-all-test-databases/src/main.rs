use anyhow::Result;
use elefant_client::PostgresConnectionSettings;

#[tokio::main]
async fn main() -> Result<()> {
    let pg_ports = vec![
        5412, 5413, 5414, 5415, 5416, 5417, 5418, 5515, 5516, 5517, 5518,
    ];

    for port in pg_ports {
        let mut client = elefant_client::tokio_connection::new_client(
            PostgresConnectionSettings::new("localhost")
                .port(port)
                .password("passw0rd"),
        )
        .await?;

        let databases = client
            .query(
                "select datname from pg_database where datname like 'test_db_%'",
                &[],
            )
            .await?
            .collect_single_column_to_vec::<String>()
            .await?;

        let version: i32 = client
            .query_simple("show server_version_num;")
            .await?
            .collect_single_column_to_vec::<String>()
            .await?
            .into_iter()
            .next()
            .expect("Expected a version number")
            .parse()?;

        for db_name in databases {
            println!("Dropping database {db_name}");

            if version >= 130000 {
                client
                    .execute_non_query(&format!("drop database {db_name} with (force);"), &[])
                    .await?;
            } else {
                client.execute_non_query(&format!("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{db_name}' AND pid != pg_backend_pid()"), &[]).await?;
                client
                    .execute_non_query(&format!("drop database {db_name};"), &[])
                    .await?;
            }
        }

        // Drop any stale test replication slots
        let test_slots = client
            .query(
                "select slot_name from pg_replication_slots where slot_name like 'test_slot_%'",
                &[],
            )
            .await?
            .collect_single_column_to_vec::<String>()
            .await?;

        for slot_name in test_slots {
            println!("Dropping replication slot {slot_name}");
            // Terminate any backend using the slot before dropping it
            client
                .execute_non_query_simple(&format!(
                    "SELECT pg_terminate_backend(active_pid) \
                     FROM pg_replication_slots \
                     WHERE slot_name = '{slot_name}' AND active;"
                ))
                .await?;
            client
                .execute_non_query_simple(&format!(
                    "SELECT pg_drop_replication_slot('{slot_name}');"
                ))
                .await?;
        }

        // Drop any stale test publications
        let test_pubs = client
            .query(
                "select pubname from pg_publication where pubname like 'test_pub_%'",
                &[],
            )
            .await?
            .collect_single_column_to_vec::<String>()
            .await?;

        for pub_name in test_pubs {
            println!("Dropping publication {pub_name}");
            client
                .execute_non_query_simple(&format!("DROP PUBLICATION {pub_name};"))
                .await?;
        }

        println!("Finished port {port}");
    }

    Ok(())
}
