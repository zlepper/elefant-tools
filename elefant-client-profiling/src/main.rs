use tokio::main;
use elefant_client::PostgresConnectionSettings;
use elefant_client::tokio_connection::TokioPostgresClient;

async fn setup() -> (TokioPostgresClient, TokioPostgresClient) {
    let connection_settings = PostgresConnectionSettings {
        password: "passw0rd".to_string(),
        port: 5416, // Postgres 16
        ..Default::default()
    };

    let source_client = elefant_client::tokio_connection::new_client(connection_settings.clone())
        .await
        .unwrap();

    let target_client = elefant_client::tokio_connection::new_client(connection_settings)
        .await
        .unwrap();

    (source_client, target_client)
}

async fn reset(
    source_client: &mut TokioPostgresClient,
    target_client: &mut TokioPostgresClient,
    size: i32,
) {
    source_client
        .execute_non_query(
            r#"
                drop table if exists source_table;
                create table source_table(id serial primary key, rand int, name text);
                "#,
            &[],
        )
        .await
        .unwrap();
    target_client
        .execute_non_query(
            r#"
                drop table if exists destination_table;
                create table destination_table(id serial primary key, rand int, name text);
                "#,
            &[],
        )
        .await
        .unwrap();

    source_client.execute_non_query(r#"
                insert into source_table (rand, name)
                select (random()*1000000)::int, md5(random()::text) from generate_series(1, $1) i;
                "#, &[&size])
        .await
        .unwrap();
}

async fn copy_using_elefant_client(
    source_client: &mut TokioPostgresClient,
    target_client: &mut TokioPostgresClient,
) {

    let mut source = source_client.copy_out("copy source_table(id, rand, name) to stdout(format binary)", &[]).await.unwrap();
    let mut target = target_client.copy_in("copy destination_table(id, rand, name) from stdin(format binary)", &[]).await.unwrap();

    source.write_to(&mut target).await.unwrap();
    target.end().await.unwrap();
}

#[main]
async fn main() {

    let (mut source_client, mut target_client) = setup().await;
    reset(&mut source_client, &mut target_client, 10000000).await;

    for _ in 0..1 {
        copy_using_elefant_client(&mut source_client, &mut target_client).await;
        target_client.execute_non_query("truncate table destination_table;", &[]).await.unwrap();
    }

}
