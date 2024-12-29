use std::io::Bytes;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput};
use elefant_client::tokio_connection::TokioPostgresClient;
use elefant_client::PostgresConnectionSettings;
use std::time::Duration;
use futures::{pin_mut, SinkExt, StreamExt};
use tokio::time::Instant;
use elefant_tools::PostgresClientWrapper;

async fn copy_using_elefant_client(
    source_client: &mut TokioPostgresClient,
    target_client: &mut TokioPostgresClient,
) {

    let mut source = source_client.copy_out("copy source_table(id, rand, name) to stdout(format binary)", &[]).await.unwrap();
    let mut target = target_client.copy_in("copy destination_table(id, rand, name) from stdin(format binary)", &[]).await.unwrap();

    source.write_to(&mut target).await.unwrap();
    target.end().await.unwrap();
}

async fn copy_using_tokio_postgres(source_client: &PostgresClientWrapper, target_client: &PostgresClientWrapper) {
    let mut source = source_client.copy_out("copy source_table(id, rand, name) to stdout(format binary)").await.unwrap();
    let mut target = target_client.copy_in::<bytes::Bytes>("copy destination_table(id, rand, name) from stdin(format binary)").await.unwrap();

    pin_mut!(source);
    pin_mut!(target);


    source.forward(target).await.unwrap();
}

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

async fn setup_tokio_postgres() -> (PostgresClientWrapper, PostgresClientWrapper) {
    let connection_string = "host=localhost user=postgres password=passw0rd port=5416 dbname=postgres";

    let source_client = PostgresClientWrapper::new(connection_string).await.unwrap();
    let target_client = PostgresClientWrapper::new(connection_string).await.unwrap();

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

pub fn criterion_benchmark(c: &mut Criterion) {
    let async_runtime = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("Copy data");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    for size in [20, 100, 1_000, 10_000, 50_000/*, 100_000, 500_000, 1_000_000, 10_000_000*/] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(
            BenchmarkId::new("elefant-client", size),
            &size,
            |b, size| {
                b.to_async(&async_runtime)
                    .iter_custom(|iterations| async move {
                        let (mut source_client, mut target_client) = setup().await;

                        let mut total = Duration::ZERO;

                        for _ in 0..iterations {
                            reset(&mut source_client, &mut target_client, *size).await;
                            let source_count: i64 = source_client.read_single_value("select count(*) from source_table", &[]).await.unwrap();

                            let start = Instant::now();

                            copy_using_elefant_client(&mut source_client, &mut target_client).await;

                            total += start.elapsed();
                            let target_count: i64 = target_client.read_single_value("select count(*) from destination_table", &[]).await.unwrap();
                            assert_eq!(source_count, target_count);
                        }

                        total
                    });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("tokio-postgres", size),
            &size,
            |b, size| {
                b.to_async(&async_runtime)
                    .iter_custom(|iterations| async move {
                        let (mut source_client, mut target_client) = setup().await;
                        let (tokio_source_client, tokio_target_client) = setup_tokio_postgres().await;

                        let mut total = Duration::ZERO;

                        for _ in 0..iterations {
                            reset(&mut source_client, &mut target_client, *size).await;
                            let source_count: i64 = source_client.read_single_value("select count(*) from source_table", &[]).await.unwrap();

                            let start = Instant::now();

                            copy_using_tokio_postgres(&tokio_source_client, &tokio_target_client).await;

                            total += start.elapsed();

                            let target_count: i64 = target_client.read_single_value("select count(*) from destination_table", &[]).await.unwrap();
                            assert_eq!(source_count, target_count);
                        }

                        total
                    });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
