use compare_with_tokio_postgres::*;
use criterion::{Criterion, Throughput};
use criterion::{criterion_group, criterion_main};
use std::hint::black_box;
use tokio_postgres::NoTls;

async fn tokio_postgres_connect() {
    let (client, connection) =
        tokio_postgres::connect(&tokio_pg_connstr("postgres"), NoTls)
            .await
            .unwrap();
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    black_box(&client);
    drop(client);
}

async fn elefant_client_connect() {
    let settings = elefant_settings("postgres");
    let client = elefant_client::tokio_connection::new_client(settings)
        .await
        .unwrap();
    black_box(&client);
    drop(client);
}

fn connection_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("connection_establishment");
    group.throughput(Throughput::Elements(1));
    group.sample_size(100);

    group.bench_function("tokio_postgres", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let start = std::time::Instant::now();
                run_block(tokio_postgres_connect());
                total += start.elapsed();
            }
            total
        });
    });

    group.bench_function("elefant_client", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let start = std::time::Instant::now();
                run_block(elefant_client_connect());
                total += start.elapsed();
            }
            total
        });
    });

    group.finish();
}

criterion_group!(benches, connection_benchmarks);
criterion_main!(benches);
