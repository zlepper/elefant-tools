use compare_with_tokio_postgres::*;
use criterion::{Criterion, Throughput};
use criterion::{criterion_group, criterion_main};
use elefant_client::QueryResultSet;
use std::hint::black_box;

fn simple_query_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("simple_query_roundtrip");
    group.throughput(Throughput::Elements(1));
    group.sample_size(500);

    // tokio-postgres: simple query protocol
    group.bench_function("tokio_postgres/simple", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let client = rt.block_on(tokio_pg_connect("postgres"));
        b.iter(|| {
            rt.block_on(async {
                let rows = client.simple_query("SELECT 1").await.unwrap();
                black_box(rows);
            });
        });
    });

    // tokio-postgres: extended query protocol
    group.bench_function("tokio_postgres/extended", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let client = rt.block_on(tokio_pg_connect("postgres"));
        b.iter(|| {
            rt.block_on(async {
                let row = client.query_one("SELECT 1::int4", &[]).await.unwrap();
                black_box(row.get::<_, i32>(0));
            });
        });
    });

    // elefant-client: simple query protocol
    group.bench_function("elefant_client/simple", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut client = rt.block_on(async {
            elefant_client::tokio_connection::new_client(elefant_settings("postgres"))
                .await
                .unwrap()
        });
        b.iter(|| {
            rt.block_on(async {
                let mut qr = client.query_simple("SELECT 1").await.unwrap();
                loop {
                    match qr.next_result_set().await.unwrap() {
                        QueryResultSet::QueryProcessingComplete => break,
                        QueryResultSet::RowDescriptionReceived(mut rr) => {
                            while let Some(row) = rr.next_row().await.unwrap() {
                                black_box(row.get_some_bytes());
                            }
                        }
                    }
                }
            });
        });
    });

    // elefant-client: extended query protocol (binary)
    group.bench_function("elefant_client/extended", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut client = rt.block_on(async {
            elefant_client::tokio_connection::new_client(elefant_settings("postgres"))
                .await
                .unwrap()
        });
        b.iter(|| {
            rt.block_on(async {
                let mut qr = client.query("SELECT 1::int4", &[]).await.unwrap();
                match qr.next_result_set().await.unwrap() {
                    QueryResultSet::RowDescriptionReceived(mut rr) => {
                        let row = rr.next_row().await.unwrap().unwrap();
                        black_box(row.get::<i32>(0).unwrap());
                        while rr.next_row().await.unwrap().is_some() {}
                    }
                    QueryResultSet::QueryProcessingComplete => panic!("expected rows"),
                }
                loop {
                    match qr.next_result_set().await.unwrap() {
                        QueryResultSet::QueryProcessingComplete => break,
                        QueryResultSet::RowDescriptionReceived(mut rr) => {
                            while rr.next_row().await.unwrap().is_some() {}
                        }
                    }
                }
            });
        });
    });

    group.finish();
}

criterion_group!(benches, simple_query_benchmarks);
criterion_main!(benches);
