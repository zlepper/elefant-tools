use compare_with_tokio_postgres::*;
use criterion::{BenchmarkId, Criterion, Throughput};
use criterion::{criterion_group, criterion_main};
use std::hint::black_box;

const BENCH_DB: &str = "large_result_benchmark_db";
const NUM_ROWS: usize = 1_000_000;

async fn setup_database() {
    ensure_database(BENCH_DB).await;
    let client = tokio_pg_connect(BENCH_DB).await;

    client
        .execute("DROP TABLE IF EXISTS large_result", &[])
        .await
        .unwrap();
    client
        .execute(
            "CREATE TABLE large_result (id BIGINT, value INTEGER, text_data TEXT)",
            &[],
        )
        .await
        .unwrap();
    client
        .execute(
            &format!(
                "INSERT INTO large_result SELECT g, g % 10000, 'text_' || g FROM generate_series(1, {NUM_ROWS}) g"
            ),
            &[],
        )
        .await
        .unwrap();
    client
        .execute("VACUUM ANALYZE large_result", &[])
        .await
        .unwrap();
}

fn large_result_set_benchmarks(c: &mut Criterion) {
    run_block(setup_database());

    let mut group = c.benchmark_group("large_result_set");
    group.throughput(Throughput::Elements(NUM_ROWS as u64));
    group.sample_size(10);

    group.bench_with_input(
        BenchmarkId::new("tokio_postgres", NUM_ROWS),
        &NUM_ROWS,
        |b, _| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let client = rt.block_on(tokio_pg_connect(BENCH_DB));
            b.iter(|| {
                rt.block_on(async {
                    let rows = client
                        .query("SELECT id, value, text_data FROM large_result", &[])
                        .await
                        .unwrap();
                    let results: Vec<(i64, i32, String)> = rows
                        .iter()
                        .map(|row| {
                            (
                                row.get::<_, i64>(0),
                                row.get::<_, i32>(1),
                                row.get::<_, String>(2),
                            )
                        })
                        .collect();
                    black_box(results);
                });
            });
        },
    );

    group.bench_with_input(
        BenchmarkId::new("elefant_client", NUM_ROWS),
        &NUM_ROWS,
        |b, _| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let mut client = rt.block_on(async {
                elefant_client::tokio_connection::new_client(elefant_settings(BENCH_DB))
                    .await
                    .unwrap()
            });
            b.iter(|| {
                rt.block_on(async {
                    let results: Vec<(i64, i32, String)> = client
                        .query("SELECT id, value, text_data FROM large_result", &[])
                        .await
                        .unwrap()
                        .collect_to_vec()
                        .await
                        .unwrap();
                    black_box(results);
                });
            });
        },
    );

    group.finish();
}

criterion_group!(benches, large_result_set_benchmarks);
criterion_main!(benches);
