use compare_with_tokio_postgres::*;
use criterion::{BenchmarkId, Criterion, Throughput};
use criterion::{criterion_group, criterion_main};

const BENCH_DB: &str = "insert_benchmark_db";
const NUM_ROWS: usize = 1_000;

async fn setup_database() {
    ensure_database(BENCH_DB).await;
    let client = tokio_pg_connect(BENCH_DB).await;

    client
        .execute("DROP TABLE IF EXISTS insert_bench", &[])
        .await
        .unwrap();
    client
        .execute(
            "CREATE TABLE insert_bench (id BIGINT, value INTEGER, text_data TEXT)",
            &[],
        )
        .await
        .unwrap();
}

fn insert_benchmarks(c: &mut Criterion) {
    run_block(setup_database());

    let mut group = c.benchmark_group("insert_throughput");
    group.throughput(Throughput::Elements(NUM_ROWS as u64));
    group.sample_size(10);

    group.bench_with_input(
        BenchmarkId::new("tokio_postgres", NUM_ROWS),
        &NUM_ROWS,
        |b, &num_rows| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let client = rt.block_on(tokio_pg_connect(BENCH_DB));
            let stmt = rt.block_on(async {
                client
                    .prepare("INSERT INTO insert_bench (id, value, text_data) VALUES ($1, $2, $3)")
                    .await
                    .unwrap()
            });
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    rt.block_on(async {
                        client.execute("TRUNCATE TABLE insert_bench", &[]).await.unwrap();
                    });
                    let start = std::time::Instant::now();
                    rt.block_on(async {
                        for i in 0..num_rows {
                            let text = format!("row_{i}");
                            client
                                .execute(&stmt, &[&(i as i64), &(i as i32), &text])
                                .await
                                .unwrap();
                        }
                    });
                    total += start.elapsed();
                }
                total
            });
        },
    );

    group.bench_with_input(
        BenchmarkId::new("elefant_client", NUM_ROWS),
        &NUM_ROWS,
        |b, &num_rows| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let mut client = rt.block_on(async {
                elefant_client::tokio_connection::new_client(elefant_settings(BENCH_DB))
                    .await
                    .unwrap()
            });
            let stmt = rt.block_on(async {
                client
                    .prepare_query(
                        "INSERT INTO insert_bench (id, value, text_data) VALUES ($1, $2, $3)",
                    )
                    .await
                    .unwrap()
            });
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    rt.block_on(async {
                        client.execute_non_query_simple("TRUNCATE TABLE insert_bench").await.unwrap();
                    });
                    let start = std::time::Instant::now();
                    rt.block_on(async {
                        for i in 0..num_rows {
                            let text = format!("row_{i}");
                            client
                                .execute_non_query(
                                    &stmt,
                                    &[&(i as i64), &(i as i32), &text],
                                )
                                .await
                                .unwrap();
                        }
                    });
                    total += start.elapsed();
                }
                total
            });
        },
    );

    group.finish();
}

criterion_group!(benches, insert_benchmarks);
criterion_main!(benches);
