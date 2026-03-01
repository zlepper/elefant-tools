use compare_with_tokio_postgres::*;
use criterion::{BenchmarkId, Criterion, Throughput};
use criterion::{criterion_group, criterion_main};
use std::hint::black_box;

const BENCH_DB: &str = "prepared_stmt_benchmark_db";
const NUM_ROWS: usize = 10_000;

async fn setup_database() {
    ensure_database(BENCH_DB).await;
    let client = tokio_pg_connect(BENCH_DB).await;

    client
        .execute("DROP TABLE IF EXISTS prepared_bench", &[])
        .await
        .unwrap();
    client
        .execute(
            "CREATE TABLE prepared_bench (id BIGINT PRIMARY KEY, value INTEGER, label TEXT)",
            &[],
        )
        .await
        .unwrap();
    client
        .execute(
            &format!(
                "INSERT INTO prepared_bench SELECT g, g % 1000, 'label_' || g FROM generate_series(1, {NUM_ROWS}) g"
            ),
            &[],
        )
        .await
        .unwrap();
    client
        .execute("VACUUM ANALYZE prepared_bench", &[])
        .await
        .unwrap();
}

fn prepared_statement_benchmarks(c: &mut Criterion) {
    run_block(setup_database());

    let mut group = c.benchmark_group("prepared_statement_throughput");
    let iterations = NUM_ROWS;
    group.throughput(Throughput::Elements(iterations as u64));
    group.sample_size(10);

    group.bench_with_input(
        BenchmarkId::new("tokio_postgres", iterations),
        &iterations,
        |b, &iterations| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let client = rt.block_on(tokio_pg_connect(BENCH_DB));
            let stmt = rt.block_on(async {
                client
                    .prepare("SELECT id, value, label FROM prepared_bench WHERE id = $1")
                    .await
                    .unwrap()
            });
            b.iter(|| {
                rt.block_on(async {
                    for i in 1..=iterations {
                        let row = client.query_one(&stmt, &[&(i as i64)]).await.unwrap();
                        black_box((
                            row.get::<_, i64>(0),
                            row.get::<_, i32>(1),
                            row.get::<_, String>(2),
                        ));
                    }
                });
            });
        },
    );

    group.bench_with_input(
        BenchmarkId::new("elefant_client", iterations),
        &iterations,
        |b, &iterations| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let mut client = rt.block_on(async {
                elefant_client::tokio_connection::new_client(elefant_settings(BENCH_DB))
                    .await
                    .unwrap()
            });
            let stmt = rt.block_on(async {
                client
                    .prepare_query("SELECT id, value, label FROM prepared_bench WHERE id = $1")
                    .await
                    .unwrap()
            });
            b.iter(|| {
                rt.block_on(async {
                    for i in 1..=iterations {
                        let result: Vec<(i64, i32, String)> = client
                            .query(&stmt, &[&(i as i64)])
                            .await
                            .unwrap()
                            .collect_to_vec()
                            .await
                            .unwrap();
                        black_box(result);
                    }
                });
            });
        },
    );

    group.finish();
}

criterion_group!(benches, prepared_statement_benchmarks);
criterion_main!(benches);
