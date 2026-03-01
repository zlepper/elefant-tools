use compare_with_tokio_postgres::*;
use criterion::{BenchmarkId, Criterion, Throughput};
use criterion::{criterion_group, criterion_main};
use std::hint::black_box;

const BENCH_DB: &str = "type_heavy_benchmark_db";
const NUM_ROWS: usize = 100_000;

async fn setup_database() {
    ensure_database(BENCH_DB).await;
    let client = tokio_pg_connect(BENCH_DB).await;

    client
        .execute("DROP TABLE IF EXISTS type_heavy", &[])
        .await
        .unwrap();
    client
        .execute(
            "CREATE TABLE type_heavy (
                id BIGINT,
                count INTEGER,
                score DOUBLE PRECISION,
                active BOOLEAN,
                label TEXT,
                small_val SMALLINT
            )",
            &[],
        )
        .await
        .unwrap();
    client
        .execute(
            &format!(
                "INSERT INTO type_heavy
                 SELECT g, g % 10000, g * 0.1, g % 2 = 0, 'label_' || g, (g % 30000)::smallint
                 FROM generate_series(1, {NUM_ROWS}) g"
            ),
            &[],
        )
        .await
        .unwrap();
    client
        .execute("VACUUM ANALYZE type_heavy", &[])
        .await
        .unwrap();
}

fn type_heavy_benchmarks(c: &mut Criterion) {
    run_block(setup_database());

    let mut group = c.benchmark_group("type_heavy_deserialization");
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
                        .query(
                            "SELECT id, count, score, active, label, small_val FROM type_heavy",
                            &[],
                        )
                        .await
                        .unwrap();
                    let results: Vec<(i64, i32, f64, bool, String, i16)> = rows
                        .iter()
                        .map(|row| {
                            (
                                row.get::<_, i64>(0),
                                row.get::<_, i32>(1),
                                row.get::<_, f64>(2),
                                row.get::<_, bool>(3),
                                row.get::<_, String>(4),
                                row.get::<_, i16>(5),
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
                    let results: Vec<(i64, i32, f64, bool, String, i16)> = client
                        .query(
                            "SELECT id, count, score, active, label, small_val FROM type_heavy",
                            &[],
                        )
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

criterion_group!(benches, type_heavy_benchmarks);
criterion_main!(benches);
