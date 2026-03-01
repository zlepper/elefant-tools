use compare_with_tokio_postgres::*;
use criterion::{BenchmarkId, Criterion, Throughput};
use criterion::{criterion_group, criterion_main};
use futures::{StreamExt, pin_mut};
use tokio_postgres::{NoTls, binary_copy::BinaryCopyInWriter, types::Type};

const SOURCE_TABLE: &str = "text_copy_source";
const TARGET_TABLE_TOKIO: &str = "text_copy_target_tokio";
const TARGET_TABLE_ELEFANT: &str = "text_copy_target_elefant";
const NUM_ROWS: usize = 10_000_000;

async fn setup_benchmark_database() {
    ensure_database(BENCHMARK_DB).await;
    let client = tokio_pg_connect(BENCHMARK_DB).await;

    client
        .execute(&format!("DROP TABLE IF EXISTS {SOURCE_TABLE}"), &[])
        .await
        .unwrap();
    client
        .execute(&format!("DROP TABLE IF EXISTS {TARGET_TABLE_TOKIO}"), &[])
        .await
        .unwrap();
    client
        .execute(&format!("DROP TABLE IF EXISTS {TARGET_TABLE_ELEFANT}"), &[])
        .await
        .unwrap();

    client
        .execute(
            &format!("CREATE TABLE {SOURCE_TABLE} (id BIGINT, value INTEGER, text_data TEXT)"),
            &[],
        )
        .await
        .unwrap();
    client
        .execute(
            &format!("CREATE TABLE {TARGET_TABLE_TOKIO} (id BIGINT, value INTEGER, text_data TEXT)"),
            &[],
        )
        .await
        .unwrap();
    client
        .execute(
            &format!("CREATE TABLE {TARGET_TABLE_ELEFANT} (id BIGINT, value INTEGER, text_data TEXT)"),
            &[],
        )
        .await
        .unwrap();

    // Populate source table using binary COPY (fastest)
    let sink = client
        .copy_in(&format!(
            "COPY {SOURCE_TABLE} (id, value, text_data) FROM STDIN BINARY"
        ))
        .await
        .unwrap();
    let writer = BinaryCopyInWriter::new(sink, &[Type::INT8, Type::INT4, Type::TEXT]);
    pin_mut!(writer);

    for i in 0..NUM_ROWS {
        let text_data = format!("test_data_row_{i}");
        writer
            .as_mut()
            .write(&[&(i as i64), &(i as i32), &text_data])
            .await
            .unwrap();
    }
    writer.as_mut().finish().await.unwrap();

    client
        .execute(&format!("VACUUM ANALYZE {SOURCE_TABLE}"), &[])
        .await
        .unwrap();
}

async fn cleanup_target_tables() {
    let client = tokio_pg_connect(BENCHMARK_DB).await;
    client
        .execute(&format!("TRUNCATE TABLE {TARGET_TABLE_TOKIO}"), &[])
        .await
        .unwrap();
    client
        .execute(&format!("TRUNCATE TABLE {TARGET_TABLE_ELEFANT}"), &[])
        .await
        .unwrap();
    client
        .execute(&format!("VACUUM {TARGET_TABLE_TOKIO}"), &[])
        .await
        .unwrap();
    client
        .execute(&format!("VACUUM {TARGET_TABLE_ELEFANT}"), &[])
        .await
        .unwrap();
}

async fn tokio_postgres_text_copy() {
    let (source_client, source_connection) =
        tokio_postgres::connect(&tokio_pg_connstr(BENCHMARK_DB), NoTls)
            .await
            .unwrap();
    let (target_client, target_connection) =
        tokio_postgres::connect(&tokio_pg_connstr(BENCHMARK_DB), NoTls)
            .await
            .unwrap();

    tokio::spawn(async move {
        if let Err(e) = source_connection.await {
            eprintln!("Source connection error: {e}");
        }
    });
    tokio::spawn(async move {
        if let Err(e) = target_connection.await {
            eprintln!("Target connection error: {e}");
        }
    });

    let source_stream = source_client
        .copy_out(&format!(
            "COPY {SOURCE_TABLE} (id, value, text_data) TO STDOUT"
        ))
        .await
        .unwrap();
    let target_sink = target_client
        .copy_in(&format!(
            "COPY {TARGET_TABLE_TOKIO} (id, value, text_data) FROM STDIN"
        ))
        .await
        .unwrap();

    pin_mut!(source_stream);
    pin_mut!(target_sink);
    source_stream.forward(target_sink).await.unwrap();
}

async fn elefant_client_text_copy() {
    let settings = elefant_settings(BENCHMARK_DB);
    let mut source_client = elefant_client::tokio_connection::new_client(settings.clone())
        .await
        .unwrap();
    let mut target_client = elefant_client::tokio_connection::new_client(settings)
        .await
        .unwrap();

    let copy_out = source_client
        .copy_out(
            &format!("COPY {SOURCE_TABLE} (id, value, text_data) TO STDOUT"),
            &[],
        )
        .await
        .unwrap();
    let mut copy_in = target_client
        .copy_in(
            &format!("COPY {TARGET_TABLE_ELEFANT} (id, value, text_data) FROM STDIN"),
            &[],
        )
        .await
        .unwrap();

    copy_out.write_to(&mut copy_in).await.unwrap();
    copy_in.end().await.unwrap();
}

fn text_copy_benchmarks(c: &mut Criterion) {
    run_block(setup_benchmark_database());

    let mut group = c.benchmark_group("text_copy_operations");
    group.sample_size(10);
    group.throughput(Throughput::Elements(NUM_ROWS as u64));

    group.bench_with_input(
        BenchmarkId::new("tokio_postgres", NUM_ROWS),
        &NUM_ROWS,
        |b, _| {
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    run_block(cleanup_target_tables());
                    let start = std::time::Instant::now();
                    run_block(tokio_postgres_text_copy());
                    total += start.elapsed();
                }
                total
            });
        },
    );

    group.bench_with_input(
        BenchmarkId::new("elefant_client", NUM_ROWS),
        &NUM_ROWS,
        |b, _| {
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    run_block(cleanup_target_tables());
                    let start = std::time::Instant::now();
                    run_block(elefant_client_text_copy());
                    total += start.elapsed();
                }
                total
            });
        },
    );

    group.finish();
}

criterion_group!(benches, text_copy_benchmarks);
criterion_main!(benches);
