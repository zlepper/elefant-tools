use criterion::{BenchmarkId, Criterion, Throughput};
use criterion::{criterion_group, criterion_main};
use tokio_postgres::{binary_copy::BinaryCopyInWriter, types::Type, NoTls};
use futures::{pin_mut, StreamExt};

const DB_HOST: &str = "localhost";
const DB_USER: &str = "postgres";
const DB_PASSWORD: &str = "passw0rd";
const DB_PORT: u16 = 5416; // PostgreSQL 16 (latest available)
const BENCHMARK_DB: &str = "copy_benchmark_db";
const SOURCE_TABLE: &str = "copy_source_table";
const TARGET_TABLE_TOKIO: &str = "copy_target_table_tokio";
const TARGET_TABLE_ELEFANT: &str = "copy_target_table_elefant";

async fn setup_benchmark_database(num_rows: usize) {
    // Connect to postgres to create database if needed
    let (client, connection) = tokio_postgres::connect(
        &format!("host={DB_HOST} port={DB_PORT} user={DB_USER} password={DB_PASSWORD} dbname=postgres"),
        NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {e}");
        }
    });

    // Create benchmark database if it doesn't exist
    let _result = client.execute(
        &format!("CREATE DATABASE {BENCHMARK_DB}"),
        &[],
    ).await;

    // Connect to the benchmark database
    let (client, connection) = tokio_postgres::connect(
        &format!("host={DB_HOST} port={DB_PORT} user={DB_USER} password={DB_PASSWORD} dbname={BENCHMARK_DB}"),
        NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {e}");
        }
    });

    // Create and populate source table
    client.execute(&format!("DROP TABLE IF EXISTS {SOURCE_TABLE}"), &[]).await.unwrap();
    client.execute(&format!("DROP TABLE IF EXISTS {TARGET_TABLE_TOKIO}"), &[]).await.unwrap();
    client.execute(&format!("DROP TABLE IF EXISTS {TARGET_TABLE_ELEFANT}"), &[]).await.unwrap();

    client.execute(
        &format!("CREATE TABLE {SOURCE_TABLE} (id BIGINT, value INTEGER, text_data TEXT)"),
        &[],
    ).await.unwrap();

    client.execute(
        &format!("CREATE TABLE {TARGET_TABLE_TOKIO} (id BIGINT, value INTEGER, text_data TEXT)"),
        &[],
    ).await.unwrap();

    client.execute(
        &format!("CREATE TABLE {TARGET_TABLE_ELEFANT} (id BIGINT, value INTEGER, text_data TEXT)"),
        &[],
    ).await.unwrap();

    // Populate source table with test data
    let sink = client.copy_in(&format!("COPY {SOURCE_TABLE} (id, value, text_data) FROM STDIN BINARY")).await.unwrap();
    let writer = BinaryCopyInWriter::new(sink, &[Type::INT8, Type::INT4, Type::TEXT]);
    pin_mut!(writer);

    for i in 0..num_rows {
        let text_data = format!("test_data_row_{i}");
        writer.as_mut().write(&[&(i as i64), &(i as i32), &text_data]).await.unwrap();
    }
    writer.as_mut().finish().await.unwrap();

    // Run VACUUM ANALYZE
    client.execute(&format!("VACUUM ANALYZE {SOURCE_TABLE}"), &[]).await.unwrap();
    client.execute(&format!("VACUUM ANALYZE {TARGET_TABLE_TOKIO}"), &[]).await.unwrap();
    client.execute(&format!("VACUUM ANALYZE {TARGET_TABLE_ELEFANT}"), &[]).await.unwrap();
}

async fn cleanup_target_tables() {
    let (client, connection) = tokio_postgres::connect(
        &format!("host={DB_HOST} port={DB_PORT} user={DB_USER} password={DB_PASSWORD} dbname={BENCHMARK_DB}"),
        NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {e}");
        }
    });

    // Clean target tables before each benchmark
    client.execute(&format!("TRUNCATE TABLE {TARGET_TABLE_TOKIO}"), &[]).await.unwrap();
    client.execute(&format!("TRUNCATE TABLE {TARGET_TABLE_ELEFANT}"), &[]).await.unwrap();
    
    // Run VACUUM
    client.execute(&format!("VACUUM {TARGET_TABLE_TOKIO}"), &[]).await.unwrap();
    client.execute(&format!("VACUUM {TARGET_TABLE_ELEFANT}"), &[]).await.unwrap();
}

async fn tokio_postgres_copy_benchmark(_num_rows: usize) {
    // Need separate connections for COPY OUT and COPY IN
    let (source_client, source_connection) = tokio_postgres::connect(
        &format!("host={DB_HOST} port={DB_PORT} user={DB_USER} password={DB_PASSWORD} dbname={BENCHMARK_DB}"),
        NoTls,
    ).await.unwrap();

    let (target_client, target_connection) = tokio_postgres::connect(
        &format!("host={DB_HOST} port={DB_PORT} user={DB_USER} password={DB_PASSWORD} dbname={BENCHMARK_DB}"),
        NoTls,
    ).await.unwrap();

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

    // Copy from source to target using tokio-postgres with separate connections
    let source_stream = source_client.copy_out(&format!("COPY {SOURCE_TABLE} (id, value, text_data) TO STDOUT BINARY")).await.unwrap();
    let target_sink = target_client.copy_in(&format!("COPY {TARGET_TABLE_TOKIO} (id, value, text_data) FROM STDIN BINARY")).await.unwrap();

    pin_mut!(source_stream);
    pin_mut!(target_sink);

    source_stream.forward(target_sink).await.unwrap();
}

async fn elefant_client_copy_benchmark(_num_rows: usize) {
    use elefant_client::tokio_connection;
    use elefant_client::PostgresConnectionSettings;

    let settings = PostgresConnectionSettings {
        host: DB_HOST.to_string(),
        port: DB_PORT,
        user: DB_USER.to_string(),
        password: DB_PASSWORD.to_string(),
        database: BENCHMARK_DB.to_string(),
    };

    let mut source_client = tokio_connection::new_client(settings.clone()).await.unwrap();
    let mut target_client = tokio_connection::new_client(settings).await.unwrap();

    // Use elefant-client COPY operations
    let copy_out = source_client.copy_out(
        &format!("COPY {SOURCE_TABLE} (id, value, text_data) TO STDOUT (FORMAT BINARY)"),
        &[]
    ).await.unwrap();

    let mut copy_in = target_client.copy_in(
        &format!("COPY {TARGET_TABLE_ELEFANT} (id, value, text_data) FROM STDIN (FORMAT BINARY)"),
        &[]
    ).await.unwrap();

    copy_out.write_to(&mut copy_in).await.unwrap();
    
    copy_in.end().await.unwrap();
}

async fn time_copy_operation<F, Fut>(iters: u64, num_rows: usize, operation: F) -> std::time::Duration 
where
    F: Fn(usize) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let mut total_duration = std::time::Duration::ZERO;
    
    for _i in 0..iters {
        // Cleanup (not timed)
        cleanup_target_tables().await;
        
        // Time only the actual COPY operation
        let start = std::time::Instant::now();
        operation(num_rows).await;
        total_duration += start.elapsed();
    }
    
    total_duration
}

fn copy_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("copy_operations");


    // Test with different data sizes
    for num_rows in [1000, 5000, 10_000, 100_000, 1_000_000, 10_000_000].iter() {
        group.sample_size((100_000 / *num_rows).max(10));

        // Setup database before benchmarks
        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            setup_benchmark_database(*num_rows).await;
        });

        // Configure throughput measurement (rows per second)
        group.throughput(Throughput::Elements(*num_rows as u64));

        group.bench_with_input(
            BenchmarkId::new("tokio_postgres", num_rows),
            num_rows,
            |b, &num_rows| {
                let rt = tokio::runtime::Runtime::new().unwrap();
                b.iter_custom(|iters| {
                    rt.block_on(time_copy_operation(iters, num_rows, tokio_postgres_copy_benchmark))
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("elefant_client", num_rows),
            num_rows,
            |b, &num_rows| {
                let rt = tokio::runtime::Runtime::new().unwrap();
                b.iter_custom(|iters| {
                    rt.block_on(time_copy_operation(iters, num_rows, elefant_client_copy_benchmark))
                });
            },
        );
    }
    
    group.finish();
}

criterion_group!(benches, copy_benchmarks);
criterion_main!(benches);