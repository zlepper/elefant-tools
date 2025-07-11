# PostgreSQL COPY Benchmark: elefant-client vs tokio-postgres

This benchmark compares the performance of PostgreSQL COPY operations between the custom `elefant-client` implementation and the standard `tokio-postgres` crate.

## Prerequisites

1. **PostgreSQL Server**: A PostgreSQL server must be running on `localhost:5416` (PostgreSQL 16) with:
   - Username: `postgres`
   - Password: `passw0rd`
   - The server must allow database creation

2. **Docker** (optional): If you're using the project's test infrastructure, start the test databases:
   ```bash
   docker-compose up -d
   ```

## Running the Benchmark

```bash
# Test database connectivity first (recommended)
cargo run

# Build in release mode (required for accurate benchmarks)
cargo build --release

# Run the benchmark
cargo bench
```

## What the Benchmark Does

### Setup Phase (Outside Timing)
1. **Database Creation**: Creates a benchmark database if it doesn't exist
2. **Table Setup**: Creates source and target tables for both implementations
3. **Data Generation**: Populates the source table with test data (1K, 10K, 100K rows)
4. **VACUUM**: Runs VACUUM ANALYZE on all tables for consistent performance

### Benchmark Phase (Timed)
For each row count (1,000, 10,000, 100,000):

1. **tokio-postgres**: 
   - Uses `copy_out()` to read from source table in binary format
   - Uses `copy_in()` to write to target table
   - Processes data through `BinaryCopyOutStream` and `BinaryCopyInWriter`

2. **elefant-client**:
   - Uses custom `copy_out()` implementation
   - Uses custom `copy_in()` implementation  
   - Leverages the direct `write_to()` method for efficient streaming

### Table Schema
```sql
CREATE TABLE (
    id BIGINT,
    value INTEGER, 
    text_data TEXT
)
```

Each row contains:
- `id`: Sequential number (0 to N-1)
- `value`: Same as ID cast to integer
- `text_data`: String in format "test_data_row_{id}"

## Expected Output

The benchmark will generate a report showing:
- Throughput comparison between implementations
- Performance across different data sizes
- Statistical analysis (mean, median, standard deviation)

Results will be saved to `target/criterion/` directory with detailed HTML reports.

## Customization

You can modify the benchmark by editing `benches/my_benchmark.rs`:

- **Database settings**: Change connection constants at the top
- **Row counts**: Modify the array `[1000, 10000, 100000]` 
- **Table schema**: Adjust the CREATE TABLE statements and data generation
- **Benchmark iterations**: Criterion will automatically determine optimal sample sizes