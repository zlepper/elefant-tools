# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Please read and maintain the @AGENT-KNOWLEDGE.md file with agent specific knowledge and learning about the project.

## Project Overview

Elefant Tools is a Rust-based alternative to `pg_dump` and `pg_restore` with advanced features like direct database-to-database copying without temporary files. The project consists of a core library (`elefant-tools`), a CLI binary (`elefant-sync`), and a custom PostgreSQL client implementation (`elefant-client`) for high-performance database operations.

## Architecture

### Workspace Structure
This is a Cargo workspace with the following packages:

**Core Packages:**
- **`elefant-tools`** - Main library containing database introspection, schema reading, and data copying functionality
- **`elefant-sync`** - CLI binary providing user interface for backup/restore operations
- **`elefant-client`** - Custom PostgreSQL wire protocol implementation with features like custom connection pooling and optimized data copying
- **`elefant-test-macros`** - Custom test macros for multi-version PostgreSQL testing

**Utility Packages:**
- **`drop-all-test-databases`** - Utility to clean up test databases
- **`benchmark-import-prepare`** - Prepares databases for benchmark runs
- **`codegen`** - Code generation utilities
- **`compare-with-usual`** - Criterion-based benchmarks comparing against pg_dump

### Elefant-Client: Custom PostgreSQL Implementation

The `elefant-client` package is a custom PostgreSQL client implementation with several key architectural components:

**Protocol Layer** (`elefant-client/src/protocol/`):
- **`messages.rs`** - Complete PostgreSQL wire protocol message definitions (BackendMessage, FrontendMessage)
- **`frame_reader/`** - Custom `Framed` implementation (similar to Tokio's Framed but optimized for PostgreSQL)
- **`postgres_connection.rs`** - Low-level connection handling with the custom Framed wrapper
- **`sasl.rs`** - SASL authentication implementation for secure connections
- **`password.rs`** - Password authentication handling (MD5, cleartext, etc.)

**Client Layer** (`elefant-client/src/postgres_client/`):
- **`establish.rs`** - Connection establishment and handshake logic
- **`query.rs`** - Query execution with result set handling and streaming
- **`copy.rs`** - Optimized COPY protocol implementation for bulk data operations
- **`statements.rs`** - Prepared statement management
- **`easy_client.rs`** - High-level client interface

**Type System** (`elefant-client/src/types/`):
- **`core.rs`** - Core type implementations (numbers, strings, etc.) with binary/text format support
- **`from_sql_row.rs`** - Row deserialization traits and implementations
- **`oid.rs`** - PostgreSQL OID type mappings
- **`standard_types.rs`** - Standard PostgreSQL type implementations

**Key Features:**
- Custom `Framed` implementation for optimized I/O buffering
- Support for both binary and text protocol formats
- SASL and legacy authentication methods
- Prepared statement caching
- Connection pooling abstraction
- Feature-gated Tokio integration

### Key Components in Elefant-Tools

**Schema Reader** (`elefant-tools/src/schema_reader/`):
- Comprehensive PostgreSQL introspection including tables, indexes, constraints, functions, triggers
- TimescaleDB extension support (hypertables, continuous aggregates, compression policies)
- Handles complex schema relationships and dependencies

**Storage Abstraction** (`elefant-tools/src/storage/`):
- **`postgres/`** - PostgreSQL connection sources/destinations with parallel processing
- **`sql_file/`** - SQL file import/export with support for different formats
- **`table_data.rs`** - Abstraction over table data with chunking support

**Custom Types**:
- **`pg_interval/`** - Complete PostgreSQL interval type implementation with parsing and formatting
- **`quoting.rs`** - PostgreSQL identifier quoting with keyword awareness
- **`parallel_runner.rs`** - Parallel task execution with semaphore-based concurrency control

## Common Development Commands

### Build Commands
```bash
# Build all packages with all features (RECOMMENDED for most development)
cargo build --all-features

# Build all packages
cargo build

# Build release version (required for benchmarks)
cargo build --release --all-features

# Build specific package with all features
cargo build --package elefant-sync --all-features
cargo build --package elefant-client --all-features

# Build specific features only (for feature-specific verification)
cargo build --package elefant-client --features tokio
cargo build --package elefant-client --features monoio
```

**Note**: Use `--all-features` for most compilation testing unless you specifically need to verify individual feature combinations. This ensures all code paths are compiled and tested.

### Testing

**Prerequisites:**
```bash
# Start test databases (required before running tests)
docker-compose up -d
```

**Test Commands:**
```bash
# Run all tests
cargo test

# Run tests for specific package
cargo test --package elefant-tools
cargo test --package elefant-client

# Run specific test module
cargo test --package elefant-tools schema_reader::tests
cargo test --package elefant-client tokio_connection

# Run single test across all PostgreSQL versions
cargo test reads_simple_schema

# Run tests with features
cargo test --package elefant-client --features tokio

# Clean up test databases after development
cargo run --package drop-all-test-databases
```

### Linting and Formatting
```bash
# Run clippy linting
cargo clippy
cargo clippy --all-targets --all-features -- -D warnings

# Format code
cargo fmt
```

### Benchmarks
```bash
# Run criterion benchmarks
cargo bench

# Run performance benchmarks against pg_dump (requires Docker)
export ELEFANT_SYNC_PATH=target/release/elefant-sync
./benchmarks/run_benchmarks.sh
```

## Test Infrastructure

### Multi-Version PostgreSQL Testing
The project uses custom `#[pg_test]` macros that automatically run tests against multiple PostgreSQL versions:

```rust
#[pg_test(arg(postgres = 15))]
#[pg_test(arg(postgres = 16))]
#[pg_test(arg(timescale_db = 15))]
async fn my_test(helper: &TestHelper) {
    // Test logic here
}
```

**PostgreSQL Version Support:**
- PostgreSQL 12-16 (ports 5412-5416)
- TimescaleDB on PostgreSQL 15-16 (ports 5515-5516)

### Test Patterns

**Integration Tests** (`src/schema_reader/tests/`):
- Comprehensive schema introspection tests organized by feature
- Uses `TestHelper` for database lifecycle management
- Each test gets isolated database with UUID-based naming

**Unit Tests** (inline `mod tests`):
- Standard Rust unit tests throughout the codebase
- Examples: `quoting.rs`, `parallel_runner.rs`, `pg_interval/interval.rs`
- Feature-gated tests in `elefant-client` (e.g., `#[cfg(feature = "tokio")]`)

**Test Database Configuration:**
- Host: `localhost`, User: `postgres`, Password: `passw0rd`
- Docker containers managed by `docker-compose.yaml`
- Automatic cleanup on test success, persistence on failure for debugging

### Running Specific Tests

**PostgreSQL Integration Tests:**
```bash
# Test specific PostgreSQL feature
cargo test --package elefant-tools foreign_keys

# Test TimescaleDB functionality
cargo test --package elefant-tools timescale
```

**Elefant-Client Tests:**
```bash
# Test connection handling
cargo test --package elefant-client --features tokio connection

# Test protocol implementation
cargo test --package elefant-client protocol
```

## TimescaleDB Support
Extensive TimescaleDB support including:
- Hypertables with compression and retention policies
- Continuous aggregates with refresh policies
- User-defined jobs and actions
- Multi-dimensional partitioning

TimescaleDB tests require Docker containers on ports 5515-5516.

## Development Patterns

### Database Connections
When working with database code, use existing abstractions:
- **`PostgresClientWrapper`** for high-level operations in elefant-tools
- **`PostgresClient`** for direct protocol access in elefant-client
- **`TestHelper`** in tests for database lifecycle management

### Error Handling
- **`elefant-tools`**: Uses `thiserror` with structured errors in `src/error.rs`
- **`elefant-client`**: Custom error types in `src/error.rs` with protocol-specific errors

### Feature Gates
The `elefant-client` uses feature gates:
- **`tokio`** feature: Enables Tokio-based async I/O (default in elefant-tools usage)
- **`test_utilities`** feature: Provides additional test helpers

### Performance Considerations
- Custom `Framed` implementation optimized for PostgreSQL protocol
- Parallel processing using semaphore-based `ParallelRunner`
- Connection pooling and prepared statement caching
- Bulk operations using PostgreSQL COPY protocol