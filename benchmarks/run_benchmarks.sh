#!/usr/bin/env bash

set -e

if [[ -z "$ELEFANT_SYNC_PATH" ]]; then
  echo "ELEFANT_SYNC_PATH not set. Please set it to the path of the elefant-sync binary"
  exit 1
fi

echo "Ready for some benching"

docker rm elefant_sync_bench --force || true
DOCKER_PID=$(docker run --rm -p "5432:5432" -e "POSTGRES_PASSWORD=passw0rd" --detach --quiet --name=elefant_sync_bench --health-cmd "pg_isready -U postgres" postgres:18)

# Ensure elefant-sync is built in release mode
cargo build --release

echo "Running benchmark against container $DOCKER_PID"

export PGPASSWORD=passw0rd
export PGHOST=localhost
export PGPORT=5432
export PGUSER=postgres
export SOURCE_DB_HOST="$PGHOST"
export SOURCE_DB_PORT="$PGPORT"
export SOURCE_DB_USER="$PGUSER"
export SOURCE_DB_PASSWORD="$PGPASSWORD"
export TARGET_DB_HOST="$PGHOST"
export TARGET_DB_PORT="$PGPORT"
export TARGET_DB_USER="$PGUSER"
export TARGET_DB_PASSWORD="$PGPASSWORD"

until pg_isready -t 10 --quiet
do
  echo "Waiting for database to start"
  sleep 1
done

echo "Restoring dvdrental database"
psql --command 'create database dvdrental;'
pg_restore -d dvdrental --exit-on-error benchmarks/dvdrental.tar
echo "dvdrental database restored"

echo "Setting up narrow benchmark database (1 column, 30M rows)"
psql --command 'CREATE DATABASE bench_narrow;'
psql --dbname bench_narrow --command 'CREATE TABLE data (value BIGINT);'
psql --dbname bench_narrow --command 'INSERT INTO data SELECT generate_series(1, 30000000);'
psql --dbname bench_narrow --command 'VACUUM ANALYZE data;'
echo "Narrow benchmark database ready"

echo "Setting up wide benchmark database (50 columns, 600K rows)"
psql --command 'CREATE DATABASE bench_wide;'

WIDE_CREATE="CREATE TABLE data ("
WIDE_INSERT="INSERT INTO data SELECT "
for i in $(seq 0 49); do
  if [ "$i" -gt 0 ]; then
    WIDE_CREATE="$WIDE_CREATE, "
    WIDE_INSERT="$WIDE_INSERT, "
  fi
  WIDE_CREATE="${WIDE_CREATE}col_${i} BIGINT"
  WIDE_INSERT="${WIDE_INSERT}g+${i}"
done
WIDE_CREATE="$WIDE_CREATE);"
WIDE_INSERT="$WIDE_INSERT FROM generate_series(1, 600000) g;"

psql --dbname bench_wide --command "$WIDE_CREATE"
psql --dbname bench_wide --command "$WIDE_INSERT"
psql --dbname bench_wide --command 'VACUUM ANALYZE data;'
echo "Wide benchmark database ready"


PG_DUMP_COMMAND_TO_SQL_INSERTS="pg_dump --dbname dvdrental -f benchmarks/results/pg_dump_result-insert.sql --rows-per-insert=1000"
PG_DUMP_COMMAND_TO_COPY="pg_dump --dbname dvdrental -f benchmarks/results/pg_dump_result-copy.sql"
ELEFANT_SYNC_COMMAND_TO_SQL_INSERTS="\"$ELEFANT_SYNC_PATH\" export --source-db-name dvdrental sql-file --path benchmarks/results/elefant_sync_result-insert.sql --format InsertStatements --max-rows-per-insert 1000"
ELEFANT_SYNC_COMMAND_TO_COPY="\"$ELEFANT_SYNC_PATH\" export --source-db-name dvdrental sql-file --path benchmarks/results/elefant_sync_result-copy.sql --format CopyStatements --max-commands-per-chunk 500"

hyperfine --command-name "pg_dump sql-insert" "$PG_DUMP_COMMAND_TO_SQL_INSERTS" \
          --command-name "pg_dump sql-copy" "$PG_DUMP_COMMAND_TO_COPY" \
          --command-name "elefant-sync sql-insert" "$ELEFANT_SYNC_COMMAND_TO_SQL_INSERTS" \
          --command-name "elefant-sync sql-copy" "$ELEFANT_SYNC_COMMAND_TO_COPY" \
          --export-markdown "benchmarks/results/export-as-sql.md"  --warmup 1

PG_RESTORE_IMPORT_SQL_INSERTS="psql --dbname dvdrental_import --file benchmarks/results/pg_dump_result-insert.sql --echo-hidden --quiet -v ON_ERROR_STOP=1"
PG_RESTORE_IMPORT_SQL_COPY="psql --dbname dvdrental_import --file benchmarks/results/pg_dump_result-copy.sql --echo-hidden --quiet -v ON_ERROR_STOP=1"
ELEFANT_SYNC_COMMAND_FROM_SQL_INSERTS="\"$ELEFANT_SYNC_PATH\" import --target-db-name dvdrental_import sql-file --path benchmarks/results/elefant_sync_result-insert.sql"
ELEFANT_SYNC_COMMAND_FROM_SQL_COPY="\"$ELEFANT_SYNC_PATH\" import --target-db-name dvdrental_import sql-file --path benchmarks/results/elefant_sync_result-copy.sql"

hyperfine --prepare "cargo run --release --package=benchmark-import-prepare --quiet" --warmup 1 \
          --export-markdown "benchmarks/results/import-from-sql.md" \
          --command-name "psql sql-copy" "$PG_RESTORE_IMPORT_SQL_COPY" \
          --command-name "psql sql-insert" "$PG_RESTORE_IMPORT_SQL_INSERTS" \
          --command-name "elefant-sync sql-copy" "$ELEFANT_SYNC_COMMAND_FROM_SQL_COPY" \
          --command-name "elefant-sync sql-insert" "$ELEFANT_SYNC_COMMAND_FROM_SQL_INSERTS"

PG_DUMP_COMMAND_TO_SQL_INSERTS="pg_dump --dbname dvdrental --rows-per-insert=1000 | psql --dbname dvdrental_import --echo-hidden --quiet -v ON_ERROR_STOP=1"
PG_DUMP_COMMAND_TO_COPY="pg_dump --dbname dvdrental | psql --dbname dvdrental_import --echo-hidden --quiet -v ON_ERROR_STOP=1"
ELEFANT_SYNC_COPY_DIRECTLY_SINGLE="\"$ELEFANT_SYNC_PATH\" --max-parallelism 1 copy --source-db-name dvdrental --target-db-name dvdrental_import"
ELEFANT_SYNC_COPY_DIRECTLY_PARALLEL="\"$ELEFANT_SYNC_PATH\" copy --source-db-name dvdrental --target-db-name dvdrental_import"
ELEFANT_SYNC_COPY_DIRECTLY_PARALLEL_DIFFERENTIAL="\"$ELEFANT_SYNC_PATH\" copy --source-db-name dvdrental --target-db-name dvdrental_import --differential"

hyperfine --prepare "cargo run --release --package=benchmark-import-prepare --quiet" --warmup 1 \
          --export-markdown "benchmarks/results/sync-between-databases.md" \
          --command-name "elefant-sync copy non-parallel" "$ELEFANT_SYNC_COPY_DIRECTLY_SINGLE" \
          --command-name "elefant-sync copy parallel" "$ELEFANT_SYNC_COPY_DIRECTLY_PARALLEL" \
          --command-name "elefant-sync copy parallel differential" "$ELEFANT_SYNC_COPY_DIRECTLY_PARALLEL_DIFFERENTIAL" \
          --command-name "pg_dump => psql sql-copy" "$PG_DUMP_COMMAND_TO_COPY" \
          --command-name "pg_dump => psql sql-insert" "$PG_DUMP_COMMAND_TO_SQL_INSERTS"

ELEFANT_NARROW_COPY="\"$ELEFANT_SYNC_PATH\" copy --source-db-name bench_narrow --target-db-name bench_narrow_import"
PG_NARROW_COPY="pg_dump --dbname bench_narrow | psql --dbname bench_narrow_import --quiet -v ON_ERROR_STOP=1"

hyperfine --prepare "cargo run --release --package=benchmark-import-prepare --quiet" --warmup 1 \
          --export-markdown "benchmarks/results/narrow-copy.md" \
          --command-name "elefant-sync copy" "$ELEFANT_NARROW_COPY" \
          --command-name "pg_dump => psql" "$PG_NARROW_COPY"

ELEFANT_WIDE_COPY="\"$ELEFANT_SYNC_PATH\" copy --source-db-name bench_wide --target-db-name bench_wide_import"
PG_WIDE_COPY="pg_dump --dbname bench_wide | psql --dbname bench_wide_import --quiet -v ON_ERROR_STOP=1"

hyperfine --prepare "cargo run --release --package=benchmark-import-prepare --quiet" --warmup 1 \
          --export-markdown "benchmarks/results/wide-copy.md" \
          --command-name "elefant-sync copy" "$ELEFANT_WIDE_COPY" \
          --command-name "pg_dump => psql" "$PG_WIDE_COPY"

echo "Finished benchmark"

docker stop "$DOCKER_PID"
echo "Stopped test container"
