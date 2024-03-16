#!/usr/bin/env bash

set -e

if [[ -z "$ELEFANT_SYNC_PATH" ]]; then
  echo "ELEFANT_SYNC_PATH not set. Please set it to the path of the elefant-sync binary"
  exit 1
fi

echo "Ready for some benching"

docker rm elefant_sync_bench --force || true
DOCKER_PID=$(docker run --rm -p "5432:5432" -e "POSTGRES_PASSWORD=passw0rd" --detach --quiet --name=elefant_sync_bench --health-cmd "pg_isready -U postgres" postgres:15)

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

hyperfine --prepare "cargo run --release --package=benchmark-import-prepare --quiet" --warmup 1 \
          --export-markdown "benchmarks/results/sync-between-databases.md" \
          --command-name "elefant-sync copy non-parallel" "$ELEFANT_SYNC_COPY_DIRECTLY_SINGLE" \
          --command-name "elefant-sync copy parallel" "$ELEFANT_SYNC_COPY_DIRECTLY_PARALLEL" \
          --command-name "pg_dump => psql sql-copy" "$PG_DUMP_COMMAND_TO_COPY" \
          --command-name "pg_dump => psql sql-insert" "$PG_DUMP_COMMAND_TO_SQL_INSERTS"

echo "Finished benchmark"

docker stop "$DOCKER_PID"
echo "Stopped test container"
