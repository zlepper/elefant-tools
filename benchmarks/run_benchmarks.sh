#!/usr/bin/env bash

if [[ -z "$PG_DUMP_PATH" ]]; then
  PG_DUMP_PATH=$(which pg_dump)
fi

if [[ -z "$PG_DUMP_PATH" ]]; then
  echo "pg_dump not found in PATH. Either add it to the PATH or set the PG_DUMP_PATH environment variable"
  HAD_ERROR=1
fi


if [[ -z "$PG_RESTORE_PATH" ]]; then
  PG_RESTORE_PATH=$(which pg_restore)
fi

if [[ -z "$PG_RESTORE_PATH" ]]; then
  echo "pg_restore not found in PATH. Either add it to the PATH or set the PG_RESTORE_PATH environment variable"
  HAD_ERROR=1
fi

if [[ -z "$PG_IS_READY_PATH" ]]; then
  PG_IS_READY_PATH=$(which pg_isready)
fi

if [[ -z "$PG_IS_READY_PATH" ]]; then
  echo "pg_isready not found in PATH. Either add it to the PATH or set the PG_IS_READY_PATH environment variable"
  HAD_ERROR=1
fi

if [[ -z "$PSQL_PATH" ]]; then
  PSQL_PATH=$(which psql)
fi

if [[ -z "$PSQL_PATH" ]]; then
  echo "psql not found in PATH. Either add it to the PATH or set the PSQL_PATH environment variable"
  HAD_ERROR=1
fi

if [[ -z "$ELEFANT_SYNC_PATH" ]]; then
  echo "ELEFANT_SYNC_PATH not set. Please set it to the path of the elefant-sync binary"
  HAD_ERROR=1
fi

if [[ -n "$HAD_ERROR" ]]; then
  exit 1
fi

set -e

echo "Ready for some benching"

echo "pg_restore path: $PG_RESTORE_PATH"
echo "pg_dump path: $PG_DUMP_PATH"
echo "psql path: $PSQL_PATH"
echo "pg_isready path: $PG_IS_READY_PATH"
echo "elefant-sync path: $ELEFANT_SYNC_PATH"


docker rm elefant_sync_bench --force || true
DOCKER_PID=$(docker run --rm -p "5432:5432" -e "POSTGRES_PASSWORD=passw0rd" --detach --name=elefant_sync_bench --health-cmd "pg_isready -U postgres" postgres:15)

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
export MAX_PARALLELISM=1

until "$PG_IS_READY_PATH" -t 10 --quiet
do
  echo "Waiting for database to start"
  sleep 1
done

echo "Restoring dvdrental database"
"$PSQL_PATH" --command 'create database dvdrental;'
"$PG_RESTORE_PATH" -d dvdrental --exit-on-error benchmarks/dvdrental.tar
echo "dvdrental database restored"


PG_DUMP_COMMAND_TO_SQL_INSERTS="\"$PG_DUMP_PATH\" --dbname dvdrental -f benchmarks/results/pg_dump_result-insert.sql --rows-per-insert=1000"
PG_DUMP_COMMAND_TO_COPY="\"$PG_DUMP_PATH\" --dbname dvdrental -f benchmarks/results/pg_dump_result-copy.sql"
ELEFANT_SYNC_COMMAND_TO_SQL_INSERTS="\"$ELEFANT_SYNC_PATH\" export --source-db-name dvdrental sql-file --path benchmarks/results/elefant_sync_result-insert.sql --format InsertStatements --max-rows-per-insert 1000"
ELEFANT_SYNC_COMMAND_TO_COPY="\"$ELEFANT_SYNC_PATH\" export --source-db-name dvdrental sql-file --path benchmarks/results/elefant_sync_result-copy.sql --format CopyStatements --max-commands-per-chunk 500"

hyperfine --command-name "pg_dump sql-insert" "$PG_DUMP_COMMAND_TO_SQL_INSERTS" \
          --command-name "pg_dump sql-copy" "$PG_DUMP_COMMAND_TO_COPY" \
          --command-name "elefant-sync sql-insert" "$ELEFANT_SYNC_COMMAND_TO_SQL_INSERTS" \
          --command-name "elefant-sync sql-copy" "$ELEFANT_SYNC_COMMAND_TO_COPY" \
          --export-markdown "benchmarks/results/export-as-sql.md"  --warmup 1 --show-output

PG_RESTORE_IMPORT_SQL_INSERTS="\"$PSQL_PATH\" --dbname dvdrental_import --file benchmarks/results/pg_dump_result-insert.sql --echo-hidden --quiet -v ON_ERROR_STOP=1"
PG_RESTORE_IMPORT_SQL_COPY="\"$PSQL_PATH\" --dbname dvdrental_import --file benchmarks/results/pg_dump_result-copy.sql --echo-hidden --quiet -v ON_ERROR_STOP=1"
ELEFANT_SYNC_COMMAND_FROM_SQL_INSERTS="\"$ELEFANT_SYNC_PATH\" import --target-db-name dvdrental_import sql-file --path benchmarks/results/elefant_sync_result-insert.sql"
ELEFANT_SYNC_COMMAND_FROM_SQL_COPY="\"$ELEFANT_SYNC_PATH\" import --target-db-name dvdrental_import sql-file --path benchmarks/results/elefant_sync_result-copy.sql"

echo "$PSQL_PATH"
echo "$PG_DUMP_PATH"

hyperfine --prepare "cargo run --release --package=benchmark-import-prepare --quiet" --warmup 1 --show-output \
          --export-markdown "benchmarks/results/import-from-sql.md" \
          --command-name "psql sql-copy" "$PG_RESTORE_IMPORT_SQL_COPY" \
          --command-name "psql sql-insert" "$PG_RESTORE_IMPORT_SQL_INSERTS" \
          --command-name "elefant-sync sql-copy" "$ELEFANT_SYNC_COMMAND_FROM_SQL_COPY" \
          --command-name "elefant-sync sql-insert" "$ELEFANT_SYNC_COMMAND_FROM_SQL_INSERTS"

echo "Finished benchmark"

docker stop "$DOCKER_PID"
echo "Stopped test container"
