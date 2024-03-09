#!/usr/bin/env bash


if [[ -z "$PSQL_PATH" ]]; then
  PSQL_PATH=$(which psql)
fi

if [[ -z "$PSQL_PATH" ]]; then
  echo "psql not found in PATH. Either add it to the PATH or set the PSQL_PATH environment variable"
  exit 1
fi


"$PSQL_PATH" --command 'drop database if exists dvdrental_import'
"$PSQL_PATH" --command 'create database dvdrental_import;'