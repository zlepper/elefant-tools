# Contributing to the project

## Reporting issues
There is probably going to be quite a few issues, especially as I'm not intimately familiar with all possible
features of Postgres. If you find an issue, please report it. If you can, please include a minimal example that
demonstrates the issue. 

If possible please include the minimal SQL needed to configure Postgres to reproduce the issue, in
addition to the Postgres version.

For example. "Columns of type `json` is not being copied correctly":
```postgresql
CREATE TABLE test_table (id serial primary key, data json);
INSERT INTO test_table (data) VALUES ('{"key": "value"}');
```


## Contributing code
If you have a fix for an issue, or a new feature, please open a pull request. Please include tests for your
changes. 

The general progress I follow when I make changes is to first add a test to the schema reader, see 
`elefant-tools/src/schema_reader/tests/comments.rs` for an example. Then I add a test to the
Postgres source `elefant-tools/src/storage/postgres/tests.rs`. Either using the `test_round_trip` macro
or by writing out a full test by hand. 

A macro is provided to easily test multiple Postgres versions. This macro injects `TestHelper` that 
provides a blank database in the requested Postgres version. Multiple `TestHelper`s can be injected in
the same test, even across different Postgres versions.

Tests can easily be run by starting the Postgres docker containers using `docker compose up -d` and 
then running `cargo test`.
