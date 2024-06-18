# Elefant Tools
Elefant Tools is a library + binary for interfacing with the PostgreSQL database in the same
way as `pg_dump` and `pg_restore`. In addition Elefant Tools can do direct copies between two databases
without the need to write to disk. This is useful for example when you want to copy a database from one
server to another, or when you want to copy a database from a server to a local development environment.

The main difference is that Elefant Tools is written in Rust, is designed to be more 
flexible than `pg_dump` and `pg_restore`, and provides a library for doing full 
customization and transformation of the structure of the database.

## Usage

For most of the commands here you will need to provide credentials and database host information. This general comes in 
the shape of these arguments. These arguments can also be provided through environment variables, where they are named 
the same, except in uppercase. For example `--source-db-host` can be provided through the environment 
variable `SOURCE_DB_HOST`. 
```bash
--source-db-host localhost --source-db-user postgres --source-db-password TopSecretPassword --source-db-name my_db
--target-db-host localhost --target-db-user postgres --target-db-password TopSecretPassword --target-db-name my_target_db
```

To keep the examples here simple, I will not include these arguments in the examples. You should always include 
them in your own commands, or provide them through environment variables. If you don't provide them, the commands will
fail and tell you which arguments are missing.


### Dump to sql file using Postgres insert statements.
This file can be passed to either `psql` or `elefant-sync` to import the data again:
```bash
# Dump to file
elefant-sync export sql-file --path my_dump.sql --format InsertStatements

# Import from file
elefant-sync import sql-file --path my_dump.sql
# or
psql --dbname my_target_db -f my_dump.sql
```

### Dump to sql file using Postgres copy statements.
This requires using elefant-sync to import the data again, but is faster:
```bash
# Dump to file
elefant-sync export sql-file --path my_dump.sql --format CopyStatements

# Import from file
elefant-sync import sql-file --path my_dump.sql
```

### Copy between two databases without temporary files
This was one of the main original use cases for this tool. It allows you to copy a database from one server to another
without writing to disk. This is useful when you have a large database and aren't sure if you have enough disk space. 
Also it's faster than dump + restore, so there's that.
```bash
elefant-sync copy --source-db-name my_source_db --target-db-name my_target_db
```

While this tool doesn't support running everything in one transaction, it does support "differential copying". 
This means that if the copy fails, you can just run the same command again, and it will continue where it left off,
which is often times what you are actually trying to achieve with the full transaction:

```bash
elefant-sync copy --source-db-name my_source_db --target-db-name my_target_db --differential
```

### The `--help` command

I would very much recommend checking out the `--help` command for each of the commands to see all the options available,
for example:
```bash
elefant-sync.exe --help
```
```txt
A replacement for db_dump and db_restore that supports advanced processing such as moving between schemas.

This tool is currently experimental and any use in production is purely on the user. Backups are recommended.

Usage: elefant-sync.exe [OPTIONS] <COMMAND>

Commands:
  export  Export a database schema to a file or directory to be imported later on
  import  Import a database schema from a file or directory that was made using the export command
  copy    Copy a database schema from one database to another
  help    Print this message or the help of the given subcommand(s)

Options:
      --max-parallelism <MAX_PARALLELISM>
          How many threads to use when exporting or importing. Defaults to the number of estimated cores on the machine. If the available parallelism cannot be determined, it defaults to 1

          [env: MAX_PARALLELISM=]
          [default: 32]

  -h, --help
          Print help (see a summary with '-h')

  -V, --version
          Print version
```

# Supported features:

Not all Postgres features are currently supported. The following is a list of features that are supported,
partially supported, and not supported. If you find a feature that is not supported, please open an issue.
If you can, providing the actual code to support the feature would be very helpful.

```
✅ Supported
❌ Not supported
➕ Partially supported (Check the nested items)
```

```
✅ Primary keys
✅ Sequences
    ❌ owned by
✅ Foreign keys
    ✅ Update/Delete cascade rules
✅ Not null constraints
✅ Check constraints
    ✅ Check constraints calling functions
✅ Unique constraints
    ✅ Distinct nulls
    ✅ Using explicit unique index
✅ Indexes
    ✅ Column direction
    ✅ Nulls first/last
    ✅ Index type
    ✅ Filtered index
    ✅ Expressions
    ✅ Included columns
    ✅ Index storage parameters
✅ Generated columns
❌ Row level security
✅ Triggers
✅ Views
✅ Materialized views
➕ Functions/Stored procedures
    ❌ Transforms
    ✅ Aggregate functions
✅ Extensions
➕ Comments (Best effort to support comments on objects. If I have forgotten to support comments on any object, please open an issue.)
✅ Partitions
✅ Inheritance
✅ Enums
❌ Collations
✅ Schemas
❌ Roles/Users
✅ Default values
✅ Quoted identifier names
✅ Array columns
❌ Exclusion constraints
✅ Domains
```

## Timescale DB support
```
✅ Hypertables
    ✅ Space partitioning
    ✅ Time partitioning
    ✅ Multiple dimensions
    ✅ Compression
    ❌ Distributed hypertables
✅ Continuous aggregates
    ✅ Compression
✅ Retention policies
✅ User defined actions/jobs
```

Do note: This uses high level features in Timescale, and doesn't operate directly on the underlying
chunks/catalog tables from timescale, unlike `pg_dump`. This means data might be chunked slightly different
from the original database, but the data should be the same. For example if you have changed the chunk 
interval at some point and have chunks with different sizes, they will all have the same size in the dump.

Continuous aggregates are recreated, which means data that was no longer in the original table will also 
be missing from the continuous aggregate.

# Installation

Elefant-sync is available on crates.io, and can be installed using cargo:
```bash
cargo install elefant-sync
```

If you don't have the rust compiler installed you can download binaries directly from GitHub actions:
https://github.com/zlepper/elefant-tools/actions

or GitHub releases:
https://github.com/zlepper/elefant-tools/releases

And lastly there is a docker image available on GitHub packages:
https://github.com/zlepper/elefant-tools/pkgs/container/elefant-tools




