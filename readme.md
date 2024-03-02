# Supported features:

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
chunks/catalog tables from timescale, unlike pg_dump. This means data might be chunked slightly different
from the original database, but the data should be the same. For example if you have changed the chunk 
interval at some point and have chunks with different sizes, they will all have the same size in the dump.

Continuous aggregates are recreated, which means data that was no longer in the original table will also 
be missing from the continuous aggregate.
