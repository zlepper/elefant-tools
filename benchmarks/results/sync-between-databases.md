| Command | Mean [s] | Min [s] | Max [s] | Relative |
|:---|---:|---:|---:|---:|
| `elefant-sync copy non-parallel` | 1.413 ± 0.127 | 1.271 | 1.728 | 2.49 ± 0.30 |
| `elefant-sync copy parallel` | 0.568 ± 0.045 | 0.534 | 0.688 | 1.00 |
| `pg_dump => psql sql-copy` | 1.525 ± 0.045 | 1.424 | 1.579 | 2.69 ± 0.23 |
| `pg_dump => psql sql-insert` | 3.627 ± 0.114 | 3.487 | 3.796 | 6.39 ± 0.54 |
