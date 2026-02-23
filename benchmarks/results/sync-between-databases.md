| Command | Mean [ms] | Min [ms] | Max [ms] | Relative |
|:---|---:|---:|---:|---:|
| `elefant-sync copy non-parallel` | 456.9 ± 30.6 | 405.5 | 513.4 | 1.82 ± 0.16 |
| `elefant-sync copy parallel` | 250.8 ± 14.7 | 229.2 | 269.6 | 1.00 |
| `elefant-sync copy parallel differential` | 299.9 ± 80.2 | 240.2 | 432.4 | 1.20 ± 0.33 |
| `pg_dump => psql sql-copy` | 629.9 ± 190.8 | 546.1 | 1170.3 | 2.51 ± 0.77 |
| `pg_dump => psql sql-insert` | 746.4 ± 33.0 | 682.4 | 785.7 | 2.98 ± 0.22 |
