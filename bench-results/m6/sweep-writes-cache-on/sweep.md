| Rate | Verdict | D005 as written | p50 | p90 | p99 | p99.9 | max | p99 of each run | Degraded, each run | Past budget | Cache hits | annshard-0 ms CPU | annshard-1 ms CPU | annshard-2 ms CPU | annshard-3 ms CPU | gateway ms CPU | ingest ms CPU | kafka ms CPU | valkey ms CPU | Noisy |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 100 | misses | misses | 2.15 | 3.59 | 4.67 | 6.31 | 9.20 | 4.82 / 4.67 / 4.36 | 0.00 / 0.02 / 0.00% | 0.02% | 42.1% | 0.26 | 0.26 | 0.26 | 0.26 | 1.52 | 0.09 | 0.52 | 0.44 | false / false / true |
| 200 | meets | meets | 1.56 | 3.34 | 4.35 | 6.63 | 12.06 | 4.23 / 4.35 / 4.39 | 0.00 / 0.00 / 0.01% | 0.00% | 49.2% | 0.20 | 0.20 | 0.21 | 0.21 | 1.27 | 0.08 | 0.32 | 0.28 | false / false / false |
| 400 | misses | meets | 1.35 | 3.29 | 4.63 | 7.61 | 16.06 | 4.78 / 4.63 / 4.38 | 1.69 / 0.00 / 0.01% | 0.02% | 58.0% | 0.17 | 0.17 | 0.17 | 0.17 | 1.08 | 0.07 | 0.14 | 0.20 | false / false / false |
| 800 | misses | misses | 1.21 | 3.17 | 5.00 | 8.47 | 16.17 | 4.63 / 5.29 / 5.00 | 0.01 / 3.41 / 0.03% | 0.10% | 68.9% | 0.10 | 0.10 | 0.11 | 0.11 | 0.77 | 0.06 | 0.10 | 0.14 | true / false / false |
