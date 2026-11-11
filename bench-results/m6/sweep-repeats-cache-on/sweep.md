| Rate | Verdict | D005 as written | p50 | p90 | p99 | p99.9 | max | p99 of each run | Degraded, each run | Past budget | Cache hits | annshard-0 ms CPU | annshard-1 ms CPU | annshard-2 ms CPU | annshard-3 ms CPU | gateway ms CPU | ingest ms CPU | kafka ms CPU | valkey ms CPU | Noisy |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 100 | meets | meets | 1.44 | 3.41 | 4.47 | 6.25 | 17.12 | 3.93 / 4.56 / 4.47 | 0.02 / 0.00 / 0.02% | 0.02% | 57.9% | 0.18 | 0.18 | 0.19 | 0.19 | 1.23 | 0.01 | 0.16 | 0.38 | false / false / false |
| 200 | misses | misses | 1.40 | 3.40 | 4.41 | 5.85 | 10.60 | 3.97 / 4.44 / 4.41 | 0.02 / 0.01 / 0.01% | 0.01% | 63.2% | 0.16 | 0.16 | 0.16 | 0.16 | 1.16 | 0.01 | 0.06 | 0.26 | false / false / true |
| 400 | misses | meets | 1.17 | 2.92 | 3.94 | 6.90 | 23.41 | 3.94 / 3.83 / 4.31 | 0.01 / 1.70 / 0.03% | 0.02% | 70.7% | 0.10 | 0.10 | 0.11 | 0.11 | 0.84 | 0.00 | 0.11 | 0.17 | false / false / false |
| 800 | misses | misses | 1.08 | 2.79 | 4.42 | 6.91 | 12.31 | 4.38 / 4.48 / 4.42 | 5.03 / 0.02 / 1.73% | 1.75% | 80.2% | 0.07 | 0.07 | 0.07 | 0.07 | 0.62 | 0.00 | 0.01 | 0.12 | false / true / true |
