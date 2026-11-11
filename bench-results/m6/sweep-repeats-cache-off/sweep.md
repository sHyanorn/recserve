| Rate | Verdict | D005 as written | p50 | p90 | p99 | p99.9 | max | p99 of each run | Degraded, each run | Past budget | Cache hits | annshard-0 ms CPU | annshard-1 ms CPU | annshard-2 ms CPU | annshard-3 ms CPU | gateway ms CPU | ingest ms CPU | kafka ms CPU | valkey ms CPU | Noisy |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 100 | meets | meets | 2.37 | 3.17 | 4.17 | 6.22 | 15.55 | 4.35 / 4.09 / 4.17 | 0.02 / 0.02 / 0.08% | 0.07% | 0.0% | 0.35 | 0.35 | 0.36 | 0.36 | 1.56 | 0.01 | 0.16 | 0.32 | false / false / false |
| 200 | misses | misses | 2.26 | 3.09 | 4.22 | 6.99 | 11.05 | 4.32 / 4.20 / 4.22 | 0.02 / 0.02 / 0.03% | 0.03% | 0.0% | 0.31 | 0.31 | 0.31 | 0.32 | 1.43 | 0.01 | 0.26 | 0.20 | false / true / true |
| 400 | misses | misses | 2.01 | 2.84 | 3.71 | 6.01 | 10.37 | 3.86 / 3.71 / 3.66 | 0.02 / 0.05 / 0.00% | 0.05% | 0.0% | 0.26 | 0.26 | 0.26 | 0.27 | 1.18 | 0.00 | 0.03 | 0.12 | true / false / false |
| 800 | misses | meets | 2.07 | 3.43 | 6.66 | 12.16 | 24.71 | 7.92 / 5.84 / 6.66 | 1.77 / 0.04 / 0.06% | 0.05% | 0.0% | 0.25 | 0.26 | 0.25 | 0.25 | 1.01 | 0.00 | 0.01 | 0.09 | false / false / false |
