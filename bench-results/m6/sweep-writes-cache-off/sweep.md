| Rate | Verdict | D005 as written | p50 | p90 | p99 | p99.9 | max | p99 of each run | Degraded, each run | Past budget | Cache hits | annshard-0 ms CPU | annshard-1 ms CPU | annshard-2 ms CPU | annshard-3 ms CPU | gateway ms CPU | ingest ms CPU | kafka ms CPU | valkey ms CPU | Noisy |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 100 | misses | misses | 2.57 | 3.31 | 4.21 | 6.89 | 21.96 | 4.37 / 4.21 / 4.07 | 0.02 / 0.02 / 0.04% | 0.00% | 0.0% | 0.40 | 0.40 | 0.41 | 0.41 | 1.75 | 0.10 | 0.36 | 0.37 | true / true / true |
| 200 | meets | meets | 2.31 | 3.12 | 4.14 | 6.72 | 8.91 | 4.14 / 3.96 / 4.18 | 0.01 / 0.00 / 0.01% | 0.01% | 0.0% | 0.33 | 0.33 | 0.34 | 0.34 | 1.57 | 0.07 | 0.20 | 0.22 | false / false / false |
| 400 | meets | meets | 2.22 | 3.04 | 4.24 | 7.21 | 14.79 | 4.26 / 4.06 / 4.24 | 0.02 / 0.03 / 0.01% | 0.01% | 0.0% | 0.30 | 0.30 | 0.30 | 0.30 | 1.39 | 0.07 | 0.13 | 0.15 | false / false / false |
| 800 | misses | meets | 2.09 | 3.32 | 6.33 | 11.93 | 30.29 | 6.33 / 7.73 / 6.03 | 0.10 / 5.09 / 0.06% | 0.10% | 0.0% | 0.26 | 0.26 | 0.26 | 0.26 | 1.07 | 0.06 | 0.09 | 0.10 | false / false / false |
