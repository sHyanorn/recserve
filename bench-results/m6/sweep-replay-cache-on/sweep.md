| Rate | Verdict | D005 as written | p50 | p90 | p99 | p99.9 | max | p99 of each run | Degraded, each run | Past budget | Cache hits | annshard-0 ms CPU | annshard-1 ms CPU | annshard-2 ms CPU | annshard-3 ms CPU | gateway ms CPU | ingest ms CPU | kafka ms CPU | valkey ms CPU | Noisy |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 100 | meets | meets | 1.75 | 3.48 | 4.52 | 6.63 | 8.81 | 5.09 / 4.48 / 4.52 | 0.00 / 0.00 / 0.02% | 0.03% | 46.7% | 0.22 | 0.22 | 0.23 | 0.23 | 1.42 | 0.01 | 0.15 | 0.41 | false / false / false |
| 200 | misses | misses | 1.49 | 3.32 | 4.27 | 6.34 | 13.98 | 4.30 / 4.09 / 4.27 | 0.01 / 0.01 / 0.00% | 0.02% | 54.9% | 0.18 | 0.18 | 0.19 | 0.19 | 1.21 | 0.00 | 0.06 | 0.26 | true / true / false |
| 400 | misses | meets | 1.23 | 2.97 | 4.17 | 7.71 | 22.72 | 4.34 / 4.17 / 3.92 | 1.67 / 0.01 / 0.02% | 0.02% | 65.3% | 0.13 | 0.12 | 0.13 | 0.13 | 0.88 | 0.00 | 0.03 | 0.17 | false / false / false |
| 800 | misses | misses | 1.09 | 2.72 | 4.55 | 7.36 | 18.11 | 4.55 / 4.70 / 4.51 | 1.69 / 1.56 / 3.79% | 1.73% | 78.4% | 0.07 | 0.07 | 0.07 | 0.07 | 0.62 | 0.00 | 0.01 | 0.12 | true / false / true |
