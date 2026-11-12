| Run | Valid | Pass | Healthy p99 | Fault p99 | Fault max | Recovery p99 | Fault degraded | Excess | Window recovery s | annshard-1 back s | annshard-1 [210, 270 s) | Recovery s | Outlier detection | Errors | Failures |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| F1-run1-a1 | true | true | 7.53 | 7.22 | 19.69 | 7.86 | 0.0% |  | 0 | 4.000 | 49.9% | 4.000 | yes, ejections in [30, 90 s): 0 | 0 |  |
| F1-run2-a1 | true | true | 8.23 | 7.76 | 18.65 | 8.45 | 0.0% |  | 0 | 1.000 | 49.9% | 1.000 | yes, ejections in [30, 90 s): 0 | 0 |  |
| F1-run3-a2 | true | true | 10.12 | 7.69 | 30.48 | 7.71 | 0.0% |  | 0 | 4.000 | 50.1% | 4.000 | yes, ejections in [30, 90 s): 0 | 0 |  |
| F2-run1-a2 | true | true | 9.04 | 20.48 | 46.77 | 8.77 | 62.4% | shards_partial +62.3 | 0 |  |  | 0.000 | yes, ejections in [30, 90 s): 0 | 0 |  |
| F2-run2-a1 | true | true | 8.33 | 19.87 | 29.76 | 8.78 | 62.4% | shards_partial +62.4 | 0 |  |  | 0.000 | yes, ejections in [30, 90 s): 0 | 0 |  |
| F2-run3-a1 | true | true | 7.94 | 19.60 | 34.30 | 8.76 | 62.4% | shards_partial +62.3 | 0 |  |  | 0.000 | yes, ejections in [30, 90 s): 0 | 0 |  |
| F3-run1-a1 | true | true | 9.64 | 3.96 | 70.64 | 9.63 | 99.9% | history_unavailable +99.8 | 1 |  |  | 1.000 | yes, ejections in [30, 90 s): 0 | 0 |  |
| F3-run2-a1 | true | false | 8.52 | 2.41 | 24.18 | 7.97 | 99.9% | history_unavailable +99.9 | 7 |  |  | 7.000 | yes, ejections in [30, 90 s): 0 | 0 | not recovered within 5 s |
| F3-run3-a1 | true | true | 8.16 | 2.73 | 22.19 | 8.91 | 99.9% | history_unavailable +99.9 | 1 |  |  | 1.000 | yes, ejections in [30, 90 s): 0 | 0 |  |
| F4-run1-a3 | false | false | 9.60 | 9.11 | 28.62 | 9.10 | 0.0% |  | 0 |  |  | 0.000 | yes, ejections in [30, 90 s): 0 | 0 |  |
| F4-run2-a2 | true | true | 9.54 | 8.97 | 25.16 | 8.62 | 0.1% |  | 0 |  |  | 0.000 | yes, ejections in [30, 90 s): 0 | 0 |  |
| F4-run3-a1 | true | true | 8.26 | 7.50 | 15.02 | 8.91 | 0.0% |  | 2 |  |  | 2.000 | yes, ejections in [30, 90 s): 0 | 0 |  |
| F5-run1-a1 | true | true | 8.93 | 7.18 | 29.88 | 7.55 | 0.1% |  | 0 | 3.000 | 50.1% | 3.000 | yes, ejections in [30, 90 s): 0 | 0 |  |
| F5-run2-a1 | true | true | 7.66 | 6.95 | 18.40 | 7.52 | 0.1% |  | 0 | 2.000 | 49.9% | 2.000 | yes, ejections in [30, 90 s): 0 | 0 |  |
| F5-run3-a1 | true | true | 8.88 | 7.42 | 26.47 | 7.94 | 0.1% |  | 0 | 3.000 | 49.9% | 3.000 | yes, ejections in [30, 90 s): 0 | 0 |  |
| F6-run1-a1 | true | true | 7.10 | 6.94 | 17.69 | 7.40 | 0.1% |  | 0 | 1.000 | 49.9% | 1.000 | yes, ejections in [30, 90 s): 0 | 0 |  |
| F6-run2-a1 | true | true | 7.73 | 7.07 | 41.45 | 7.65 | 0.1% |  | 0 | 1.000 | 50.0% | 1.000 | yes, ejections in [30, 90 s): 0 | 0 |  |
| F6-run3-a1 | true | true | 7.05 | 6.84 | 18.62 | 7.12 | 0.0% |  | 0 | 1.000 | 50.0% | 1.000 | yes, ejections in [30, 90 s): 0 | 0 |  |

| Attempt | Valid | Invalid because | Breaker openings |
|---|---|---|---|
| F1-run1-a1 | true |  | 0 |
| F1-run2-a1 | true |  | 0 |
| F1-run3-a1 | false | noise baseline flagged | 0 |
| F1-run3-a2 | true |  | 0 |
| F2-run1-a1 | false | noise baseline flagged | 0 |
| F2-run1-a2 | true |  | 0 |
| F2-run2-a1 | true |  | 0 |
| F2-run3-a1 | true |  | 0 |
| F3-run1-a1 | true |  | 1 |
| F3-run2-a1 | true |  | 1 |
| F3-run3-a1 | true |  | 1 |
| F4-run1-a1 | false | noise baseline flagged | 0 |
| F4-run1-a2 | false | noise baseline flagged | 0 |
| F4-run1-a3 | false | noise baseline flagged | 0 |
| F4-run2-a1 | false | noise baseline flagged | 0 |
| F4-run2-a2 | true |  | 0 |
| F4-run3-a1 | true |  | 0 |
| F5-run1-a1 | true |  | 0 |
| F5-run2-a1 | true |  | 0 |
| F5-run3-a1 | true |  | 0 |
| F6-run1-a1 | true |  | 0 |
| F6-run2-a1 | true |  | 0 |
| F6-run3-a1 | true |  | 0 |

| Fault | Runs | Valid runs | Attempts | Invalid attempts | Verdict |
|---|---|---|---|---|---|
| F1 | 3 | 3 | 4 | 1 | pass |
| F2 | 3 | 3 | 4 | 1 | pass |
| F3 | 3 | 3 | 3 | 0 | fail: F3-run2 |
| F4 | 3 | 2 | 6 | 4 | not judged: 2 valid runs |
| F5 | 3 | 3 | 3 | 0 | pass |
| F6 | 3 | 3 | 3 | 0 | pass |
