| Run | Valid | Pass | Healthy p99 | Fault p99 | Fault max | Recovery p99 | Fault degraded | Excess | Window recovery s | annshard-1 back s | annshard-1 [210, 270 s) | Recovery s | Outlier detection | Errors | Failures |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| F2-run1 | true | true | 6.21 | 19.10 | 29.09 | 7.09 | 62.4% | shards_partial +62.4 | 0 |  |  | 0.000 | yes, ejections in [30, 90 s): 0 | 0 |  |
| F2-run2 | false | true | 7.64 | 19.32 | 28.30 | 5.81 | 62.4% | shards_partial +62.4 | 0 |  |  | 0.000 | yes, ejections in [30, 90 s): 0 | 0 |  |
| F2-run3 | true | false | 6.27 | 18.79 | 25.41 | 5.99 | 62.9% | history_unavailable +1.7, shards_partial +61.2 | 0 |  |  | 0.000 | yes, ejections in [30, 90 s): 0 | 0 | history_unavailable exceeds the healthy phase's share by 1.68 points |
| F5-run1 | true | true | 5.37 | 5.10 | 17.92 | 5.57 | 0.1% |  | 0 | 3.996 | 50.0% | 3.996 | yes, ejections in [30, 90 s): 0 | 0 |  |
| F5-run2 | true | true | 5.52 | 5.47 | 17.57 | 5.98 | 0.0% |  | 0 | 2.996 | 49.9% | 2.996 | yes, ejections in [30, 90 s): 0 | 0 |  |
| F5-run3 | true | true | 5.96 | 5.48 | 17.75 | 5.87 | 0.0% |  | 0 | 1.995 | 50.0% | 1.995 | yes, ejections in [30, 90 s): 0 | 0 |  |
| F6-run1 | true | true | 6.33 | 6.37 | 34.04 | 6.55 | 0.1% |  | 0 | 0.996 | 50.0% | 0.996 | yes, ejections in [30, 90 s): 0 | 0 |  |
| F6-run2 | true | true | 6.25 | 5.71 | 18.39 | 6.71 | 0.1% |  | 0 | 0.996 | 50.0% | 0.996 | yes, ejections in [30, 90 s): 0 | 0 |  |
| F6-run3 | true | false | 5.08 | 5.95 | 19.33 | 5.83 | 1.8% | history_unavailable +1.7 | 13 | 0.996 | 49.9% | 13.000 | yes, ejections in [30, 90 s): 0 | 0 | not recovered within 5 s |
