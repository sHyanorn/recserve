| Run | Valid | Pass | Healthy p99 | Fault p99 | Fault max | Recovery p99 | Fault degraded | Excess | Recovery s | Errors | Failures |
|---|---|---|---|---|---|---|---|---|---|---|---|
| F1-run1 | true | true | 9.66 | 8.66 | 22.14 | 5.87 | 0.0% |  | 0 | 0 |  |
| F1-run2 | true | true | 10.58 | 9.02 | 38.55 | 6.01 | 0.0% |  | 0 | 0 |  |
| F1-run3 | true | true | 10.48 | 10.14 | 42.52 | 6.08 | 0.0% |  | 0 | 0 |  |
| F2-run1 | true | true | 13.08 | 21.08 | 34.31 | 8.55 | 35.0% | shards_partial +34.9 | 0 | 0 |  |
| F2-run2 | true | true | 11.68 | 21.20 | 54.87 | 8.11 | 34.9% | shards_partial +34.9 | 0 | 0 |  |
| F2-run3 | true | true | 11.37 | 21.19 | 35.12 | 8.95 | 34.5% | shards_partial +34.4 | 0 | 0 |  |
| F3-run1 | true | true | 11.52 | 2.99 | 24.84 | 7.83 | 99.9% | history_unavailable +99.9 | 1 | 0 |  |
| F3-run2 | true | true | 8.23 | 2.58 | 14.93 | 7.82 | 99.9% | history_unavailable +99.9 | 1 | 0 |  |
| F3-run3 | true | true | 9.45 | 3.06 | 18.13 | 7.65 | 99.9% | history_unavailable +99.9 | 1 | 0 |  |
| F4-run1 | true | true | 12.11 | 9.12 | 27.50 | 8.53 | 0.0% |  | 0 | 0 |  |
| F4-run2 | true | true | 11.80 | 10.60 | 38.67 | 9.26 | 0.1% |  | 0 | 0 |  |
| F4-run3 | true | false | 11.75 | 10.59 | 26.73 | 10.52 | 1.7% | history_unavailable +1.7 | 0 | 0 | history_unavailable exceeds the healthy phase's share by 1.67 points |
| F5-run1 | true | true | 10.59 | 18.89 | 25.42 | 6.69 | 2.6% | shards_partial +2.6 | 0 | 0 |  |
| F5-run2 | true | true | 12.44 | 18.79 | 24.01 | 7.59 | 2.6% | shards_partial +2.6 | 0 | 0 |  |
| F5-run3 | true | false | 11.57 | 19.12 | 28.91 | 6.70 | 4.4% | history_unavailable +1.6, shards_partial +2.6 | 0 | 0 | history_unavailable exceeds the healthy phase's share by 2.22 points |
| F6-run1 | true | true | 11.71 | 20.49 | 41.77 | 6.86 | 11.5% | shards_partial +11.4 | 0 | 0 |  |
| F6-run2 | true | true | 10.59 | 20.10 | 34.66 | 7.00 | 11.3% | shards_partial +11.2 | 0 | 0 |  |
| F6-run3 | true | true | 10.00 | 19.58 | 33.33 | 6.37 | 11.5% | shards_partial +11.5 | 0 | 0 |  |
