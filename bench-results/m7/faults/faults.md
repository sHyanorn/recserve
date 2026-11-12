| Run | Valid | Pass | Healthy p99 | Fault p99 | Fault max | Recovery p99 | Fault degraded | Excess | Window recovery s | annshard-1 back s | annshard-1 [210, 270 s) | Recovery s | Outlier detection | Errors | Failures |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| F1-run1 | true | true | 5.36 | 4.79 | 13.74 | 5.37 | 0.0% |  | 0 | 1.995 | 50.0% | 1.995 | no | 0 |  |
| F1-run2 | true | true | 5.62 | 5.16 | 24.06 | 6.46 | 0.0% |  | 0 | 3.996 | 50.0% | 3.996 | no | 0 |  |
| F1-run3 | true | true | 5.02 | 4.38 | 17.22 | 4.83 | 0.0% |  | 0 | 4.996 | 50.0% | 4.996 | no | 0 |  |
| F2-run1 | true | false | 4.70 | 18.73 | 36.92 | 5.16 | 63.1% | history_unavailable +1.7, shards_partial +61.4 | 0 |  |  | 0.000 | no | 0 | history_unavailable exceeds the healthy phase's share by 1.71 points |
| F2-run2 | true | true | 5.10 | 18.60 | 27.14 | 5.04 | 62.4% | shards_partial +62.4 | 0 |  |  | 0.000 | no | 0 |  |
| F2-run3 | true | true | 5.12 | 18.69 | 46.15 | 5.73 | 62.4% | shards_partial +62.4 | 0 |  |  | 0.000 | no | 0 |  |
| F3-run1 | true | true | 5.19 | 1.75 | 10.11 | 6.12 | 99.9% | history_unavailable +99.9 | 1 |  |  | 1.000 | no | 0 |  |
| F3-run2 | true | true | 5.42 | 1.75 | 15.97 | 6.14 | 99.9% | history_unavailable +99.9 | 1 |  |  | 1.000 | no | 0 |  |
| F3-run3 | true | true | 6.19 | 1.91 | 17.28 | 5.58 | 99.9% | history_unavailable +99.8 | 1 |  |  | 1.000 | no | 0 |  |
| F4-run1 | true | true | 5.37 | 5.49 | 13.48 | 5.89 | 0.0% |  | 0 |  |  | 0.000 | no | 0 |  |
| F4-run2 | false | false | 7.12 | 5.63 | 53.27 | 6.44 | 1.7% | history_unavailable +1.7 | 0 |  |  | 0.000 | no | 0 | history_unavailable exceeds the healthy phase's share by 1.72 points |
| F4-run3 | true | true | 6.83 | 6.46 | 22.60 | 7.59 | 0.0% |  | 0 |  |  | 0.000 | no | 0 |  |
| F5-run1 | true | true | 5.52 | 17.70 | 21.67 | 5.61 | 5.5% | shards_partial +5.5 | 0 | 1.996 | 50.0% | 1.996 | no | 0 |  |
| F5-run2 | true | true | 5.37 | 17.45 | 21.19 | 5.25 | 5.5% | shards_partial +5.4 | 0 | 0.996 | 50.0% | 0.996 | no | 0 |  |
| F5-run3 | true | true | 5.53 | 17.57 | 33.40 | 6.10 | 5.5% | shards_partial +5.4 | 0 | 1.996 | 50.0% | 1.996 | no | 0 |  |
| F6-run1 | true | true | 6.33 | 18.39 | 75.41 | 6.29 | 25.0% | shards_partial +25.0 | 0 | 0.996 | 50.0% | 0.996 | no | 0 |  |
| F6-run2 | true | true | 4.87 | 18.17 | 58.96 | 5.16 | 24.9% | shards_partial +24.9 | 0 | 0.995 | 50.0% | 0.995 | no | 0 |  |
| F6-run3 | true | true | 5.41 | 18.31 | 46.30 | 5.51 | 24.9% | shards_partial +24.9 | 0 | 0.995 | 50.0% | 0.995 | no | 0 |  |
