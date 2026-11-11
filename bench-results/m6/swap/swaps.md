| Run | Valid | Noisy | Swap windows p99 | max | GCs | GC pauses ms | Other windows p99 | max | GCs | GC pauses ms | Prepare ms | Drain ms | Gateway CPU ms | Replicas CPU ms |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| gc-off-run1 | true | false | 6.93 | 20.19 | 0 | 0.00 | 5.54 | 52.22 | 0 | 0.00 | 65.3 to 166 | 0.0022 to 2.93 | 87.9 to 180 | 27.7 to 239 |
| gc-off-run2 | true | false | 11.33 | 23.39 | 0 | 0.00 | 6.34 | 31.16 | 0 | 0.00 | 76.8 to 271 | 0.0022 to 2.7 | 97.7 to 361 | 33.8 to 228 |
| gc-off-run3 | true | false | 6.50 | 23.49 | 0 | 0.00 | 5.48 | 29.25 | 0 | 0.00 | 65.9 to 151 | 0.00276 to 2.59 | 87 to 211 | 28.5 to 219 |
| gc-run1 | true | false | 6.90 | 18.65 | 7 | 2.92 | 5.99 | 25.79 | 207 | 73.40 | 74.8 to 157 | 0.00243 to 2.05 | 108 to 172 | 35.7 to 207 |
| gc-run2 | true | false | 7.50 | 25.58 | 6 | 1.44 | 5.19 | 17.25 | 205 | 78.37 | 70.4 to 864 | 0.00206 to 2.02 | 92.7 to 462 | 29.3 to 505 |
| gc-run3 | true | false | 6.73 | 11.88 | 7 | 1.93 | 5.18 | 19.85 | 207 | 72.00 | 74.7 to 145 | 0.00226 to 3.02 | 99.2 to 158 | 28.3 to 166 |

GOGC=off: median max over swap windows 23.39 ms, over the other windows 31.16 ms (ratio 0.75): GC
