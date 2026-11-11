| Quantity | Before | After | Verdict |
|---|---|---|---|
| BenchmarkDot32/100 ns/op | 30.3 to 34.2 | 10.5 to 11 | lower after |
| BenchmarkDot32/64 ns/op | 20.2 to 20.5 | 7.1 to 7.43 | lower after |
| annbench ef 120 p99 ms | 1.59 to 1.68 | 0.838 to 0.966 | lower after |
| annbench ef 200 p99 ms | 2.25 to 2.37 | 1.24 to 1.34 | lower after |
| annbench ef 400 p99 ms | 3.99 to 4.12 | 2.3 to 2.4 | lower after |
| annbench ef 800 p99 ms | 7.53 to 7.96 | 4.21 to 4.51 | lower after |
| annbench ef 120 recall@100 | 0.7121 | 0.7121 | within 0.001 |
| annbench ef 200 recall@100 | 0.7738 | 0.7738 | within 0.001 |
| annbench ef 400 recall@100 | 0.8423 | 0.8423 | within 0.001 |
| annbench ef 800 recall@100 | 0.8979 | 0.8979 | within 0.001 |
| 200/s p99 ms | 5.47 to 5.94 | 5.21 to 6.14 | not resolved on this machine |
| 200/s replicas' CPU ms per request | 1.76 to 1.83 | 1.71 to 1.88 | not resolved on this machine |
| 200/s gateway's CPU ms per request | 1.84 to 1.86 | 1.75 to 1.87 | not resolved on this machine |
| 400/s p99 ms | 5.99 to 6.5 | 5.4 to 5.98 | lower after |
| 400/s replicas' CPU ms per request | 1.43 to 1.52 | 1.39 to 1.47 | not resolved on this machine |
| 400/s gateway's CPU ms per request | 1.48 to 1.53 | 1.44 to 1.5 | not resolved on this machine |

annbench points left noisy after three attempts: 0 of 24
