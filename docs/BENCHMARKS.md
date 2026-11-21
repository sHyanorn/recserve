# Benchmarks

recserve is a recommendation serving system in Go: retrieval, ranking and rerank behind one HTTP API, with sharded vector search, caching and degradation. This file holds its measured results, newest first. The headline is 550 requests/s on a 4-vCPU cloud VM: the highest rate with p99 ≤ 50 ms and at most 1% of the responses degraded, replaying the test split with the cache off, with p99 4.6 to 5.1 ms. Every service shares that VM's CPUs, so a figure is that machine's and that topology's, and figures from different VMs are not compared (Limits).

How each figure is measured is in [BENCHMARK_PROTOCOL.md](BENCHMARK_PROTOCOL.md), written before the runs; amendments made after some results, such as D054's, are dated in place with their reasons. Code comments and decisions that cite `docs/BENCHMARKS.md` with a protocol section such as Load tests or Noise baseline mean that file. Defects found after the runs are in [BENCHMARK_ERRATA.md](BENCHMARK_ERRATA.md). D-numbers are entries of [DECISIONS.md](DECISIONS.md); F1 to F6 are the faults of the fault runs; M0 to M8 are the milestones.

## Current state

| What | Result | Section |
|---|---|---|
| Headline, replay, cache off, with the earlier 3 ms history budget | 550 requests/s, p99 4.61, 4.59 and 5.07 ms; one gateway on 2 CPUs about 1,800 requests/s, an estimate | [M6](#m6-2026-11-11) |
| Faults at 275 requests/s, cache off, outlier detection, three runs each, second set (a second VM) | pass: replica killed, 50 ms on a shard, replica paused, searches hanging, fault-phase p99 6.8 to 20.5 ms; fail: Valkey down, one run recovering in 7 s with 5 allowed; not judged: consumer paused. The first set, another VM, failed 50 ms on a shard and searches hanging on one run each. No breaker opening in five no-fault runs, cache on | [M7, second set](#m7-second-set-2026-11-12) |
| Rollouts and a swap in kind, 100 requests/s, SPEC §5's budgets | no failed response; at most 0.133% of a phase's responses degraded | [M6](#m6-2026-11-11) |
| recall@10 on 43,249 Video_Games test events, each degradation forced | 0.0181 healthy, 0.0098 without co-occurrence, 0.0050 without the ranker; without the encoder, with a shard missing or with HNSW at ef 400, within 0.0004 of healthy | [M7](#m7-2026-11-12) |
| recall@10 with state kept from the event stream against the bundle frozen, Musical_Instruments, its one registered test read | 0.0317 against 0.0200, +0.0117, 95% interval over users +0.0096 to +0.0137, confirming the registered hypothesis; Video_Games, exploratory, 0.0375 against 0.0197 | [M8](#m8-2026-11-14-and-15) |
| Headline on the recommendations, test events requested then written in time order, cache off | 150 recommend requests/s with the stream state, 300 without; gateway CPU per recommendation 1.79 to 1.91 times as high with it; not compared with M6's or M7's | [M8, stream state](#m8-stream-state-in-serving-2026-11-15) |
| `vec.Dot` in AVX2, `vec.Dot32` in AVX | replicas' CPU per request 37% lower and p99 15 to 26% lower at 200/s; `Dot32` 2.7 to 3.3 times faster, p99 in the service lower only at 400/s, by 0.01 ms, not attributed | [M6](#m6-2026-11-11) |
| HNSW on glove-100, one thread | p99 1.0 to 1.3 times faiss's at the same ef; recall@100 reaches 0.95 only at p99 7.66 ms, above M2's 5 ms | [M6](#m6-2026-11-11) |
| seqrec-lab's bundle against the baseline | not run yet (TASKS) | |

## Results

### M8, stream state in serving (2026-11-15)

By [Stream state in serving](BENCHMARK_PROTOCOL.md#stream-state-in-serving-m8-d062), every step on one boot, f730b1e7, of one VM (Intel Xeon @ 2.10GHz, 4 vCPU, 15.7 GiB, kernel 6.18.44, Go 1.27.4, Docker 29.9.2, cgroup v1; noise reference 711,611 ns per pass). Files: `bench-results/m8/serving/`. Rates are recommend requests/s. Commit 7e7639a, made after the protocol, changes only `BenchmarkRealStreamTake`, to keep the loaded data live between its two heap readings.

**Headline: 150 with the state, 300 without** (phase 2, all 12 runs valid, none noisy).

- Without the state the grid stopped at 1,200 (median p99 101.47 ms). 200 is its highest grid rate that meets the condition; 300 meets it, with p99 16.02 to 16.35 ms and 0.35 to 0.41% degraded, and 350 does not, with 1.19 to 1.59%.
- With the state the grid stopped at 800 (median p99 319.11 ms). 100 is its highest grid rate that meets the condition; 150 meets it, with p99 19.48 to 23.68 ms and 0.17 to 0.96% degraded.

**Phase 1**, three runs per group and rate, all 24 valid and none noisy (`phase1/table.md`), medians for the ratios:

| Rate | p99 off, ms | p99 on, ms | Degraded off | Degraded on | Gateway CPU per recommendation, on over off | Resident memory before load, on minus off |
|---:|---|---|---|---|---|---:|
| 100 | 9.66 to 10.68 | 15.50 to 17.82 | 0.00 to 0.03% | 0.03 to 0.23% | 1.865 | +243 MB |
| 200 | 11.80 to 13.18 | 31.59 to 37.41 | 0.07 to 0.22% | 1.12 to 1.57% | 1.790 | +215 MB |
| 400 | 23.38 to 28.22 | 65.12 to 99.14 | 2.24 to 6.00% | 10.63 to 25.51% | 1.827 | +246 MB |
| 800 | 44.79 to 47.94 | 287.57 to 627.01 | 32.26 to 39.82% | 76.75 to 78.25% | 1.906 | +232 MB |

- Kafka's CPU per recommendation, on over off, is 1.445, 1.254, 1.023 and 0.922; heap in use before load is 222 to 274 MB higher.
- The gateway keeps the state at 100 on this setup: each on run is valid and unflagged, with p99 ≤ 50 ms and at most 1% degraded. 200 does not show it (1.12 to 1.57% degraded), and at 400 and 800 each on run misses both conditions.
- At 100 the replicas' CPU per call is higher with the state in every run, in ranges that do not overlap on either shard (shard 0: 2.9488 to 3.0249 ms off, 3.0398 to 3.1123 ms on; shard 1: 3.0167 to 3.0869 ms off, 3.0981 to 3.1864 ms on), although the second replicas asked per recommendation are the same, 0.1740 to 0.1742. By the protocol this marks 100 as a measurement anomaly on both shards; its cause is not known. At 200, 400 and 800 a run has more than 1% degraded, so the reading is not judged.
- In every on run no event taken was a duplicate or poison and no record was left to read at the window's end. The gateway served 2.80 to 3.56 s after its container started in the off runs and 5.00 to 5.69 s in the on runs; its peak resident memory was 346 to 405 MB and 573 to 662 MB.

**Correctness gate: pass.** On the preload, after 2,000 writes and after a swap away and back, the gateway answered the requests of test events 0 to 999, 2,000 to 2,999 and 3,000 to 3,999 with the items, in order, that the pipeline in process gives, none degraded, with a largest score difference of 0 (`gate/gate-f730b1e7.txt`).

**Micro-benchmarks** (ten counts each on one core, `micro/micro.md`): taking a valid event into the state takes 2,990.5 ns (2,795 to 3,820) and adds 230.5 B of heap; over every tenth valid event a request through the pipeline in process takes 2.383 ms (2.191 to 2.501) on B1 without the state and 4.639 ms (4.243 to 4.765) on B1s8 with it. Measured separately, a popular list read that misses the kept list takes a median of 0.413 ms and the co-occurrence rows of a request's seeds 1.384 ms (5.533 seeds on average).

**Swaps** (one run of the on group at 200, valid, not noisy, peak resident memory 890 MB, `swap/swap.md`): to B1s6, B1 and B1s8, prepare took 760, 690 and 4,055 ms and drain 1, 27 and 3 ms. Over the 7 windows of 1 s that overlap a swap the recommendations' p99 is 88.00 ms and their max 114.88 ms; over the other 143, 48.76 ms and 202.30 ms.

**Profiles at 400** (`profile/`), both runs in overload (6.49% off and 29.08% on degraded, p99 29.03 and 109.50 ms): gateway CPU per recommendation 1.643 ms off and 3.161 ms on; with the state `fresh.top` has 42.19% of the samples cumulative.

### M8 (2026-11-14 and 15)

By [the registration](registration/m8-freshness.md) and its amendment "2026-11-15: Video_Games read 2 stopped before any metric". Each read sends one request per test event through the pipeline in process, every stage budget off, MMR included; the references are computed without a request. The reads record no environment, and per-event ranks are not committed (D011); the end of each read's file gives the SHA-256 of each row's rank file.

**H1 confirmed.** On Musical_Instruments' 42,595 test events, S3's recall@10 is 0.0317 and F0's 0.0200. S3 − F0 is +0.0117, with a 95% interval over users of [+0.0096, +0.0137] and over events of [+0.0102, +0.0131]; the interval over users is above 0 (section 8). The SHA-256 of the rank files: F0 49588aab22c8ad3f094ae69325a8209bac3c198d6bb31034e40eb093ee72910d, S3 7cf93b94779ad5a76b93e0a389d0d2d4f6b1ca8871ad218557d6f05457d47c04.

**The conditions of section 7**, Musical_Instruments (MI) with a window of 30 days, Video_Games (VG) with 14; VG's rows are exploratory (section 11). Recall@10 of all test events (NDCG@10 and Recall@20 are in the reads' files):

| Row | MI Recall@10 | VG Recall@10 |
|---|---:|---:|
| F0, bundle frozen | 0.0200 | 0.0197 |
| S1, popularity from the stream | 0.0186 | 0.0247 |
| S2, S1 with co-occurrence from the stream | 0.0197 | 0.0298 |
| S3a, B1s6 (no recency features) with S2's sources | 0.0237 | 0.0431 |
| S3, B1s8 (recency features) with S2's sources | 0.0317 | 0.0375 |
| R7, bundle rebuilt every 7 days | 0.0242 | 0.0381 |
| R30, rebuilt every 30 days | 0.0231 | 0.0355 |
| popularity reference | 0.0155 | 0.0320 |

| Δ Recall@10 | MI mean, 95% CI over users | VG mean, 95% CI over users |
|---|---|---|
| S3 − F0 | +0.0117 [+0.0096, +0.0137] | +0.0178 [+0.0152, +0.0202] |
| S1 − F0 | -0.0014 [-0.0020, -0.0008] | +0.0050 [+0.0037, +0.0062] |
| S3 − S3a | +0.0081 [+0.0062, +0.0099] | -0.0057 [-0.0075, -0.0039] |
| S3 − R7 | +0.0075 [+0.0056, +0.0094] | -0.0006 [-0.0035, +0.0019] |
| S3 − R30 | +0.0086 [+0.0067, +0.0105] | +0.0019 [-0.0009, +0.0044] |

- In both categories S3 is above F0 with intervals above 0. The parts differ: the stream's popularity alone (S1 − F0) is -0.0014 on MI and +0.0050 on VG, and the two recency features (S3 − S3a) +0.0081 and -0.0057, each interval excluding 0.
- Against rebuilding, S3 beats R7 and R30 on MI, with intervals above 0, and does not on VG, where the intervals include 0. Rebuilt every 7 days is above the frozen bundle on both (R7 − F0 +0.0042 and +0.0184).
- Recall@10 by whether the event's item has a train event (MI 28,609 with and 13,986 without, VG 17,091 and 26,158; `read-*.md`): F0 is 0.0299 and 0.0000 on MI and 0.0499 and 0.0000 on VG; S3 is 0.0284 and 0.0386 on MI and 0.0105 and 0.0551 on VG, so on VG S3 is far below F0 where the item has a train event, and above it elsewhere. R7 is 0.0317 and 0.0089 on MI, 0.0557 and 0.0266 on VG.
- Read 1 (`read-1.md`): B1's healthy recall@10 is above B0's in both, 0.0200 against 0.0187 on MI and 0.0197 against 0.0181 on VG; on VG, B0 is b7ff0634dd7b and gives M7's healthy and reference recall@10 again, 0.0181 and 0.0043.

**Choices on valid (section 6).** The ALS settings and windows are in `bench-results/m8/<category>/` (`als-grid.txt`, `windows.txt`) and the bundles' versions at the top of each read's file: the window is 14 days for Video_Games (14 and 30 days tie at recall@10 0.0581, and 14 has the higher NDCG@10) and 30 for Musical_Instruments.

**Reads** (`bench-results/m8/test-reads.log`): Video_Games' read 1 at 868f9be (2026-11-14T22:01:40Z); its read 2, stopped by the kernel for memory before any metric, run again under the amendment at b1df253 (2026-11-15T00:37:15Z); Musical_Instruments' one read at 6bfdbab (02:36:09Z). Two tests before it and `TestRealDataStreamGolden` after it opened Musical_Instruments' test file through `loadRealData` for its earliest event time only; none sent a test event as a request. Between Video_Games' read 1 and the stopped read 2 a script read its test file's event times, and a temporary test outside the repository built the stream state from all three splits, the 43,249 test events included, to check golden stream inputs, printing no metric (the amendment).

**Validity (section 10).** A read that fails a condition stops before it writes its file, and each of the three counted reads wrote its file. The checks, at a5ff644 in `internal/recommend` (the two on-valid ones earlier, at d0eb293 for Video_Games and 6bfdbab for Musical_Instruments): no response of a condition is degraded and the forced conditions report their reasons exactly when forced; every test event is at or after 1658002729837; each request's window count equals a binary search of its item's sorted event times; Video_Games' read 2 repeats read 1's F0 and vector-reference ranks; on valid, the popularity reference equals fresh.py's for every window and B1's vector reference equals the grid script's.

### M7, second set (2026-11-12)

By [Second M7 set](BENCHMARK_PROTOCOL.md#second-m7-set-breaker-on-a-failure-rate-cover-benchsum-as-written), on one VM other than the first set's (Intel Xeon @ 2.10GHz, 4 vCPU, 15.7 GiB, kernel 6.18.44, Go 1.27.4, Docker 29.9.2, cgroup v1; `bench-results/m7/rerun/*/env.txt`), with images built from e0f2d69. Session reference 619,668 ns (a baseline above 681,635 ns is flagged); bundle b7ff0634dd7b. These results are judged against the criteria only.

The runs' baselines record a different noisebase sha256 (6d64fe17…) from the reference's (e11ef169…) and uncommitted changes: `make bench-fault` rebuilt the binaries while the new results were untracked in the tree; built again at e0f2d69, `cmd/noisebase` gives e11ef169… from a clean tree and 6d64fe17… with an untracked file.

**Breaker openings with no fault: 0 in 5 runs, as expected** (`rerun/breaker/`). Five runs took five attempts, all valid; the healthy phases had a p99 of 7.01 to 8.62 ms and no error.

**Fault runs: F1, F2, F5 and F6 pass; F3 fails on one run's recovery; F4 is not judged.** `rerun/faults/`, with the cache off, hedge delay 2.5 ms, history budget 5 ms and outlier detection, by the criteria of Faults amended for M7, and for F2, F5 and F6 also those of outlier detection. 18 runs took 23 attempts; 6 were invalid, all on flagged baselines. The valid runs' healthy phases had a p99 of 7.05 to 10.12 ms and at most 0.22% degraded; no run had an error.

| Fault | Result | Fault phase p99, ms | Degraded in the fault phase | Recovery, s |
|---|---|---|---|---|
| F1 replica killed | pass | 7.22, 7.76, 7.69 | 0.0% | 4.0, 1.0, 4.0 |
| F2 50 ms on shard 1 | pass | 20.48, 19.87, 19.60 | 62.4%, `shards_partial` | 0, 0, 0 |
| F3 Valkey unavailable | fail, run 2 | 3.96, 2.41, 2.73 | 99.9%, `history_unavailable` | 1, 7, 1 |
| F4 consumer paused | not judged: run 1 has no valid attempt in three, all flagged | 8.97, 7.50 (runs 2 and 3) | 0.1, 0.0% | 0, 2 |
| F5 replica paused | pass | 7.18, 6.95, 7.42 | 0.1% | 3.0, 2.0, 3.0 |
| F6 searches hang | pass | 6.94, 7.07, 6.84 | 0.1, 0.1, 0.0% | 1.0, 1.0, 1.0 |

- The replica killed, paused or hanging is back: annshard-1's share of shard 0's calls in [210, 270 s) is 49.9 to 50.1% in every F1, F5 and F6 run. F5's `shards_partial` share in [0, 13 s) after the pause is 0.14 to 0.17%, F6's over the fault phase 0.03 to 0.04 points above the healthy phase's, and `recserve_shard_ejections_total` is 0 at 30 and at 90 s in every valid run, so the criteria of outlier detection are met. The breaker did not open outside F3's fault phase in any of the 23 attempts.
- F3 opened the breaker once per run, at 90.51 to 90.52 s, attributed to errors. **F3's failure:** run 2's healthy p99 of 8.52 ms puts its limit at 10.23 ms, and the windows starting 0 to 6 s after removal had a p99 of 10.36 to 10.80 ms, with 6.80% degraded at 0 s against a limit of 1.07%; the window at 7 s had 9.27 ms. The windows that failed hold 156 s, a second with a p99 of 15.86 ms (11.22 ms at 158 s, 25.18 ms at 159 s) in which no history read ran past its budget; in run 2 one history read ran past it in 159 s and two in 160 s, and run 1, with 24.04 ms at 159 s, recovered after 1 s against its limit of 11.57 ms. The breaker stayed closed and no Valkey error occurred (`F3-valkey-per-second.tsv`). What raised run 2's p99 was not established.
- F4's attempts were flagged 4 of 6 times, against 2 of 17 for the other faults; three of its five flagged baselines were taken after the load, more than 270 s after Kafka's recreation, and two before it. What raises them was not established.
- Neither of shard 1's replicas was taken out while both had the 50 ms latency: annshard-3's count is 0 at 150 s and 1 at 151 s in every F2 run (`ejections-per-second.tsv`). In the invalid attempt F2-run1-a1 both of shard 1's replicas had been taken out once by 30 s, which D041's rule, asserted by `TestOneReplicaOutAtATime`, does not allow while the first is out; the files do not give the order or duration, since that attempt's per-second reads were not committed.

### M7 (2026-11-12)

Every M7 run was on one VM: Intel Xeon @ 2.10GHz, 4 vCPU, 15.7 GiB, kernel 6.18.44, Go 1.27.4, Docker 29.9.2, cgroup v1 (`bench-results/m7/*/env.txt`; `quality.md` has none). Session reference 473,053 ns; bundle b7ff0634dd7b. The breaker's and the fault runs used images built from c3679be, before outlier detection; `faults/env.txt` records its code as uncommitted changes in the working tree, which no image or binary of those runs contains. An invalid attempt's files are removed before its rerun, so its verdict, flag and count of breaker openings come from the script's log and are not committed.

#### Valkey circuit breaker openings: the two attributed openings are calls that ran out of their budget; five of the six openings fell in invalid attempts

`make bench-fault FAULT=none RATE=275` (`breaker/`). Five valid runs took 9 attempts. Each of the 4 invalid attempts had 1.70% of its healthy phase degraded and the breaker open in it (1, 1, 2 and 1 openings); of the valid runs only none-run3 had an opening.

- Attributed by the rule: none-run3's attempt 2 at 18.992 s, in the warmup, with 6 `timeout` and 0 `error` calls failed; none-run5's attempt 1 (invalid) at 86.599 s, in the healthy phase, with 6 `timeout` (5 history, 1 cache read) and 0 `error`. Both are calls that ran out of their budget, which D037 point 3 counts as failures; no Valkey error is among them. The other four openings are not attributed. Why five history reads in a row passed 5 ms is not established.
- The openings of none-run2's attempts 1 and 2 and none-run3's attempt 1 are not attributed, since the rerun replaced their files; none-run5's attempt 1 was attributed from a copy of its gateway log and reads made outside the repository, so its row cannot be checked from the committed files. The fault runs at c3679be show the same: outside F3's fault, whose openings are attributed to errors, all six openings in their files are attributed to timeouts (F1-run2 at 11.8 and 243.8 s, F2-run1 at 105.3 s, F3-run1 at 203.5 s, the invalid F4-run2 at 113.4 and 237.0 s).

#### Fault runs with the cache off: F1, F3, F5 and F6 pass; F2 fails on one breaker opening; F4 is not judged

`make bench-fault FAULT=F1 ... F6 RATE=275` (`faults/`), with the cache off, hedge delay 2.5 ms, history budget 5 ms, by the criteria of Faults amended for M7. 18 runs took 27 attempts; 10 were invalid, 7 on flagged baselines and 3 with 1.72 to 1.74% of the healthy phase degraded. The valid runs' healthy phases had a p99 of 4.70 to 6.83 ms; no run had an error.

Fault-phase p99 in ms, three runs each: F1 pass, 4.79, 5.16, 4.38 (recovery 2.0, 4.0, 5.0 s); F3 pass, 1.75, 1.75, 1.91, with 99.9% `history_unavailable`; F5 pass, 17.45 to 17.70, 5.5% `shards_partial`; F6 pass, 18.17 to 18.39, 24.9 to 25.0% `shards_partial`, reported; F2 fail on run 1, 18.60 to 18.73, 62.4 to 63.1% `shards_partial`. F4 is not judged: run 2 has no valid attempt in three, all flagged.

- annshard-1's share of shard 0's calls in [210, 270 s) is 50.0% in every F1, F5 and F6 run.
- F2 fails on run 1 alone, whose fault phase had the breaker open for a second (`history_unavailable` 1.71 points above the healthy phase, from the opening at 105.3 s); runs 2 and 3 pass.
- F5's `shards_partial` share in [0, 13 s) after the pause is 25.1%, F6's over the fault phase 24.9 to 25.0%, both above the 1% by which M6 adopted outlier detection; the 40% the protocol expected for F5 without it is not a criterion.
- F4 had 4 of its 6 attempts flagged, the other faults 3 of 21. The only flagged F4 attempt whose files remain, run 2's third, measured 535,526 ns in its baseline after Kafka's recreation against the reference's 473,053; what raised it was not measured.

#### Recommendation quality on b7ff0634dd7b

`make bench-quality` (`quality.md`), by the protocol as amended for M7: 43,249 test events, one request each, 8,378 of whose histories give no user vector. Every condition is valid. Differences from the healthy pipeline, with paired bootstrap 95% intervals:

| Condition | Recall@10 | Δ Recall@10 |
|---|---:|---|
| healthy | 0.0181 | |
| reference (exact scoring of all items on the encoder's vector) | 0.0043 | -0.0139 [-0.0151, -0.0126] |
| encoder fails | 0.0182 | +0.0001 [-0.0002, +0.0004] |
| co-occurrence fails | 0.0098 | -0.0083 [-0.0093, -0.0074] |
| ranker fails | 0.0050 | -0.0131 [-0.0143, -0.0119] |
| shard 1 of 2 fails | 0.0182 | +0.0001 [+0.0000, +0.0003] |
| HNSW, ef 400 | 0.0181 | +0.0000 [+0.0000, +0.0000] |

Without co-occurrence or without the ranker every metric (Recall@10 above; Recall@20 and NDCG@10 in `quality.md`) is lower than healthy, with intervals below 0, and the two conditions' intervals do not overlap. Without the encoder, with shard 1 missing, and with HNSW, every difference is within 0.0004 of 0; some intervals exclude 0 by up to 0.0001.

#### Outlier detection: F5 passes; F6 fails on one run's recovery and F2 on one breaker opening; no replica was taken out in a healthy phase

`OUT=bench-results/m7/faults-outlier make bench-fault FAULT=F2, F5, F6 RATE=275` (`faults-outlier/`), on the same VM and setup, with images built from 03bb156, which adds outlier detection (D041). 9 runs took 11 attempts; 3 were invalid, all on flagged baselines, the three of F2's run 2, which so has no valid attempt.

Fault-phase p99 in ms: F5 passes, 5.10, 5.47, 5.48, with 0.0 to 0.1% degraded and recovery 4.0, 3.0, 2.0 s; F6 fails on run 3, 6.37, 5.71, 5.95, with 0.1, 0.1 and 1.8% degraded and recovery 1.0, 1.0 and 13.0 s; F2 fails on run 3 (19.10 and 18.79 ms in runs 1 and 3, 62.4 and 62.9% `shards_partial`, recovery 0).

- The criteria of outlier detection: F5's `shards_partial` share in [0, 13 s) after the pause is 0.14 to 0.17%, F6's over the fault phase 0.02 to 0.04 points above the healthy phase's, and `recserve_shard_ejections_total` is 0 at 30 and at 90 s in every run; F2 fails its criteria, so the third condition is not met. Against the runs before it at c3679be, F5's fault phase p99 is 5.10 to 5.48 ms against 17.45 to 17.70 and F6's 5.71 to 6.37 ms against 18.17 to 18.39, with no overlap.
- F2's run 3 had the breaker open at 90.7 s, in the fault phase, attributed to timeouts; F5's run 1, which passes, had it open at 238.668 s, in the recovery phase, also attributed to timeouts. F6's run 3 recovered after 13 s: its windows' p99 stayed above 1.2 times the healthy phase's 5.08 ms, with seconds of 7 to 12.7 ms in the first 10 s after removal, while annshard-1 was back after 1.0 s; its fault phase also had the breaker open at 144.1 s, attributed to timeouts. What raised F6's p99 after the removal was not established; neither failure involves a replica taken out in its healthy phase.
- In F2 annshard-3's count is 0 at 90 s and 1 just after the removal in the committed reads; the per-second reads, in `raw/` and not committed, show 0 at 150.0 s and 1 at 151.0 s in all three runs, so none was taken out while both replicas of shard 1 were slow. The script deletes the toxics of annshard-2 and then annshard-3, one request after the other; the order was not measured. F1, F3 and F4 were not run with outlier detection.

### M6 (2026-11-11)

Charts are in [bench-results/m6/report.md](../bench-results/m6/report.md). Every M6 figure was measured with Go 1.27.3; the toolchain moved to 1.27.4 after the last of them for a security fix in `net/http` (D001), and nothing was rerun. Four VMs of one kind ran it (Intel Xeon, 4 vCPU, 15.7 GiB, kernel 6.18.44, Docker 29.9.2, cgroup v1), the services in compose on bundle b7ff0634dd7b. Figures from different VMs are not compared.

VM 1 ran the profiles and `vec.Dot`'s comparison (2.10GHz, session reference 698,757 ns), VM 2 the load sweeps (2.30GHz, 657,992 ns), VM 3 the faults, hedge delay, swaps and kind, with the history budget at 5 ms (2.10GHz, 887,645 ns), and VM 4 the ANN runs and `vec.Dot32`'s comparison (2.10GHz, 779,905 ns).

On VM 2 a baseline above 0.724 ms, 10% over its reference's median of 0.658 ms, flags a run; 52 of the 88 attempts of the headline sweep were flagged; 10 of them were the last of their three attempts and stand as noisy. The `cmd/noisebase` binaries of a session's reference and baselines differ in the commit stamped into them and in their SHA-256 (`env.txt`), since every `make bench-*` rebuilds `bin/`; the noise baseline's code did not change in M6 (fa06dde).

#### Load sweep, replay with the cache off: headline 550 requests/s

`make bench-sweep SCENARIO=replay CACHE=off` (`sweep-replay-cache-off/`, the table in `sweep.md`), on VM 2.

- **Headline, 550 requests/s** (D005 as amended by D054): p99 4.61, 4.59 and 5.07 ms, 0.04, 0.02 and 0.07% degraded. 500 meets the condition; 600 does not, with one run 1.69% degraded and two flagged.
- The grid ran in full to 4,000 and once at 4,400, 98.79% degraded, where it was stopped. Under D005 as written the grid also meets the condition at 800, 1,600, 2,000, 2,400 and 3,200, rates with up to most of the responses degraded, and 600, 1,200, 2,800, 3,600 and 4,000 miss it on flagged baselines alone (D054). From 1,200 on p99 stays between 11 and 17 ms while the degraded share grows: 12 to 19% at 1,200, 55 to 69% at 1,600, 84 to 90% at 2,000, 94 to 96% at 2,400, 98 to 99% from 3,600.
- Nearly every degraded response is `history_unavailable`: the history read failed, ran past its 3 ms budget, or was not tried while the circuit breaker was open. A request without a history has no user vector and skips vector retrieval, so the replicas' CPU per request falls from 1.01 ms at 550 to 0.02 ms or less from 3,200 on. At 1,200 the degraded responses come in whole seconds (run 1: 45 of 60 seconds under 10%, 7 at 70% or more, from raw samples that are not committed), which matches the breaker leaving Valkey alone for a second (D037); the gateway exported no metric of its state to confirm it.
- **Capacity** (D050), an estimate from one quantity measured while the gateway shared its CPUs with the rest of the service: the gateway's CPU per request at 550 in the median-p99 run is c = 1.098 ms (1.098 to 1.199 in the three runs), the four replicas' 1.01 ms and Valkey's 0.11 ms, so one gateway on 2 CPUs is estimated at 2/c = 1,821 requests/s, and `-max-rate` becomes 1,450 and `-max-inflight` 152 (`deploy/kind/gateway.yaml`).
- **Cross-check** (`bench-vegeta RATE=275`, `vegeta/`, at 05d909c with uncommitted changes): at half the headline loadgen's p99 is 3.89, 3.92 and 4.14 ms (the last two noisy after three attempts) and vegeta's 3.35, 3.26 and 3.38 ms, every one of its 16,500 requests per run answered 200. vegeta times a request from its send and loadgen from its due time, so loadgen's figure also holds any wait before the send.

#### The other five scenarios, 100 to 800 requests/s

`make bench-sweep SCENARIO=... CACHE=... RATES="100 200 400 800"`. p99 is between 3.7 and 5.3 ms at every rate to 400 and between 4.4 and 7.9 ms at 800; where a rate misses, a run has more than 1% degraded or flagged baselines.

| Scenario | Cache hits, 100 / 200 / 400 / 800 per s | Most degraded run, 400 and 800/s | `/v1/events` p99, ms |
|---|---|---|---|
| replay, cache on | 47 / 55 / 65 / 78% | 1.67%, 3.79% | |
| writes, cache on | 42 / 49 / 58 / 69% | 1.69%, 3.41% | 2.2 to 2.9 |
| writes, cache off | | 0.03%, 5.09% | 2.1 to 3.7 |
| repeats, cache on | 58 / 63 / 71 / 80% | 1.70%, 5.03% | |
| repeats, cache off | | 0.05%, 1.77% | |

- Replay hits the cache here, where M3's runs had no hit: compose preloads only the train and valid splits, so a user's later test events have the first one's history and the same key (D030). Repeated requests add 2 to 11 points of hit rate, fewer at higher rates.
- **History budget (D039).** In replay with the cache on the share past the 3 ms budget is at most 0.03% at 100 and 200, but one run of 400 has 1.74%, so the rule runs the headline rate with 5 ms and 8 ms: 0.02, 0.00 and 0.00% with 5 ms in three unflagged runs, 0.00% with 8 ms in all three (two flagged). SPEC §5's budget becomes 5 ms; the headline itself was measured with 3 ms.

#### Fault runs: F1, F2, F3 and F6 pass by the criteria as written; F4 and F5 fail; two criteria cannot show what they state

`make bench-fault FAULT=F1 ... F6 RATE=275` (`faults/`), on VM 3, the cache on, the history budget 5 ms and the hedge delay then the default, 5 ms (2.5 ms came after). Every fault has three valid runs; 9 of the 27 attempts were invalid, 3 on flagged baselines and 6 with 1.73 to 3.47% of the healthy phase degraded (their files were replaced by the reruns). No valid run has an error in any phase, and every fault phase has p99 ≤ 50 ms.

| Fault | Result | Fault phase p99, ms | Degraded in the fault phase |
|---|---|---|---|
| F1 replica killed | pass as written; criteria defects 1 and 2 | 8.66, 9.02, 10.14 | 0.0% |
| F2 50 ms on shard 1 | pass as written; defect 2 | 21.08, 21.20, 21.19 | 35.0, 34.9, 34.5%, all `shards_partial` |
| F3 Valkey unavailable | pass | 2.99, 2.58, 3.06 | 99.9%, `history_unavailable` |
| F4 consumer paused | fail | 9.12, 10.60, 10.59 | 0.0, 0.1, 1.7% |
| F5 replica paused | fail; defects 1 and 2 | 18.89, 18.79, 19.12 | 2.6, 2.6, 4.4% |
| F6 searches hang | pass as written; defect 2 | 20.49, 20.10, 19.58 | 11.44, 11.18, 11.51% `shards_partial` |

- **F4 and F5 fail on one second each, with the gateway's Valkey circuit breaker open.** In every valid run each second outside F3's fault with more than 20% `history_unavailable` starts with the breaker changing from closed to open in the gateway's log, and it closes 1.0 s later: 15 openings in the 18 runs, 11 of them in the first 30 s, 2 in fault phases (F4 run 3 at 112.7 s, F5 run 3 at 145.5 s) and 2 in recovery phases. `history_unavailable` is 1.67 points above the healthy phase in F4's run 3 and 2.22 points in F5's run 3. The breaker then opened after five failed Valkey calls in a row (D037); what made them fail was not established here. M7 attributed two openings in runs with no fault to calls that ran out of their budget, and why those ran past it is not established.
- F2: mean coverage 0.72, shard 1's hedges 4,088 to 4,139 denied and 464 to 470 lost per run, none won. F6: mean coverage 0.91, shard 0's hedges 1,521 to 1,559 denied and 385 to 390 won. F4's lag was back at 0 in 1.15 to 2.11 s and property 9 holds in all three runs. F5's `shards_partial` is 11.75, 12.06 and 12.11% in [0, 13 s) after the pause 0.36, 0 and 0% in [13, 15 s) and 0% from 15 s on.
- **What the criteria's defects leave unshown.** (1) That annshard-1 serves searches again after F1 and F5: the check is met by CPU alone, and in the last 60 s annshard-1 used 7, 5 and 28 ms in F1 against 4 to 27 ms for the replicas never killed, and 236 to 248 ms in F5 against 231 to 258 ms; a replica's CPU without searches was not measured. (2) How fast searches recover after F1, F2, F5 and F6: the cache answers 86 to 99% of the lookups in the recovery phases (59 to 67% in the healthy phases), so shard 0 gets 2 to 25 calls per second there against 73 to 79 while healthy, and the criterion was met without power at this load. (3) Nothing changes: no run had an error in the warmup, and F4's warmup p99 per second is at most 20.1 ms.
- **Outlier detection (D041).** F5's `shards_partial` share in [0, 13 s) and F6's over its fault phase (about 39% of the responses that searched) are above 1%, so D041 records outlier detection as adopted; it was not built in M6. The healthy phases' p99 is 8.2 to 13.1 ms, with toxiproxy on every call to the replicas and Valkey and the cache on.

#### Hedge delay: 2.5 ms

`make bench-hedge RATE=275` (`hedge/`), on VM 3, the cache off. With hedging off the p95 of a call is 1.91 to 2.12 ms on shard 0 and 1.99 to 2.20 ms on shard 1, so the default becomes 2.5 ms, the median of the larger shard's, 2.05 ms, rounded up (D041). With 2.5 ms, 1.0 to 1.3% of each shard's searches sent a hedge, fewer than the 5% the rule aims at, since the rounding puts the delay past most of the slowest 5% of calls. p99 is 5.84 to 6.56 ms with hedging off and 5.70 to 6.32 ms with 2.5 ms, so a difference is not resolved. No run is invalid or flagged.

#### Bundle swaps: a higher p99 in the swap windows, with GC on or off; a lower maximum with GC off

`make bench-swap RATE=275` (`swap/`), on VM 3 with the 2.5 ms hedge delay, the cache off: nine swaps per run between b7ff0634dd7b and 5de55036a382, three runs with GC as configured and three with `GOGC=off`; none invalid or flagged. A swap's prepare took 65 to 864 ms, its drain at most 3.0 ms.

With GC on, p99 over the swap windows is 6.73 to 7.50 ms against 5.18 to 5.99 ms in the other windows, and their max 11.88 to 25.58 ms against 17.25 to 25.79 ms; with `GOGC=off`, 6.50 to 11.33 ms against 5.48 to 6.34 ms, and max 20.19 to 23.49 ms against 29.25 to 52.22 ms.

p99 over the swap windows is above the other windows' in every run, so turning GC off does not remove it. The rule's ratio of the maxima with `GOGC=off`, 23.39 over 31.16 ms, 0.75, reads as GC, and that reading does not hold: with GC on the swap windows' maximum is above the other windows' in one run and below in two, with overlapping ranges, and with `GOGC=off` it is below in every run while the other windows reach 52.22 ms with no GC at all. This record attributes nothing to GC and names no cause.

#### Rollouts and a swap in kind with SPEC §5's budgets

`make bench-kind VERSION=b7ff0634dd7b` (`kind/`), on VM 3 with the images of the 2.5 ms hedge delay: three new clusters, the gateways' stage budgets at SPEC §5's values, `kind-check`'s load of 100 requests/s through the gateways' rollout, the replicas' and a swap. Not judged (D022). No error in any phase; at most 0.133% of a phase's responses are degraded (single `vector_timeout` and `shards_partial` responses). p99 by phase is 3.80 to 9.91 ms, highest in the gateways' rollout (7.68 to 9.91), and the largest single response took 26.77 ms. `kind/` has no `env.txt`; its VM and its images, of 89fac9f, are stated here only.

#### Profiles, `vec.Dot` in AVX2 and `vec.Dot32` in AVX

**Profile at 400** (`make bench-profile` at d49b06e, `profile/`, VM 1): replay, cache off, p99 6.41 ms. CPU per request: the four replicas 1.82 ms together, the gateway 1.31 ms, Valkey 0.13 ms, Kafka 0.07 ms. In a replica `vec.Dot` has 54.2% of the samples; in the gateway `vec.Dot` has 8.6% and `vec.Dot32` 3.1%. The replicas use the most CPU per request and `vec.Dot` leads their profile, so it is the target (D053), not `vec.Dot32`, which exact search does not call.

**`vec.Dot` in AVX2** (`make bench-compare BEFORE=a3f48ae`, `optimization/`, VM 1): before a3f48ae, after ed28e3b; BenchmarkDot on CPU 1 and the service, replay, cache off, before and after alternating, three runs each, all first attempts, valid, none flagged.

BenchmarkDot at 64 dims takes 47.5 to 50.6 ns before and 10.5 to 11.4 after. p99 is 5.14 to 5.32 ms before and 3.93 to 4.39 after at 200/s, and 6.90 to 7.79 and 4.27 to 4.64 at 400/s; the replicas' CPU per request 1.94 to 1.98 and 1.23 to 1.29 ms at 200/s, 1.80 to 1.87 and 1.10 to 1.16 at 400/s; all lower after. The gateway's CPU per request at 400/s, 1.29 to 1.33 and 1.26 to 1.32 ms, is not resolved on this machine.

The inner product takes 4.2 to 5.4 times less time, the replicas use about 37% less CPU per request, and p99 falls by 15 to 26% at 200/s and 33 to 45% at 400/s. The gateway's CPU per request is lower after at 200/s. The after runs of BenchmarkDot ran from ed28e3b's working tree with the benchmark moved to its own file (the next commit); `vec.Dot` itself was ed28e3b's. The tests, both bundles' golden cases and the sixteen-lane definition hold (D053).

**annbench's profile on glove** (`make bench-ann STEP=profile`, VM 4): a graph with M = 16 and efConstruction 200, queried at ef 400, k 100: recall@100 0.8423, p99 4.33 ms, `vec.Dot32` 75.7% of the samples, so it gets the SIMD that TASKS made conditional on this (D055). **`vec.Dot32` in AVX** (`make bench-compare BEFORE=9a75fd5 MICRO=Dot32 ANN_GRAPH=...`, `optimization-dot32/`, VM 4): before 9a75fd5, after 20a8363; BenchmarkDot32, annbench on that graph built by the code before, and the service, three runs each before and after, all first attempts, valid, none flagged.

BenchmarkDot32 at 64 dims takes 20.2 to 20.5 ns before and 7.10 to 7.43 after; annbench's p99 on glove is 1.59 to 1.68 ms before and 0.84 to 0.97 after at ef 120, and 7.53 to 7.96 and 4.21 to 4.51 at ef 800. In the service p99 at 200/s, 5.47 to 5.94 ms before and 5.21 to 6.14 after, is not resolved on this machine, and at 400/s it is lower after, 5.99 to 6.50 and 5.40 to 5.98.

The inner product takes 2.7 to 3.3 times less time and annbench's p99 after is 0.50 to 0.61 times the p99 before at every ef; recall@100 is the same to its last digit (0.7121, 0.7738, 0.8423 and 0.8979 at ef 120, 200, 400 and 800). In the service `Dot32` runs only in MMR; the replicas' and the gateway's CPU per request are not resolved at either rate, and p99 at 400/s is lower after by 0.01 ms only, the after runs' largest against the before runs' smallest, which this record does not attribute to `Dot32`. The tests and the 32-lane definition hold (D055).

#### ANN: synth passes M2's criterion and glove fails it on latency, as in M2; p99 1.0 to 1.3 times faiss's on glove

`make bench-ann STEP=table` (`ann/`), on VM 4 with `vec.Dot32` in AVX: annbench as in M2, single thread, graphs built with 4 goroutines (2 per shard), faiss 1.15.1 with one thread on the same sets; every query point is a first attempt with no baseline flagged, and no build or faiss run is flagged.

| Set | Index | M / efC | recall@100 at ef = 120 / 800 | p99 ms at ef = 120 / 800 |
|---|---|---|---|---|
| synth | recserve/hnsw | 16 / 200 | 0.9956 / 1.0000 | 0.43 / 1.20 |
| synth | faiss | 16 / 200 | 0.9963 / 1.0000 | 0.41 / 1.50 |
| glove | recserve/hnsw | 16 / 200 | 0.7119 / 0.8978 | 0.83 / 4.83 |
| glove | faiss | 16 / 200 | 0.7185 / 0.8961 | 0.74 / 3.71 |
| glove | recserve/hnsw | 32 / 400 | 0.7946 / 0.9575 | 1.50 / 7.66 |
| glove | faiss | 32 / 400 | 0.8008 / 0.9610 | 1.12 / 6.21 |
| glove | recserve/hnsw, 2 shards | 32 / 400 | 0.8506 / 0.9805 | 1.78 / 8.63 |

- At the same ef, recserve's p99 is 0.80 to 1.05 times faiss's on synth and 1.01 to 1.33 times on glove; recall agrees within 0.007 at every point. The graphs are new builds with the new `Dot32`, and recall on glove agrees with M2's to the digits M2 shows.
- By M2's criterion synth meets it at ef 120 (0.9956, 0.43 ms). On glove recall@100 reaches 0.95 only with M = 32 at ef 800 (0.9575), at p99 7.66 ms, above 5 ms, so glove fails on latency alone; faiss reaches 0.9610 there at 6.21 ms. Two shards reach 0.94997 at ef 400, just under 0.95, so their lowest qualifying ef is 800, at 8.63 ms.

### M3 (2026-11-10)

Another cloud VM: Intel Xeon @ 2.10GHz, 4 vCPU, 15.7 GiB, kernel 6.18.44, Go 1.27.3, Valkey 9.1 and Kafka 4.4.0 in Docker over loopback (`bench-results/m3/env.txt`). Session reference 800,115 ns; the twelve baselines of the six runs lie between 780,814 and 834,455 ns, none flagged. Code at 47bad71, baseline bundle 5de55036a382, 814,586 events in the histories.

**Stage check of the Valkey path: fail, in the round that computes** (`stage-run*.log`, three runs). The stage "history read, then cache lookup" on Valkey with the cache on has a p99 of 2,902, 4,343 and 5,796 µs in the first round, where every request misses and computes, median 4,343 µs, above SPEC §5's 3 ms (run 1's 2,902 µs is below it); in the second round, where every request hits, 812, 2,353 and 1,679 µs, median 1,679 µs, within it. The tail is in the rounds that compute, where four workers without think time keep the four CPUs busy with about 2.4 ms of computation a request; the Valkey server and the client's goroutines waiting for a CPU fits that, but no CPU reading tests it, and these runs carried Kafka's health-check load (D039). The per-user history version is not built (D039): the control, 2,000 reads of a 16-byte value, has a p99 of 306, 745 and 450 µs, but it ran while nothing computed, and under the failing round's load the cache lookup, a `GET` that finds nothing, has the p99 of the history read (2,145 / 3,176 / 3,878 against 2,336 / 2,973 / 3,971 µs).

**Load at 200 requests/s on Valkey and Kafka: pass (M3 interim)** (`load-200-run*.json`). Cache on, no writes; the three runs are valid first attempts with 12,000 of 12,000 requests answered 200, p99 10.94, 10.97 and 11.99 ms. Degraded responses are at most 0.35% of a run (49 of 14,000), `history_unavailable` 0.22 to 0.31% from history reads past the 3 ms budget, with the circuit breaker never open; cache errors, 0.75 to 1.04%, are lookups and writes past it. No request hit the cache: 14,000 events of the split do not repeat a user with an unchanged history.

### M2 (2026-11-09)

A different cloud VM from M0: Intel Xeon @ 2.30GHz with AVX-512 and AMX, 4 vCPU, 15.7 GiB, kernel 6.18.44, Go 1.27.3 (`bench-results/m2/env.txt`). M0's noise binary ran about 28% faster here, so M0's latencies are not comparable with these; faiss and exact search were rerun here with the unchanged M0 code. Session reference 652,267 ns (threshold 717,494 ns).

**Hand-written HNSW: fail, on glove latency only** (`ann/*.json`). On synth, M = 16 and efConstruction = 200 pass: recall@100 reaches 0.95 first at ef = 120 (0.9956), with p99 0.57 ms. On glove, M = 16 never reaches 0.95 (0.898 at ef = 800, p99 7.63 ms) and M = 32 does only at ef = 800 (0.9575), with p99 15.48 ms, above 5 ms, in a point that was not flagged; faiss has 0.896 at 3.75 ms and 0.961 at 6.45 ms there. Only the latency part fails, so the criterion fails as written and no threshold changes; the index stays in use with its measured p99 given wherever retrieval latency is reported. No qualifying point has a p99 between 3.6 and 5 ms, so the verdict does not depend on this CPU being faster than M0's. Two shards, each with half of the vectors, reach 0.94997 at ef = 400, so the lowest ef reaching 0.95 is 800, with p99 15.71 ms: on 4 vCPUs two shards do not lower p99. Recall agrees with faiss within 0.007; p99 is 1.3 to 2.4 times faiss's on glove and 0.8 to 1.2 times on synth.

Of the 40 query points, 15 needed a third attempt and 8 (20%) were still flagged, among them recserve's synth and glove M = 16 points at ef = 800; the synth faiss run (the 0.8 to 1.2 ratio uses it), the glove exact run and both glove builds are flagged too. The session reference is the median of ten runs in two groups, 587,389 to 595,671 ns and 708,863 to 821,079 ns, so the threshold lies about 20% above the lower group and inside the upper one: a flag mostly records which group the machine was in, not a slowdown of a few percent.

### M0 (2026-11-08 and 09)

One cloud VM: Intel Xeon @ 2.10GHz, 4 vCPU, 15 GiB, kernel 6.18.44, Go 1.27.3. Session noise reference 820,984 ns per pass (`bench-results/m0/noise-reference.json`, upper median; threshold 903,082 ns); for the coder/hnsw rerun 816,650 ns (`noise-reference-2.json`, true median).

**R1: pass, with intra-op threads = 1.** Parity (max abs diff, Go against Python) is 0 for the encoders at L=50 and L=200 and the ranker, and 1.8e-7 for the pure-Go MLP. At concurrency 4, p99 is 1.02 ms for the encoder at L=50 (limit 5), 4.55 ms at L=200 and 2.91 ms for the ranker at C=500 (limit 10). The image builds and passes parity inside `distroless/cc-debian13`, 91.9 MB, built with two extra lines trusting the cloud session's CA (`onnx/image-build.txt`); the committed Dockerfile is unchanged. With ONNX Runtime's default thread pool p99 is 2.7 to 8.3 times higher, so services set intra-op threads to 1 (D015).

**R2: fail; the fallback is adopted** (`ann/*.json`). coder/hnsw v0.6.1, the pinned version, does not reach recall@100 ≥ 0.95 at any ef, on either set, with either setting (0.035 and 0.064 on synth, 0.023 and 0.033 on glove, at every ef; the rerun with `Ml = 1/M`, D017): its layer search keeps only k results, and its heap's `Max` and `PopLast` take the last array slot of a min-heap (`graph.go` `layerNode.search`, `heap/heap.go`), so ef only bounds the candidate queue. Its GC criterion holds, with the longest pause at most 0.26 ms. Upstream fixed both defects on 2026-06-22 (commit 36cab60, untagged); measured outside the criterion, on synth its recall@100 levels off at 0.824 while p99 grows to 21.3 ms and its build took 49 min, so it would fail too. faiss with the same settings reaches 1.000 on synth, so the data, ground truth and recall code agree; on glove it reaches 0.95 only at M = 32, ef = 800 (0.961), with p99 6.59 ms in a run flagged by a baseline 11% over the first reference (911,710 ns), so the outside reference does not meet the p99 ≤ 5 ms part of the criterion there either. The criterion is unchanged and applies to the hand-written index in M2. Six coder/hnsw points of synth M = 16 stayed flagged after three attempts, none affecting the verdict, which rests on recall.

## Limits

- **Machine.** Every service shares three CPUs of a 4-vCPU cloud VM, with loadgen on the fourth (the M0 and M2 ANN runs and the M3 stage check were not pinned). VMs share physical CPUs with other tenants and differ in speed (session references from 473,053 to 887,645 ns per pass), so a figure is that machine's and that topology's; figures from different VMs, boots and sets are not compared, and none predicts the capacity of a multi-machine deployment. A run whose baseline is over 10% above its session's reference is flagged and rerun at most twice; the last attempt is marked noisy if still flagged.
- **Capacity and headline.** The 1,821 requests/s is 2/c from one quantity, c = 1.098 ms of CPU per request at the headline (1.098 to 1.199 ms in the three runs), measured while the gateway shared its CPUs with the rest of the service. The headline ran with the 3 ms history budget; SPEC §5's is now 5 ms.
- **Faults.** M6's criteria could not show that a restarted replica serves again or how fast searches recover; M7's runs, with the cache off, can, and answer how the retrieval path behaves with every response searching; the cache-on behaviour is M6's result. The attributed breaker openings outside F3's fault (F3's are attributed to errors) are calls past their budget; why they ran past it is not established, and F2's two failures in the first set each rest on such an opening. F4's baselines were flagged most often (4 of 6 attempts in each M7 set) for a cause not established, and F4 is not judged in either. The second set's F3 run 2 (7 s to recover with 5 allowed) and the first set's F6 run 3 (13 s) have no established cause, and the order in which F2's two toxics were removed was not measured.
- **Records not committed.** The per-second reads (`raw/`) of M6 and M7; the figures of M6's and the first M7 set's invalid fault attempts; the gateway log from which none-run5's attempt 1 was attributed; `kind/`'s `env.txt`; M8's per-run scrapes and attempt records in serving, so which counted runs followed a flagged attempt is not shown; the per-event ranks of M8's reads (D011); the readings of Kafka's health check in compose, 639 to 686 ms of CPU per second when it started a JVM every 2 s and 24 ms after the fix (D039), whose only trace is the message of commit c802a68 ("about two thirds of a CPU"), so the M3 and M5 runs in compose carry that load; the measurement of F4's baseline after Kafka's recreation that the protocol's second M7 set cites. `quality.md` and M8's reads record no environment.
- **Tool defects.** The defects of `cmd/benchsum` found after the runs and their fixes are in [BENCHMARK_ERRATA.md](BENCHMARK_ERRATA.md); none changes a verdict. The committed tables and `*.fault.json` are the earlier tool's, not regenerated; an invalid attempt's Pass reads `false` in them.
- **Not resolved.** A higher p99 in the swap windows is not attributed to GC; the hedge delay's and `Dot32`'s effects on p99 are not resolved.
- **Retrieval and quality.** glove fails M2's p99 criterion in recserve's index and in faiss, and two shards on 4 vCPUs do not lower p99. M7's figures come from one bundle trained once and not updated over the test period. M8's confirmatory result is one test read of one category; Video_Games' rows are exploratory.
- **Stream state in serving.** One boot, not compared with M6 or M7; the replicas' CPU per call at 100 recommend requests/s was higher with the state on both shards, for a cause not known.
- **Not measured.** The sharded tier through annshard on glove; the cost of tracing (compose samples every request, a production deployment would sample about 1%, D051); `Dot32`'s Go code on CPUs without AVX2; seqrec-lab's bundle against the baseline.

## Reproduce

Most targets take DATA, the data directory, and VERSION, the baseline bundle; the compose scripts write an `env.txt` into each result directory. A session starts with `make bench-ref`; the sections named below are in [BENCHMARK_PROTOCOL.md](BENCHMARK_PROTOCOL.md).

- `bench-sweep`: `bench-results/m6/sweep-*`, `budget-history-*`; Load tests, Additions for M6.
- `bench-fault`: `m6/faults`, `m7/breaker`, `m7/faults`, `m7/faults-outlier`, `m7/rerun/*`; Faults, setup and pass criteria (M6) as amended for M7.
- `bench-hedge`, `bench-swap`, `bench-kind`, `bench-vegeta`: `m6/hedge`, `m6/swap`, `m6/kind`, `m6/vegeta`; Hedge delay, Bundle swaps under load, Rollouts with SPEC §5's budgets (with Kubernetes rollout check), and the cross-check of Load tests (`bench-vegeta` needs vegeta v12.12.0).
- `bench-profile`, `bench-compare`: `m6/profile`, `m6/optimization`, `m6/optimization-dot32`; Profiles and an optimization.
- `bench-ann` (PY with faiss-cpu 1.15.1, h5py and numpy): `m6/ann`; ANN in M6.
- `bench-quality`: `m7/quality.md`; Recommendation quality. `bench-bundles`: `m7/bundles-<B>.md`, none committed yet; seqrec-lab's bundle against the baseline.
- `bench-als-grid`, `valid-reference`, `bench-windows`, `rebuilds`, `bench-read`: `m8/<category>/`; [the registration](registration/m8-freshness.md). `bench-stream`: `m8/serving/`; Stream state in serving.
- `report` (needs uv): the charts in `m6/figures`.
