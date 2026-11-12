# Task List

Check off each item when it is done. All tests must pass at the end of every milestone.

## M0 Research and risk checks
- [x] SPEC, DECISIONS, TASKS
- [x] Benchmark and evaluation protocol in docs/BENCHMARKS.md, fixed before any result
- [x] ONNX Runtime from Go: parity, latency, image (experiments/m0/onnx)
- [x] Pure-Go HNSW against exact search and faiss on 1M-scale vectors (experiments/m0/ann)
- [x] Record the outcomes against the criteria; adopt the fallbacks where a criterion fails

## M1 Single-node request path
- [x] go.mod, Makefile, golangci-lint, CI
- [x] `make data`: download Amazon 2023 Video_Games (5-core timestamp split, metadata), verify checksums
- [x] Baseline item vectors (offline matrix factorization) exported as a bundle (SPEC §8)
- [x] Bundle loader with checksum and golden verification
- [x] Point-in-time history store behind an interface (in memory for now); property test 1
- [x] Retrieval: exact vector search, co-occurrence, popularity
- [x] Merge, seen filter, dedup, MMR; property test 2
- [x] HTTP API `/v1/recommend`, `/v1/events`, `/livez`, `/readyz`, `/metrics`
- [x] `cmd/loadgen` and its coordinated-omission test (property 7)

## M2 Models, ANN, degradation, swaps
- [x] User encoder through ONNX Runtime, one intra-op thread (D015, D026)
- [x] Ranker contract and its ONNX call path (D015, D027)
- [x] Hand-written HNSW (D016), measured per docs/BENCHMARKS.md (Hand-written HNSW): synth passes, glove fails on latency only
- [x] In-process shards of the item vectors (`shard.Of`, two by default), exact or HNSW search per shard; property test 6 (D028)
- [x] Per-stage deadlines and degradation reasons in responses (D029)
- [x] Result cache keyed by version and history (in memory for now); property test 5 (D030)
- [x] Two-phase bundle swap; property test 4 (D031)

## M3 Event pipeline and shared state
- [x] Kafka (KRaft) and Valkey in docker compose (D019, D020)
- [x] `/v1/events` produces to the `events` topic, keyed by user, idempotent producer (D034)
- [x] `cmd/ingest`: consumer group, idempotent apply to Valkey, commit after apply; property tests 8 and 9 (D035)
- [x] History store on Valkey behind `-valkey-addr`; properties 1 and 8 as one contract over both stores (D033, D038)
- [x] Result cache on Valkey; singleflight; property test 11. A hit still reads the history to key it (D036)
- [x] History store keeps raw item IDs, so a swap may change the item IDs; the equal-IDs check of D031 is lifted (D032)
- [x] Circuit breaker and `history_unavailable` path; property test 10 (D037)
- [x] Count the result cache lookup of a request that fails after it
- [x] Integration tests against real Kafka and Valkey containers: the binaries, with ingest killed, Valkey paused and Kafka stopped (`make e2e`)
- [x] Stage check and load at 200 requests/s on Valkey (D039): the load runs pass, the stage check fails under saturation
- [x] Valkey connections opened and replaced outside the request's history budget, shown with latency added to connection setup only (D037 point 7)

## M4 Sharded vector tier
- [x] annshard service over gRPC (D040)
- [x] Scatter-gather over gRPC with per-shard deadlines, partial results and coverage, merging as the in-process shards do; property test 6 rerun over gRPC (D028, D040)
- [x] Replica failover and hedged requests; property tests 12 and 13 (D041)
- [x] Fault tests: slow shard, dead replica, failed swap; the gateway and four annshard binaries with a replica killed, a swap, an empty restart and a rejected swap (D041, D042, `make e2e`)
- [x] A `Prepare` names the version the gateway serves, which a replica never drops (D043)
- [x] The real bundle on replicas: every response equal to the in-process one, and a one-off latency measurement (D040, `make test-real`)

## M5 Operations and deployment
- [x] Rate limiting and bounded concurrency with 503 (D050)
- [x] A/B assignment; property test 3 with its broken variant; the arm joins the result cache key (D030, D049)
- [x] OpenTelemetry tracing, with Jaeger in the compose profile `app` (D051)
- [x] Images of the gateway, annshard and ingest; docker compose with Prometheus and Grafana, and an alert rule on the growth of `recserve_shard_failovers_total`, which counts the failures that another replica makes up for (D041, D047)
- [x] kind manifests: probes, resource limits, HPA with metrics-server (D022). The gateway's `-shutdown-delay` longer than the endpoint removal, and `terminationGracePeriodSeconds` above it plus the drains (D045). The replicas' readiness probe on `grpc.health.v1`, their addresses resolvable while not ready, and `minReadySeconds` above the 5 s reconnect wait with several gateways; check that the gateway finds a restarted Pod's new address in time (D044)
- [x] NetworkPolicy that lets only the gateways reach the annshard port, its enforcement checked in kind in CI: the shard API has no TLS or authentication (D040, D047)
- [x] Swaps across several gateways in an order that keeps at most two versions in use: the replicas hold two (D042, D048)
- [x] annshard readiness on `grpc.health.v1`: ready while it holds the version served, which the gateway has a replica prepare as soon as its connection is up and again whenever that version changes (D044)
- [x] Graceful shutdown: on SIGTERM annshard reports NOT_SERVING and drains its calls within `-drain`; the gateway answers `/readyz` with 503, closes connections after their response for `-shutdown-delay`, then drains; ingest commits its offsets as before (D045)
- [x] A rolling restart of the replicas through the gateway binary, one process at a time with SIGTERM, each waited for until ready: every response complete (D044, D045)
- [x] Zero-error check under load through a rollout restart of the gateways and of the replicas and through a bundle swap, in kind. Also how long a fresh deployment takes to settle when the gateways start before the replicas' names resolve: a gateway that cannot prepare its first bundle exits and is restarted
- [x] Through the gateway binary on annshard replicas: swaps in a row over three bundles, with a search of the previous version arriving after each prepare, and no shard skipped for `FAILED_PRECONDITION` (D043)
- [x] A bundle's version must be the digest of its files, so a replica given other vectors under a version's name fails to prepare it; no proto change (D046)
- [x] Logistic-regression ranker over the six features of SPEC §8, trained in `tools/bundle` and exported as `ranker.onnx` (D027), before M6. Its bundle carries a feature consistency golden: Python and Go compute the features of the same fixed requests, and the largest difference must be at most 1e-6
- [x] With that ranker, the load check changes more than row 0: several rows, and rows past the first, must leave the other rows' scores unchanged
- [x] An ONNX Runtime error that coincides with the end of the stage's context is reported as a timeout, since the call wraps the context's error; classify it by the error the run returned

## M6 Benchmarks and fault injection
- [x] Fault-injection pass criteria committed to docs/BENCHMARKS.md before any fault run
- [x] Load sweeps per docs/BENCHMARKS.md: headline 550 requests/s on this VM (D054), six scenarios, vegeta cross-check
- [x] From the sweeps, a gateway's capacity, and `-max-rate` and `-max-inflight` set from it in place of the one measurement of D050: about 1,800 requests/s on 2 CPUs (an estimate), 1,450 and 152 in kind
- [x] Per rate of the sweep, the share of requests whose history read or cache call runs past `-budget-history`; decide on the budget or the per-user history version left open in D039: the budget becomes 5 ms, no history version (D039)
- [x] A cache-on workload with repeated requests: replaying the test split never hits (BENCHMARKS, M3); in M6 replay hits 47 to 78%, repeats 58 to 80%
- [x] Fault runs with toxiproxy and container control (D021): F1, F2, F3 and F6 pass by the criteria as written; F4 and F5 fail, each on one second with the Valkey circuit breaker open. Two criteria cannot show what they state: that the restarted replica serves searches again (F1, F5) and recovery, at this load (F1, F2, F5, F6) (BENCHMARKS, M6)
- [x] In the fault runs, a replica that goes silent with its connection up, and one that answers keepalive pings while its searches hang: the share of responses with `shards_partial` during the 13 s before the keepalive acts, and for as long as the second lasts; decide on outlier detection (D041): about 12% and 11%, so it is adopted
- [x] Shard search latency through annshard and the hedge delay's default, from measured latencies (D041): 2.5 ms, from a p95 of a call of about 2 ms
- [x] ANN and sharded recall and latency on glove-100 and synthetic vectors: p99 1.0 to 1.3 times faiss's on glove at the same ef; glove reaches recall@100 0.95 only at 7.66 ms (BENCHMARKS, M6)
- [x] `annbench` in shards mode labels results with the `-m` and `-efc` flags even when it reuses graph files built with other values; record the graphs' own parameters: each shard reports its graph's, and shards that differ are an error
- [x] Latency during bundle swaps under load, and the cause of its largest values, GC or CPU contention with prepare (D031): the seconds with a swap have a higher p99, with GC on or off, and a lower maximum with GC off; no cause named
- [x] Latency through rolling restarts and swaps with SPEC §5's budgets: in kind the first requests of a new gateway took longer than 15 ms, so the zero-error check runs the gateways at 100 ms (D047); with SPEC §5's budgets, three runs had no error and at most 0.133% of a phase degraded (BENCHMARKS, M6)
- [x] Profiles and at least one optimization backed by before/after data: `vec.Dot`, 54% of a replica's CPU, in AVX2 (D053, BENCHMARKS M6)
- [x] `vec.Dot32` in SIMD, for the HNSW mode, only if annbench's profile on glove puts it first: first at 76%; in AVX, annbench's p99 on glove 0.50 to 0.61 times before at the same recall (D055, BENCHMARKS M6)
- [x] Report with charts: bench-results/m6/report.md, drawn by `make report`

## M7 Integration and evaluation
- [ ] Load seqrec-lab's exported bundle; reject one whose `train_cutoff_ts` is not before the first test event
- [ ] Golden top-10 lists may hold items whose vector is zero, which the shards leave out (D028); decide with seqrec-lab whether its export excludes them, so a golden case matches what the service retrieves
- [x] Quality evaluation with each degradation forced (BENCHMARKS, M7)
- [x] Replica fault criteria that measure what they state (BENCHMARKS, M6, the dated defects): a call count per replica, or the runs with the cache off, and the amended criteria committed before any run; then F1, F2, F5 and F6 again. Not compared side by side with M6's F3 and F4, from another VM
- [x] Outlier detection on the replicas, adopted in M6 (D041): a replica whose calls end at the shard deadline a number of times in a row leaves the selection until a trial search is answered; F2, F5 and F6 again with it (BENCHMARKS, M7)
- [x] The Valkey circuit breaker's state and calls as metrics; its openings in five runs without a fault, those that could be attributed by a rule committed before them (D037; BENCHMARKS, M7)
- [x] Of the breaker's 6 openings at 275 requests/s with no fault, the 2 that could be attributed came after calls that ran out of their budget (BENCHMARKS, M7): keep D037 point 3, count only errors, or open on a looser condition. It opens on a failure rate in a window of 1 s, timeouts still failures (D037, revised in M7); in five runs without a fault it did not open (BENCHMARKS, M7, second set)
- [x] README
- [ ] `cmd/benchsum`: read the healthy phase's ejections from the last read before 30 s to the last before 90 s, count F4's lag drain from 150 s, and give the breaker table's phases by 90 and 150 s, as reading 1 of Second M7 set says (BENCHMARKS, M7, second set, defects of benchsum)
- [ ] `annshard.Client.Prepare` returns only once every replica's `Prepare` has returned, so one replica whose `Prepare` never returns while its connection stays up, answering the keepalive's pings, holds a swap for `-prepare-timeout` (2 minutes) while the previous version serves, and a gateway's start as long; a replica gone silent loses its connection to the keepalive after about 13 s (D041 point 8). Return once each shard has a replica that holds the version and leave the others to catch up (catchUp), after a test that a `Prepare` arriving late cannot make a replica drop a version that a gateway still serves (D042 point 4, D048 point 3)
