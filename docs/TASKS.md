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
- [ ] Fault tests: slow shard, dead replica, failed swap

## M5 Operations and deployment
- [ ] Rate limiting and bounded concurrency with 503
- [ ] A/B assignment; property test 3 with its broken variant; the arm joins the result cache key (D030)
- [ ] OpenTelemetry tracing
- [ ] Dockerfiles and docker compose with Prometheus, Grafana and Jaeger
- [ ] kind manifests: probes, resource limits, HPA with metrics-server (D022)
- [ ] NetworkPolicy that lets only the gateways reach the annshard port: the shard API has no TLS or authentication (D040)
- [ ] Graceful shutdown: readiness flip, preStop wait, drain, offset commit
- [ ] Zero-error check under load through a rollout restart and a bundle swap
- [ ] Logistic-regression ranker over the six features of SPEC §8, trained in `tools/bundle` and exported as `ranker.onnx` (D027), before M6. Its bundle carries a feature consistency golden: Python and Go compute the features of the same fixed requests, and the largest difference must be at most 1e-6
- [ ] With that ranker, the load check changes more than row 0: several rows, and rows past the first, must leave the other rows' scores unchanged
- [ ] An ONNX Runtime error that coincides with the end of the stage's context is reported as a timeout, since the call wraps the context's error; classify it by the error the run returned

## M6 Benchmarks and fault injection
- [ ] Fault-injection pass criteria committed to docs/BENCHMARKS.md before any fault run
- [ ] Load sweeps per docs/BENCHMARKS.md
- [ ] Per rate of the sweep, the share of requests whose history read or cache call runs past `-budget-history`; decide on the budget or the per-user history version left open in D039
- [ ] A cache-on workload with repeated requests: replaying the test split never hits (BENCHMARKS, M3)
- [ ] Fault runs with toxiproxy and container control (D021)
- [ ] ANN and sharded recall and latency on glove-100 and synthetic vectors
- [ ] `annbench` in shards mode labels results with the `-m` and `-efc` flags even when it reuses graph files built with other values; record the graphs' own parameters
- [ ] Latency during bundle swaps under load, and the cause of its largest values, GC or CPU contention with prepare (D031)
- [ ] Profiles and at least one optimization backed by before/after data
  - Candidate: SIMD inner product in the HNSW search; on glove the p99 is 1.3 to 2.4 times faiss's at equal recall on the same machine (BENCHMARKS, M2), and `vec.Dot32` is scalar Go
- [ ] Report with charts

## M7 Integration and evaluation
- [ ] Load seqrec-lab's exported bundle; reject one whose `train_cutoff_ts` is not before the first test event
- [ ] Golden top-10 lists may hold items whose vector is zero, which the shards leave out (D028); decide with seqrec-lab whether its export excludes them, so a golden case matches what the service retrieves
- [ ] Quality evaluation with each degradation forced
- [ ] README
