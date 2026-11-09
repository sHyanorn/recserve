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
- [ ] Kafka (KRaft) and Valkey in docker compose (D019, D020)
- [ ] `/v1/events` produces to the `events` topic, keyed by user, idempotent producer
- [ ] `cmd/ingest`: consumer group, idempotent apply to Valkey, commit after apply; property tests 8 and 9
- [ ] History store and result cache on Valkey; singleflight; property test 11. A hit still reads the history to key it (D030); if that costs too much, key by a per-user history version instead
- [ ] History store keeps raw item IDs, so a swap may change the item IDs; lift the equal-IDs check of D031
- [ ] Circuit breaker and `history_unavailable` path; property test 10
- [ ] Count the result cache lookup of a request that fails after it; today only answered requests are counted
- [ ] Integration tests against real Kafka and Valkey containers

## M4 Sharded vector tier
- [ ] annshard service over gRPC
- [ ] Scatter-gather over gRPC with per-shard deadlines, partial results and coverage, merging as the in-process shards do; property test 6 rerun over gRPC (D028)
- [ ] Replica failover and hedged requests
- [ ] `hnsw.Read` checks only the arrays' shapes; check link targets, counts and levels before a shard serves an index from a file
- [ ] Fault tests: slow shard, dead replica, failed swap

## M5 Operations and deployment
- [ ] Rate limiting and bounded concurrency with 503
- [ ] A/B assignment; property test 3 with its broken variant; the arm joins the result cache key (D030)
- [ ] OpenTelemetry tracing
- [ ] Dockerfiles and docker compose with Prometheus, Grafana and Jaeger
- [ ] kind manifests: probes, resource limits, HPA with metrics-server (D022)
- [ ] Graceful shutdown: readiness flip, preStop wait, drain, offset commit
- [ ] Zero-error check under load through a rollout restart and a bundle swap
- [ ] Logistic-regression ranker over the six features of SPEC §8, trained in `tools/bundle` and exported as `ranker.onnx` (D027), before M6. Its bundle carries a feature consistency golden: Python and Go compute the features of the same fixed requests, and the largest difference must be at most 1e-6
- [ ] With that ranker, the load check changes more than row 0: several rows, and rows past the first, must leave the other rows' scores unchanged
- [ ] An ONNX Runtime error that coincides with the end of the stage's context is reported as a timeout, since the call wraps the context's error; classify it by the error the run returned

## M6 Benchmarks and fault injection
- [ ] Fault-injection pass criteria committed to docs/BENCHMARKS.md before any fault run
- [ ] Load sweeps per docs/BENCHMARKS.md
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
