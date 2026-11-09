# Task List

Check off each item when it is done. All tests must pass at the end of every milestone.

## M0 Research and risk checks
- [x] SPEC, DECISIONS, TASKS
- [x] Benchmark and evaluation protocol in docs/BENCHMARKS.md, fixed before any result
- [x] ONNX Runtime from Go: parity, latency, image (experiments/m0/onnx)
- [x] Pure-Go HNSW against exact search and faiss on 1M-scale vectors (experiments/m0/ann)
- [x] Record the outcomes against the criteria; adopt the fallbacks where a criterion fails

## M1 Single-node request path
- [ ] go.mod, Makefile, golangci-lint, CI
- [ ] `make data`: download Amazon 2023 Video_Games (5-core timestamp split, metadata), verify checksums
- [ ] Baseline item vectors (offline matrix factorization) exported as a bundle (SPEC §8)
- [ ] Bundle loader with checksum and golden verification
- [ ] Point-in-time feature store with event ingestion; property test 1
- [ ] Retrieval: exact vector search, co-occurrence, popularity
- [ ] Merge, seen filter, dedup, MMR; property test 2
- [ ] HTTP API `/v1/recommend`, `/v1/events`, `/healthz`, `/metrics`
- [ ] `cmd/loadgen` and its coordinated-omission test (property 7)

## M2 Models, ANN, degradation, swaps
- [ ] User encoder and ranker through ONNX Runtime, one intra-op thread (D015)
- [ ] Hand-written HNSW (D016), measured with the R2 protocol
- [ ] Per-stage deadlines and degradation reasons in responses
- [ ] Result cache keyed by versions and arm; property test 5
- [ ] Two-phase bundle swap; property test 4

## M3 Sharded vector tier
- [ ] annshard service over gRPC
- [ ] Scatter-gather with per-shard deadlines, partial results and coverage; property test 6
- [ ] Replica failover and hedged requests
- [ ] Fault injection tests: slow shard, dead replica, failed swap

## M4 Operations
- [ ] Rate limiting and bounded concurrency with 503
- [ ] A/B assignment; property test 3 with its broken variant
- [ ] OpenTelemetry tracing
- [ ] Dockerfiles and docker compose with Prometheus, Grafana and Jaeger

## M5 Benchmarks
- [ ] Load sweeps per docs/BENCHMARKS.md
- [ ] ANN and sharded recall and latency on glove-100 and synthetic vectors
- [ ] Profiles and at least one optimization backed by before/after data
- [ ] Report with charts

## M6 Integration and evaluation
- [ ] Load seqrec-lab's exported bundle
- [ ] Quality evaluation with each degradation forced
- [ ] README
