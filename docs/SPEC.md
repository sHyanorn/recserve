# recserve Specification

## 1. Summary

An online recommendation and search serving system written in Go. A request goes through multi-source retrieval, point-in-time feature lookup, model ranking and re-ranking, under a latency budget, with degradation, caching, A/B assignment, index hot swaps and a sharded vector tier. Serving engineering is the focus: latency, throughput, availability, distribution and observability. The models are simple or come from the sister project seqrec-lab.

## 2. Scope

### Required

- Retrieval from several sources in parallel: vector nearest neighbors (HNSW or exact), item co-occurrence, popularity fallback
- Features known at request time only: a user's recent events before the request timestamp, item features
- Ranking with an ONNX model; re-ranking with seen-item filtering, deduplication and diversity (MMR)
- Per-stage deadlines with degradation: a retrieval source that times out is skipped, a ranker that times out falls back to retrieval order, a missing shard yields partial results; every response reports what was degraded and the shard coverage
- Result cache keyed by user, request, index version, model version and experiment arm
- Rate limiting and overload protection (bounded concurrency, fail fast with 503)
- A/B assignment by deterministic hash of the user ID, configurable traffic split, arm and config version in every response
- Prometheus metrics and OpenTelemetry tracing
- Index and model hot swap: a new version loads and verifies in the background, then all shards switch atomically; no request mixes versions
- Sharded vector tier: shards with replicas, scatter-gather with per-shard deadlines, hedged requests, replica failover
- Event ingestion (`POST /v1/events`) that updates user histories online
- Reproducible open-loop load generator and a benchmark report
- docker compose that starts the whole system with one command

### Optional

- BM25 keyword retrieval fused with vector retrieval (hybrid search)
- A small fresh-item tier searched exactly beside the HNSW index

### Out of scope

- Consensus, automatic resharding, dynamic membership: the cluster layout is a static file
- Model training beyond simple baselines; seqrec-lab trains the sequence models
- A public online demo (D011)

## 3. Scale, stated plainly

The catalog served is Amazon Reviews 2023 Video_Games, 5-core: 25.6k items (137.2k before filtering). Exact search over that many 64-dimensional vectors takes about 2 ms on one core (scaled from the 1M-vector measurement in M0), so at this size neither ANN nor sharding is needed; the service shards it anyway to exercise the mechanism. ANN and shard performance is measured separately on 1M-scale vectors (glove-100-angular and synthetic vectors, D008), and the report says which figures come from which.

## 4. Architecture

```
                    client (HTTP/JSON)
                          │
        ┌─────────────────▼──────────────────────────────┐
        │ gateway (stateless, N instances)                │
        │  rate limit → A/B arm → cache                   │
        │  features: user history before request ts       │
        │  user encoder (ONNX)                            │
        │  retrieval fan-out ──────────┐                  │
        │    co-occurrence, popularity │ (in memory)      │
        │  merge → rank (ONNX) → filter, dedup, MMR       │
        └──────────────────────────────┼─────────────────┘
                                       │ gRPC scatter-gather,
                                       │ per-shard deadline, hedging
             ┌─────────────────────────┼───────────────────┐
     ┌───────▼───────┐          ┌──────▼────────┐
     │ annshard 0    │          │ annshard 1    │   each shard: R replicas
     │ replica a, b  │          │ replica a, b  │   HNSW + exact search
     └───────────────┘          └───────────────┘   versioned index snapshot
```

- **gateway** holds the feature store, co-occurrence table, popularity list and models in memory, loaded from versioned snapshots; it is stateless across requests apart from its cache.
- **annshard** holds one shard of the item vectors. Items are assigned to shards by hash of their internal ID.
- **Versions.** A bundle version names the vectors, the ID map and the models together. A swap is two-phase: every shard loads and verifies the new version (prepare), then the gateway activates it and pins each request to one version; shards keep the previous version until no request uses it.

## 5. Request path and latency budget

| Stage | p99 budget | On timeout |
|---|---|---|
| Rate limit, A/B, cache lookup | 1 ms | — |
| Feature lookup | 3 ms | request fails (500) |
| User encoder | 5 ms | vector retrieval skipped |
| Retrieval (parallel; shard deadline 15 ms) | 15 ms | source skipped, partial shards reported |
| Ranking (500 candidates) | 15 ms | retrieval order kept |
| Re-ranking and filters | 2 ms | — |
| Other overhead | 9 ms | — |
| **Server-side end to end** | **50 ms** | |

The service target is a server-side p99 of 50 ms; the headline figure is the highest request rate that meets it (D005). Filters are never skipped: a degraded response is still filtered.

## 6. API (draft)

| Method | Path | Description |
|---|---|---|
| POST | `/v1/recommend` | `{"user_id", "k", "ts"?, "exclude"?}` → items, scores, arm, versions, degraded reasons, coverage |
| POST | `/v1/events` | Append user events (`user_id`, `item_id`, `ts`, `kind`) |
| POST | `/v1/admin/swap` | Prepare and activate a bundle version |
| GET | `/healthz`, `/metrics` | Health and Prometheus metrics |

`ts` defaults to the server clock; benchmarks and evaluation pass the event time from the dataset so the same request always sees the same history.

## 7. Correctness properties

Each property is an executable test; those marked * also run against a deliberately broken implementation to show the test catches it.

1. * Features use only events strictly before the request timestamp (property test against a naive scan)
2. * No response contains an item from the user's history before the request, or the same item twice
3. * A/B assignment is uniform (chi-square over 1M IDs), stable across processes and restarts (golden cases), and matches the configured split within tolerance; the broken variant assigns with an in-process random seed
4. * During concurrent queries and repeated swaps, every returned score recomputes exactly from the vectors of the version the response names
5. * The cache never serves a result from another index or model version
6. * Scatter-gather top-k in exact mode equals single-node exact top-k item by item on small data; ties break by internal ID ascending
7. * The load generator charges a server stall to every request it delayed (coordinated omission)

## 8. Model bundle contract with seqrec-lab

A bundle is a directory `bundle/<version>/`:

| File | Content |
|---|---|
| `manifest.json` | `format_version`, `model_version`, `dataset`, `train_cutoff_ts` (latest event time used in training), `item_count`, `dim`, `metric` (`dot`), `normalized`, `max_seq_len`, `pad_id` (0), `opset`, input and output names, sha256 of every file |
| `items.ids` | One raw item ID per line; line i is internal ID i, starting at 1 |
| `items.npy` | float32, N × D, little-endian, rows in `items.ids` order |
| `user_encoder.onnx` | Input `item_seq` int64 [B, L], left-padded with 0; output `user_emb` float32 [B, D]; score = dot product |
| `ranker.onnx` | Optional |
| `golden.jsonl` | Sample inputs with expected embeddings and top-10 IDs |

The service verifies checksums and replays `golden.jsonl` (tolerance 1e-4) before it accepts a bundle; a failure rejects the swap.

## 9. Data

Data is downloaded by `make data` and never committed. Development and benchmarks use Amazon Reviews 2023 Video_Games (5-core, timestamp split); MovieLens and Steam are optional on a local machine (D013). See D011 for licenses.

## 10. Testing strategy

- Unit and table-driven tests for every stage; property tests for section 7; fuzzing for request parsing and the bundle loader
- Fault injection: slow and dead shards, slow ranker, failed swaps
- All tests run under `-race`
- Benchmarks and evaluation follow docs/BENCHMARKS.md, fixed before results are looked at

## 11. Deliverables

- Runnable services and images, docker compose
- README with architecture, results table and quick start
- docs/BENCHMARKS.md with load-test and retrieval-quality results
