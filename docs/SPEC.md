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
- Event ingestion through a Kafka-protocol topic: `POST /v1/events` produces to it, keyed by user; an ingest consumer applies events to the user histories at least once, idempotently, and resumes from its committed offsets after a restart
- User histories and the result cache in Valkey, shared by all gateway instances; stampede protection with singleflight; degradation when Valkey is unavailable
- Fault injection under load: a dead shard replica, shard latency, Valkey unavailable, a stalled consumer
- Deployment on a local kind cluster with readiness and liveness probes, resource limits and an HPA, and a check that a rolling update and a model swap under load produce no errors
- Reproducible open-loop load generator and a benchmark report
- docker compose that starts the whole system with one command; Kubernetes manifests for kind

### Optional

- BM25 keyword retrieval fused with vector retrieval (hybrid search)
- A small fresh-item tier searched exactly beside the HNSW index

### Out of scope

- Consensus, automatic resharding, dynamic membership: the cluster layout is a static file
- Model training beyond simple baselines; seqrec-lab trains the sequence models
- A public online demo (D011)

## 3. Scale, stated plainly

The catalog served is Amazon Reviews 2023 Video_Games, 5-core: 25.6k items (137.2k before filtering). Exact search over that many 64-dimensional vectors takes about 2 ms on one core (scaled from the 1M-vector measurement in M0), so at this size neither ANN nor sharding is needed; the service shards it anyway to exercise the mechanism. ANN and shard performance is measured separately on 1M-scale vectors (glove-100-angular and synthetic vectors, D008), and the report says which figures come from which.

All load tests run on one 4-vCPU VM. Kafka, Valkey, the shards and their replicas, the gateway, the ingest consumer and the load generator share its CPUs and memory, so the figures show behavior under load and relative effects (a fault, a cache, a degradation); they do not predict the capacity of a real multi-machine deployment. Headline figures come from docker compose; the kind cluster is used to check deployment behavior, not for throughput, since its own control plane runs on the same CPUs.

## 4. Architecture

```
                    client (HTTP/JSON)
                          │
        ┌─────────────────▼──────────────────────────────┐
        │ gateway (stateless, N instances)                │
        │  rate limit → A/B arm → cache ◄─────────────────┼── Valkey: result cache,
        │  features: user history before request ts ◄─────┼── user histories
        │  POST /v1/events ───────────────────────────────┼─► Kafka topic `events`
        │                                                 │   (keyed by user) ──► ingest
        │                                                 │   consumer ──► Valkey
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

- **gateway** holds item features, the co-occurrence table, the popularity list and the models in memory, loaded from versioned bundles; per-user state lives in Valkey, so gateway instances are interchangeable.
- **ingest** is a Kafka consumer group that applies events to the user histories in Valkey (§7, properties 8 and 9).
- **Valkey** holds each user's recent events as a sorted set (member: item and event ID, score: event time; D023 on retention) and the result cache. Item features stay in the gateway: they are part of the bundle, immutable per version, and a request reads about 500 of them (D020).
- **annshard** holds one shard of the item vectors. Items are assigned to shards by hash of their internal ID.
- **Versions.** A bundle version names the vectors, the ID map and the models together. A swap is two-phase: every shard loads and verifies the new version (prepare), then the gateway activates it and pins each request to one version; shards keep the previous version until no request uses it.

## 5. Request path and latency budget

| Stage | p99 budget | On timeout |
|---|---|---|
| Rate limit, A/B, cache lookup | 1 ms | — |
| Feature lookup (Valkey) | 3 ms | history from the request only, reason `history_unavailable` |
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
| POST | `/v1/recommend` | `{"user_id", "k", "ts"?, "exclude"?, "recent"?}` → items, scores, arm, versions, degraded reasons, coverage |
| POST | `/v1/events` | Append user events (`user_id`, `item_id`, `ts`, `kind`); 202 once the broker has acknowledged them |
| POST | `/v1/admin/swap` | Prepare and activate a bundle version |
| GET | `/livez`, `/readyz`, `/metrics` | Liveness (the process serves), readiness (a bundle is active and the instance is not draining), Prometheus metrics |

`recent` is the client's own list of the user's latest item IDs; it is used only when the stored history cannot be read. An event's ID is a hash of its four fields, so an identical event sent twice is one event. Readiness does not depend on Valkey or Kafka: their failure is handled by degradation, and failing readiness on it would take every instance out of service at once.

`ts` defaults to the server clock; benchmarks and evaluation pass the event time from the dataset so the same request always sees the same history.

## 7. Correctness properties

Each property is an executable test; those marked * also run against a deliberately broken implementation to show the test catches it.

1. * Features use only events strictly before the request timestamp (property test against a naive scan)
2. * No response contains an item from the user's history before the request, or the same item twice
3. * A/B assignment is uniform (chi-square over 1M IDs), stable across processes and restarts (golden cases), and matches the configured split within tolerance; the broken variant assigns with an in-process random seed
4. * During concurrent queries and repeated swaps, every returned score recomputes exactly from the vectors of the version the response names
5. * The cache never serves a result from another index or model version, including right after a swap
6. * Scatter-gather top-k in exact mode equals single-node exact top-k item by item on small data; ties break by internal ID ascending
7. * The load generator charges a server stall to every request it delayed (coordinated omission)
8. * Delivering any event more than once, in any order relative to other events, leaves every user history and every response unchanged; the broken variant appends without the event ID
9. * After the ingest consumer is killed at random points and restarted, every user history equals the one built by applying each produced event exactly once
10. * With Valkey unavailable, every response is 200, uses only the request's `recent` items as history, reports `history_unavailable`, and is never written to the cache, also after Valkey returns; the broken variant caches it
11. * Concurrent identical requests that miss the cache compute the result once per gateway instance (singleflight)

## 8. Model bundle contract with seqrec-lab

A bundle is a directory `bundle/<version>/`:

| File | Content |
|---|---|
| `manifest.json` | `format_version`, `model_version`, `dataset`, `train_cutoff_ts` (latest event time used in training), `item_count`, `dim`, `metric` (`dot`), `normalized`, `max_seq_len`, `pad_id` (0), `user_encoder` (`mean` or `onnx`), build parameters, sha256 of every other file |
| `items.ids` | One raw item ID per line; line i is internal ID i, starting at 1 |
| `items.npy` | float32, (item_count + 1) × D, little-endian; row i is internal ID i, row 0 is padding and all zeros |
| `user_encoder.onnx` | With `user_encoder: onnx`: input `item_seq` int64 [B, L], left-padded with 0; output `user_emb` float32 [B, D]. With `mean`, absent: the user vector is the mean of the last `max_seq_len` history items' vectors. Score = dot product |
| `ranker.onnx` | Optional |
| `cooc_ids.npy`, `cooc_counts.npy` | Optional. int64 and float32, (item_count + 1) × 50: each item's most frequent co-occurring items and their counts, 0-padded |
| `popular.npy` | Optional. int64: the most popular internal IDs, best first |
| `golden.jsonl` | Sample histories with the expected user vector and top-10 IDs (history excluded, ties by internal ID) |

The service verifies checksums and replays `golden.jsonl` (tolerance 1e-4) before it accepts a bundle, and rejects one whose `train_cutoff_ts` is not before the first test event of the evaluation split; a failure rejects the swap. `tools/bundle` builds the baseline bundle (implicit-feedback ALS on the train split) and verifies a bundle with the same rules in Python.

## 9. Data

Data is downloaded by `make data` and never committed. Development and benchmarks use Amazon Reviews 2023 Video_Games (5-core, timestamp split); MovieLens and Steam are optional on a local machine (D013). See D011 for licenses.

## 10. Testing strategy

- Unit and table-driven tests for every stage; property tests for section 7; fuzzing for request parsing and the bundle loader
- Fault injection: slow and dead shards, slow ranker, failed swaps, Valkey down, consumer killed; under load per docs/BENCHMARKS.md
- Integration tests against real Kafka and Valkey containers
- All tests run under `-race`
- Benchmarks and evaluation follow docs/BENCHMARKS.md, fixed before results are looked at

## 11. Deliverables

- Runnable services and images, docker compose, kind manifests
- README with architecture, results table and quick start
- docs/BENCHMARKS.md with load-test and retrieval-quality results
