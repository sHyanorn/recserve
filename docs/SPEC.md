# recserve Specification

## 1. Summary

An online recommendation and search serving system written in Go. A request goes through multi-source retrieval, point-in-time feature lookup, model ranking and re-ranking, under a latency budget, with degradation, caching, A/B assignment, index hot swaps and a sharded vector tier. Serving engineering is the focus: latency, throughput, availability, distribution and observability. The models are simple or come from the sister project seqrec-lab.

## 2. Scope

### Required

- Retrieval from several sources in parallel: vector nearest neighbors (HNSW or exact), item co-occurrence, popularity fallback
- Features known at request time only: a user's recent events before the request timestamp, item features
- Ranking with an ONNX model; re-ranking with seen-item filtering, deduplication and diversity (MMR)
- Per-stage deadlines with degradation: a retrieval source that times out is skipped, a ranker that times out falls back to retrieval order, a missing shard yields partial results; every response reports what was degraded and the shard coverage
- Result cache keyed by user, request, bundle version, experiment arm and the user's history before the request
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
        │  rate limit → A/B arm → features: user history ◄┼── Valkey: user histories,
        │  before request ts → cache keyed by it ◄────────┼── result cache
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

| Stage | p99 budget | On timeout or error |
|---|---|---|
| Rate limit, A/B | 1 ms | — |
| Feature lookup (Valkey), then cache lookup | 3 ms | history from the request's `recent` only: `history_unavailable` |
| User encoder | 5 ms | vector retrieval skipped: `encoder_timeout`, `encoder_error` |
| Retrieval (parallel; shard deadline 15 ms) | 15 ms | source skipped: `cooc_timeout`, `cooc_error`, `popular_timeout`, `popular_error`; shards that did not answer skipped: `shards_partial`, or `vector_timeout` (every shard timed out) or `vector_error` (none answered, one failed) |
| Ranking (at most 512 candidates) | 15 ms | retrieval order kept: `ranker_timeout`, `ranker_error` |
| Re-ranking and filters | 2 ms | — |
| Other overhead | 9 ms | — |
| **Server-side end to end** | **50 ms** | |

The service target is a server-side p99 of 50 ms; the headline figure is the highest request rate that meets it (D005). Filters are never skipped: a degraded response is still filtered.

The cache is looked up after the history, since its key includes the history (D030); a response is cached only if it was not degraded, so a request whose history cannot be read neither reads nor writes it.

Each stage runs under its own deadline, set by the gateway flags `-budget-history`, `-budget-encoder`, `-budget-retrieval`, `-budget-shard` and `-budget-ranker` (0 for none); a shard search ends at the earlier of its own deadline and the retrieval deadline. A stage that runs out of time or fails is skipped or falls back as the table says, and the response lists the reasons in `degraded`, in the table's order, each once; `recserve_degraded_total{reason}` counts them, and an error other than a timeout is logged. The stages check their deadline themselves (exact search every 1,024 rows, ONNX Runtime between nodes), and the gateway waits for each to return, so a stage ends shortly after its deadline (D029). A request that its client cancels fails with status 499, and one past a deadline its caller set fails with 504; neither is a degradation.

## 6. API (draft)

| Method | Path | Description |
|---|---|---|
| POST | `/v1/recommend` | `{"user_id", "k", "ts"?, "exclude"?, "recent"?}` → items, scores, arm, versions, degraded reasons (§5), coverage |
| POST | `/v1/events` | Append user events (`user_id`, `item_id`, `ts`, `kind`); 202 once the broker has acknowledged them |
| POST | `/v1/admin/swap` | Prepare and activate a bundle version |
| GET | `/livez`, `/readyz`, `/metrics` | Liveness (the process serves), readiness (a bundle is active and the instance is not draining), Prometheus metrics |

`recent` is the client's own list of the user's latest item IDs, oldest first, at most 500; it is used only when the stored history cannot be read, and IDs the bundle does not know are ignored. `coverage` is `{"shards", "answered"}`: the shards searched for vector retrieval and those that answered in time; it is null when vector retrieval did not run (no user vector, or the encoder failed). An event's ID is a hash of its four fields, so an identical event sent twice is one event. Readiness does not depend on Valkey or Kafka: their failure is handled by degradation, and failing readiness on it would take every instance out of service at once.

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
| `manifest.json` | `format_version`, `model_version`, `dataset`, `train_cutoff_ts` (latest event time used in training), `item_count`, `dim`, `metric` (`dot`), `normalized`, `max_seq_len`, `pad_id` (0), `user_encoder` (`mean` or `onnx`), `ranker_features` (with a ranker), build parameters, sha256 of every other file |
| `items.ids` | One raw item ID per line; line i is internal ID i, starting at 1 |
| `items.npy` | float32, (item_count + 1) × D, little-endian; row i is internal ID i, row 0 is padding and all zeros |
| `user_encoder.onnx` | With `user_encoder: onnx`: input `item_seq` int64 [B, L], left-padded with 0; output `user_emb` float32 [B, D]. The service passes B = 1 and L = `max_seq_len`: the last `max_seq_len` history items. With `mean`, absent: the user vector is the mean of the last `max_seq_len` history items' vectors. A user vector of zeros counts as none; from an encoder given items whose vectors are not zero, it is an error. Score = dot product |
| `ranker.onnx` | Optional. Input `features` float32 [C, F], one row per candidate, the columns named by `ranker_features` in the manifest; output `score` float32 [C], higher is better, any finite range. Each row's score must depend on that row only: the service passes C = 512 rows, those past the candidates set to zero. `ranker_features` must equal the list below, in order, and is absent without a ranker |
| `cooc_ids.npy`, `cooc_counts.npy` | Optional. int64 and float32, (item_count + 1) × 50: each item's most frequent co-occurring items and their counts, 0-padded |
| `popular.npy` | Optional. int64: the most popular internal IDs, best first |
| `golden.jsonl` | Sample histories with the expected user vector (from the encoder, if the bundle has one) and top-10 IDs (history excluded, ties by internal ID) |

### Ranker features

H is the request's history: every stored event of the user strictly before the request time, ordered by time, then event ID, one entry per event (an item may appear more than once); when the stored history cannot be read, the request's `recent` items the bundle knows, in their order (§6). u is the user vector, computed from the last `max_seq_len` entries of H (none if H is empty, the vector is zero, or the encoder fails or times out); v_i is row i of `items.npy`. Every value is computed in float64 from these float32 inputs and rounded to float32. For candidate i:

1. `vector_score`: u · v_i; 0 without a user vector
2. `cooc_score`: the largest 1 / ((s + 1)(r + 1)) over the seeds s = 0, 1, …, min(10, |H|) − 1, seed s being the entry H[|H| − 1 − s] (s = 0 is the latest), and the positions r, counted from 0, where i appears in that item's row of `cooc_ids.npy` before the row's first 0; 0 if there is none
3. `popular_score`: 1 / (r + 1) for the smallest r < 100 with `popular.npy`[r] = i; 0 if there is none
4. `item_norm`: ‖v_i‖ = √(v_i · v_i)
5. `last_item_cosine`: v_i · v_l / (‖v_i‖ ‖v_l‖), l being the last entry of H; 0 if H is empty or either norm is 0
6. `log_history_len`: ln(1 + |H|)

The candidates are the union of the retrieval sources' results without the seen and excluded items. Each has a retrieval score: u · v_i, or without a user vector its best source score (the values above for `cooc_score` and `popular_score`, and their maximum). With a ranker, the 512 candidates with the highest retrieval scores (ties to the lower internal ID) are ranked and the rest are dropped and counted in the metrics; a candidate's score becomes the ranker's. If the ranker times out or fails, the retrieval scores stay. MMR scales the scores to [0, 1] over the candidates (min-max) before weighing them against similarity, so the ranker's scale does not change the diversity weight.

The service verifies checksums and replays `golden.jsonl` through the user encoder (tolerance 1e-4 on the user vector, exact top-10), and runs the ranker twice on 512 rows, all zero, then with row 0 changed, requiring the other rows' scores to stay the same bit for bit, before it accepts a bundle, and rejects one whose `train_cutoff_ts` is not before the first test event of the evaluation split; a failure rejects the swap. `tools/bundle` builds the baseline bundle (implicit-feedback ALS on the train split) and verifies a bundle with the same rules in Python.

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
