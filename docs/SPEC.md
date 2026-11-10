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
     │ replica a, b  │          │ replica a, b  │   exact search or HNSW
     └───────────────┘          └───────────────┘   indexes of two versions
```

- **gateway** holds item features, the co-occurrence table, the popularity list and the models in memory, loaded from versioned bundles; per-user state lives in Valkey, so gateway instances are interchangeable.
- **ingest** is a Kafka consumer group that applies events to the user histories in Valkey (§7, properties 8 and 9).
- **Valkey** holds each user's events as a sorted set (member: event ID and raw item ID, score: event time; D033), none of them trimmed or expired, and the result cache. Item features stay in the gateway: they are part of the bundle, immutable per version, and a request reads about 500 of them (D020).
- **annshard** is one replica of a shard of the item vectors. Items are assigned to shards by hash of their internal ID. A replica has no shard settings of its own: every request names its index (the bundle version, the shard and the number of shards, the mode and the HNSW build parameters), and the replica answers from that index or refuses (D040). The gateway lists the replicas in `-shard-addrs`; without the flag it holds the shards itself, behind the same interface, and its responses are the same.
- **Versions.** A bundle version names the vectors, the ID map and the models together. A swap is two-phase. Prepare: the gateway loads the bundle and checks its checksums, golden cases and cutoff in the background, then has the shards built, in the process or by a `Prepare` to every replica; each shard needs one replica that holds the version, or the swap is rejected and the active version stays. Activate: one atomic store in the gateway; each request runs on one version and names it in every shard search, so no response mixes versions whatever the replicas hold, and the previous bundle is closed once no request holds it. A replica that missed the prepare, or restarted empty, is sent the version served as soon as the gateway's connection to it is up, and again when a search finds it lacking, while the shard's other replica answers; it is ready, by `grpc.health.v1`, while it holds the version served (D044). A replica keeps the indexes of two versions and drops the one named longest ago for a third, never the one the gateway says it serves (D042, D043). The new bundle may hold other items: histories are stored by raw item ID, and each request maps its history with the bundle it runs on (D032).

## 5. Request path and latency budget

| Stage | p99 budget | On timeout or error |
|---|---|---|
| Rate limit, A/B | 1 ms | — |
| Feature lookup (Valkey), then cache lookup | 3 ms | history from the request's `recent` only: `history_unavailable` |
| User encoder | 5 ms | vector retrieval skipped: `encoder_timeout`, `encoder_error` |
| Retrieval (parallel; shard deadline 15 ms, replicas included) | 15 ms | shards that did not answer skipped: `vector_timeout` (every shard timed out), `vector_error` (none answered, one failed) or `shards_partial` (some answered); source skipped: `cooc_timeout`, `cooc_error`, `popular_timeout`, `popular_error` |
| Ranking (at most 512 candidates) | 15 ms | retrieval order kept: `ranker_timeout`, `ranker_error` |
| Re-ranking and filters | 2 ms | — |
| Other overhead | 9 ms | — |
| **Server-side end to end** | **50 ms** | |

The service target is a server-side p99 of 50 ms; the headline figure is the highest request rate that meets it (D005). Filters are never skipped: a degraded response is still filtered.

The cache is looked up after the history, since its key includes the history (D030); a response is cached only if it was not degraded, so a request whose history cannot be read neither reads nor writes it. With `-valkey-addr` the cache is on Valkey and shared by the gateways; the cache read shares the history budget with the history read, the cache write gets it once more, and a cache that cannot be read or written within it costs the request nothing but the cache: it is answered in full as a miss, and is not degraded (D036). The gateway's calls to Valkey go through a circuit breaker: after five failures in a row it answers from `recent` without asking Valkey, and tries Valkey again after a second (D037). Connections to Valkey are opened in the background: a request that finds none waits for one within its history budget, and falls back the same way without counting as a failure. Requests that miss on the same key at the same time compute the response once and share it.

Each stage runs under its own deadline, set by the gateway flags `-budget-history`, `-budget-encoder`, `-budget-retrieval`, `-budget-shard` and `-budget-ranker` (0 for none); a shard search ends at the earlier of its own deadline and the retrieval deadline. A stage that runs out of time or fails is skipped or falls back as the table says, and the response lists the reasons in `degraded`, in the table's order, each once; `recserve_degraded_total{reason}` counts them, and an error other than a timeout is logged. The stages check their deadline themselves (exact search every 1,024 rows, ONNX Runtime between nodes), and the gateway waits for each to return, so a stage ends shortly after its deadline (D029). A request that its client cancels fails with status 499, and one past a deadline its caller set fails with 504; neither is a degradation.

With the shards on replicas, a shard search is one call to one replica, within the shard deadline, which the call carries to the replica. Among a shard's replicas with a ready connection the gateway takes a different one each time. If the call fails, the search goes to the next replica at once, each at most once. If the first replica has not answered after `-hedge-delay` (5 ms), the search also asks a second one, takes the first answer and cancels the other call; a shard's hedges are limited to 10 in reserve plus one for every ten searches, so slow replicas are not sent twice the load. A shard is skipped only when no replica answers in time: as a timeout if its calls ended at the deadline, as an error if a replica failed. A replica from which nothing was read for 10 s is pinged, and its connection closed after 3 s without an answer (D041).

## 6. API (draft)

| Method | Path | Description |
|---|---|---|
| POST | `/v1/recommend` | `{"user_id", "k", "ts"?, "exclude"?, "recent"?}` → items, scores, arm, versions, degraded reasons (§5), coverage |
| POST | `/v1/events` | Append user events (`user_id`, `item_id`, `ts`, `kind`); 202 once the broker has acknowledged them, or, without `-kafka-brokers`, once they are stored; 503 if that fails or takes longer than `-events-timeout` |
| POST | `/v1/admin/swap` | `{"bundle": dir}`: prepare and activate the bundle in a directory → version, previous version, prepare and drain times; 409 while another swap runs or if the version is active, 422 if the bundle fails to load or verify. Served on its own listener (`-admin-addr`, 127.0.0.1:8081 by default), without authentication |
| GET | `/livez`, `/readyz`, `/metrics` | Liveness (the process serves), readiness (a bundle is active and the instance is not stopping: 503 from SIGTERM on, D045), Prometheus metrics |

`recent` is the client's own list of the user's latest item IDs, oldest first, at most 500; it is used only when the stored history cannot be read, and IDs the bundle does not know are ignored. `coverage` is `{"shards", "answered"}`: the shards searched for vector retrieval and those that answered in time; it is null when vector retrieval did not run (no user vector, or the encoder failed). An event's ID is a hash of its four fields, so an identical event sent twice is one event; its `ts` is at most 2^53 ms. `/v1/events` answers `{"accepted": n}`, and every event is stored under its raw item ID, also for an item the active bundle does not know: a request ignores the history items its bundle does not know, and a later bundle may know them. Through Kafka the 202 does not promise that the next request sees the event: ingest applies it later (D034). After a 503 some of the events may have been accepted, and sending them again is safe. Readiness does not depend on Valkey or Kafka: their failure is handled by degradation, and failing readiness on it would take every instance out of service at once.

`ts` defaults to the server clock; benchmarks and evaluation pass the event time from the dataset so the same request always sees the same history.

## 7. Correctness properties

Each property is an executable test; those marked * also run against a deliberately broken implementation to show the test catches it.

1. * Features use only events strictly before the request timestamp (property test against a naive scan)
2. * No response contains an item from the user's history before the request, or the same item twice
3. * A/B assignment is uniform (chi-square over 1M IDs), stable across processes and restarts (golden cases), and matches the configured split within tolerance; the broken variant assigns with an in-process random seed
4. * During concurrent queries and repeated swaps, every returned score recomputes exactly from the vectors of the version the response names; the broken variant scores with the bundle active when scoring starts. Also with the shards on replicas, where the broken variant is a replica that answers from the version it prepared last
5. * The cache never serves a result from another index or model version, including right after a swap
6. * Scatter-gather top-k in exact mode equals single-node exact top-k item by item on small data; ties break by internal ID ascending. The same check runs on the in-process shards and on replicas over gRPC
7. * The load generator charges a server stall to every request it delayed (coordinated omission)
8. * Delivering any event more than once, in any order relative to other events, leaves every user history and every response unchanged; the broken variant appends without the event ID
9. * After the ingest consumer is killed at random points and restarted, every user history equals the one built by applying each produced event exactly once
10. * With Valkey unavailable, every response is 200, uses only the request's `recent` items as history, reports `history_unavailable`, and is never written to the cache, also after Valkey returns; the broken variant caches it
11. * Concurrent identical requests that miss the cache compute the result once per gateway instance (singleflight)
12. * With one replica of every shard lost (it refuses connections, does not answer, or answers with errors), every response equals the one with all replicas healthy: every shard answered, nothing degraded; for the replica that does not answer, within the hedge budget. The broken variant knows only the lost replicas
13. * With every replica slower than the hedge delay, n searches of a shard send at most 10 + n/10 hedges, and each hedge that loses is cancelled; the broken variant hedges every slow search

## 8. Model bundle contract with seqrec-lab

A bundle is a directory `bundle/<version>/`:

| File | Content |
|---|---|
| `manifest.json` | `format_version`, `model_version` (the first 12 hex digits of the SHA-256 of every other file's name followed by its sha256 below, in name order), `dataset`, `train_cutoff_ts` (latest event time used in training), `item_count`, `dim`, `metric` (`dot`), `normalized`, `max_seq_len`, `pad_id` (0), `user_encoder` (`mean` or `onnx`), `ranker_features` (with a ranker), build parameters, sha256 of every other file |
| `items.ids` | One raw item ID per line; line i is internal ID i, starting at 1 |
| `items.npy` | float32, (item_count + 1) × D, little-endian; row i is internal ID i, row 0 is padding and all zeros |
| `user_encoder.onnx` | With `user_encoder: onnx`: input `item_seq` int64 [B, L], left-padded with 0; output `user_emb` float32 [B, D]. The service passes B = 1 and L = `max_seq_len`: the last `max_seq_len` history items. With `mean`, absent: the user vector is the mean of the last `max_seq_len` history items' vectors. A user vector of zeros counts as none; from an encoder given items whose vectors are not zero, it is an error. Score = dot product |
| `ranker.onnx` | Optional. Input `features` float32 [C, F], one row per candidate, the columns named by `ranker_features` in the manifest; output `score` float32 [C], higher is better, any finite range. Each row's score must depend on that row only: the service passes C = 512 rows, those past the candidates set to zero. `ranker_features` must equal the list below, in order, and is absent without a ranker |
| `cooc_ids.npy`, `cooc_counts.npy` | Optional. int64 and float32, (item_count + 1) × 50: each item's most frequent co-occurring items and their counts, 0-padded |
| `popular.npy` | Optional. int64: the most popular internal IDs, best first |
| `golden.jsonl` | Sample histories with the expected user vector (from the encoder, if the bundle has one) and top-10 IDs (history excluded, ties by internal ID) |

### Ranker features

H is the request's history: every stored event of the user strictly before the request time whose item the bundle knows, ordered by time, then event ID, one entry per event (an item may appear more than once); when the stored history cannot be read, the request's `recent` items the bundle knows, in their order (§6). u is the user vector, computed from the last `max_seq_len` entries of H (none if H is empty, the vector is zero, or the encoder fails or times out); v_i is row i of `items.npy`. Every value is computed in float64 from these float32 inputs and rounded to float32. For candidate i:

1. `vector_score`: u · v_i; 0 without a user vector
2. `cooc_score`: the largest 1 / ((s + 1)(r + 1)) over the seeds s = 0, 1, …, min(10, |H|) − 1, seed s being the entry H[|H| − 1 − s] (s = 0 is the latest), and the positions r, counted from 0, where i appears in that item's row of `cooc_ids.npy` before the row's first 0; 0 if there is none
3. `popular_score`: 1 / (r + 1) for the smallest r < 100 with `popular.npy`[r] = i; 0 if there is none
4. `item_norm`: ‖v_i‖ = √(v_i · v_i)
5. `last_item_cosine`: v_i · v_l / (‖v_i‖ ‖v_l‖), l being the last entry of H; 0 if H is empty or either norm is 0
6. `log_history_len`: ln(1 + |H|)

When the co-occurrence or popularity source is skipped (§5), `cooc_score` or `popular_score` is 0 for every candidate. `vector_score` depends only on the user vector, so it stays when shards are skipped.

The candidates are the union of the retrieval sources' results without the seen and excluded items. Each has a retrieval score: u · v_i, or without a user vector its best source score (the values above for `cooc_score` and `popular_score`, and their maximum). With a ranker, the 512 candidates with the highest retrieval scores (ties to the lower internal ID) are ranked and the rest are dropped and counted in the metrics; a candidate's score becomes the ranker's. If the ranker times out or fails, the retrieval scores stay. MMR scales the scores to [0, 1] over the candidates (min-max) before weighing them against similarity, so the ranker's scale does not change the diversity weight.

The service verifies the checksums and that `model_version` is the one they give, so a version names the content of every file (D046), and replays `golden.jsonl` through the user encoder (tolerance 1e-4 on the user vector, exact top-10), and runs the ranker twice on 512 rows, all zero, then with row 0 changed, requiring the other rows' scores to stay the same bit for bit, before it accepts a bundle; it rejects a bundle whose directory holds an optional file (`user_encoder.onnx`, `ranker.onnx`, `cooc_ids.npy`, `cooc_counts.npy`, `popular.npy`) that the manifest does not list, and one whose `train_cutoff_ts` is not before the first test event of the evaluation split; a failure rejects the swap. `tools/bundle` builds the baseline bundle (implicit-feedback ALS on the train split) and verifies a bundle with the same rules in Python.

## 9. Data

Data is downloaded by `make data` and never committed. Development and benchmarks use Amazon Reviews 2023 Video_Games (5-core, timestamp split); MovieLens and Steam are optional on a local machine (D013). See D011 for licenses.

## 10. Testing strategy

- Unit and table-driven tests for every stage; property tests for section 7; fuzzing for request parsing and the bundle loader
- Fault injection: slow and dead shards, lost, silent and restarted replicas (ready again with no request), slow ranker, failed swaps, Valkey down, consumer killed; under load per docs/BENCHMARKS.md
- Integration tests against real Kafka and Valkey containers; `internal/e2e` runs the gateway and ingest binaries on them, kills ingest, pauses Valkey and stops Kafka, and runs the gateway on four `annshard` processes through a killed replica, a swap, an empty restart and a rejected swap (`make e2e`, on its own, since it takes the services away from other tests)
- The replicas of the shard tests are real gRPC servers on loopback ports, in the test's process, with faults put in the network by a TCP proxy of the test or in the replica by a server interceptor
- `make test-real` runs the pipeline on the real data and bundle, also with the shards on replicas, where every response must equal the in-process one
- All tests run under `-race`
- Benchmarks and evaluation follow docs/BENCHMARKS.md, fixed before results are looked at

## 11. Deliverables

- Runnable services and images, docker compose, kind manifests
- README with architecture, results table and quick start
- docs/BENCHMARKS.md with load-test and retrieval-quality results
