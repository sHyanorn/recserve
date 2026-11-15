# recserve

[![CI](https://github.com/sHyanorn/recserve/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/sHyanorn/recserve/actions/workflows/ci.yml?query=branch%3Amain)

A recommendation serving system in Go: retrieval, features, ranking and rerank behind one HTTP API, with a sharded vector tier, caching, degradation, rate limiting, A/B assignment, metrics and tracing. Its headline, the highest rate with p99 ≤ 50 ms and at most 1% of the responses degraded, is 550 requests/s on a 4-vCPU cloud VM, replaying requests with the cache off; p99 there is 4.6 to 5.1 ms. Every service shares that VM's CPUs, so the figures show behavior under load and do not predict the capacity of a multi-machine deployment. In an offline evaluation registered before the Musical_Instruments test split was read, state kept from the event stream for retrieval and ranking raised that category's recall@10 from 0.0200 to 0.0317, each on a bundle trained once; the gateway does not keep that state yet.

## Results

The load and fault rows of M6 and M7, except the one in kind, were measured on 4-vCPU cloud VMs, with the load generator on one CPU and every service on the other three. Figures from different VMs are not compared. The load and fault runs are in [docs/BENCHMARKS.md](docs/BENCHMARKS.md), and M6 has a [report with charts](bench-results/m6/report.md). Faults were judged by criteria committed before the runs. The quality rows run the pipeline in process, offline, one request per test event of an Amazon Reviews 2023 category; M8's are judged by [a registration](docs/registration/m8-freshness.md) committed before its runs.

| What | Result |
|---|---|
| Headline: the highest rate with p99 ≤ 50 ms and at most 1% of the responses degraded; replay, cache off, with the earlier 3 ms history budget (M6) | 550 requests/s, p99 4.6 to 5.1 ms |
| One gateway on 2 CPUs (M6) | about 1,800 requests/s, an estimate from its CPU per request |
| Faults at 275 requests/s, cache off, outlier detection, three runs each (M7, second set, another VM) | pass: replica killed, replica paused, searches hanging, 50 ms on a shard, with a fault-phase p99 of 6.8 to 20.5 ms; fail: Valkey down, one run recovering in 7 s with 5 allowed; not judged: consumer paused, two valid runs of three |
| Rolling restarts of gateways and replicas and a swap in kind, 100 requests/s, SPEC §5's stage budgets, three runs (M6) | no failed response; at most 0.133% of a phase's responses degraded |
| recall@10 of one bundle trained once, b7ff0634dd7b, on 43,249 Video_Games test events, each degradation forced (M7) | 0.0181 healthy, 0.0098 without co-occurrence, 0.0050 without the ranker; without the encoder, with a shard missing or with HNSW at ef 400, within 0.0004 of healthy |
| recall@10 with state kept from the event stream for retrieval and ranking, against the bundle frozen: Musical_Instruments, its one test read, registered before it (M8) | 0.0317 against 0.0200, +0.0117, 95% interval over users +0.0096 to +0.0137, confirming the registered hypothesis; bundles rebuilt every 7 or 30 days 0.0242 and 0.0231 |
| The same on Video_Games, exploratory (M8) | 0.0375 against 0.0197, +0.0178, 95% interval over users +0.0152 to +0.0202; a bundle rebuilt every 7 days about the same, 0.0381 |
| `vec.Dot` in AVX2 | the replicas' CPU per request 37% lower, p99 a quarter lower at 200 requests/s |
| HNSW on glove-100, one thread | p99 1.0 to 1.3 times faiss's at the same ef; recall@100 reaches 0.95 only at a p99 of 7.66 ms |

### Scale

The catalog served has 25.6k items (Amazon Reviews 2023 Video_Games, 5-core). Exact search over the 22,976 items that have a vector, at 256 dimensions, takes a median of 1.784 ms on one core ([bench-results/m8/exact-search.txt](bench-results/m8/exact-search.txt)). At this size sharding only demonstrates the mechanism. ANN and shard performance is measured separately on 1M-scale vectors, and every reported figure says which data it comes from.

## How it works

The gateway runs each request through retrieval, features, ranking and rerank, each stage with its own deadline. Retrieval draws on co-occurrence, popularity and item vectors. The vectors are split into shards and searched exactly or with a hand-written HNSW, in the gateway or on `annshard` replicas over gRPC. A search goes to a shard's next replica when one fails, and to a second one when the first is slow.

User histories reach Valkey from Kafka through an ingest consumer. The result cache is in Valkey too, shared by every gateway, behind a circuit breaker. A model bundle's version is the digest of its files. A swap is two-phase: the new version is prepared and verified in the background, then activated atomically; across replicas, one per shard is enough to go on. Every response reports its degradation reasons and shard coverage.

It runs as processes, in compose with Prometheus, Grafana and Jaeger, and in kind with only the gateways and the replicas. [docs/FEATURES.md](docs/FEATURES.md) lists every component with its decision; the design is in [docs/SPEC.md](docs/SPEC.md) and [docs/DECISIONS.md](docs/DECISIONS.md).

## Run it

```bash
make data DATA=data      # Amazon Reviews 2023 Video_Games, 5-core timestamp split
make bundle DATA=data    # baseline ALS bundle into data/bundle/<version>
make build
bin/gateway -bundle data/bundle/<version> -test-split data/Video_Games.test.csv.gz -preload data/Video_Games.train.csv.gz,data/Video_Games.valid.csv.gz
bin/loadgen -split data/Video_Games.test.csv.gz -rate 200 -duration 30s
```

[docs/RUNNING.md](docs/RUNNING.md) covers the ONNX encoder and the ranker, bundle swaps, A/B arms, tracing, replicas, Kafka and Valkey, compose and kind. `make e2e` runs the tests on the built binaries under faults, and the `make bench-*` targets rerun the measurements, except `bench-quality`, `bench-bundles` and `bench-read`, which compute quality on a test split: once a category's registered reads are logged, another is refused.

## Limits

- The one-gateway capacity is an estimate made while the gateway shared three CPUs with the rest of the service. kind's rate and in-flight limits come from it and apply per gateway (D050).
- The ranker's effect on users is for an A/B experiment to show. Offline, before MMR, its recall@10 is 0.091 against 0.026 for retrieval order on 680 held-out users (D027).
- M7's quality figures come from one bundle trained once and not updated over the test period, July 2022 to September 2023. The target item of 60% of the Video_Games test events, and of 33% of Musical_Instruments', has no train event; on those events every row of M8's first Video_Games test read has recall@10 0.0000 ([its file](bench-results/m8/Video_Games/read-1.md)).
- M8's figures come from the pipeline in process; the state's cost in serving is not measured yet.
- M8's confirmatory result is one test read of one category. Video_Games' test split had been used in M7 and for 18 exploratory configurations, so its figures are exploratory (D061).
- User histories keep every event, with no cap and no expiry. When they fill Valkey's memory, `/v1/events` answers 503 (D023, D033).
- Valkey persists with snapshots only, so events since the last snapshot are lost if it stops uncleanly.
- The circuit breaker on Valkey never opens below 100 calls a second; there, while Valkey keeps its connections open without answering, every request waits out its 5 ms history budget (D037).
- With Kafka, a request may not see an event sent just before it.
- The gateway and the replicas talk without TLS or authentication (D040). The admin API, on 127.0.0.1:8081, has no authentication either. kind's NetworkPolicy is enforced only on a kernel with nftables' queue expression (D047).
- The kind manifests deploy only the gateways and the replicas, with stage budgets of 100 ms. A production deployment also needs Valkey, Kafka and ingest (D047).
- When both replicas of a shard are slow, neither is left out. Beyond the hedge budget the shard is left out and responses report `shards_partial` (D041).
- A replica holds two bundle versions. With three in use at once, a replica drops one that another gateway still serves, and a starting gateway can have a replica drop the version the running gateways serve. While no replica of a shard holds that version, responses report `shards_partial`, or `vector_error` when that holds for every shard (D042, D043, D044, D048).
- Exposure lines can be dropped or lost (D049). The cost of tracing is not measured, and Valkey and Kafka calls have no spans of their own (D051).

[docs/RUNNING.md](docs/RUNNING.md#limits-in-detail) gives each limit in full, with its measurements.

## License

MIT. Datasets are downloaded by script under their own terms and are not redistributed.
