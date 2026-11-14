# recserve

[![CI](https://github.com/sHyanorn/recserve/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/sHyanorn/recserve/actions/workflows/ci.yml?query=branch%3Amain)

A recommendation serving system in Go: retrieval, features, ranking and rerank behind one HTTP API, with a sharded vector tier, caching, degradation, rate limiting, A/B assignment, metrics and tracing. Its headline, the highest rate with p99 ≤ 50 ms and at most 1% of the responses degraded, is 550 requests/s on a 4-vCPU cloud VM, replaying requests with the cache off; p99 there is 4.6 to 5.1 ms. In the latest fault runs, at 275 requests/s with the cache off, four of six faults passed their criteria; Valkey down failed and the paused ingest consumer was not judged. Every service shares that VM's CPUs, so the figures show behavior under load and do not predict the capacity of a multi-machine deployment.

## Results

Measured in M6 and M7 on 4-vCPU cloud VMs, with the load generator on one CPU and every service on the other three. Figures from different VMs are not compared. The load and fault runs are in [docs/BENCHMARKS.md](docs/BENCHMARKS.md), and M6 has a [report with charts](bench-results/m6/report.md). Faults were judged by criteria committed before the runs.

| What | Result |
|---|---|
| Headline: the highest rate with p99 ≤ 50 ms and at most 1% of the responses degraded; replay, cache off, with the earlier 3 ms history budget (M6) | 550 requests/s, p99 4.6 to 5.1 ms |
| One gateway on 2 CPUs (M6) | about 1,800 requests/s, an estimate from its CPU per request |
| Faults at 275 requests/s, cache off, outlier detection, three runs each (M7, second set, another VM) | pass: replica killed, replica paused, searches hanging, 50 ms on a shard, with a fault-phase p99 of 6.8 to 20.5 ms; fail: Valkey down, one run recovering in 7 s with 5 allowed; not judged: consumer paused, two valid runs of three |
| Faults with the cache off, three runs each (M7, first set) | pass: replica killed, replica paused, searches hanging, Valkey down; fail: 50 ms on a shard, on one second with the Valkey breaker open; not judged: consumer paused, two valid runs of three |
| Faults at half the headline, three runs each (M6) | pass by the criteria as written: replica killed, 50 ms on a shard, Valkey down, searches hanging; fail: ingest paused, replica paused, each on one second with Valkey's circuit breaker open |
| Outlier detection, cache off (M7) | replica paused: fault-phase p99 5.1 to 5.5 ms against 17.4 to 17.7 without it, pass; searches hanging: 5.7 to 6.4 ms against 18.2 to 18.4, fail, one run of three recovering in 13 s with 5 allowed; 50 ms on a shard: fail, on one second with the Valkey breaker open, one run of three not valid |
| Rolling restarts of gateways and replicas and a swap in kind, 100 requests/s, SPEC §5's stage budgets, three runs (M6) | no failed response; at most 0.133% of a phase's responses degraded |
| recall@10 on 43,249 test events, each degradation forced (M7) | 0.0181 healthy, 0.0098 without co-occurrence, 0.0050 without the ranker; without the encoder, with a shard missing or with HNSW at ef 400, within 0.0004 of healthy |
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

[docs/RUNNING.md](docs/RUNNING.md) covers the ONNX encoder and the ranker, bundle swaps, A/B arms, tracing, replicas, Kafka and Valkey, compose and kind. `make e2e` runs the tests on the built binaries under faults, and the `make bench-*` targets rerun the measurements.

## Limits

- The budget for the history read and cache lookup is now 5 ms, against 3 ms when the headline was measured (D039).
- The one-gateway capacity is an estimate made while the gateway shared three CPUs with the rest of the service. kind's rate and in-flight limits come from it and apply per gateway (D050).
- In M6, for the replica faults, the criteria cannot show that a restarted replica serves again or how fast searches recover. M7's criteria add that the replica answers again within 5 s of the fault's end and takes half of its shard's calls in the last minute.
- The exact-search time comes from ten runs of 2,000 searches on one core of an Intel Xeon at 2.30 GHz, ranging from 1.663 to 1.890 ms. Items whose vector is zero are left out of the shards (D028).
- The ranker's effect on users is for an A/B experiment to show. Offline, before MMR, its recall@10 is 0.091 against 0.026 for retrieval order on 680 held-out users (D027).
- User histories keep every event, with no cap and no expiry. When they fill Valkey's memory, `/v1/events` answers 503 (D023, D033).
- Valkey persists with snapshots only, so events since the last snapshot are lost if it stops uncleanly.
- The circuit breaker on Valkey opens only once at least 100 calls have ended in its 1 s window and half of them failed. Below 100 calls a second, while Valkey keeps its connections open without answering, every request waits out its 5 ms history budget (D037).
- With Kafka, a request may not see an event sent just before it.
- The gateway and the replicas talk without TLS or authentication (D040). The admin API, on 127.0.0.1:8081, has no authentication either. kind's NetworkPolicy is enforced only on a kernel with nftables' queue expression (D047).
- The kind manifests deploy only the gateways and the replicas, with stage budgets of 100 ms. A production deployment also needs Valkey, Kafka and ingest (D047).
- A gateway that cannot prepare its first bundle exits. If the replicas come long after the gateways, the kubelet's restart back-off, up to 5 minutes, delays the first deployment (D047).
- When both replicas of a shard are slow, neither is left out. Beyond the hedge budget the shard is left out and responses report `shards_partial` (D041).
- A replica holds two bundle versions. With three in use at once, a replica drops one that another gateway still serves, and a starting gateway can have a replica drop the version the running gateways serve. While no replica of a shard holds that version, responses report `shards_partial`, or `vector_error` when that holds for every shard (D042, D043, D044, D048).
- A restarted replica is not ready until the gateway has it prepare the served version again (D044).
- Exposure lines can be dropped or lost (D049). The cost of tracing is not measured, and Valkey and Kafka calls have no spans of their own (D051).

[docs/RUNNING.md](docs/RUNNING.md#limits-in-detail) gives each limit in full, with its measurements.

## License

MIT. Datasets are downloaded by script under their own terms and are not redistributed.
