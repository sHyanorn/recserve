# recserve

A recommendation serving system in Go: retrieval, features, ranking and rerank behind one HTTP API, with a sharded vector tier, caching, degradation, rate limiting, A/B assignment, metrics and tracing.

Status: M3 is done. One gateway process serves the full request path from in-process shards; events go through Kafka and an ingest consumer into user histories in Valkey, which also holds the result cache; a circuit breaker keeps the gateway answering, from the items the request carries, while Valkey is down. The gRPC shard tier follows in M4. See [docs/SPEC.md](docs/SPEC.md), [docs/DECISIONS.md](docs/DECISIONS.md), [docs/TASKS.md](docs/TASKS.md) and [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

## Quick start

```bash
make data DATA=data      # Amazon Reviews 2023 Video_Games, 5-core timestamp split
make bundle DATA=data    # baseline ALS bundle into data/bundle/<version>
make build
bin/gateway -bundle data/bundle/<version> -test-split data/Video_Games.test.csv.gz -preload data/Video_Games.train.csv.gz,data/Video_Games.valid.csv.gz
bin/loadgen -split data/Video_Games.test.csv.gz -rate 200 -duration 30s
```

`make bundle DATA=data ENCODER=onnx` builds the same bundle with an ONNX user encoder; the gateway then needs `-ort-lib` pointing to ONNX Runtime 1.31.x (`libonnxruntime.so`). To swap the served bundle while the gateway runs (the new bundle may hold other items):

```bash
curl -X POST localhost:8081/v1/admin/swap -d '{"bundle":"data/bundle/<version>"}'
```

The admin API listens on 127.0.0.1:8081 (`-admin-addr`) and has no authentication.

`make infra-up` starts Kafka and Valkey in Docker (`deploy/compose.yaml`). With `-valkey-addr localhost:6379` the gateway keeps the user histories and the result cache in Valkey, where every instance shares them; without it they stay in the process. With `-kafka-brokers localhost:9094`, `POST /v1/events` produces to the `events` topic and answers 202 once the broker has the events; they reach the histories through the ingest consumer, so a request may not see an event sent just before it. Without the flag the gateway writes the histories itself and a request sees the events sent before it.

Start order with Kafka: `make infra-up`, then `bin/ingest -kafka-brokers localhost:9094 -valkey-addr localhost:6379`, which creates the topic, then the gateways. A gateway started earlier answers 503 on `/v1/events` until the topic exists.

## In place after M3

- Retrieval from co-occurrence, popularity and vectors, searched exactly or with a hand-written HNSW in two in-process shards (D016, D024, D028)
- User encoder and ranker through ONNX Runtime (D026, D027)
- Seen-item filter, deduplication and MMR; per-stage deadlines with degradation reasons and shard coverage in every response (D029)
- Result cache keyed by bundle version and the user's history (D030)
- Two-phase bundle swap: prepare and verify in the background, atomic activation, previous bundle closed once no request holds it (D031)
- Events through Kafka to an ingest consumer that applies them idempotently and commits after applying (D034, D035)
- User histories and the result cache on Valkey, shared by the gateways; concurrent misses on one key computed once (D033, D036)
- Circuit breaker on the gateway's Valkey calls (D037)
- Tests on the built binaries with ingest killed, Valkey paused and Kafka stopped (`make e2e`)
- Prometheus metrics; load generator corrected for coordinated omission

## Scale

The catalog served has 25.6k items (Amazon Reviews 2023 Video_Games, 5-core). At that size exact search takes about 2 ms on one core, so sharding the catalog only demonstrates the mechanism; it is not needed for speed. ANN and shard performance is measured separately on 1M-scale vectors, and every reported figure says which data it comes from.

## Limits

- User histories keep every event: no cap per user and no expiry (D023, D033). In Valkey they are never evicted (`maxmemory` is 1 GB in compose; a long history takes about 87 bytes per event). Once they fill it, reads go on and writes fail: `/v1/events` answers 503 until memory is added or histories are deleted.
- Valkey persists with snapshots only, so events since the last snapshot are lost if it stops uncleanly.
- The gateway opens its Valkey connections within a request's history budget (3 ms): where connection setup takes longer, requests degrade and the circuit breaker can stay open with Valkey healthy (D037).
- With four workers saturating the four CPUs that Valkey shares, the history read and cache lookup took 4.3 ms at p99 against a budget of 3 ms; at 200 requests/s, 0.2 to 0.3% of the responses fell back for that reason (docs/BENCHMARKS.md, M3).

## License

MIT. Datasets are downloaded by script under their own terms and are not redistributed.
