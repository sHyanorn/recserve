# recserve

A recommendation serving system in Go: retrieval, features, ranking and rerank behind one HTTP API, with a sharded vector tier, caching, degradation, rate limiting, A/B assignment, metrics and tracing.

Status: M2 is done. One gateway process serves the full request path from in-process shards; Kafka, Valkey and the gRPC shard tier follow in M3 and M4. See [docs/SPEC.md](docs/SPEC.md), [docs/DECISIONS.md](docs/DECISIONS.md), [docs/TASKS.md](docs/TASKS.md) and [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

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

## In place after M2

- Retrieval from co-occurrence, popularity and vectors, searched exactly or with a hand-written HNSW in two in-process shards (D016, D024, D028)
- User encoder and ranker through ONNX Runtime (D026, D027)
- Seen-item filter, deduplication and MMR; per-stage deadlines with degradation reasons and shard coverage in every response (D029)
- Result cache keyed by bundle version and the user's history (D030)
- Two-phase bundle swap: prepare and verify in the background, atomic activation, previous bundle closed once no request holds it (D031)
- Prometheus metrics; load generator corrected for coordinated omission

## Scale

The catalog served has 25.6k items (Amazon Reviews 2023 Video_Games, 5-core). At that size exact search takes about 2 ms on one core, so sharding the catalog only demonstrates the mechanism; it is not needed for speed. ANN and shard performance is measured separately on 1M-scale vectors, and every reported figure says which data it comes from.

## License

MIT. Datasets are downloaded by script under their own terms and are not redistributed.
