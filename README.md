# recserve

A recommendation serving system in Go: retrieval, features, ranking and rerank behind one HTTP API, with a sharded vector tier, caching, degradation, rate limiting, A/B assignment, metrics and tracing.

Status: single-node request path (M1). See [docs/SPEC.md](docs/SPEC.md), [docs/DECISIONS.md](docs/DECISIONS.md), [docs/TASKS.md](docs/TASKS.md) and [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

## Quick start

```bash
make data DATA=data      # Amazon Reviews 2023 Video_Games, 5-core timestamp split
make bundle DATA=data    # baseline ALS bundle into data/bundle/<version>
make build
bin/gateway -bundle data/bundle/<version> -test-split data/Video_Games.test.csv.gz -preload data/Video_Games.train.csv.gz,data/Video_Games.valid.csv.gz
bin/loadgen -split data/Video_Games.test.csv.gz -rate 200 -duration 30s
```

## Scale

The catalog served has 25.6k items (Amazon Reviews 2023 Video_Games, 5-core). At that size exact search takes about 2 ms on one core, so sharding the catalog only demonstrates the mechanism; it is not needed for speed. ANN and shard performance is measured separately on 1M-scale vectors, and every reported figure says which data it comes from.

## License

MIT. Datasets are downloaded by script under their own terms and are not redistributed.
