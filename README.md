# recserve

A recommendation serving system in Go: retrieval, features, ranking and rerank behind one HTTP API, with a sharded vector tier, caching, degradation, rate limiting, A/B assignment, metrics and tracing.

The gateway searches item vectors split into shards, in its own process or on `annshard` replicas over gRPC. User histories, fed by Kafka through an ingest consumer, and the result cache are in Valkey. It runs in compose, and in kind with only the gateways and the replicas.

In the latest fault runs (275 requests/s on one 4-vCPU VM, result cache off, three runs per fault), four faults passed their criteria with no error and a fault-phase p99 of 6.8 to 20.5 ms: a replica killed, a replica paused, a replica whose searches hang, and 50 ms of latency on a shard. Valkey down failed: one run took 7 s to recover with 5 s allowed. The ingest consumer paused was not judged: one of its runs had no valid attempt. The runs are in [docs/BENCHMARKS.md](docs/BENCHMARKS.md) (M7, second set), judged by its fault criteria of M6 as amended for M7.

See [docs/SPEC.md](docs/SPEC.md), [docs/DECISIONS.md](docs/DECISIONS.md), [docs/TASKS.md](docs/TASKS.md) and M6's [report with charts](bench-results/m6/report.md).

## Quick start

```bash
make data DATA=data      # Amazon Reviews 2023 Video_Games, 5-core timestamp split
make bundle DATA=data    # baseline ALS bundle into data/bundle/<version>
make build
bin/gateway -bundle data/bundle/<version> -test-split data/Video_Games.test.csv.gz -preload data/Video_Games.train.csv.gz,data/Video_Games.valid.csv.gz
bin/loadgen -split data/Video_Games.test.csv.gz -rate 200 -duration 30s
```

`make bundle DATA=data ENCODER=onnx` builds the same bundle with an ONNX user encoder, and `RANKER=1` adds a logistic-regression ranker trained on the valid split; the gateway then needs `-ort-lib` pointing to ONNX Runtime 1.31.x (`libonnxruntime.so`). To swap the served bundle while the gateway runs (the new bundle may hold other items):

```bash
curl -X POST localhost:8081/v1/admin/swap -d '{"bundle":"data/bundle/<version>"}'
```

The admin API listens on 127.0.0.1:8081 (`-admin-addr`) and has no authentication.

An A/B experiment splits users between arms by a hash of the experiment's name and the user ID; an arm ranks with the bundle's ranker or keeps retrieval order, and the response names the arm and what ordered its items. `-exposure-log` writes one JSON line per answered request to stdout. `-max-rate` and `-max-inflight` turn requests beyond them away with 503; both are off by default. Tracing is on when `OTEL_EXPORTER_OTLP_ENDPOINT` is set, in the gateway and the replicas:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 bin/gateway -bundle data/bundle/<version> \
  -experiment ranker-v1 -arms control=retrieval:50,treatment=ranker:50 -exposure-log -max-rate 400 -max-inflight 64
```

By default the gateway holds the shards itself. To put them on replicas, here two shards of two replicas each, start `annshard` processes over the directory that holds the bundle versions and name them to the gateway, shards separated by `;` and a shard's replicas by `,`:

```bash
for port in 9101 9102 9103 9104; do bin/annshard -addr 127.0.0.1:$port -bundles data/bundle & done
bin/gateway -bundle data/bundle/<version> -shard-addrs "127.0.0.1:9101,127.0.0.1:9102;127.0.0.1:9103,127.0.0.1:9104"
```

A replica has no settings of its own: the gateway tells it which shard of which version to load, at start and in a swap, so every replica needs each bundle under its `-bundles` directory, in a directory named by the version, before the gateway is given it. The replicas' port has no TLS and no authentication (see Limits).

`make infra-up` starts Kafka and Valkey in Docker (`deploy/compose.yaml`). With `-valkey-addr localhost:6379` the gateway keeps the user histories and the result cache in Valkey, where every instance shares them; without it they stay in the process. With `-kafka-brokers localhost:9094`, `POST /v1/events` produces to the `events` topic and answers 202 once the broker has the events; they reach the histories through the ingest consumer, so a request may not see an event sent just before it. Without the flag the gateway writes the histories itself and a request sees the events sent before it.

Start order with Kafka: `make infra-up`, then `bin/ingest -kafka-brokers localhost:9094 -valkey-addr localhost:6379`, which creates the topic, then the gateways. A gateway started earlier answers 503 on `/v1/events` until the topic exists.

To run it in containers, `make images` builds the gateway, annshard and ingest images. `make up DATA=data VERSION=<version>` starts them in compose on Kafka and Valkey, with four replicas, Prometheus (localhost:9090, with an alert on replica failovers), Grafana (localhost:3000) and Jaeger (localhost:16686), to which the gateway and the replicas send every request's trace; `make down` stops them. In kind (v0.33, with kubectl):

```bash
make kind-up DATA=data                        # the cluster, the images, deploy/kind; prints how long it took to be ready
make kind-check DATA=data VERSION=<version>   # load through rolling restarts and a swap to VERSION: no error, nothing degraded
make kind-swap VERSION=<version>              # a swap is a rollout of the gateways, one at a time
make kind-down
```

## In place

- Retrieval from co-occurrence, popularity and vectors, searched exactly or with a hand-written HNSW in two shards, in the gateway or on replicas (D016, D024, D028)
- Shard tier over gRPC: `annshard` replicas answer only from the index a request names, and the gateway merges their results as it does its own shards' (D040)
- Per shard, a search goes to the next replica at once when one fails and to a second one when the first is slow, at most one hedge per search and about one search in ten; a keepalive takes a replica that went silent out of use (D041)
- Bundle swaps across replicas: every replica prepares the version, one per shard is enough to go on, and a replica that missed it, or restarted, catches up as soon as the gateway's connection to it is up; a replica keeps two versions and never drops the one the gateway serves (D042, D043)
- Replica readiness on `grpc.health.v1`: ready while it holds the version the gateway serves, so a rolling restart that waits for it keeps every shard served (D044)
- Shutdown on SIGTERM: a replica reports not ready and drains its calls within `-drain`; the gateway answers `/readyz` with 503, goes on answering for `-shutdown-delay` with each response closing its connection, then drains (D045)
- A bundle's version is the digest of its files, checked by the gateway and by every replica (D046)
- User encoder and ranker through ONNX Runtime (D026, D027); a logistic-regression ranker over six features, trained on the valid split in `tools/bundle`, whose bundle carries golden features that the gateway recomputes at load, within 1e-6 (D027)
- A/B arms by a hash of the experiment and the user, ranked or in retrieval order, in the response, the cache key and an exposure log (D049)
- Rate and in-flight limits answering 503 at once, without touching readiness; kind's values come from M6's capacity estimate (D050)
- OpenTelemetry traces from the gateway's API through each stage to the replicas' searches, exported over OTLP; Jaeger in compose (D051)
- Seen-item filter, deduplication and MMR; per-stage deadlines with degradation reasons and shard coverage in every response (D029)
- Result cache keyed by bundle version, A/B arm and the user's history (D030)
- Two-phase bundle swap: prepare and verify in the background, atomic activation, previous bundle closed once no request holds it (D031)
- Events through Kafka to an ingest consumer that applies them idempotently and commits after applying (D034, D035)
- User histories and the result cache on Valkey, shared by the gateways; concurrent misses on one key computed once (D033, D036)
- Circuit breaker on the gateway's Valkey calls; connections to Valkey opened and replaced outside the request path (D037)
- Tests on the built binaries with ingest killed, Valkey paused, Kafka stopped, and, on four `annshard` processes, a replica killed, a swap, an empty restart, a rejected swap, swaps in a row over three bundles and a rolling restart of the replicas under requests (`make e2e`)
- Prometheus metrics, among them each replica call's latency, from which the hedge delay is set (D041); pprof on the gateway's admin listener and, with `-debug-addr`, on the replicas (D052)
- Load generator corrected for coordinated omission, with per-second windows and a workload of repeated requests; `make bench-*` targets that run the load sweeps, six fault injections with toxiproxy and container control, bundle swaps under load and the ANN measurements, each with noise baselines and an environment record, judged by `cmd/benchsum` against criteria committed before the runs (D052)
- Inner products in AVX2 for exact search and in AVX for HNSW, each summed in one fixed order that the Go code on other CPUs matches to the bit (D053, D055)
- Images run as a non-root user on distroless; compose with Prometheus, an alert on replica failovers, and Grafana (D047)
- kind manifests: replicas as a StatefulSet ready by `grpc.health.v1`, gateways with an HPA, Pod Security `restricted`, and a NetworkPolicy that only the gateways pass, whose enforcement CI checks; a swap is a rollout of the gateways that keeps the replicas to two versions; at 100 requests/s through rolling restarts of both and a swap, with the gateways' stage budgets at SPEC §5's values, three runs had no failed response and at most 0.133% of a phase's responses degraded (D047, D048; `bench-results/m6/kind`)

## Results

Measured in M6 and M7 on 4-vCPU cloud VMs, with every service on the same three CPUs ([report with charts](bench-results/m6/report.md), [docs/BENCHMARKS.md](docs/BENCHMARKS.md)). The figures are that machine's and that topology's, and figures from different VMs are not compared.

| What | Result |
|---|---|
| Headline: the highest rate with p99 ≤ 50 ms and at most 1% of the responses degraded; replay, cache off | 550 requests/s, p99 4.6 to 5.1 ms (with the earlier 3 ms history budget) |
| One gateway on 2 CPUs | about 1,800 requests/s, an estimate from its CPU per request |
| Faults at half the headline, three runs each | replica killed, 50 ms on a shard, Valkey down, searches hanging: pass by the criteria as written, though for the replica faults the criteria cannot show that a restarted replica serves again or how fast searches recover; ingest paused, replica paused: fail, each on one second with Valkey's circuit breaker open |
| Faults with the cache off, three runs each (M7) | replica killed, replica paused, searches hanging, Valkey down: pass; the replica answering again within 5 s of the fault's end and taking half of its shard's calls in the last minute; 50 ms on a shard: fail, on one second with the Valkey breaker open; consumer paused: not judged, two valid runs of three |
| Outlier detection (M7), cache off | replica paused: fault phase p99 5.1 to 5.5 ms against 17.4 to 17.7 without it, pass; searches hanging: 5.7 to 6.4 ms against 18.2 to 18.4, fail, one run of three recovering in 13 s with 5 allowed; 50 ms on a shard: fail, on one second with the Valkey breaker open, one run of three not valid |
| Faults with the cache off and outlier detection, second set (M7, another VM), three runs each | replica killed, 50 ms on a shard, replica paused, searches hanging: pass, fault phase p99 6.8 to 20.5 ms; Valkey down: fail, one run recovering in 7 s with 5 allowed; consumer paused: not judged, two valid runs of three; with no fault and the cache on, no breaker opening in five runs |
| Recommendation quality on 43,249 test events, each degradation forced (M7) | recall@10 0.0181 healthy, 0.0098 without co-occurrence, 0.0050 without the ranker; without the encoder, with a shard missing or with HNSW at ef 400, every difference within 0.0004 of healthy |
| `vec.Dot` in AVX2 | the replicas' CPU per request 37% lower, p99 a quarter lower at 200 requests/s |
| HNSW on glove-100, one thread | p99 1.0 to 1.3 times faiss's at the same ef; recall@100 reaches 0.95 only at a p99 of 7.66 ms |

## Scale

The catalog served has 25.6k items (Amazon Reviews 2023 Video_Games, 5-core). At that size exact search takes about 2 ms on one core, so sharding the catalog only demonstrates the mechanism; it is not needed for speed. ANN and shard performance is measured separately on 1M-scale vectors, and every reported figure says which data it comes from.

## Limits

- User histories keep every event: no cap per user and no expiry (D023, D033). In Valkey they are never evicted (`maxmemory` is 1 GB in compose; a long history takes about 87 bytes per event). Once they fill it, reads go on and writes fail: `/v1/events` answers 503 until memory is added or histories are deleted.
- Valkey persists with snapshots only, so events since the last snapshot are lost if it stops uncleanly.
- Once every connection to Valkey is lost, and while the server accepts new ones without answering, each request waits out its history budget (5 ms) for up to a second; after that its calls fail at once, each counted as a failure, and the circuit breaker opens once at least 100 calls have ended within one of its 1 s windows, so never below 100 calls a second (D037).
- The history read and cache lookup budget is 5 ms: with the cache on, up to 1.74% of the requests at 400 requests/s ran past the earlier 3 ms, and at most 0.02% run past 5 ms at 550 requests/s (D039).
- The circuit breaker on Valkey opens for a second once at least 100 calls have ended in its current window of 1 s and at least half of them failed, a call that runs out of its budget included; for that second every response reports `history_unavailable`. Below 100 calls a second, as on a gateway serving fewer than 100 requests a second without the cache, it never opens; there, while Valkey keeps its connections open without answering, every request waits out its 5 ms history budget. At 275 requests/s with no fault it did not open in five runs with the cache on, nor, with the cache off, in any fault run outside Valkey's outage. Under the earlier rule, five failed calls in a row, it opened 6 times in 9 such attempts on another VM, and its openings failed F2 in M7's first set and F4 and F5 in M6. Its state and calls are metrics (D037; BENCHMARKS, M6 and M7).

- The kind manifests deploy the gateways and the replicas; the gateways keep histories in the process. A production deployment also needs Valkey, Kafka and ingest, with manifests of their own. In kind the gateways' stage budgets are 100 ms, since one node shares its CPUs with the control plane and the load generator (D047).
- A gateway that cannot prepare its first bundle exits; if the replicas come long after the gateways, the kubelet's restart back-off, up to 5 minutes, delays the first deployment (D047).
- The gateway and the `annshard` replicas talk without TLS or authentication (D040). A deployment must make the replicas' port reachable from the gateways only; the kind manifests do it with a NetworkPolicy, which kind enforces only on a kernel with nftables' queue expression (D047).
- A replica whose searches end at the shard deadline five times in a row is left out, and asked again one search a second, while the other replica of its shard answers in time; when both are slow, as with latency on a whole shard, neither is left out and the hedge budget (10 in reserve, then one search in ten) covers the shard, beyond which it is left out and the response reports `shards_partial`. With the cache off at 275 requests/s, a paused replica left 0.14 to 0.17% of the responses partial in the 13 s after its pause, against 25.1% without outlier detection; a replica whose searches hang, 0.02 to 0.04 points above the healthy share, against 24.9 to 25.0 points (D041; BENCHMARKS, M7).
- A replica holds two bundle versions. With three in use at once, as when several gateways swap in no agreed order, a replica drops one that another gateway still serves, and that gateway's responses report `shards_partial` until the replica has prepared it again (D042, D043). In kind, `make kind-swap` keeps the gateways to two (D048); with compose or processes the order of swaps is the operator's.
- Exposure lines are dropped when the writer's queue is full or stdout fails, and lost if the process is killed with lines queued (D049).
- The rate and in-flight limits are per gateway. kind's values, 1,450 requests/s and 152 in flight, follow from an estimate of one gateway's capacity, made from its CPU per request while it shared three CPUs with the rest of the service (D050).
- The cost of tracing when on is not measured; Valkey and Kafka calls have no spans of their own (D051).
- On the 680 held-out users the ranker's recall@10 is 0.091 against 0.026 for retrieval order, measured before MMR (D027). In an offline run of the pipeline in process, with no stage budgets and MMR included, on 43,249 test events, recall@10 is 0.0181, and 0.0050 with the ranker forced to fail, which keeps the retrieval scores: a difference of −0.0131, 95% interval over test events −0.0143 to −0.0119 (BENCHMARKS, M7). Its effect on users is for an A/B experiment to show.
- A replica that restarts is empty and not ready until the gateway has it prepare the version served again, which it does as soon as its connection is back (D044); for up to a second after a swap, a replica reports ready on the previous version.

## License

MIT. Datasets are downloaded by script under their own terms and are not redistributed.
