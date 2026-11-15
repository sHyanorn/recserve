# Running recserve

The README has the quick start. This file covers the rest of running it, then each limit in full.

## Encoder, ranker, swaps, A/B arms and tracing

`make bundle DATA=data ENCODER=onnx` adds an ONNX user encoder, which gives a history the user vector ALS would fit for it (D058), and `RANKER=1` adds a logistic-regression ranker trained on the valid split; the gateway then needs `-ort-lib` pointing to ONNX Runtime 1.31.x (`libonnxruntime.so`). To swap the served bundle while the gateway runs (the new bundle may hold other items):

```bash
curl -X POST localhost:8081/v1/admin/swap -d '{"bundle":"data/bundle/<version>"}'
```

The admin API listens on 127.0.0.1:8081 (`-admin-addr`) and has no authentication.

An A/B experiment splits users between arms by a hash of the experiment's name and the user ID; an arm ranks with the bundle's ranker or keeps retrieval order, and the response names the arm and what ordered its items. `-exposure-log` writes one JSON line per answered request to stdout. `-max-rate` and `-max-inflight` turn requests beyond them away with 503; both are off by default. Tracing is on when `OTEL_EXPORTER_OTLP_ENDPOINT` is set, in the gateway and the replicas:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 bin/gateway -bundle data/bundle/<version> \
  -experiment ranker-v1 -arms control=retrieval:50,treatment=ranker:50 -exposure-log -max-rate 400 -max-inflight 64
```

## Replicas

By default the gateway holds the shards itself. To put them on replicas, here two shards of two replicas each, start `annshard` processes over the directory that holds the bundle versions and name them to the gateway, shards separated by `;` and a shard's replicas by `,`:

```bash
for port in 9101 9102 9103 9104; do bin/annshard -addr 127.0.0.1:$port -bundles data/bundle & done
bin/gateway -bundle data/bundle/<version> -shard-addrs "127.0.0.1:9101,127.0.0.1:9102;127.0.0.1:9103,127.0.0.1:9104"
```

A replica has no settings of its own: the gateway tells it which shard of which version to load, at start and in a swap, so every replica needs each bundle under its `-bundles` directory, in a directory named by the version, before the gateway is given it. The replicas' port has no TLS and no authentication (see [Limits in detail](#limits-in-detail)).

## Kafka and Valkey

`make infra-up` starts Kafka and Valkey in Docker (`deploy/compose.yaml`). With `-valkey-addr localhost:6379` the gateway keeps the user histories and the result cache in Valkey, where every instance shares them; without it they stay in the process. With `-kafka-brokers localhost:9094`, `POST /v1/events` produces to the `events` topic and answers 202 once the broker has the events; they reach the histories through the ingest consumer, so a request may not see an event sent just before it. Without the flag the gateway writes the histories itself and a request sees the events sent before it.

Start order with Kafka: `make infra-up`, then `bin/ingest -kafka-brokers localhost:9094 -valkey-addr localhost:6379`, which creates the topic, then the gateways. A gateway started earlier answers 503 on `/v1/events` until the topic exists. One whose bundle names a stream window waits up to a minute for the topic at start, or else exits.

A bundle that names a stream window (`make bundle ... RANKER=1 STREAM=DAYS`) is served from the stream state, which the gateway builds at start from the `-preload` files. With `-kafka-brokers` it lists the events topic's end offsets before the preload and reads every partition from them after it, so its state holds every event produced after it started; without the flag it takes the events of its own `/v1/events` only. A gateway started on a bundle without a window keeps no state and rejects one with a window in a swap (D062).

## Compose and kind

To run it in containers, `make images` builds the gateway, annshard and ingest images. `make up DATA=data VERSION=<version>` starts them in compose on Kafka and Valkey, with four replicas, Prometheus (localhost:9090, with an alert on replica failovers), Grafana (localhost:3000) and Jaeger (localhost:16686), to which the gateway and the replicas send every request's trace; `make down` stops them. In kind (v0.33, with kubectl):

```bash
make kind-up DATA=data                        # the cluster, the images, deploy/kind; prints how long it took to be ready
make kind-check DATA=data VERSION=<version>   # load through rolling restarts and a swap to VERSION: no error, nothing degraded
make kind-swap VERSION=<version>              # a swap is a rollout of the gateways, one at a time
make kind-down
```

## Limits in detail

- User histories keep every event: no cap per user and no expiry (D023, D033). In Valkey they are never evicted (`maxmemory` is 1 GB in compose; a long history takes about 87 bytes per event). Once they fill it, reads go on and writes fail: `/v1/events` answers 503 until memory is added or histories are deleted.
- Valkey persists with snapshots only, so events since the last snapshot are lost if it stops uncleanly.
- Once every connection to Valkey is lost, and while the server accepts new ones without answering, each request waits out its history budget (5 ms) for up to a second; after that its calls fail at once, each counted as a failure, and the circuit breaker opens once at least 100 calls have ended within one of its 1 s windows, so never below 100 calls a second (D037).
- The history read and cache lookup budget is 5 ms: with the cache on, up to 1.74% of the requests at 400 requests/s ran past the earlier 3 ms, and at most 0.02% run past 5 ms at 550 requests/s (D039).
- The circuit breaker on Valkey opens for a second once at least 100 calls have ended in its current window of 1 s and at least half of them failed, a call that runs out of its budget included; for that second every response reports `history_unavailable`. Below 100 calls a second, as on a gateway serving fewer than 100 requests a second without the cache, it never opens; there, while Valkey keeps its connections open without answering, every request waits out its 5 ms history budget. At 275 requests/s with no fault it did not open in five runs with the cache on, nor, with the cache off, in any fault run outside Valkey's outage. Under the earlier rule, five failed calls in a row, it opened 6 times in 9 such attempts on another VM, and its openings failed F2 in M7's first set and F4 and F5 in M6. Its state and calls are metrics (D037; BENCHMARKS, M6 and M7).

- The kind manifests deploy the gateways and the replicas; the gateways keep histories in the process. A production deployment also needs Valkey, Kafka and ingest, with manifests of their own. In kind the gateways' stage budgets are 100 ms, since one node shares its CPUs with the control plane and the load generator (D047).
- A gateway that cannot prepare its first bundle exits; if the replicas come long after the gateways, the kubelet's restart back-off, up to 5 minutes, delays the first deployment (D047).
- The gateway and the `annshard` replicas talk without TLS or authentication (D040). A deployment must make the replicas' port reachable from the gateways only; the kind manifests do it with a NetworkPolicy, which kind enforces only on a kernel with nftables' queue expression (D047).
- A replica whose searches end at the shard deadline five times in a row is left out, and asked again one search a second, while the other replica of its shard answers in time; when both are slow, as with latency on a whole shard, neither is left out and the hedge budget (10 in reserve, then one search in ten) covers the shard, beyond which it is left out and the response reports `shards_partial`. With the cache off at 275 requests/s, a paused replica left 0.14 to 0.17% of the responses partial in the 13 s after its pause, against 25.1% without outlier detection; a replica whose searches hang, 0.02 to 0.04 points above the healthy share, against 24.9 to 25.0 points (D041; BENCHMARKS, M7).
- In M6, for the replica faults, the criteria cannot show that a restarted replica serves again or how fast searches recover. M7's criteria, with the cache off so that every response searches, add that the replica serves again within the fault's recovery limit and answers at least 40% of its shard's calls in the last minute (BENCHMARKS, M6 and M7).
- A replica holds two bundle versions. With three in use at once, as when several gateways swap in no agreed order, a replica drops one that another gateway still serves, and that gateway's responses report `shards_partial` until the replica has prepared it again (D042, D043). In kind, `make kind-swap` keeps the gateways to two (D048); with compose or processes the order of swaps is the operator's. Two in use can still lose one: a gateway that starts names the version it prepares as served, so a replica that holds the version the running gateways serve and the one before it, and was last sent a search or `Prepare` of the one before, drops the version they serve when a starting gateway has it prepare a third. Their searches then go to the shard's next replica until the replica has prepared that version again, and while no replica of a shard holds it their responses report `shards_partial`, or `vector_error` when that holds for every shard (D043, D044, D048).
- Exposure lines are dropped when the writer's queue is full or stdout fails, and lost if the process is killed with lines queued (D049).
- The rate and in-flight limits are per gateway. kind's values, 1,450 requests/s and 152 in flight, follow from an estimate of one gateway's capacity, made from its CPU per request while it shared three CPUs with the rest of the service (D050).
- The cost of tracing when on is not measured; Valkey and Kafka calls have no spans of their own (D051).
- The README's exact-search time comes from ten runs of 2,000 searches on one core of an Intel Xeon at 2.30 GHz, ranging from 1.663 to 1.890 ms. Items whose vector is zero are left out of the shards (D028).
- On the 680 held-out users the ranker's recall@10 is 0.091 against 0.026 for retrieval order, measured before MMR (D027). In an offline run of the pipeline in process, with no stage budgets and MMR included, on 43,249 test events and with one bundle trained once, recall@10 is 0.0181, and 0.0050 with the ranker forced to fail, which keeps the retrieval scores: a difference of −0.0131, 95% interval over test events −0.0143 to −0.0119 (BENCHMARKS, M7). Its effect on users is for an A/B experiment to show.
- M7's quality figures come from one bundle trained once and not updated over the test period, July 2022 to September 2023. The target item of 60% of the Video_Games test events, and of 33% of Musical_Instruments', has no train event; on those events every row of M8's first Video_Games test read has recall@10 0.0000 ([its file](../bench-results/m8/Video_Games/read-1.md)).
- M8's figures come from the pipeline in process; the state's cost in serving is not measured yet.
- A gateway whose first bundle names a stream window keeps every event it has taken, with no cap and no expiry, in addition to the state's own lists, whose pairs are never dropped (D059 point 5). Its state holds the events of its `-preload` files and those produced after it started; events produced before, as when a gateway restarts while others serve, are missing from it, and without Kafka so are the events other gateways receive, as in kind, where a swap is a rollout of the gateways (D048). An event that arrives after a later event of its user is left out of that later event's pairs, which may hold an older event in its place (D062).
- M8's confirmatory result is one test read of one category. Video_Games' test split had been used in M7 and for 18 exploratory configurations, so its figures are exploratory (D061).
- A replica that restarts is empty and not ready until the gateway has it prepare the version served again, which it does as soon as its connection is back (D044); for up to a second after a swap, a replica reports ready on the previous version.
