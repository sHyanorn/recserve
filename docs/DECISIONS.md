# Technical Decision Log

Format: ID, date, decision, rationale, alternatives. Append new decisions at the end.

## D001 Go 1.27.3, module github.com/sHyanorn/recserve, MIT
- Date: 2026-11-08
- Decision: go.mod says `go 1.27.3`, the latest release on go.dev/dl (2026-10-19). The module path is `github.com/sHyanorn/recserve`; the license is MIT
- Rationale: Go supports each major release until two newer ones are out, so 1.27 and 1.26 are supported. grpc v1.85.0, client_golang v1.25.0 and otel v1.47.0 all require Go 1.26 or later in their go.mod
- Alternatives: Go 1.26 (supported, but the toolchain the sister project matching-engine already uses is 1.27.3)

## D002 HTTP/JSON outside, gRPC inside
- Date: 2026-11-08
- Decision: Clients call the gateway over HTTP/JSON. The gateway calls shards over gRPC (google.golang.org/grpc v1.85.0, Apache-2.0)
- Rationale: JSON is easy to call and to read in a demo. Between services, gRPC gives deadlines that propagate, cancellation, connection reuse, health checking and generated code; a shard answers with float32 scores and int32 IDs, which protobuf encodes compactly
- Alternatives: HTTP/JSON everywhere (deadline propagation and cancellation by hand); gRPC everywhere (harder to try from a browser or curl)

## D003 Two process kinds: gateway and annshard
- Date: 2026-11-08
- Decision: The gateway is stateless across requests and runs the whole request path; it keeps the feature store, co-occurrence table, popularity list and models in memory. annshard holds one shard of item vectors and answers nearest-neighbor queries. Ranking runs in the gateway process
- Rationale: Only vector search grows with the catalog and benefits from sharding. Features and co-occurrence for these datasets fit in memory, and a separate feature or ranking service would add a network hop to every request without showing anything new. Vespa splits stateless containers from stateful content nodes the same way
- Alternatives: a feature service and a ranking service (more hops, more processes to operate); one process with everything (no distribution to show)

## D004 A static cluster layout
- Date: 2026-11-08
- Decision: A config file lists shards and their replica addresses. The gateway health-checks replicas, retries another replica on failure and hedges slow requests. There is no consensus, automatic resharding or dynamic membership
- Rationale: Failover, partial results and hedging are what a request sees; membership changes are an operations feature that needs a consensus store to do right, and doing it halfway would be worse than leaving it out
- Alternatives: etcd-based membership (a dependency and a failure mode of its own for a 4-replica layout)

## D005 Server-side p99 of 50 ms; headline is the highest rate meeting it
- Date: 2026-11-08
- Decision: The service target is a server-side end-to-end p99 of 50 ms, split by stage as in SPEC §5. The headline result is the highest offered rate at which all three runs are valid and have p99 ≤ 50 ms
- Rationale: A latency figure without its load, or a throughput figure without its latency, says little. 50 ms leaves room for a client-facing budget of the usual 100 to 200 ms
- Alternatives: maximum throughput alone (ignores the tail); a fixed throughput target (unknown before measuring on this hardware)

## D006 ONNX Runtime through onnxruntime_go
- Date: 2026-11-08
- Decision: Models run with ONNX Runtime 1.31.x (MIT) through `github.com/yalue/onnxruntime_go` v1.38.0 (MIT), which uses cgo and loads the shared library with dlopen. The binding is built against the 1.31.0 C API, so the library version is pinned to 1.31.x. This stands only if the M0 check (docs/BENCHMARKS.md, M0) passes; otherwise the fallback named there applies
- Rationale: ONNX is the exchange format seqrec-lab can export from PyTorch, and ONNX Runtime is the reference runtime for it. Twitter's navi also serves its home ranker through ONNX. Pure-Go ONNX runtimes are experimental (owulveryck/onnx-go) or unmaintained (gorgonia, no commits since 2023-10)
- Alternatives: a separate inference server (a network hop per request); pure-Go inference written by hand (fine for a small MLP, a lot of work for a Transformer encoder)

## D007 ANN library chosen by the M0 check
- Date: 2026-11-08
- Decision: Use `github.com/coder/hnsw` (CC0) if it meets the M0 criterion; otherwise write HNSW by hand with adjacency in flat int32 arrays
- Rationale: coder/hnsw is the only maintained pure-Go HNSW found. faiss and hnswlib need cgo bindings, which are stale (go-faiss, last release 2021) or unreleased (evan176/hnswgo, no tags); USearch's Go binding has no release tag and needs a separately installed C library
- Alternatives: cgo to faiss (a second native library to ship)

## D008 ANN performance on 1M-scale vectors; end to end on real data
- Date: 2026-11-08
- Decision: ANN and shard benchmarks use glove-100-angular from ann-benchmarks (1,183,514 vectors, 100 dimensions, ground truth included) and seeded synthetic vectors (1M × 64, Gaussian mixture). End-to-end benchmarks and quality evaluation use the real catalogs. glove-100-angular is built by ann-benchmarks (MIT) from Stanford's `glove.twitter.27B.100d` vectors, which the GloVe README releases under the ODC Public Domain Dedication and License (PDDL)
- Rationale: The real catalogs have at most 25.6k items (Video_Games 5-core), too few for ANN to matter (SPEC §3). glove-100 has published results to compare with; synthetic vectors scale freely. Neither is committed
- Alternatives: synthetic vectors only (no outside reference point); Deep1B subsets (license not checked)

## D009 An open-loop load generator of our own, on separate CPUs
- Date: 2026-11-08
- Decision: `cmd/loadgen` sends request i at t0 + i/rate whatever the replies and measures latency from that due time, as matching-engine's D030 does. Percentiles are exact, from sorted samples. During benchmarks loadgen and the services run on disjoint CPUs (taskset). vegeta cross-checks one rate
- Rationale: Closed-loop generators wait for replies and hide stalls (wrk2 README; Schroeder et al., NSDI 2006). ghz runs closed-loop unless `--async` and measures from the actual send; k6 is AGPL and adds a JavaScript runtime. Our generator needs dataset-driven request bodies and per-response checks (arm, versions, degraded reasons), which off-the-shelf tools do not do
- Alternatives: vegeta alone (no per-response checks); wrk2 (Lua scripting, HTTP/1 only)

## D010 Model bundle contract
- Date: 2026-11-08
- Decision: seqrec-lab exports the bundle described in SPEC §8: manifest, item ID list, item vectors as `.npy`, user encoder in ONNX, optional ranker, golden cases. The service verifies checksums and golden outputs before a swap
- Rationale: `.npy` is a short header plus raw float32, trivial to read in Go and to write from NumPy. Golden cases catch a mismatched ID order or preprocessing, which would otherwise serve wrong items silently
- Alternatives: vectors inside the ONNX graph (cannot be sharded or searched); a custom binary format (one more writer and reader)

## D011 Data by script only; no public demo by default
- Date: 2026-11-08
- Decision: Scripts download the data; nothing derived from rows is committed. There is no public online demo. If one is built later, it uses KuaiRec (Zenodo: CC-BY-4.0; the GitHub repository says CC BY-SA 4.0; the stricter terms apply) with attribution
- Rationale: GroupLens does not typically permit public redistribution of MovieLens, and its READMEs forbid commercial use without permission. Steam (UCSD) and Amazon Reviews 2023 state no data license
- Alternatives: a MovieLens demo (redistribution of derived titles and vectors is close to what its terms forbid)

## D012 Images on distroless/cc, pinned by digest
- Date: 2026-11-08
- Decision: Binaries that use cgo run on `gcr.io/distroless/cc-debian13:nonroot`, which has glibc; the ONNX Runtime library is copied in. Base images are pinned by digest
- Rationale: onnxruntime_go needs cgo and a glibc-linked libonnxruntime.so; distroless/static has no libc
- Alternatives: a Debian slim base (a shell and more packages to patch)

## D013 Development data: Amazon Reviews 2023 Video_Games
- Date: 2026-11-08
- Decision: M1 onward develops and benchmarks on Amazon Reviews 2023 Video_Games, the 5-core timestamp split published by the dataset authors (`benchmark/5core/timestamp/`), with item metadata from `meta_Video_Games`. MovieLens and Steam are optional and run only on a local machine
- Rationale: From the cloud sessions files.grouplens.org could not be reached, and cseweb.ucsd.edu (Steam) sends an incomplete certificate chain that standard clients reject; mcauleylab.ucsd.edu serves the Amazon files. Video_Games is one of seqrec-lab's datasets. seqrec-lab builds its own split, so the bundle's `train_cutoff_ts` must precede recserve's test period, and the loader enforces it
- Alternatives: KuaiRec (licensed for a demo, but not among seqrec-lab's datasets); Steam (not reachable from the cloud sessions)

## D014 Every benchmark run is bracketed by a noise baseline
- Date: 2026-11-08
- Decision: Before and after each benchmark run a fixed single-core workload is timed (docs/BENCHMARKS.md, Noise baseline). A run whose baseline is more than 10% slower than the session reference is flagged and rerun, at most twice; a run still flagged is reported as such
- Rationale: Cloud VMs share CPUs with other tenants; matching-engine saw stalls of up to 128 ms with no engine code running. A baseline measured around each run separates a slower machine from a slower service
- Alternatives: hiccup meter only (catches stalls, not a slower CPU share)

## D015 ONNX Runtime sessions run with one intra-op thread
- Date: 2026-11-09
- Decision: R1 passed (docs/BENCHMARKS.md, Results), so D006 stands. Every session is created with intra-op threads = 1; concurrency comes from goroutines sharing a session, bounded by the service's concurrency limit
- Rationale: At concurrency 4 on 4 CPUs, the default thread pool raised p99 by 2.7 to 8.3 times over one intra-op thread, because each call's worker threads compete with the other callers
- Alternatives: the default pool (faster single calls for the L = 200 encoder, worse under load)

## D016 HNSW written by hand
- Date: 2026-11-09
- Decision: R2 failed, so D007's fallback applies: M2 implements HNSW (Malkov and Yashunin) in the service, with vectors in one float32 array, neighbor lists in flat int32 arrays per layer, beam search bounded by ef, and the neighbor-selection heuristic. It is measured in M2 with the R2 protocol unchanged, against the same exact and faiss baselines
- Rationale: coder/hnsw v0.6.1 reached recall@100 of at most 0.064 on both sets and both settings, the same at every ef, while faiss reached 1.000 on the synthetic set with the same settings. Its layer search keeps only k results, stops at the first expansion without improvement, and its heap evicts an arbitrary result. Upstream's untagged fix of the search (36cab60) levels off at recall@100 0.824 on the synthetic set with p99 21 ms at ef = 800, so the graph it builds is the limit too: neighbors are pruned by distance alone, without the selection heuristic
- Alternatives: upstream coder/hnsw at 36cab60 (untagged, fails the recall target); patch it further (search and graph construction both replaced); cgo to faiss (a second native library, rejected in D007); exact search only (enough for the 25.6k-item catalog, but leaves the 1M-scale tier without an index)

## D017 Measurement corrections after the first M0 results
- Date: 2026-11-09
- Decision: (1) The GC CPU fraction is no longer measured; GC is judged by stop-the-world pauses and cycle counts. (2) Medians are true medians (mean of the two middle values) for the noise baseline and its reference. (3) The baselines around faiss runs are checked against the reference like any other run. (4) A result file keeps the baselines of the reported attempt only. (5) coder/hnsw is built with `Ml = 1/M`, and its R2 runs are repeated under a new session reference
- Rationale: (1) The runtime updates `/cpu/classes/*` only when a GC cycle ends, so over a query phase with zero or one cycle the fraction is 0 or covers the wrong interval. (2) The upper median was used by mistake; recomputing all committed baselines with the true median changes no flag. (3) Those baselines were recorded but not compared. (4) That is what the code does; the protocol text said otherwise. (5) coder/hnsw's `Ml` is the probability of promoting a node one layer up, 1/M in the paper's terms; 1/ln M made about 15 layers instead of 6 and inflated build time and heap. None of these changes a pass criterion
- Alternatives: keep the first coder/hnsw results (build time and heap measured on a wrongly shaped graph)

## D018 Scope: event queue, Valkey, fault injection under load, kind
- Date: 2026-11-09
- Decision: Four items join Required (SPEC §2): events through a Kafka-protocol topic with an ingest consumer (D019); user histories and the result cache in Valkey (D020); fault injection during load tests (D021); deployment on kind with probes, an HPA and a zero-error rollout check (D022). Milestones become M1 to M7 (docs/TASKS.md), with a new M3 for the event pipeline and shared state. The estimated work grows by about 1.9 times the size of M1: M3 about 1, kind about 0.5, fault injection about 0.4
- Rationale: In-process histories tie a user to one gateway instance and are lost on restart, so the gateway cannot scale out or be replaced without losing state; a durable log decouples writes from the serving path and lets the consumer be restarted and replayed. Fault injection under load measures the degradation paths that the property tests only check for correctness. kind checks that probes, rollout and swaps behave as designed under an orchestrator
- Alternatives: keep histories in process (no shared state, no restart safety); a database instead of a log (writes on the serving path)

## D019 Apache Kafka in KRaft mode, franz-go client
- Date: 2026-11-09
- Decision: `apache/kafka:4.4.0`, one node, combined broker and controller (KRaft), heap capped at 1 GB, offsets topic replication 1. Client `github.com/twmb/franz-go` v1.22. The gateway produces with the idempotent producer (`acks=all`), keyed by user ID; topic `events` has 6 partitions. The ingest consumer group commits offsets manually, after an event batch is applied to Valkey. Delivery is at least once; applying an event is idempotent (D020), so redelivery is harmless. Exactly-once holds only inside Kafka and is not relied on
- Rationale: Keying by user keeps each user's events in order within a partition. Committing after the apply means a crash replays events instead of losing them. Redpanda's Community Edition is under the Business Source License and its dev-container mode skips fsync, which makes its figures incomparable. franz-go is pure Go and supports idempotent production and manual commits; confluent-kafka-go needs cgo and librdkafka; segmentio/kafka-go documents no idempotent producer
- Alternatives: Redpanda (license, fsync); NATS JetStream (not the Kafka protocol); Go channels in process (no durability)

## D020 Valkey for user histories and the result cache
- Date: 2026-11-09
- Decision: `valkey/valkey:9.1`, client `github.com/redis/go-redis/v9`. Each user's history is a sorted set: member = item ID and event ID, score = event time; retention follows D023. Applying an event is one `ZADD`, so applying it twice changes nothing. Feature lookup reads the members with score below the request time, so late or reordered events still respect property 1. The result cache key holds the index version, model version, arm, user, k and the ID of the user's latest event, with a TTL plus jitter; a swap or a new event changes the key, and old entries expire. Misses are computed once per key per instance with `golang.org/x/sync/singleflight`. Item features stay in the gateway. If Valkey fails or times out, a circuit breaker opens: requests use the `recent` items they carry, report `history_unavailable`, and skip the cache in both directions
- Rationale: Shared per-user state lets any instance serve any user. Item features are part of the bundle and immutable per version; a request reads about 500 of them, which in Valkey would add a 500-key read to every request. Redis 7.4 and later is under RSALv2/SSPLv1 (and AGPLv3 from 8.0); Valkey is BSD-3-Clause. go-redis does not list Valkey as supported, so the integration tests run against Valkey itself. Cross-instance duplicate computation is bounded by the number of instances, so no distributed lock is added
- Alternatives: Redis 8 (license); item features in Valkey (a 500-key read per request); a distributed lock against stampedes (another failure mode for a bounded saving); probabilistic early refresh (Vattani et al., VLDB 2015; worth it only for long, expensive recomputations)

## D021 Fault injection with toxiproxy and container control
- Date: 2026-11-09
- Decision: In the compose benchmark setup, toxiproxy (v2.13) sits between the gateway and each shard replica, Valkey and Kafka; faults are added through its Go client. Faults: a shard replica killed (`docker kill`), latency on one shard (toxiproxy `latency`), Valkey unavailable (proxy disabled), the ingest consumer stalled (`docker pause`). Each is injected in the middle of a load test; the protocol and its pass criteria are in docs/BENCHMARKS.md and are committed before M6 runs
- Rationale: toxiproxy is a plain TCP proxy, so it works the same in compose and in kind and needs no kernel module or NET_ADMIN capability, unlike `tc netem`. Kafka advertises its own listener address, so its advertised listener points at the proxy
- Alternatives: `tc netem` (NET_ADMIN, per-interface); Chaos Mesh (Kubernetes only)

## D022 kind for deployment checks
- Date: 2026-11-09
- Decision: kind v0.33 (Kubernetes 1.37) with plain manifests and kustomize. metrics-server for the HPA, started with `--kubelet-insecure-tls`, which is for test clusters only. Liveness checks the process; readiness checks that a bundle is active and the instance is not draining. Deployments roll with `maxUnavailable: 0`, `maxSurge: 1`; on SIGTERM an instance fails readiness, waits in `preStop` for endpoint removal, drains requests, and the consumer commits its offsets. A load test at a fixed rate runs through a rollout restart and through a bundle swap; both must show zero errors. docker compose remains the one-command start and the setup for headline benchmarks
- Rationale: Kubernetes removes endpoints at the same time as it starts termination, with no ordering, so without the readiness flip and the preStop wait in-flight connections are cut. kind runs on the benchmark VM, so its figures include its control plane's CPU use; it checks behavior, not capacity
- Alternatives: minikube or k3d (equivalent for this purpose); Helm (templating not needed for one environment)

## D023 Baseline bundle contract and full history retention
- Date: 2026-11-09
- Decision: (1) The bundle contract (SPEC §8) gains `user_encoder: mean`, a zero padding row 0 in `items.npy`, optional `cooc_ids.npy`, `cooc_counts.npy` and `popular.npy`, and drops `opset` and the input and output names until an ONNX encoder is exported. Golden cases need a history, a full user vector and exactly 10 IDs, and include histories longer than `max_seq_len`. The version is a hash of every file. ALS is trained on the train split's users and items only; items seen only later get zero vectors, and a user vector that is zero counts as absent. (2) User histories keep every event: the cap of 200 is dropped. Requests read the whole history before their time for the seen filter and the last `max_seq_len` items for the user vector
- Rationale: (1) An ONNX user encoder comes from seqrec-lab later; the baseline needs the same file layout without it. With the item set of all splits in the ALS input, the test split changed every trained vector through the random initialization. (2) Evicting old events made the seen filter miss items a user had rated and made earlier request times lose events; the property tests applied the same cap to their reference, so they could not see it. The full history of Video_Games fits in memory; M3 decides retention in Valkey without weakening properties 1 and 2
- Alternatives: keep the cap and a separate unbounded seen set (two structures to keep consistent); per-user time windows (loses old seen items)

## D024 Hand-written HNSW: what follows the paper, hnswlib and faiss
- Date: 2026-11-09
- Decision: `internal/hnsw` implements Algorithms 1, 2, 4 and 5 of Malkov and Yashunin (arXiv:1603.09320). Sources read: the paper; hnswlib `hnswalg.h` at commit ca42672 (the reference implementation, by the paper's first author); faiss v1.15.1 `impl/HNSW.cpp`, `impl/HNSW.h` and `IndexHNSW.cpp` (the outside reference of R2). The choices, fixed before any measurement:
  1. Parameters as the paper recommends and hnswlib uses: level multiplier mL = 1/ln M, at most M links per node on layers 1 and up, at most Mmax0 = 2M on layer 0; queries search layer 0 with max(ef, k)
  2. A new node selects M neighbors on every layer it lives on (paper, Algorithm 1; hnswlib). faiss selects up to its layer's maximum, 2M on layer 0
  3. Neighbor selection is the heuristic of Algorithm 4, always, also when there are fewer candidates than the target; hnswlib and faiss keep every candidate unpruned in that case. This differs only on layers, or early in the build, where fewer than M nodes are reachable
  4. A candidate is rejected when an already selected neighbor is strictly closer to it than the new node is; ties keep it (hnswlib, faiss). The paper's wording would reject ties
  5. When a link overflows a node's list, the node selects again with the same heuristic from its old neighbors plus the new one, down to the maximum (paper, Algorithm 1; hnswlib). faiss v1.15.1 prunes to 80% of the maximum (`prune_headroom` 0.2)
  6. The search on the next layer down starts from all nodes found on the layer above (paper, Algorithm 1: ep ← W). hnswlib starts from the closest selected neighbor only; faiss from the one node found by the greedy descent
  7. The paper's options `extendCandidates` and `keepPrunedConnections` are off, as in hnswlib; faiss's `keep_max_size_level0` is off by default
  8. Layer search follows Algorithm 2 with hnswlib's stopping rule: stop when the nearest unexpanded candidate is farther than the farthest result and the result set is full. A filter, as hnswlib's `isIdAllowed` and faiss's `IDSelector`, keeps nodes out of the results but not out of the traversal
  9. Visited nodes are marked in a per-searcher array of epoch tags, so it is not cleared between searches (hnswlib's visited list pool)
  10. Levels are drawn up front from a seeded generator and nodes are inserted in ID order, so a single-threaded build is reproducible. The parallel build follows hnswlib: one lock per node, held while its neighbor lists are read or written during the build, and a global lock while a node above the current top level is inserted. faiss v1.15.1 builds differently: points bucketed by level and inserted in prefix-doubling batches against a frozen snapshot, reverse links merged afterwards (after ParlayANN)
  11. Similarity is the float32 inner product (`vec.Dot32`), the distance its negative; on L2-normalized vectors the order equals cosine. faiss runs `METRIC_INNER_PRODUCT`
  12. Layout without pointers (D016): vectors are the caller's float32 array, unchanged; layer-0 links are one int32 array with 1 + 2M slots per node, the first holding the count; links of layers 1 and up are one int32 array with M + 1 slots per node and layer, found through a per-node offset; levels are bytes
- Rationale: The paper fixes the algorithm and hnswlib is its reference implementation; where they differ, the index follows the paper, and only in points 3 and 6, where the paper is explicit. faiss is the outside reference of R2, so its differences (points 2, 5, 10) are recorded here before results, as possible reasons for different recall at equal settings
- Alternatives: port faiss's batch build (reproducible in parallel, but a larger graph builder and not the classic algorithm); `keepPrunedConnections` (fills lists to the maximum; off in both reference implementations)

## D025 Noise baselines come from one noisebase build
- Date: 2026-11-09
- Decision: From M2 on, the session reference and every noise baseline are taken by running the same `cmd/noisebase` binary, pinned to the CPU named; benchmark commands run it as a subprocess instead of timing the workload in their own process, and result files record the binary's SHA-256. A reference is compared with another session's only if both come from builds of the same commit and toolchain; Go builds are reproducible, so an earlier build can be recreated from its commit
- Rationale: The workload's speed depends on where the linker places its inner loop. On the M2 machine, builds with identical loop instructions measured about 480,000 or about 590,000 ns per pass: in the faster ones the 34-byte loop lies inside one 64-byte line, in the slower ones it straddles two. Two builds that differ only in an unrelated import, one of each kind, were timed alternately six times, and the straddling one was slower every time. In M0 the reference came from `experiments/m0/cmd/noise`, whose loop straddles, while the R2 command, as built from b334c91 for the coder/hnsw rerun, timed its baselines in its own process, whose loop does not; if the M0 machine behaves the same way, the R2 baselines read faster than the reference and understate the flags. The R2 verdict rests on recall and does not change
- Alternatives: the loop in assembly with fixed alignment (the same placement in every binary, but a different workload from M0 and assembly to maintain); baselines in the measured process (the placement differs between binaries)
