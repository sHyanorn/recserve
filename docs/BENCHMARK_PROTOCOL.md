# Benchmark protocol

How every figure in the project is measured. It was written before the runs; an amendment made after some results, such as D054's, is dated in place and its reason recorded in DECISIONS.md.

The results are in [BENCHMARKS.md](BENCHMARKS.md): where this file names "Results" with a milestone, look there. Defects found after the runs are in [BENCHMARK_ERRATA.md](BENCHMARK_ERRATA.md): where this file names a defect, look there.

## Environment record

Every result directory under `bench-results/` holds an `env.txt` with: CPU model, vCPU count, memory, kernel, Go version, ONNX Runtime version, GOMAXPROCS, CPU sets of each process, the commit, and the date. Unless a section says otherwise, results come from one cloud VM; such VMs share physical CPUs with other tenants, so their speed varies from run to run and the noise baseline below is part of every run.

## Noise baseline

`experiments/m0/cmd/noise` (later `cmd/noisebase`) runs a fixed single-core workload: a dot product of a 64-dimensional float32 query against 16,384 float32 vectors, repeated for 200 passes, timed 30 times; the result is the median time of one pass.

- **One build.** From M2 on, the reference and every baseline of a session are taken by the same `cmd/noisebase` binary, run as its own process; result files record its SHA-256. A reference is compared with another session's only if both come from builds of the same commit with the same toolchain (D025).
- **Reference.** At the start of a benchmark session, on an idle machine, the baseline runs 10 times; the median is the session reference.
- **Per run.** The baseline runs right before and right after each benchmark run, pinned to the CPU the measured process uses (or the first one, if it uses several).
- **Flag.** A run is flagged when either baseline is more than 10% slower than the reference. A flagged run is rerun, at most twice; if all three attempts are flagged, the last one is reported and marked "noisy".
- The baselines of the reported attempt and its attempt number are kept in its result file.

## M0 risk checks

The checks run on the benchmark VM. Each has a pass criterion set here; if it fails, the named fallback is adopted. Criteria are not changed after results exist.

### R1: ONNX Runtime from Go

**Models** (`experiments/m0/onnx/export.py`, PyTorch, random weights with a fixed seed; latency does not depend on the weights):

- User encoder, shaped like SASRec: item embedding table of 140,000 × 64 (above the 137.2k items of the full Video_Games catalog), learned positions, 2 Transformer encoder layers (2 heads, feed-forward 256, causal mask), the last position's output as the user vector. Input `item_seq` int64 [B, L], output `user_emb` float32 [B, 64]. Exported for L = 50 and L = 200.
- Ranker: inputs `user` float32 [B, 64], `items` float32 [B, C, 64], `feats` float32 [B, C, 8]; each candidate's concatenation (136 values) goes through an MLP 136 → 128 → 64 → 1; output `scores` float32 [B, C].
- Opset 17, dynamic B, L and C.

**Measurements** (`experiments/m0/onnx`, Go with onnxruntime_go v1.38.0 and ONNX Runtime 1.31.1):

- **Parity.** 100 seeded random inputs per model; the maximum absolute difference between Go and Python (onnxruntime 1.31.1) outputs.
- **Latency.** B = 1. Encoder at L = 50 and L = 200; ranker at C = 500. Concurrency 1, 2, 4 and 8: that many goroutines share one session and call it in a loop for 20 s after a 5 s warmup; every call's duration is recorded, percentiles are exact. Each configuration runs twice: with ONNX Runtime's intra-op threads at 1, and at its default. The process is limited to 4 CPUs; the noise baseline brackets each configuration.
- **Pure-Go reference.** The same ranker MLP and a plain dot-product scorer written in Go with the exported weights, measured the same way.
- **Image.** A multi-stage Dockerfile builds the Go program with cgo and runs it on `gcr.io/distroless/cc-debian13:nonroot` with the ONNX Runtime library copied in; it must pass the parity check inside the container.

**Pass** if all hold for at least one thread setting:

1. Parity: maximum absolute difference ≤ 1e-5 for both models
2. At concurrency 4: encoder (L = 50) p99 ≤ 5 ms and ranker (C = 500) p99 ≤ 10 ms
3. The image builds and passes parity

**Fallback.** If 1 or 3 fails: run models in a separate inference process. If only 2 fails for the ranker: rank with the pure-Go MLP and keep ONNX for the encoder; if it fails for the encoder: compute user vectors on event ingestion instead of per request, and measure that path in M2.

### R2: pure-Go HNSW

**Data** (downloaded by script, never committed):

- glove-100-angular from ann-benchmarks: base vectors and 10,000 queries. Vectors are L2-normalized, and inner product is the similarity. Ground truth is recomputed by brute force (faiss `IndexFlatIP`); its overlap with the file's neighbors is printed by `prep.py`.
- Synthetic: 1,000,000 base and 10,000 query vectors of 64 dimensions from a mixture of 1,000 Gaussians (seed 1), L2-normalized; exact ground truth computed by brute force.

**Indexes:**

- `coder/hnsw` (pinned version), M = 16 and efConstruction = 200, then M = 32 and efConstruction = 400 if the first setting does not reach the recall target. Cosine distance on normalized vectors.
- Exact search in pure Go (flat float32 array, single goroutine), as the baseline.
- faiss `IndexHNSWFlat` (faiss-cpu 1.15.1, Python, inner product), same M and efConstruction, one thread, as the outside reference.

**Measurements**, one query at a time on one thread, as ann-benchmarks does:

- ef ∈ {10, 20, 40, 80, 120, 200, 400, 800} (only ef ≥ k for each k)
- recall@10 and recall@100, defined as in ann-benchmarks: a returned item counts if it is at least as similar as the true k-th neighbor, so ties do not lower recall. The tolerance is 1e-5 on inner product (ann-benchmarks uses 1e-3 on distance)
- per-query latency p50 and p99 over the 10,000 queries, after one warm pass over 1,000 queries
- build time, heap in use after build (runtime/metrics), and for the Go indexes the GC pauses (`/sched/pauses/total/gc:seconds`) and GC cycles over the query phase (D017)
- the noise baseline brackets each index and dataset

**Pass** if on both datasets coder/hnsw has an ef with recall@100 ≥ 0.95 and single-thread p99 ≤ 5 ms, and its longest GC pause during the query phase is ≤ 5 ms.

**Fallback.** Write HNSW by hand with neighbor lists in flat int32 arrays (no pointers for the GC to scan), measured with the same protocol in M2.

### Hand-written HNSW (M2, D016)

R2 failed, so M2 writes HNSW by hand. It is measured with the R2 protocol above: same data, ground truth, ef values, recall definition, single-thread latency, GC statistics, noise baseline, and the same exact and faiss baselines.

**Pass** if on both datasets there is an ef, with M = 16 and efConstruction = 200 or with M = 32 and efConstruction = 400, at which recall@100 ≥ 0.95 and single-thread p99 ≤ 5 ms, and the longest GC pause during the query phase is ≤ 5 ms. This is the R2 criterion unchanged.

**Known before measuring.** In M0, faiss itself does not meet the latency part on glove: it reaches recall@100 ≥ 0.95 only at M = 32, ef = 800 (0.961), with p99 6.59 ms (Results, R2). The hand-written index may fail the same way.

**If only the glove latency part fails** (synthetic passes, and on glove recall@100 ≥ 0.95 is reached but only with p99 > 5 ms), the criterion is reported as failed and no threshold is changed. The report then gives, for glove:

- the recall@100 and p99 curve over all ef values, beside faiss's curve
- p99 at the lowest ef reaching recall@100 ≥ 0.95, through scatter-gather over two shards (two annshard processes, each with half the base vectors and its own index, built with the same settings; the merged top-100 is scored against the full ground truth)

The index is still used in later milestones, with its measured p99 stated wherever retrieval latency is reported. **If any other part fails**, retrieval at 1M-scale stays on exact search, the shard tier serves exact search, and the hand-written index is reported as a negative result.

## Retrieval quality

- ANN recall@k against exact search, defined as in R2, for k = 10 and 100, on one node and through scatter-gather over the shards.
- Exact mode through scatter-gather must equal single-node exact search item by item (SPEC §7, property 6); this is a test, not a measurement.

## Recommendation quality

- **Split.** The Amazon 2023 Video_Games 5-core timestamp split (D013). seqrec-lab splits the same data its own way, so an exported bundle is evaluated only if its `train_cutoff_ts` is before the first test event; the loader checks this.
- **Requests.** One request per test event: the user, k = 20, and `ts` = the event's timestamp, with the user's history before `ts` loaded.
- **Metrics.** Recall@10, Recall@20 and NDCG@10 of the held-out item over the full catalog, no negative sampling. Items in the user's history are excluded from every candidate list, the reference's included.
- **Conditions.**
  - reference: exact dot-product scoring of all items offline
  - full pipeline, healthy
  - each degradation forced on its own through a test hook: vector retrieval skipped (encoder timeout), co-occurrence skipped, ranker skipped (retrieval order), one of two shards missing, HNSW instead of exact search
- **Reporting.** Each condition's metrics and its difference from the healthy pipeline, with a paired bootstrap 95% interval over test events (1,000 resamples, seed 1).
- **Amended 2026-11-12, before the first run.** The test hook is the pipeline's `stall` field (`internal/recommend`), which is unexported, nil in production and set only by the package's tests; TestRealDataDegradation already uses it. The evaluation is `TestRealDataQuality`, which runs the pipeline in process on the real data and bundle (`RECSERVE_DATA`, `RECSERVE_BUNDLE`), with the test split's events preloaded and every stage budget off, so nothing degrades by time. The encoder, co-occurrence and the ranker are forced to fail at their stage, and shard 1 of two at its search; HNSW is the shards' mode, with the gateway's default ef. The reference's user vector is the bundle's encoder on the same history; a request without one has an empty reference. A condition is valid if no response of the healthy pipeline is degraded, and in a forced condition every response reports its reason exactly when the hook forced the stage in that request, and no other reason; HNSW reports none. Results in `bench-results/m7/quality.md`, first on bundle b7ff0634dd7b. The intervals are percentile intervals of the mean difference.

### seqrec-lab's bundle against the baseline (M7, D056)

Set 2026-11-12, before any seqrec-lab bundle exists.

- **Bundles.** The baseline is b7ff0634dd7b. The other is the bundle exported from the seqrec-lab run that D056 point 2 picks; its `model_version` is known only after the export and is given with the results. It is evaluated only after seqrec-lab has committed its Video_Games test results (D056 point 3).
- **What is compared.** Each bundle's reference row of Recommendation quality: its encoder's user vector from the request's history, exact dot-product scoring of all of its items, the history excluded, an empty list for a request without a user vector. The seqrec-lab bundle has no co-occurrence, popularity or ranker, so the pipeline rows are not compared between the bundles; its healthy pipeline row is reported, described only. seqrec-lab's own test figures, on its own split, are not compared with these.
- **Primary result, the only one.** On all 43,249 test events, the seqrec-lab bundle's reference minus the baseline's, for Recall@10, Recall@20 and NDCG@10, each with a paired bootstrap 95% interval over test events (1,000 resamples, seed 1). A difference whose interval excludes 0 is reported as a difference, otherwise as none shown; there is no pass or fail. A target outside a bundle's items is a miss.
- **Secondary readings**, reported with their event counts and intervals, not used to qualify the primary result: the events whose target is in the seqrec-lab bundle's items (41.8% of the test events are in its vocabulary of 21,930); those whose target has a non-zero vector in the baseline; and the events in both.
- **Also reported.** The number of requests without a user vector under each bundle. The training periods: the baseline's item vectors come from recserve's train split, to 2021-08-11, its ranker from the valid split; the seqrec-lab model's weights from events at or before T_val = 1618224866975 (2021-04-12), early stopping on validation targets in (T_val, T] and its vocabulary from the 5-core of events at or before T = 1649219168790 (2022-04-06), all of the 0-core file deduplicated by (user, item) (seqrec-lab `data/splits.py`, `global_temporal`; `data/preprocess.py`, `build_log`). The histories: for each test event, the length of recserve's history and the number of the same user's events before it in seqrec-lab's data (0-core, deduplicated, in its vocabulary), with the median of each and the share of events where they differ; and the repeated items in recserve's histories, 0 in its three split files.
- **Histories, 2026-11-12**, computed before any seqrec-lab bundle existed: `tools/bundle/histories.py` on recserve's split files and the 0-core `Video_Games.csv.gz` (sha256 30692f9806e33a7b5d8b599b73d7ca33dbfefb0abdfc1578052a101f2e64b3cd, as in seqrec-lab's registry), following seqrec-lab's code at 6aa5d87; its output is bench-results/m7/histories.txt. It gives seqrec-lab's figures: T = 1649219168790, T_val = 1618224866975, a vocabulary of 21,930 items, and 18,092 of the 43,249 test targets in it (41.83%). On both sides an event's history holds the events with an earlier ts. recserve's history: median 5 (p10 2, p90 17, max 427). The same user's events in seqrec-lab's data, deduplicated and in its vocabulary: median 3 (p10 0, p90 12, max 386). They differ in 25,950 events (60.00%), each with fewer in seqrec-lab's data. recserve's histories hold no repeated item.

### Freshness (M8, D061)

Registered before any of its runs in [registration/m8-freshness.md](registration/m8-freshness.md), which fixes the bundles, the stream state, how the choices on valid are made, the conditions, the criteria and the counted test reads; amended in [registration/m8-freshness-amendments.md](registration/m8-freshness-amendments.md). `make bench-read` runs a read, TestRealDataFreshness. The state's cost in serving is measured under [Stream state in serving](#stream-state-in-serving-m8-d062), committed before its runs (the registration, section 1).

## Load tests

**Generator.** `cmd/loadgen` (D009): request i is due at t0 + i/rate, sent over a fixed pool of keep-alive connections whatever the replies; latency counts from the due time. Service time and send lag are kept apart. A 1 ms sleep loop records the generator's own pauses.

**Workload.** The test events of the split in time order, one `/v1/recommend` request per event as in Recommendation quality, cycled when exhausted. A second scenario adds `/v1/events` writes as 10% of requests. Each scenario runs with the cache on and off; the hit rate is reported.

**Run.**

- Fresh services per run, loaded from the same bundle.
- 10 s warmup, then 60 s measured.
- loadgen and the services on disjoint CPUs: loadgen on CPU 0, services on CPUs 1 to 3.
- Noise baseline before and after.

**Valid run.**

- At least 99% of the target requests are sent within the window.
- No request fails (HTTP 5xx or a transport error). 503 from overload protection counts as a failure here.
- Every response carries an arm and versions.

The share of degraded responses is reported per run, and runs above 1% are marked.

**Sweep.**

- Rates 100, 200, 400, 800 requests/s, then steps of 400.
- Each rate runs three times; the run with the median p99 represents it.
- The sweep stops at the first rate with an invalid run, or whose median p99 exceeds 100 ms.

**Reported.**

- Per rate: p50, p90, p99, p99.9 and max; the p99 of each run; service CPU per request; degraded share; cache hit rate; worst hiccup; noise flags.
- The headline (D005) is the highest rate at which all three runs are valid and unflagged with p99 ≤ 50 ms; amended in M6, also with at most 1% of each run's responses degraded (D054, Amendment below).

**Cross-check.** One rate is also driven by vegeta at the same constant rate, and the two p99 values are reported side by side.

### Additions for M6 (D052)

Fixed before the first M6 run.

**Setup.** `deploy/bench.compose.yaml` over `deploy/compose.yaml`: the gateway, two shards of two annshard replicas, Valkey, Kafka and ingest, on the bundle with every stage, b7ff0634dd7b (`make bundle ENCODER=onnx RANKER=1`). The containers run on CPUs 1 to 3 (`cpuset`); loadgen runs on CPU 0 and sends to the gateway's address on the compose network, not through the published port; the noise baseline runs on CPU 1. Tracing is off (the OTLP variables are empty), the exposure log is off, and Prometheus, Grafana and Jaeger do not run: the scripts read the services' `/metrics` themselves. The gateway has compose's flags, `-cache-size 0` when the cache is off, and no admission limits. Before each run Valkey is emptied, and ingest, the replicas and the gateway are recreated; the gateway preloads the train and valid splits, as in compose, and the run starts once it is ready.

**Scenarios.** Three workloads, each with the cache on and off:

- replay: the test split in time order, as above;
- writes: replay with 10% of the requests writing their event to `/v1/events` instead;
- repeats: replay in which each request, with probability 0.3, is replaced by a copy of one of the 1 to 1,000 requests before it, drawn uniformly with a fixed seed. Without writes a copy has the same history, so the same cache key. 0.3 is a chosen value; no traffic it could be taken from exists.

The headline sweep is replay with the cache off. The other five scenarios run the rates of its grid, without the bisection, and stop by the same rule.

**Bisection.** After the grid of the headline sweep, the rate halfway between the highest grid rate that meets the headline condition and the next grid rate, rounded to a multiple of 50, runs three times, and the half that holds the boundary is kept, until the two rates are 50 apart. The headline is the lower one. If 100 requests/s does not meet the condition, 50 runs as well and the result says so.

**Also reported, per rate.** Each container's CPU time over the measured window (its cgroup's counter) per recommendation answered, `/v1/events` requests not counted, so in the writes scenario about 1.11 times the time per request answered (2026-11-13: this read "per request answered" while `cmd/benchsum` divided, as it still does, by the recommendations answered; the tables now name it per recommendation; no figure of the writes scenario is cited in this document). The share of recommend requests whose history read or cache call ran past `-budget-history`: `history_unavailable` in `recserve_degraded_total` plus `error` in `recserve_cache_lookups_total`, over the recommend requests. In the writes scenario, the latency of `/v1/events` apart from that of `/v1/recommend`.

**Capacity and admission limits (D050).** c is the gateway's CPU time per request at the headline rate. A gateway's capacity with the 2 CPUs of its kind limit is estimated as 2/c requests/s: an estimate from one measured quantity, since here the gateway shares three CPUs with the rest of the service. `-max-rate` in kind becomes 80% of it, rounded down to a multiple of 50, and `-max-inflight` 2 × `-max-rate` × 50 ms, rounded up to a multiple of 8.

**History budget (D039).** In the replay sweep with the cache on, if the share past the budget is at most 1% at every rate up to the headline, the 3 ms budget stays, the per-user history version is not built and D039 is closed. Otherwise the headline rate runs three more times with `-budget-history` 5 ms, then 8 ms, and SPEC §5 takes the smallest that brings the share to at most 1% in all three runs; if neither does, the budget is recorded as not met at the headline and stays.

**Cross-check.** vegeta drives half the headline rate, three runs.

**Amendment, 2026-11-11 (D054).** Made after the grid of the headline sweep had run from 100 to 4,400 requests/s, and before its bisection.

- The headline condition also requires that at most 1% of each run's responses are degraded. 1% is the share this section already marks and the history budget rule above uses; it is not chosen anew.
- Reason: a degraded response skips work. A request whose history read runs past its budget has no user vector and no vector retrieval, so it costs little. In the grid the degraded share rose from at most 1.93% of a run at 800 requests/s to 12 to 19% at 1,200/s and 94% at 2,400/s, while p99 stayed between 11 and 17 ms: p99 ≤ 50 ms held where almost no response was complete, and the condition as written did not find the highest rate that meets the service target.
- The grid of the headline sweep ran in full to 4,000 requests/s and once at 4,400/s, 98.79% degraded, and was stopped there; its stop rule was not reached. Its bisection runs as above, from the highest grid rate that meets the amended condition. Every grid rate is also reported with its verdict under D005 as written.
- Every rule that takes the headline takes the amended one, with its ratios unchanged: capacity and admission limits, the history budget, the cross-check, and the fault runs, hedge delay and swaps below.
- The other five scenarios run 100, 200, 400 and 800 requests/s, set with this amendment and before any of their runs, stopping earlier by the stop rule.
- Amended 2026-11-11, later the same day (D054): a rule here ended every grid at its first rate that missed the amended condition, with the bisection below it; it changed the stop rule and the bisection, which the amendment was not to change, and is withdrawn. Nothing that was run changes, and the headline stays 550 requests/s.

## Valkey path (M3, D039)

Two checks of the gateway with its histories and result cache on Valkey, fixed here before either was run. The second is an interim figure for M3: the sweep and the headline stay with M6.

**Stage check.** `TestRealDataValkey` in `internal/recommend`, on the baseline bundle and the 2,000 requests of the M2 real-data tests (every 20th test event in time order, k = 50, the request time the event's), with the events of all three splits as histories.

- Four workers share the requests; each request is sent once per round, in two rounds, so that with a cache the first round misses and the second hits.
- Four configurations: history store and cache in the process, or both on the compose Valkey over loopback through the gateway's circuit breaker; each with the cache on and off. No budgets, so a slow read is measured and not cut off.
- Per request: the latency, and the time in the stage "history read, then cache lookup": the sum of the two calls, timed around the store and the cache, which leaves out the digest of the key between them.
- Control, for the Valkey configurations: the four workers send 2,000 `GET`s of one 16-byte value, timed the same way.
- The test runs three times, each in a new process, with a noise baseline before and after on CPU 0; the processes are not pinned, since the workers use all four CPUs. All three runs are reported; the median of their p99 values counts.
- **Pass** if, on Valkey with the cache on, the stage's p99 is at most 3 ms (SPEC §5) in the first round and in the second.
- If it fails: a per-user history version in the cache key (D036 point 6) is taken up only if the control's p99 is at least 30% below the p99 of the history read alone in the same run. Otherwise the budget is recorded as not met and the decision is left open.

**Load at 200 requests/s.** One rate of Load tests, by its Run and Valid run rules: a gateway started fresh for each run with `-valkey-addr`, `-kafka-brokers` and its default flags (cache on, two exact shards, the budgets of SPEC §5) on the baseline bundle; loadgen with its defaults (k = 20, 64 connections) replaying the test split; 10 s warmup and 60 s measured; loadgen on CPU 0, the gateway and the Valkey and Kafka containers on CPUs 1 to 3; a noise baseline on CPU 1 before and after, a flagged run rerun as Noise baseline says.

- Valkey holds the histories of the three splits, loaded once before the first run; its result cache entries are deleted before each run. Kafka runs and receives nothing: the scenario has no writes.
- Arms come with M5, so "every response carries an arm and versions" is not checked; loadgen's own checks stand for a valid run (99% of the target sent, no failed request).
- Per run: p50, p90, p99, p99.9 and max from the due time, the degraded share by reason and the cache hit rate from the gateway's `/metrics`, the noise baselines.
- **Pass** if all three runs are valid and each has p99 ≤ 50 ms.

## Fault injection under load

Faults (D021), each in its own run: one shard replica killed; 50 ms of added latency on every connection to one shard; Valkey unavailable; the ingest consumer paused, with `/v1/events` writes at 10% of requests; and, from D041, a replica that goes silent with its connection up and one that answers keepalive pings while its searches hang.

**Run.** One rate: half the headline rate of the cache-off sweep. 30 s warmup, 60 s healthy, 60 s with the fault, then the fault is removed and the run continues for 120 s. Three runs per fault. Noise baseline before and after.

**Reported per run**, for each of the three phases and as a time series in 1 s windows:

- p50, p99 and max latency from the due time
- error rate (HTTP 5xx and transport errors) and 503 rate
- share of degraded responses, by reason, and mean shard coverage
- recovery time: from fault removal until the 10 s rolling p99 is within 1.2 times the healthy phase's p99 and the degraded share is within 1 percentage point of the healthy phase's
- consumer pause only: consumer lag and the time to drain it after resuming; and whether every user history after the run equals the one built from all produced events (property 9)

**Pass criteria** are added to this section and committed before the first fault run (M6). They are not changed after results exist.

### Faults, setup and pass criteria (M6)

Committed before the first fault run. A criterion found not to measure what it states is amended with its date and reason, and the runs it judges are repeated.

**Setup.** As in Load tests (M6), with toxiproxy 2.13 between the gateway and each replica and between the gateway and Valkey (D021), in every phase, so the healthy phase is the comparison. Kafka is not proxied. Faults are set through toxiproxy's HTTP API and `docker`. The gateway has compose's flags, with the cache on. Shard 0 is annshard-0 and annshard-1, shard 1 annshard-2 and annshard-3. The workload is replay, except for F4.

| Fault | Injected | Removed |
|---|---|---|
| F1 a replica killed | `docker kill annshard-1` | `docker start` |
| F2 latency on one shard | toxiproxy `latency` of 50 ms on the replies of annshard-2 and annshard-3 | toxic deleted |
| F3 Valkey unavailable | the gateway's proxy to Valkey disabled: open connections closed, new ones refused | enabled |
| F4 consumer paused | `docker pause ingest`, with the writes scenario | `docker unpause` |
| F5 a replica silent, its connection up | `docker pause annshard-1` | `docker unpause` |
| F6 a replica that answers keepalive pings while its searches hang | annshard-1 runs `cmd/faultshard` for the whole run; `SIGUSR1` makes every search wait until its caller cancels it | `SIGUSR2` |

**Phases.** From the first request's due time: warmup [0, 30 s), healthy [30, 90 s), fault [90, 150 s), recovery [150, 270 s). Errors are transport errors and HTTP 5xx, 503 included, on either endpoint. The gateway's `/metrics` is read at the four phase boundaries, giving each phase's hedges by result and failovers.

**Valid run.** In the healthy phase: no error, p99 ≤ 50 ms, at most 1% degraded; at least 99% of the target requests sent; neither noise baseline flagged. An invalid run is rerun, at most twice, and is neither a pass nor a fail.

**Recovery time.** The first second t after removal at which the window [t, t + 10 s) has a p99 within 1.2 times the healthy phase's and a degraded share within 1 percentage point of it, measured from removal; "not recovered" if none starts within the recovery phase.

**A reason's excess** is its share of the fault phase's responses minus its share of the healthy phase's.

**Pass criteria.** A fault passes if all three of its valid runs pass. In every run: no error in any phase, and p99 ≤ 50 ms in the fault phase. Further:

- F1: every reason's excess at most 1 percentage point (failover answers the searches that the killed replica held); recovery within 10 s; annshard-1 uses CPU in the last 60 s of the run (its cgroup's counter), so searches reach it again.
- F2: every reason other than `shards_partial` has an excess of at most 1 percentage point, so the other shard still answers; recovery within 5 s.
- F3: every reason other than `history_unavailable` has an excess of at most 1 percentage point; recovery within 5 s.
- F4: p99 ≤ 50 ms in every phase; every reason's excess at most 1 percentage point; the consumer's lag (events the gateway accepted minus records ingest applied, read every second) back at 0 within 30 s of removal; after the run, every user's history in Valkey equals the one built from the preloaded events and the events the gateway accepted (property 9).
- F5: in [15, 60 s) after the pause, every reason's excess at most 1 percentage point, the keepalive having taken the replica out; recovery within 10 s; annshard-1 uses CPU in the last 60 s.
- F6: recovery within 5 s. Its degraded share is reported, not judged: D041 Limits point 2 expects about 40% of the responses with `shards_partial` for as long as the fault lasts.

**Also reported.** F5's share of responses with `shards_partial` in [0, 13 s), [13, 15 s) and [15, 60 s) after the pause; F6's over the fault phase.

**Outlier detection (D041).** If F5's share in [0, 13 s) or F6's over the fault phase exceeds 1%, D041 records outlier detection as adopted, with a design (a replica taken out after a number of searches in a row end at the shard deadline, and asked again after a wait), and TASKS gets an item for it; it is not built in M6. Otherwise D041 records it as not needed at this rate.

### Faults, amended for M7

Committed 2026-11-12, before the first M7 fault run. M6's setup, criteria and results above stay as they are. M7's runs are not compared with them, nor with runs on any other VM.

**Why the cache is off.** The result cache is on in production (`-cache-size` 13,000). These runs turn it off so that every response searches the shards and the criteria can tell recovery apart: with it on, M6's recovery phases had 86 to 99% hits and 2 to 25 calls per second to shard 0. They answer how the retrieval path behaves under each fault; how the service behaves with the cache on is M6's result.

**Setup.** As in M6, with these changes:

- the gateway runs with the cache off (`CACHE_SIZE=0`), and with the defaults of the commit named with the results (hedge delay 2.5 ms, history budget 5 ms);
- from the first request's due time, the gateway's `/metrics` and each container's CPU time (its cgroup's `cpuacct.usage`) are read every second, each read with its time. The gateway's give the calls of each replica by result (`recserve_shard_calls_total`), the Valkey calls by result and the breaker's state (D037);
- the results go to `bench-results/m7/faults`, and those of the runs with outlier detection to `bench-results/m7/faults-outlier`.

**A replica's share** in an interval is its answered calls over all answered calls to its shard's replicas in that interval, taken from the reads at the interval's ends.

**Changes to M6's criteria.**

1. F1, F5 and F6: "annshard-1 uses CPU in the last 60 s" is replaced by: annshard-1's share in [210, 270 s) is at least 40%. At 275 requests/s shard 0 answers about 16,500 calls in those 60 s, and taking the ready replicas in turn gives annshard-1 about half, with a binomial standard deviation of about 0.4 points. 40% is far below that, and far above a replica that is not back (0%) and one back for only the last half of the interval (about 25%). Health checks and keepalive pings are not calls, so a replica that answers only them has a share of 0.
2. Recovery is judged as in M6, on all responses, which now all search. A 10 s window holds about 2,750 responses: its p99 is the 28th largest, and 1 percentage point is about 28 responses, while the healthy share's standard deviation over a window is about 0.1 point. F1, F5 and F6 also need the replica back: the first whole second t after removal such that annshard-1's share is at least 30% in each second of [t, t + 10 s). In a second shard 0 answers about 275 calls and annshard-1 about 137, with a standard deviation of about 8; 30% is about 83, six standard deviations below, and a second in which the replica answers for half of it is at about 25%, so its return is resolved to about a second. A fault's recovery time is the later of the two, within M6's limits.
3. M6's defect 3 is fixed: "no error in any phase" and F4's p99 are judged from 0 s, and the recovery window's degraded share must be within 1 percentage point of the healthy phase's either way.
4. F6 without outlier detection: its `shards_partial` share over the fault phase is reported, not judged, as in M6.

**Outlier detection (D041)**, judged in the runs with it, of F2, F5 and F6: F5's `shards_partial` share in [0, 13 s) after the pause, and F6's over the fault phase, exceed the healthy phase's by at most 1 percentage point; no replica is taken out in any healthy phase (`recserve_shard_ejections_total` the same at 30 and 90 s); F2 passes its criteria. F5's [0, 13 s) holds about 3,575 responses, so 1 point is about 36. Without outlier detection about 40% of them are expected to be partial, since half of shard 0's first calls go to the paused replica and the hedge budget covers about a fifth of those; with it, about 8, until five calls have ended at the deadline. F6's fault phase holds about 16,500 responses, 1 point about 165. 1% is the share by which M6 adopted outlier detection.

**Runs.** All six faults, three valid runs each, on one VM, at the commit before outlier detection; then F2, F5 and F6, three each, at the commit that adds it. F1, F3 and F4 are not run with it: it does not touch Valkey or ingest, and a killed replica's calls fail at once while its connection is not ready.

### Valkey circuit breaker openings (M7)

Committed before these runs. In M6, 15 of the 18 valid fault runs had the breaker open for 1.0 s, and F4 and F5 failed on such a second; what made five Valkey calls in a row fail was not established (Results, M6).

**Runs.** The M7 fault setup with the cache on, as in M6 (`CACHE_SIZE` unset), and no fault injected (`FAULT=none`): 5 runs of 270 s at 275 requests/s, results in `bench-results/m7/breaker`. From M6's 15 openings in 18 runs, 3 to 5 openings are expected; the expectation plays no part in the attribution, and if none opens no run is added.

**Attribution**, for each change of the breaker from closed to open in the gateway's log, by the Valkey calls that failed (`recserve_valkey_calls_total`) in the interval between reads that holds the change and the one before it:

- at least 5 with `timeout` and none with `error`: calls that ran out of their budget;
- at least 5 with `error` and none with `timeout`: errors, which the gateway's log of those seconds names;
- otherwise not attributed.

Reported for the same interval, not as causes: the gateway's GC pause time (`go_gc_duration_seconds_sum`) and each container's CPU time, beside the median interval of the run's [30, 90 s).

### Second M7 set: breaker on a failure rate, cover, benchsum as written

Committed 2026-11-12, before these runs. The first M7 set (Results, M7) stays as it is; the two sets come from different VMs and are not compared side by side, each only against the criteria.

**What changed since the first set.** The breaker opens on a failure rate in a window of 1 s, no longer on 5 failures in a row (D037, revision of M7). A replica is taken out only if the other answered since its run of calls ended at the deadline began (D041, M7 point 7). `scripts/bench-fault.sh` keeps each attempt's files under its own prefix, `FAULT-runN-aK`, and `cmd/benchsum` gives each run's valid attempt, every attempt with why it was invalid and its breaker openings, and each fault's verdict by the rule; the outlier detection of 03bb156 is in the build, so every fault runs with it.

**benchsum as the criteria are written.** The criteria and the validity rule are not changed; benchsum's readings of them are, each with a test:

1. The phases are cut at 90 and 150 s from the first request's due time, not at the script's marks a few ms later, and the times "after the pause" and "after removal" count from 90 and 150 s. F5's [15, 60 s) after the pause so ends at 150 s.
2. A recovery window, and annshard-1's return, may start at any whole second of the recovery phase, [150, 270 s): "not recovered" if none starts within it. A window that runs past 270 s holds the responses due before 270 s, and the seconds of the return's window before 270 s.
3. Both recoveries count from 150 s, on the same whole seconds.
4. An invalid run is neither a pass nor a fail: it is not judged.
5. The breaker table's medians take the intervals between reads that lie within [30, 90 s).

**Expectation.** With no fault, 0 openings of the breaker in the 5 runs: at 275 requests/s with the cache on its window holds about 550 to 825 calls, and half of them must fail. The expectation plays no part in the attribution, which is as in Valkey circuit breaker openings (M7), nor in validity. In the fault runs, with the cache off, the window holds about 275 history reads.

**F4's baseline.** Not changed: each F4 attempt still recreates Kafka before its first baseline. A measurement on the VM of these changes (one-off, not committed) did not confirm that Kafka's start raises the baseline. It planned three rounds of three baselines: right after Kafka's recreation and the services' (`fresh`), as F4 takes it; with Kafka not recreated, as the other faults take it; and 60 s after the recreation. Against that VM's reference of 520,455 ns, right after: 478,575, 480,579 and 701,455 ns, the last flagged; not recreated: 490,804 and 481,444; after 60 s: 569,584 and 667,428, the last flagged. The VM restarted before the last two. Kafka's CPU was about 100 ms a second in the 15 s after its recreation and 20 to 39 ms a second in each 15 s from then to 90 s. What flagged the first set's F4 attempts is still not established.

**Runs**, all on one VM, with images and binaries built from the commit that records this section or a later one that changes no code, in this order:

1. `make bench-ref REF_FILE=bench-results/m7/rerun/noise-reference.json`;
2. `OUT=bench-results/m7/rerun/breaker make bench-fault FAULT=none RATE=275 REF_FILE=bench-results/m7/rerun/noise-reference.json`, 5 runs;
3. `OUT=bench-results/m7/rerun/faults make bench-fault FAULT=F1 ... F6 RATE=275 REF_FILE=bench-results/m7/rerun/noise-reference.json`, three valid runs each, judged by Faults, setup and pass criteria (M6) as amended for M7, and F2, F5 and F6 also by its criteria of outlier detection.

No run is added after the results. The quality evaluation is not run again: `TestRealDataQuality` uses neither the breaker nor the replicas' client, which its package links only for other tests, so neither change reaches it.

## Hedge delay (M6, D041)

The gateway's `recserve_shard_search_seconds{shard}` observes every call to a replica, from its send until it returns, whatever the result. Three runs as in Load tests (M6): replay, cache off, half the headline rate, `-hedge-delay 0`, so that no call is cancelled by a hedge that won. Each run's p95 per shard comes from the histogram by linear interpolation within a bucket, as Prometheus's `histogram_quantile`. The default becomes the median over the runs of the larger shard's p95, rounded up to 0.5 ms: on healthy replicas about 5% of the searches then send a hedge, half of what the budget allows. Three more runs with that default report hedges per search and p99.

## Bundle swaps under load (M6, D031)

As in Load tests (M6): replay, cache off, half the headline rate, 10 s warmup, 150 s measured. Every 15 s from 15 s on, the admin API swaps to the other of b7ff0634dd7b and 5de55036a382, which have the same items, so nine swaps. The gateway runs with `GODEBUG=gctrace=1`.

- Per run: the p99 and max over the 1 s windows that overlap a swap (from the admin request until its answer) and over the other windows; prepare and drain times from the swaps' answers; GC cycles and pauses in each group, from the trace; the CPU time of the gateway and of the replicas during each swap.
- Three runs as is, three with `GOGC=off`.
- If with `GOGC=off` the median over runs of the max over swap windows is at most 1.2 times the median of the max over the other windows, the largest values are attributed to GC. Otherwise the record says that turning GC off does not remove them, gives the CPU readings, and names no other cause.

## Rollouts with SPEC §5's budgets (M6, D047)

`make kind-check`'s rollouts and swap with the gateways' stage budgets at SPEC §5's values instead of 100 ms, three runs, each on a new cluster. Reported per 1 s window and per phase (before the first rollout, the gateways' rollout, the replicas', the swap, after): p50, p99 and max, failures, and the degraded share by reason. No pass or fail: the control plane shares the CPUs (D022).

## Profiles and an optimization (M6)

**Profiles.** As in Load tests (M6): replay, cache off, 400 requests/s; 30 s CPU profiles of the gateway (`/debug/pprof/profile` on the admin listener) and of annshard-0 (`-debug-addr`), with each container's CPU per request; and annbench's query on glove, M = 16, ef = 400, with a CPU profile. The top functions by flat and cumulative share are kept as text.

**Target.** The function with the largest flat share in the process kind, gateway or replicas taken together, that uses the most CPU per request. If it is not `vec.Dot32`, the profile's conclusion is reported before any change.

**Before and after**, on the same benchmarks: the function's `go test -bench`; annbench on glove, M = 16, one graph file, ef 120, 200, 400 and 800; the service at 200 and 400 requests/s, replay, cache off. Runs alternate before and after, three of each, with noise baselines. A difference is reported only if the three runs after are all better than the three before; otherwise as not resolved on this machine, with both ranges.

**Unchanged results.** The optimized code returns what the code before it returns, or, where it changes the order of floating-point operations, what the definition it records in DECISIONS states, checked by tests; and on the same graph file, recall@100 before and after differs by at most 0.001 at each ef.

## ANN in M6

After the optimization, annbench as in M2 (single thread, recall@100 and p99 at ef 120, 200, 400 and 800): synth with M = 16, glove with M = 16 and 32, glove with M = 32 on two shards; faiss on the same machine for the same sets and settings. Graphs are built once with 4 goroutines; the shards mode records each graph's own M and efConstruction, read from its file. The sharded tier through annshard is measured on the real bundle by the hedge-delay runs, not on glove (D052).

## Kubernetes rollout check

On kind (D022), loadgen sends a fixed rate of 100 requests/s for 5 minutes while, after 60 s, `kubectl rollout restart` runs on the gateway, and, after 180 s, a bundle swap is triggered. **Pass** if no request fails (HTTP 5xx, 503 or transport error) and every response names a single bundle version. Latency is reported but is not part of the check, since the control plane shares the CPUs.

## Stream state in serving (M8, D062)

Committed 2026-11-15, before any of its runs, as the registration's section 1 requires. It measures what the gateway spends in CPU, memory and latency to keep the stream state (D062), and sets no pass line. Rates count recommend requests per second throughout. All runs of a phase are built from one commit, which each run records; if a failure of the gate is fixed after this section is committed, the gate runs again on the new commit and every later run uses it. Results go to `bench-results/m8/serving/`.

**Boots.** A boot is one start of the machine that runs Docker and loadgen, told apart by `/proc/sys/kernel/random/boot_id`, which each run's files record. On each boot, before its first run, the scripts take a noise reference on the idle machine (Noise baseline) into `bench-results/m8/serving/noise-reference-<the boot ID's first 8 characters>.json`, check the bundles against the three versions this section names, stopping everything if one differs, and add the boot's CPU, memory, kernel, reference and image IDs to the environment record.

**Groups.** off: Video_Games' B1, a8d9c526fb03, which names no window, so the gateway keeps no state. on: Video_Games' B1s8, 74ad6c83bf3c, window 14 days, so the gateway reads the events topic and keeps the state. Both are built from `make data` by `make bundle ENCODER=onnx RANKER=1`, B1s8 with `STREAM=14 RECENCY=1`. They have the same `items.ids`, `items.npy` and `user_encoder.onnx`, so with the same histories the replicas search the same vectors for each request in both groups.

**Setup.** As in Load tests (M6): `deploy/bench.compose.yaml`; the gateway, two shards of two replicas, Valkey, Kafka and ingest on CPUs 1 to 3; loadgen on CPU 0, sending to the gateway's address on the compose network; the noise baseline on CPU 1; tracing and the exposure log off; Prometheus, Grafana and Jaeger not running; the cache off (`-cache-size 0`); no admission limits. Before each run, once ingest has applied every event the previous run's gateway accepted, that is once ingest's `recserve_ingest_records_total` reaches the gateway's 202 answers to `/v1/events`, one event each, Valkey is emptied and ingest, the replicas and the gateway are recreated; the gateway preloads the train and valid splits, and the run starts once it answers `/readyz` with 200. Kafka keeps its topic from run to run: ingest reads on from its group's committed offsets and the gateway from the topic's end, so a run's histories and state hold the preload and that run's events.

**Workload: the stream scenario.** The Video_Games test events, numbered in time order from 0; for each, its `/v1/recommend` request, then a `/v1/events` request that writes it: request 2j, counted from the first warmup request, asks for event j's recommendations and request 2j + 1 writes event j. Loadgen sends twice as many requests as the rate. A run of 10 s warmup and 60 s measured uses 70 times the rate in events, so above 617 recommend requests/s the 43,249 test events run out; they start again, and on the k-th pass after the first every time moves on by k times the span from the first test event to the last plus 1 ms, so that every write is a new event. Both groups run this workload. The evaluation gave the state every event (the registration, section 5); replay writes nothing, so its window would hold no event past the test period's first 14 days, and the writes scenario writes 10% of the events.

**Runs.** As in Load tests: 10 s warmup and 60 s measured, with a noise baseline before and after each run against the reference of its boot. Only the noise baseline flags a run (Noise baseline, Flag): a flagged run runs again at once, at most twice, the last attempt counts, and one still flagged is noisy. A group's three runs at a rate are the three attempts that count. A run is valid as in Load tests (Valid run), plus: at least 99% of the window's requests, 2 × 60 × the rate, are sent within it; no request due in the window fails, of either kind; every recommendation names the group's bundle version; and no service container restarts during the run. Its share of degraded recommendations, over those due in the window, is marked above 1% and does not make it run again.

**Phase 1.** 100, 200, 400 and 800 recommend requests/s, in that order; at each rate three runs of each group, in the order off, on, on, off, off, on. Every rate runs, whatever the rates before it gave.

**Phase 2.** Each group's headline, as in Load tests and its M6 additions with D054: the highest rate before the group's stop at which each of its three runs is valid and unflagged, with its recommendations' p99 ≤ 50 ms and at most 1% of them degraded; the rates that meet it need not be consecutive. Phase 1's four rates are the first rates of both grids only if all four ran on the boot phase 2 starts on; otherwise all four run again. The grid goes on in steps of 400 up to 4,000, the last rate M6's grid ran in full (D054); while both groups run, each rate's six runs keep phase 1's order, and once one group has stopped the other runs its three alone. A group's grid stops at its first rate where one of its own runs is invalid or the median of its three p99s is above 100 ms, and that rate does not count. The last rate, 4,000, runs and can be the headline; if it meets the condition, the headline is at least 4,000 and there is no bisection. Otherwise, once its grid has stopped, each group bisects as in M6 (Bisection), between its highest grid rate that meets the condition and the next grid rate, with three runs of its own at each rate; when both groups bisect, off goes first. If no grid rate meets the condition, 50 runs as Bisection says, and a group that meets it at neither 100 nor 50 has no headline.

**Correctness gate.** Before any run above, on the on group: compose as above, on a new topic (`make down` first). The requests of the 100 earliest valid events go first, not compared, and no event is written. Then the gateway answers the requests of test events 0 to 999, sent one at a time, with the items, in order, that the pipeline in process gives over the same bundle, histories and state (`fresh.Events` over the train and valid splits, shards in process, every stage budget off), and none of its responses is degraded. Then test events 0 to 1,999 are written through `/v1/events` one at a time; once the gateway has applied all 2,000 with no record left to read and ingest has applied them, the requests of test events 2,000 to 2,999 are compared the same way, the process's histories and state holding the 2,000 as well. Then the admin API swaps to B1 and back to B1s8, whose state is built anew from the events kept, and the requests of test events 3,000 to 3,999 are compared again. A failure is a bug; after its fix the gate runs again, and no run of this section starts before it passes. The gate measures nothing.

**Reported**, for each run:

- the recommendations' p50, p90, p99, p99.9 and max, and their degraded share, over the recommendations due in the window; the p99 of `/v1/events`;
- each container's CPU time over the window per recommendation answered: its cgroup's counter read when the window starts and after loadgen has stopped, over the recommendations due in the window and answered, degraded ones included, as for M6 (Additions for M6);
- per shard, its two replicas' CPU time together over the same span per call the gateway made to the shard, hedges included (`recserve_shard_search_seconds`), and the second replicas asked per recommendation answered (`recserve_shard_hedges_total`, `won` and `lost`);
- the gateway's resident memory and Go heap in use (`process_resident_memory_bytes` and `go_memstats_heap_inuse_bytes` on its `/metrics`) just before loadgen starts, after the noise baseline, and when the window ends; and its peak resident memory (`VmHWM` of its process) after loadgen has stopped;
- the time from its container's start to its log line that it serves;
- in the on group, the events applied, duplicate and poison from the window's start until loadgen has stopped (`recserve_stream_events_total`), and the records left to read when the window ends (`recserve_stream_lag_records`).

A value that could not be taken is reported as missing, never as 0. Per rate, both groups' runs side by side.

**Readings.** No pass line. Readings 1 to 4 use phase 1's four rates. A median is over a group's three counted runs at a rate, invalid and noisy ones included and marked. A reading that needs a missing value is missing. Differences are on minus off, and p99 is the recommendations'.

1. Per rate, the gateway's CPU per recommendation, the median over the on runs over the median over the off runs, and the same for Kafka, whose every partition the on gateway reads; and the median p99 of the on runs minus that of the off runs; every run listed.
2. At a rate where each of the three on runs is valid and unflagged, with its p99 ≤ 50 ms and at most 1% of its recommendations degraded, the gateway keeps the state at that rate on this setup. At another rate, the reading says the rate does not show it, and which run misses which condition.
3. The replicas get the same searches in both groups while few requests degrade, hedges aside (D041). At a rate where every run of both groups has at most 1% of its recommendations degraded, for each shard, if its replicas' CPU per call over the three on runs and over the three off runs form ranges that do not overlap, the rate is marked as a measurement anomaly; ranges that touch overlap. At another rate the reading is not judged.
4. Memory: at each rate, the median over the on runs of the resident memory and of the heap in use just before loadgen starts, minus the median over the off runs.
5. Phase 2: each group's headline, and the on group's minus the off group's. A group without a headline is reported as such, and the difference is then missing.

**Within one boot.** No figure here is compared with M6's or M7's: those come from the VMs their sections record, from b7ff0634dd7b with a mean encoder and 64 dimensions, from scenarios that write none or 10% of the events, and from a gateway that read no topic. Every reading uses runs of one boot.

**Restarts.** If the machine restarts, which stops the scripts and the services:

- A run cut off, without its summary, runs again from its start.
- Phase 1: a rate is complete when its six counted runs have summaries. If the machine restarts within a rate, that rate's runs from the earlier boot move to `bench-results/m8/serving/phase1/set-aside/<the boot ID's first 8 characters>/`, are reported apart, and the rate runs again in full; complete rates stand.
- Phase 2 runs on one boot. If the machine restarts within it, its own runs so far move to `bench-results/m8/serving/phase2/set-aside/<the boot ID's first 8 characters>/` and it starts again; phase 1's runs and readings stay.
- The gate, the swap run or a profile run that a restart cuts off starts again in full; each benchmark's ten counts come from one boot.

**Also measured.**

- Micro-benchmarks, without compose, on one core (`taskset -c 1`, `-cpu 1`), ten counts each, the median and range reported. On the train split's events as the gateway keeps them, with B1s8's items, each of the valid split's 34,510 events taken one at a time in time order: the time to take an event (kept, paired and added to the state, D062 point 2) and the heap it adds, after GC. At each valid event's time: a popular list read that misses the kept list, the co-occurrence rows of its request's seeds (its 10 latest history items), and the window count and first event of its item.
- The pipeline in process, every stage budget off, shards in process, histories and state from the train and valid splits: the time per request, on one core, ten counts, of the requests of valid events 0, 10, 20 and so on, numbered in time order from 0, on B1 without the state and on B1s8 with it.
- Swaps: one run of the on group at 200 recommend requests/s, 10 s warmup and 150 s measured. At 40, 80 and 120 s after the first measured request the admin API swaps to B1s6 (d6d2691322ca, built with `STREAM=14` without `RECENCY`; same items, so the state is shared), then to B1 (no window: the state is dropped, the events kept), then back to B1s8 (its state built from the events kept). Reported for each swap: prepare and drain times from the admin answer, and the gateway's resident memory, heap in use and records left to read just before its admin request and just after its answer; for the run, the peak resident memory, and the recommendations' p99 and max over the 1 s windows that overlap a swap, from its admin request to its answer, pooled, and over the others, as in Bundle swaps under load (M6).
- Profiles: one run of each group at 400 recommend requests/s, with a 30 s CPU profile of the gateway (`/debug/pprof/profile` on its admin listener) from 20 s after the first measured request; the top functions by flat and cumulative share are kept as text, with the run's gateway CPU per recommendation. These runs are outside phase 1's tables.

**Order.** The gate, the micro-benchmarks and the pipeline in process, phase 1, the swap run, the profiles, phase 2.
