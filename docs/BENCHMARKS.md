# Benchmarks

This file fixes how every figure in the project is measured. The protocol sections were written before any result was looked at; results are appended under Results, and a change to the protocol after results exist is recorded in DECISIONS.md with its reason.

## Environment record

Every result directory under `bench-results/` holds an `env.txt` with: CPU model, vCPU count, memory, kernel, Go version, ONNX Runtime version, GOMAXPROCS, CPU sets of each process, the commit, and the date. Unless a section says otherwise, results come from one cloud VM; such VMs share physical CPUs with other tenants, so their speed varies from run to run and the noise baseline below is part of every run.

In compose, Kafka's health check starts a JVM. Every 2 s, as compose.yaml had it until M6, an idle broker used 639 to 686 ms of CPU per second on the M6 VM (five one-minute readings); with checks 2 s apart only until it is healthy and 10 minutes apart after, 24 ms. Figures taken in compose before M6 carry the first load (D039).

The VM that ran M6's sweeps and later runs is noisier than the earlier ones: on the idle machine its reference's ten runs spread from 0.56 to 0.87 ms per pass (median 0.658 ms), three of them at 0.81 ms or more. A baseline more than 10% above the reference, 0.724 ms, flags its run, which is rerun: 52 of the 88 attempts of the headline sweep were flagged. The protocol is unchanged; a rate that misses the headline condition on flags alone is reported as such.

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

**Also reported, per rate.** Each container's CPU time over the measured window (its cgroup's counter) per request answered. The share of recommend requests whose history read or cache call ran past `-budget-history`: `history_unavailable` in `recserve_degraded_total` plus `error` in `recserve_cache_lookups_total`, over the recommend requests. In the writes scenario, the latency of `/v1/events` apart from that of `/v1/recommend`.

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

## Results

### M0 (2026-11-08 and 09)

One cloud VM: Intel Xeon @ 2.10GHz, 4 vCPU, 15 GB, kernel 6.18.44, Go 1.27.3. Session noise reference 820,984 ns per pass (`bench-results/m0/noise-reference.json`, upper median; threshold 903,082 ns); for the coder/hnsw rerun 816,650 ns (`noise-reference-2.json`, true median). Raw results are in `bench-results/m0`.

#### R1: pass, with intra-op threads = 1

| Criterion | Result |
|---|---|
| 1. Parity (max abs diff, Go vs Python) | 0 for encoder L=50, encoder L=200 and ranker; pure-Go MLP 1.8e-7 |
| 2. Concurrency 4, p99 | encoder L=50 1.02 ms (≤ 5), ranker C=500 2.91 ms (≤ 10) |
| 3. Image | builds; parity 0 inside `distroless/cc-debian13`; 91.9 MB |

Latency in µs, B = 1 (`onnx/latency.json`):

| Model | Threads | Conc. | Calls/s | p50 | p99 | max |
|---|---|---|---|---|---|---|
| encoder L=50 | 1 | 1 | 2,624 | 338 | 684 | 6,590 |
| encoder L=50 | 1 | 4 | 9,068 | 365 | 1,021 | 25,174 |
| encoder L=50 | 1 | 8 | 8,816 | 462 | 8,510 | 47,103 (noisy) |
| encoder L=50 | default | 4 | 4,310 | 406 | 8,452 | 28,488 |
| encoder L=200 | 1 | 1 | 618 | 1,521 | 2,408 | 8,883 |
| encoder L=200 | 1 | 4 | 2,165 | 1,635 | 4,546 | 14,570 |
| encoder L=200 | default | 4 | 1,279 | 1,640 | 12,433 | 26,263 |
| ranker C=500 | 1 | 1 | 3,275 | 271 | 550 | 5,818 |
| ranker C=500 | 1 | 4 | 10,503 | 288 | 2,905 | 8,881 |
| ranker C=500 | default | 4 | 7,214 | 252 | 7,864 | 24,341 |
| pure-Go MLP C=500 | | 1 | 99 | 9,627 | 15,585 | 21,809 |
| pure-Go dot C=500 | | 1 | 60,702 | 13 | 43 | 4,741 |

- With ONNX Runtime's default thread pool, concurrent callers contend for the 4 CPUs; p99 at concurrency 4 is 2.7 to 8.3 times that with one intra-op thread. Services set intra-op threads to 1 (D015).
- At concurrency 8 (more callers than CPUs) p99 rises to 8 to 13 ms with one intra-op thread and 13 to 21 ms with the default.
- The pure-Go MLP is a plain scalar loop, about 35 times slower than ONNX Runtime; it serves only as a parity reference.
- Image build: the cloud session intercepts TLS with its own CA, so the build stage ran with two extra lines trusting that CA (`onnx/image-build.txt`); the committed Dockerfile is unchanged.

#### R2: fail; the fallback is adopted

recall@100 by ef, and single-thread p99 at ef = 800 (`ann/*.json`). coder/hnsw rows are the rerun with `Ml = 1/M` under the second session reference, 816,650 ns (D017).

| Set | Index | M / efC | recall@100 at ef = 120 / 200 / 400 / 800 | p99 at ef = 800 | Build |
|---|---|---|---|---|---|
| synth | coder/hnsw v0.6.1 | 16 / 200 | 0.035 at every ef | 0.67 ms | 147 s |
| synth | coder/hnsw v0.6.1 | 32 / 400 | 0.064 at every ef | 0.53 ms | 307 s |
| synth | coder/hnsw 36cab60 | 16 / 200 | 0.792 / 0.812 / 0.821 / 0.824 | 21.3 ms | 2,929 s |
| synth | faiss | 16 / 200 | 0.996 / 1.000 / 1.000 / 1.000 | 1.34 ms | 322 s |
| synth | faiss | 32 / 400 | 0.999 / 1.000 / 1.000 / 1.000 | 2.02 ms | 976 s |
| glove | coder/hnsw v0.6.1 | 16 / 200 | 0.023 at every ef | 0.54 ms | 169 s |
| glove | coder/hnsw v0.6.1 | 32 / 400 | 0.033 at every ef | 0.66 ms | 429 s |
| glove | faiss | 16 / 200 | 0.719 / 0.778 / 0.844 / 0.896 | 3.43 ms | 666 s |
| glove | faiss | 32 / 400 | 0.801 / 0.859 / 0.920 / 0.961 | 6.59 ms | 2,159 s |
| synth | exact (Go) | | 1.000 | 87 ms | |
| glove | exact (Go) | | 1.000 | 161 ms | |

- coder/hnsw v0.6.1, the pinned version, does not reach recall@100 ≥ 0.95 at any ef, on either set, with either setting; the criterion fails. Its recall does not change with ef: its layer search keeps only k results and stops after the first expansion that does not improve the best distance, and its heap's `Max` and `PopLast` take the last array slot of a min-heap, which is not the largest element (`graph.go` `layerNode.search`, `heap/heap.go`). ef only bounds the candidate queue. Its GC criterion holds: the longest pause in any query phase is at most 0.26 ms; heap after build is 1.0 to 2.0 GiB.
- Upstream fixed both search defects on 2026-06-22 (commit 36cab60, untagged). Measured as extra information outside the criterion: on synth its recall@100 levels off at 0.824 while p99 grows to 21 ms, and its build took 49 min, so it would fail too. Its glove run was stopped by a 2-hour job limit and not repeated.
- faiss with the same settings reaches 1.000 on synth, so the data, ground truth and recall code agree; on glove its recall is lower at every ef.
- On glove, faiss reaches recall@100 ≥ 0.95 only at M = 32, ef = 800, with p99 6.59 ms. The single-thread p99 ≤ 5 ms part of the criterion is therefore not met there by the outside reference either. This is recorded as found; the criterion is unchanged and applies to the hand-written index in M2.
- Noise: six coder/hnsw points of synth M = 16 in the rerun stayed flagged after three attempts (marked noisy in the JSON); none of them affects the verdict, which rests on recall. The faiss runs are bracketed but were not rerun; the baseline before glove faiss M = 32 was 911,710 ns, 11% over the first reference, so that run is flagged. faiss is the outside reference and does not enter the criterion.
- Settings: coder/hnsw has no separate efConstruction; it builds with `EfSearch`, which was set to efConstruction during the build. The faiss latencies include Python's per-call overhead, about 6 to 11 µs. The Go heap figure is live heap after a GC; faiss's figure in its JSON is RSS growth.

### M2 (2026-11-09)

A different cloud VM from M0: Intel Xeon @ 2.30GHz with AVX-512 and AMX (M0: Xeon @ 2.10GHz), 4 vCPU, 15.7 GiB, kernel 6.18.44, Go 1.27.3 (`bench-results/m2/env.txt`). M0's noise binary ran about 28% faster here (591,451 and 593,274 ns per pass against 820,984 ns), so M0's latencies are not comparable with these; faiss and exact search were rerun here with the unchanged M0 code. Session reference 652,267 ns (`noise-reference.json`, threshold 717,494 ns), from the one noisebase build of D025; its ten runs fell into two groups, about 590,000 and 700,000 to 820,000 ns, with nothing else running.

#### Hand-written HNSW: fail, on glove latency only

recall@100 and single-thread p99 by ef (`ann/*.json`). The hand-written graphs were built with 4 goroutines (`Workers = 4`), faiss's with one thread, so build times are not comparable.

| Set | Index | M / efC | recall@100 at ef = 120 / 200 / 400 / 800 | p99 ms at ef = 120 / 200 / 400 / 800 | Build | Unreachable |
|---|---|---|---|---|---|---|
| synth | recserve/hnsw | 16 / 200 | 0.996 / 1.000 / 1.000 / 1.000 | 0.57 / 0.73 / 0.91 / 1.21 | 109 s | 89 |
| synth | faiss | 16 / 200 | 0.996 / 1.000 / 1.000 / 1.000 | 0.46 / 0.60 / 0.83 / 1.46 | 324 s (disturbed) | |
| glove | recserve/hnsw | 16 / 200 | 0.712 / 0.774 / 0.842 / 0.898 | 1.22 / 2.09 / 3.31 / 7.63 | 362 s | 24 |
| glove | faiss | 16 / 200 | 0.719 / 0.778 / 0.844 / 0.896 | 0.94 / 1.02 / 1.70 / 3.75 | 728 s | |
| glove | recserve/hnsw | 32 / 400 | 0.795 / 0.852 / 0.914 / 0.958 | 2.70 / 4.12 / 7.72 / 15.48 | 1,164 s | 3 |
| glove | faiss | 32 / 400 | 0.801 / 0.859 / 0.920 / 0.961 | 1.11 / 1.84 / 3.60 / 6.45 | 1,930 s | |
| glove | recserve/hnsw, 2 shards | 32 / 400 | 0.851 / 0.900 / 0.94997 / 0.980 | 3.76 / 5.26 / 8.61 / 15.71 | 1,110 s per shard | 2 + 1 |
| synth | exact (Go) | | 1.000 | 110.8 | | |
| glove | exact (Go) | | 1.000 | 199.4 | | |

- synth passes with M = 16, efConstruction = 200: recall@100 reaches 0.95 first at ef = 120 (0.9956), with p99 0.57 ms, and no GC cycle ran in any synth query phase. M = 32 was not run on synth.
- glove: with M = 16 recall@100 never reaches 0.95 (0.898 at ef = 800). With M = 32 it does only at ef = 800 (0.9575), with p99 15.48 ms, above 5 ms; that point was not flagged (first attempt, baselines 589,900 and 588,214 ns). The longest GC pause in any glove query phase is at most 0.20 ms. Only the latency part fails, so the criterion fails as written and no threshold changes; as the protocol states, the index stays in use with its measured p99 given wherever retrieval latency is reported. No qualifying point has a p99 between 3.6 and 5 ms, so the verdict does not depend on this CPU being faster than M0's.
- Two shards: two shard processes, each with the half of the vectors `shard.Of` assigns to it and its own graph (2 build goroutines each); the parent sends every query to both over pipes and merges their top 100, scored against the full ground truth. recall@100 is 0.94997 at ef = 400, so the lowest ef reaching 0.95 is 800, with p99 15.71 ms: on 4 vCPUs, two shards searching with the same ef as one index do not lower p99. The GC statistics of this run are the parent's only; the shard processes, which hold the graphs, were not measured. M6 measures the sharded tier again through annshard and gRPC.
- Against faiss on this machine, recall agrees within 0.007 at every point. p99 is 1.3 to 2.4 times faiss's on glove, and 0.8 to 1.2 times on synth. faiss's recall equals M0's to the digits shown, and checkgt verified the ground truth of 100 queries per set against an exact scan, so the regenerated data and ground truth match M0's. M0's faiss p99 at ef = 800, from the slower CPU: 1.34 ms (synth, M = 16), 3.43 ms and 6.59 ms (glove, M = 16 and 32).
- Unreachable nodes are those layer 0 does not reach from the entry point (`hnsw.Index.Reachable`). Heap in use after a collection, with the vectors: synth 386 MiB (244 MiB of vectors), glove 619 MiB with M = 16 and 763 MiB with M = 32 (451 MiB of vectors). These figures also hold the queries and ground truth, 6.3 MiB on synth and 7.6 MiB on glove, 2 to 5% of the heap beyond the vectors.
- Noise: of the 40 query points (12 per single-index run, 4 for two shards), 15 needed a third attempt and 8 (20%) were still flagged after it; each point's baselines and attempt are in its JSON. Builds are not repeated: both glove builds are flagged. The synth faiss run is flagged (867,327 and 830,893 ns before and after), and its build overlapped a test run pinned to CPU 3, so its build time is for reference only; the glove faiss runs are not flagged. The glove exact run is flagged before and after (790,304 and 742,262 ns, `glove-exact-noise-*.json`). The noise record inside `glove-exact.json` holds the baselines `m0ann` timed in its own process (482,413 and 749,650 ns), which D025 does not compare with the reference; the run passed `-ref 0`, which turns flagging off, so its `noisy: false` means not checked. faiss and exact search are references and do not enter the criterion.
- Noise threshold: the session reference is the median of ten runs that fell into two groups, 587,389 to 595,671 ns and 708,863 to 821,079 ns, so the threshold (717,494 ns) lies about 20% above the lower group and inside the upper one, above one of its five runs. A baseline from the lower group never flags and one from most of the upper group does: a flag mostly records which group the machine was in, not a slowdown of a few percent.

### M3 (2026-11-10)

Another cloud VM: Intel Xeon @ 2.10GHz, 4 vCPU, 15.7 GiB, kernel 6.18.44, Go 1.27.3, Valkey 9.1 and Kafka 4.4.0 in Docker over loopback (`bench-results/m3/env.txt`). Session reference 800,115 ns (`noise-reference.json`, ten runs from 775,013 to 838,451 ns); the twelve baselines of the six runs lie between 780,814 and 834,455 ns, and none is flagged. Code at 47bad71, baseline bundle 5de55036a382, 814,586 events in the histories.

#### Stage check of the Valkey path: fail, in the round that computes

Time in the stage "history read, then cache lookup", in µs, runs 1 / 2 / 3 (`stage-run*.log`). In the first round every request misses and computes; in the second, with a cache, every request hits.

| Store and cache | Round | Stage p50 | Stage p99 | History read p99 | Cache lookup p99 | Request p50 |
|---|---|---|---|---|---|---|
| process, cache off | 1 | 3 / 3 / 3 | 31 / 16 / 24 | 31 / 16 / 24 | | 2,506 / 2,432 / 2,402 |
| process, cache off | 2 | 3 / 3 / 3 | 11 / 9 / 8 | 11 / 9 / 8 | | 2,329 / 2,401 / 2,329 |
| process, cache on | 1 | 3 / 3 / 3 | 12 / 9 / 15 | 7 / 7 / 12 | 2 / 1 / 1 | 2,339 / 2,367 / 2,531 |
| process, cache on | 2 | 2 / 3 / 3 | 100 / 154 / 180 | 4 / 5 / 4 | 99 / 143 / 179 | 5 / 7 / 6 |
| Valkey, cache off | 1 | 372 / 682 / 435 | 2,762 / 5,361 / 2,817 | 2,762 / 5,361 / 2,817 | | 2,628 / 3,493 / 2,922 |
| Valkey, cache off | 2 | 489 / 400 / 379 | 5,649 / 3,996 / 2,678 | 5,649 / 3,996 / 2,678 | | 3,115 / 2,832 / 2,789 |
| Valkey, cache on | 1 | 477 / 627 / 572 | 2,902 / 4,343 / 5,796 | 2,336 / 2,973 / 3,971 | 2,145 / 3,176 / 3,878 | 3,275 / 4,184 / 4,464 |
| Valkey, cache on | 2 | 408 / 568 / 512 | 812 / 2,353 / 1,679 | 459 / 1,419 / 742 | 501 / 1,130 / 936 | 416 / 579 / 526 |

Control, 2,000 reads of a 16-byte value by the four workers: p50 157 / 211 / 186 µs, p99 306 / 745 / 450 µs.

- **Fail.** On Valkey with the cache on, the stage's p99 in the first round is 2,902, 4,343 and 5,796 µs, median 4,343 µs, above 3 ms. In the second round it is 812, 2,353 and 1,679 µs, median 1,679 µs, within it.
- The stage's p50 is 0.4 to 0.6 ms in both rounds, two round trips; the tail is in the rounds that compute. There four workers without think time keep the four CPUs busy with about 2.4 ms of computation a request, and the Valkey server and the client's goroutines wait for a CPU with them. With the cache off both rounds compute, and the history read alone has a p99 of 2.7 to 5.6 ms in both.
- The per-user history version is not built (D039). The control's p99 is 33 to 89% below the p99 of the history read in every run and round, but the control ran while nothing computed, which is not the condition of the round that fails. Under that round's load the cache lookup, a `GET` that finds nothing, has the p99 of the history read (2,145 / 3,176 / 3,878 against 2,336 / 2,973 / 3,971 µs): a small read in the history read's place would wait as long. D039 asks for the same conditions and is judged by this second reading. The version would also save the history read only on a hit, where the stage already passes.
- A hit on Valkey takes p50 0.42 to 0.58 ms against 5 to 7 µs in the process (D036), and a miss about 0.9 to 1.9 ms more at p50 than in the process: the history read, the lookup and the write.

#### Load at 200 requests/s on Valkey and Kafka: pass (M3 interim)

One rate, cache on, no writes; not the headline, which M6 measures with the sweep. From the due time, in ms (`load-200-run*.json`); the counters are the gateway's and cover the 14,000 requests of warmup and measurement (`load-200-run*-metrics.txt`).

| Run | p50 | p90 | p99 | p99.9 | max | Valid | `history_unavailable` | `shards_partial` | Cache errors | Cache hits | Baselines ns |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 4.25 | 6.22 | 11.99 | 18.66 | 24.38 | yes | 41 | 6 | 146 | 0 | 834,455 / 782,159 |
| 2 | 4.15 | 6.04 | 10.97 | 19.34 | 34.51 | yes | 44 | 5 | 110 | 0 | 805,541 / 799,375 |
| 3 | 4.18 | 6.18 | 10.94 | 16.82 | 31.85 | yes | 31 | 0 | 105 | 0 | 780,814 / 800,725 |

- **Pass.** All three runs are valid (12,000 of 12,000 requests answered 200, 200.0 requests/s sent) with p99 from 10.94 to 11.99 ms, below 50 ms. Each is a first attempt.
- Degraded responses are at most 0.34% of a run (47 of 14,000), below the 1% mark. The `history_unavailable` ones, 0.22 to 0.31%, are history reads that ran past the 3 ms budget; the circuit breaker did not open in any run (no change of state in the gateway's log). Cache errors, 0.75 to 1.04%, are lookups and writes that ran past the budget; such a request is answered in full.
- No request hit the cache: the histories hold the test events, each request is at its event's time, and 14,000 events of the split do not repeat a user with an unchanged history. In this workload the cache costs every request a read and a write and saves none; a workload with repeats is M6's cache-on scenario to define.
- Service time p99 is 11.25, 10.35 and 10.28 ms; the send lag p99 is 1.08 to 1.09 ms and the worst pause of the generator 8.4 to 12.8 ms.


### M6 (2026-11-11)

The VM of M3's description: Intel Xeon @ 2.10GHz, 4 vCPU, 15.7 GiB, kernel 6.18.44, Go 1.27.3, Docker 29.9.2 with cgroup v1 (`bench-results/m6/*/env.txt`). Session reference 698,757 ns (`bench-results/m6/noise-reference-1.json`, ten runs from 676,015 to 748,753 ns). Bundle b7ff0634dd7b, services in compose by `deploy/bench.compose.yaml`. The profiles and the optimization below ran on this VM.

The sweeps ran on another VM of the same kind: Intel Xeon @ 2.30GHz, 4 vCPU, 15.7 GiB, the same kernel, Go and Docker. Its reference is 657,992 ns (`bench-results/m6/noise-reference-2.json`, ten runs from 559,758 to 869,192 ns, on an idle machine).

The fault runs and those after them ran on a third: Intel Xeon @ 2.10GHz, 4 vCPU, 15.7 GiB, the same kernel, Go and Docker, with the images rebuilt from the code of the sweeps' last commit, whose history budget is 5 ms. Its reference is 887,645 ns (`bench-results/m6/noise-reference-3.json`, ten runs from 839,028 to 949,394 ns, on an idle machine).

#### Kafka's health check

An idle Kafka in compose used 639 to 686 ms of CPU per second in five one-minute readings: its health check started a JVM every 2 s. Checked every 2 s only until healthy and every 10 minutes after, it used 24 ms (Environment record, D039). The profile run below was first taken with the old check, unrecorded: p99 was 10.97 ms there against 6.41 ms with the new one.

#### Profiles at 400 requests/s

`make bench-profile` at d49b06e (`profile/`): replay, cache off, valid and unflagged, p99 6.41 ms. CPU per request: the four replicas 0.45 to 0.46 ms each, 1.82 ms together; the gateway 1.31 ms; Valkey 0.13 ms; Kafka 0.07 ms. In a replica's profile `vec.Dot` has 54.2% of the samples, the system calls 9.6%, the top-k heap 4.1%; in the gateway's, the system calls 13.8%, ONNX Runtime's cgo calls 11.3%, `vec.Dot` 8.6%, `vec.Dot32` 3.1%, MMR 8.8% cumulative. The replicas taken together use the most CPU per request, and `vec.Dot` leads their profile, so it is the target (D053), not `vec.Dot32`, which only the HNSW mode calls.

#### vec.Dot in AVX2: lower after on every quantity but the gateway's CPU at 400/s

`make bench-compare BEFORE=a3f48ae` (`optimization/`): before a3f48ae, after ed28e3b; BenchmarkDot on CPU 1 and the service, replay, cache off, before and after alternating, three runs each. All 18 runs are first attempts, valid, with no noise baseline flagged.

| Quantity | Before | After | Verdict |
|---|---|---|---|
| BenchmarkDot, 64 dims, ns | 47.5 to 50.6 | 10.5 to 11.4 | lower after |
| BenchmarkDot, 100 dims, ns | 74.2 to 80.2 | 14.9 to 15.9 | lower after |
| 200/s: p99, ms | 5.14 to 5.32 | 3.93 to 4.39 | lower after |
| 200/s: the replicas' CPU per request, ms | 1.94 to 1.98 | 1.23 to 1.29 | lower after |
| 200/s: the gateway's CPU per request, ms | 1.52 to 1.57 | 1.46 to 1.52 | lower after |
| 400/s: p99, ms | 6.90 to 7.79 | 4.27 to 4.64 | lower after |
| 400/s: the replicas' CPU per request, ms | 1.80 to 1.87 | 1.10 to 1.16 | lower after |
| 400/s: the gateway's CPU per request, ms | 1.29 to 1.33 | 1.26 to 1.32 | not resolved on this machine |

- The inner product takes 4.5 to 5 times less time; the replicas use about 37% less CPU per request, and p99 falls by about a quarter at 200/s and a third at 400/s.
- The after runs of BenchmarkDot ran from ed28e3b's working tree with the benchmark moved to its own file (the next commit); `vec.Dot` itself was ed28e3b's.
- Results: the tests, both bundles' golden cases (5de55036a382 checked by starting a gateway on it, b7ff0634dd7b by the after runs) and the sixteen-lane definition hold (D053). No committed file changed.

#### Load sweep, replay with the cache off: headline 550 requests/s

`make bench-sweep SCENARIO=replay CACHE=off` (`sweep-replay-cache-off/`, the table in `sweep.md`), on the second VM. The figures are this VM's and this topology's, with every service on the same three CPUs.

- **Headline, 550 requests/s** (D005 as amended by D054): p99 4.61, 4.59 and 5.07 ms in the three runs, 0.04, 0.02 and 0.07% of the responses degraded. 500/s meets the condition; 600/s does not, with one run 1.69% degraded and two flagged.
- **Under D005 as written** the grid meets it at every rate run up to 550/s, at 800/s and at 1,600, 2,000, 2,400 and 3,200/s; 600, 1,200, 2,800, 3,600 and 4,000/s miss it on flagged noise baselines alone, with p99 between 5 and 17 ms. The grid ran in full to 4,000/s and once at 4,400/s, 98.79% degraded, and was stopped there; its stop rule never acted (`GRID_MAX=4000` repeats it). From 1,200/s on p99 stays between 11 and 17 ms while the degraded share grows: 12 to 19% of the responses at 1,200/s, 55 to 69% at 1,600, 84 to 90% at 2,000, 94 to 96% at 2,400 and 98 to 99% from 3,600 on.
- Nearly every degraded response is `history_unavailable`: the history read through Valkey ran past its 3 ms budget. A request without a history has no user vector and skips vector retrieval, so the four replicas' CPU per request falls from 1.01 ms at 550/s to 0.02 ms or less from 3,200/s on, and the gateway's from 1.10 to 0.23 to 0.24 ms. At 1,200/s the degraded responses come in whole seconds: in run 1, 45 of the 60 seconds have under 10% and 7 have 70% or more. That matches the circuit breaker on the gateway's Valkey calls, which after five failures in a row leaves Valkey alone for a second (D037); the gateway exports no metric of the breaker's state that could confirm it.
- CPU per request at 550/s in the run with the median p99: the gateway 1.10 ms, the four replicas together 1.01 ms, Valkey 0.11 ms.
- **Capacity and admission limits** (D050): c = 1.098 ms (1.098 to 1.199 ms in the three runs), so a gateway on 2 CPUs is estimated at 2/c = 1,821 requests/s. `-max-rate` becomes 1,450 and `-max-inflight` 152 (`deploy/kind/gateway.yaml`). This is an estimate from one quantity, measured while the gateway shared its CPUs with the rest of the service.

#### The other five scenarios, 100 to 800 requests/s

`make bench-sweep SCENARIO=... CACHE=... RATES="100 200 400 800"` (`sweep-<scenario>-cache-<on|off>/`): the rates set with the amendment (D054). p99 is between 3.7 and 5.3 ms at every rate to 400/s in every scenario, and between 4.4 and 7.9 ms at 800/s. Where a rate misses, it is on a run with more than 1% degraded or on flagged baselines.

| Scenario | Cache hits, 100 / 200 / 400 / 800 per s | Most degraded run, 400 and 800/s | `/v1/events` p99, ms |
|---|---|---|---|
| replay, cache on | 47 / 55 / 65 / 78% | 1.67%, 3.79% | |
| writes, cache on | 42 / 49 / 58 / 69% | 1.69%, 3.41% | 2.2 to 2.9 |
| writes, cache off | | 0.03%, 5.09% | 2.1 to 3.7 |
| repeats, cache on | 58 / 63 / 71 / 80% | 1.70%, 5.03% | |
| repeats, cache off | | 0.05%, 1.77% | |

- Replaying the test split hits the cache here, where M3's runs had no hit: compose preloads only the train and valid splits, so a user's later test events have the history of the first and the same key (D030); M3's Valkey also held the test split. Repeated requests add 6 to 11 points of hit rate at each rate.
- With the cache on, the gateway's CPU per request falls with the hit rate, from 1.42 ms at 100/s to 0.62 ms at 800/s in replay.
- **History budget (D039).** In replay with the cache on, the share of requests past the 3 ms budget is at most 0.03% at 100 and 200/s, but one run of 400/s has 1.74% (0.02% in the other two), so the rule runs the headline rate, 550/s, with 5 ms and 8 ms (`budget-history-5ms/`, `budget-history-8ms/`). With 5 ms the share is 0.02, 0.00 and 0.00% in three unflagged runs, with 8 ms 0.00% in all three (two of them flagged), so SPEC §5's budget for the history read and cache lookup becomes 5 ms. The headline itself was measured with 3 ms.
- **Cross-check** (`make bench-vegeta RATE=275`, `vegeta/`): at half the headline, loadgen's p99 is 3.89, 3.92 and 4.14 ms and vegeta's 3.35, 3.26 and 3.38 ms, every one of vegeta's 16,500 requests per run answered 200. vegeta times a request from its send and loadgen from its due time, so loadgen's figure also holds any wait before the send.

#### Fault runs: F1, F2, F3 and F6 pass; F4 and F5 fail

`make bench-fault FAULT=F1 ... F6 RATE=275` (`faults/`, the table in `faults.md`), on the third VM, with the 5 ms history budget and the cache on. Every fault has three valid runs; 9 of the 27 attempts were invalid, 3 on flagged baselines and 6 with 1.73 to 3.47% of the healthy phase degraded (their files were replaced by the reruns). No valid run has an error in any phase, and every fault phase has p99 ≤ 50 ms.

| Fault | Result | Fault phase p99, ms | Degraded in the fault phase | Recovery, s | Also |
|---|---|---|---|---|---|
| F1 replica killed | pass | 8.66, 9.02, 10.14 | 0.0% | 0, 0, 0 | no failover in any fault phase; annshard-1 used 7, 5 and 28 ms of CPU in the last 60 s |
| F2 50 ms on shard 1 | pass | 21.08, 21.20, 21.19 | 35.0, 34.9, 34.5%, all `shards_partial` | 0, 0, 0 | mean coverage 0.72; shard 1's hedges 4,088 to 4,139 denied and 464 to 470 lost per run, none won |
| F3 Valkey unavailable | pass | 2.99, 2.58, 3.06 | 99.9%, `history_unavailable` | 1, 1, 1 | |
| F4 consumer paused | fail | 9.12, 10.60, 10.59 | 0.0, 0.1, 1.7% | 0, 0, 0 | lag back at 0 in 2.11, 1.44 and 1.15 s; property 9 holds in all three; run 3: `history_unavailable` 1.67 points above the healthy phase |
| F5 replica paused | fail | 18.89, 18.79, 19.12 | 2.6, 2.6, 4.4% | 0, 0, 0 | `shards_partial` in [0, 13 s) after the pause 11.75, 12.06, 12.11%; in [13, 15 s) 0.36, 0, 0%; in [15, 60 s) 0%; run 3: `history_unavailable` 2.22 points above the healthy phase in [15, 60 s) |
| F6 searches hang | pass | 20.49, 20.10, 19.58 | 11.44, 11.18, 11.51% `shards_partial` | 0, 0, 0 | mean coverage 0.91; shard 0's hedges 1,521 to 1,559 denied and 385 to 390 won per run |

- **F4 and F5 fail on one second each, in which the gateway's Valkey circuit breaker was open.** In every valid run, each second outside F3's fault in which more than 20% of the responses report `history_unavailable` starts with a change of the breaker from closed to open in the gateway's log (`*.breaker`), and the breaker closes 1.0 s later on its first trial call: 15 openings in the 18 runs, 11 of them in the first 30 s, 2 in fault phases (F4 run 3 at 112.7 s, F5 run 3 at 145.5 s) and 2 in recovery phases. The breaker opens after five failed Valkey calls in a row (D037); what made those calls fail is not established.
- **The cache serves most of the late traffic.** 59 to 67% of the lookups hit in the healthy phases, 63 to 75% in the fault phases (none in F3's) and 86 to 99% in the recovery phases, since replay repeats keys as the run goes on. Shard 0 gets 73 to 79 calls per second while healthy and 2 to 25 in the recovery phases, so the recovery times of the replica faults rest on that much search traffic, and the shares above are of all responses: in F6's fault phase, with 71% hits, about 39% of the responses that searched were partial.
- **Outlier detection (D041).** F5's share in [0, 13 s) and F6's over its fault phase are above 1%, so D041 records outlier detection as adopted, and TASKS has it; it is not built in M6.
- The healthy phases' p99 is 8.2 to 13.1 ms, against 3.9 to 4.1 ms for loadgen at the same rate in the cross-check: here toxiproxy is on every call to the replicas and Valkey, the cache is on, and the VM is another. Which of these accounts for the difference was not measured.

#### Hedge delay: 2.5 ms

`make bench-hedge RATE=275` (`hedge/`, the table in `hedge.md`), on the third VM, the cache off. With hedging off the p95 of a call is 2.12, 1.97 and 1.91 ms on shard 0 and 2.20, 2.05 and 1.99 ms on shard 1, so the default becomes 2.5 ms, the median of the larger shard's, 2.05 ms, rounded up (D041). With 2.5 ms, 1.0 to 1.3% of each shard's searches sent a hedge, fewer than the 5% the rule aims at: the rounding up puts the delay past most of the slowest 5% of calls. p99 is 5.84 to 6.56 ms with hedging off and 5.70 to 6.32 ms with 2.5 ms, ranges that overlap, so a difference in p99 is not resolved on this machine. No run is invalid or flagged.

#### Bundle swaps: a higher p99 in the swap windows, with GC on or off, and no higher maximum

`make bench-swap RATE=275` (`swap/`, the table in `swaps.md`), on the third VM with the 2.5 ms hedge delay, the cache off: nine swaps per run between b7ff0634dd7b and 5de55036a382, three runs with GC as configured and three with `GOGC=off`. No run is invalid or flagged.

| GC | Swap windows p99, ms | Swap windows max | Other windows p99 | Other windows max |
|---|---|---|---|---|
| on | 6.90, 7.50, 6.73 | 18.65, 25.58, 11.88 | 5.99, 5.19, 5.18 | 25.79, 17.25, 19.85 |
| off | 6.93, 11.33, 6.50 | 20.19, 23.39, 23.49 | 5.54, 6.34, 5.48 | 52.22, 31.16, 29.25 |

- A swap's prepare took 65 to 864 ms and its drain at most 3.0 ms. During a swap the gateway used 87 to 462 ms of CPU and the four replicas 28 to 505 ms.
- By the rule above, the median max over the swap windows with `GOGC=off`, 23.39 ms, is 0.75 times that over the other windows, 31.16 ms, which the rule reads as GC. The rule supposes that the swap windows hold the largest values. They do not, with GC on either (medians 18.65 against 19.85 ms), so there is no excess for GC to explain and this record attributes nothing to GC. The other windows' largest values, up to 52.22 ms, also occur with no GC at all.
- p99 over the swap windows is above the other windows' in every run, with GC on (6.73 to 7.50 against 5.18 to 5.99 ms) and off (6.50 to 11.33 against 5.48 to 6.34 ms): turning GC off does not remove it. The CPU readings above are the only other figures; no cause is named.
- With GC on, the swap windows hold 6 or 7 collections per run, 1.4 to 2.9 ms of pauses in all; the other windows 205 to 207, 72 to 78 ms.

#### Rollouts and a swap in kind with SPEC §5's budgets

`make bench-kind VERSION=b7ff0634dd7b` (`kind/`, the table in `kind.md`), on the third VM with the images of the 2.5 ms hedge delay: each run on a new cluster, the gateways' stage budgets at the binary's defaults, SPEC §5's, then `kind-check`'s load of 100 requests/s through the gateways' rollout, the replicas' and a swap. Reported, not judged (D022).

- No error in any phase of the three runs. At most 0.133% of a phase's responses are degraded: one `vector_timeout` while the gateways rolled in run 1, one `shards_partial` while the replicas rolled in run 2, and one of each during the swap in run 3.
- p99 by phase, over the three runs: before 3.80 to 4.71 ms, the gateways' rollout 7.68 to 9.91, the replicas' 5.87 to 6.13, the swap 7.09 to 8.84, after 5.12 to 5.23; the largest single response took 26.77 ms, during the replicas' rollout in run 2.

