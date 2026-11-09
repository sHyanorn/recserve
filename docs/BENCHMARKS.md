# Benchmarks

This file fixes how every figure in the project is measured. The protocol sections were written before any result was looked at; results are appended under Results, and a change to the protocol after results exist is recorded in DECISIONS.md with its reason.

## Environment record

Every result directory under `bench-results/` holds an `env.txt` with: CPU model, vCPU count, memory, kernel, Go version, ONNX Runtime version, GOMAXPROCS, CPU sets of each process, the commit, and the date. Unless a section says otherwise, results come from one cloud VM; such VMs share physical CPUs with other tenants, so their speed varies from run to run and the noise baseline below is part of every run.

## Noise baseline

`experiments/m0/noise` (later `cmd/noisebase`) runs a fixed single-core workload: a dot product of a 64-dimensional float32 query against 16,384 float32 vectors, repeated for 200 passes, timed 30 times; the result is the median time of one pass.

- **Reference.** At the start of a benchmark session, on an idle machine, the baseline runs 10 times; the median is the session reference.
- **Per run.** The baseline runs right before and right after each benchmark run, pinned to the CPU the measured process uses (or the first one, if it uses several).
- **Flag.** A run is flagged when either baseline is more than 10% slower than the reference. A flagged run is rerun, at most twice; if all three attempts are flagged, the last one is reported and marked "noisy".
- The baselines of every run are kept in its result file.

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

- glove-100-angular from ann-benchmarks: base vectors, 10,000 queries, ground truth. Vectors are L2-normalized, and inner product is the similarity.
- Synthetic: 1,000,000 base and 10,000 query vectors of 64 dimensions from a mixture of 1,000 Gaussians (seed 1), L2-normalized; exact ground truth computed by brute force.

**Indexes:**

- `coder/hnsw` (pinned version), M = 16 and efConstruction = 200, then M = 32 and efConstruction = 400 if the first setting does not reach the recall target. Cosine distance on normalized vectors.
- Exact search in pure Go (flat float32 array, single goroutine), as the baseline.
- faiss `IndexHNSWFlat` (faiss-cpu 1.15.1, Python, inner product), same M and efConstruction, one thread, as the outside reference.

**Measurements**, one query at a time on one thread, as ann-benchmarks does:

- ef ∈ {10, 20, 40, 80, 120, 200, 400, 800} (only ef ≥ k for each k)
- recall@10 and recall@100, with the ann-benchmarks definition: a returned item counts if its distance is at most the distance of the true k-th neighbor, so ties do not lower recall
- per-query latency p50 and p99 over the 10,000 queries, after one warm pass over 1,000 queries
- build time, heap in use after build (runtime/metrics), and for the Go indexes the GC pauses (`/sched/pauses/total/gc:seconds`) and GC CPU fraction over the query phase
- the noise baseline brackets each index and dataset

**Pass** if on both datasets coder/hnsw has an ef with recall@100 ≥ 0.95 and single-thread p99 ≤ 5 ms, and its longest GC pause during the query phase is ≤ 5 ms.

**Fallback.** Write HNSW by hand with neighbor lists in flat int32 arrays (no pointers for the GC to scan), measured with the same protocol in M2.

## Retrieval quality

- ANN recall@k against exact search, defined as in R2, for k = 10 and 100, on one node and through scatter-gather over the shards.
- Exact mode through scatter-gather must equal single-node exact search item by item (SPEC §7, property 6); this is a test, not a measurement.

## Recommendation quality

- **Split.** The Amazon 2023 Video_Games 5-core timestamp split (D013), the split seqrec-lab uses for this dataset where they coincide.
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
- The headline (D005) is the highest rate at which all three runs are valid and unflagged with p99 ≤ 50 ms.

**Cross-check.** One rate is also driven by vegeta at the same constant rate, and the two p99 values are reported side by side.

## Results

### M0 (2026-11-08 and 09)

One cloud VM: Intel Xeon @ 2.10GHz, 4 vCPU, 15 GB, kernel 6.18.44, Go 1.27.3. Session noise reference 820,984 ns per pass (`bench-results/m0/noise-reference.json`); the flag threshold is 903,082 ns. Raw results are in `bench-results/m0`.

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

recall@100 and the highest single-thread p99 over ef ∈ {120, 200, 400, 800} (`ann/*.json`):

| Set | Index | M / efC | recall@100 at ef = 120 / 200 / 400 / 800 | p99 at ef = 800 | Build |
|---|---|---|---|---|---|
| synth | coder/hnsw | 16 / 200 | 0.061 at every ef | 0.92 ms | 249 s |
| synth | coder/hnsw | 32 / 400 | 0.112 at every ef | 0.92 ms | 495 s |
| synth | faiss | 16 / 200 | 0.996 / 1.000 / 1.000 / 1.000 | 1.34 ms | 322 s |
| synth | faiss | 32 / 400 | 0.999 / 1.000 / 1.000 / 1.000 | 2.02 ms | 976 s |
| glove | coder/hnsw | 16 / 200 | 0.032 at every ef | 0.89 ms | 347 s |
| glove | coder/hnsw | 32 / 400 | 0.053 at every ef | 1.12 ms | 759 s |
| glove | faiss | 16 / 200 | 0.719 / 0.778 / 0.844 / 0.896 | 3.43 ms | 666 s |
| glove | faiss | 32 / 400 | 0.801 / 0.859 / 0.920 / 0.961 | 6.59 ms | 2,159 s |
| synth | exact (Go) | | 1.000 | p99 87 ms | |
| glove | exact (Go) | | 1.000 | p99 161 ms | |

- coder/hnsw v0.6.1 does not reach recall@100 ≥ 0.95 at any ef, on either set, with either setting; the criterion fails. Its recall does not change with ef because its layer search keeps only k results and stops after the first expansion that does not improve the best distance; ef only bounds the candidate queue (`graph.go`, `layerNode.search`). Its GC criterion holds: the longest pause in any query phase is at most 0.33 ms, and heap after build is 1.4 to 2.6 GiB.
- faiss with the same settings reaches 1.000 on synth, so the data, ground truth and recall code agree;; on glove its recall is lower at every ef.
- On glove, faiss reaches recall@100 ≥ 0.95 only at M = 32, ef = 800, with p99 6.59 ms. The single-thread p99 ≤ 5 ms part of the criterion is therefore not met there by the outside reference either. This is recorded as found; the criterion is unchanged and applies to the hand-written index in M2.
- Noise: three coder/hnsw points on glove M = 32 stayed flagged after three attempts (marked noisy in the JSON). The faiss runs are bracketed but not rerun; the baseline before glove faiss M = 32 was 911,710 ns, 11% over the reference, so that run is flagged. faiss is the outside reference and does not enter the criterion.
- Settings: `Ml = 1/ln M`. coder/hnsw has no separate efConstruction; it builds with `EfSearch`, which was set to efConstruction during the build.
