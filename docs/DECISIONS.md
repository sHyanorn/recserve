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
- Rationale: Only vector search grows with the catalog and benefits from sharding. Features and co-occurrence for these datasets fit in memory, and a separate feature or ranking service would add a network hop to every request without showing anything new. Vespa splits stateless containers from stateful content nodes the same way; x-algorithm runs its whole pipeline in one service
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
- Rationale: coder/hnsw is the only maintained pure-Go HNSW found. faiss and hnswlib would need cgo bindings that are stale (go-faiss, last release 2021) or do not exist (hnswlib); USearch's Go binding has no release tag and needs a separately installed C library
- Alternatives: cgo to faiss (a second native library to ship)

## D008 ANN performance on 1M-scale vectors; end to end on real data
- Date: 2026-11-08
- Decision: ANN and shard benchmarks use glove-100-angular from ann-benchmarks (1,183,514 vectors, 100 dimensions, ground truth included) and seeded synthetic vectors (1M × 64, Gaussian mixture). End-to-end benchmarks and quality evaluation use the real catalogs. glove-100-angular is built by ann-benchmarks (MIT) from Stanford's `glove.twitter.27B.100d` vectors, which the GloVe README releases under the ODC Public Domain Dedication and License (PDDL)
- Rationale: The real catalogs have at most 137.2k items, too few for ANN to matter (SPEC §3). glove-100 has published results to compare with; synthetic vectors scale freely. Neither is committed
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
- Rationale: MovieLens allows research use only and forbids redistribution without permission. Steam (UCSD) and Amazon Reviews 2023 state no data license
- Alternatives: a MovieLens demo (redistribution of derived titles and vectors is close to what its terms forbid)

## D012 Images on distroless/cc, pinned by digest
- Date: 2026-11-08
- Decision: Binaries that use cgo run on `gcr.io/distroless/cc-debian13:nonroot`, which has glibc; the ONNX Runtime library is copied in. Base images are pinned by digest
- Rationale: onnxruntime_go needs cgo and a glibc-linked libonnxruntime.so; distroless/static has no libc
- Alternatives: a Debian slim base (a shell and more packages to patch)

## D013 Development data: Amazon Reviews 2023 Video_Games
- Date: 2026-11-08
- Decision: M1 onward develops and benchmarks on Amazon Reviews 2023 Video_Games, the 5-core timestamp split published by the dataset authors (`benchmark/5core/timestamp/`), with item metadata from `meta_Video_Games`. MovieLens and Steam are optional and run only on a local machine
- Rationale: From the cloud sessions files.grouplens.org could not be reached, and cseweb.ucsd.edu (Steam) sends an incomplete certificate chain that standard clients reject; mcauleylab.ucsd.edu serves the Amazon files. Video_Games is one of seqrec-lab's datasets, and a published split lets both projects use the same boundaries
- Alternatives: KuaiRec (licensed for a demo, but not among seqrec-lab's datasets); Steam (not reachable from the cloud sessions)

## D014 Every benchmark run is bracketed by a noise baseline
- Date: 2026-11-08
- Decision: Before and after each benchmark run a fixed single-core workload is timed (docs/BENCHMARKS.md, Noise baseline). A run whose baseline is more than 10% slower than the session reference is flagged and rerun, at most twice; a run still flagged is reported as such
- Rationale: Cloud VMs share CPUs with other tenants; matching-engine saw stalls of up to 128 ms with no engine code running. A baseline measured around each run separates a slower machine from a slower service
- Alternatives: hiccup meter only (catches stalls, not a slower CPU share)
