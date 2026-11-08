"""faiss IndexHNSWFlat reference for R2 (docs/BENCHMARKS.md), on one thread.

usage: faiss_ref.py DATA SET M EFC OUT_JSON

Builds the index, then for each k in (10, 100) and ef >= k answers the queries
one at a time after a warm pass over the first 1,000, and writes the result ids
to DATA/faiss_SET_M{M}_k{k}_ef{ef}.i32; recall is computed by the Go command
from those files, with the same definition as for the Go indexes.
"""

import json
import sys
import time

import faiss
import numpy as np

EFS = (10, 20, 40, 80, 120, 200, 400, 800)


def rss_mib():
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024
    return 0.0


def main(data, name, m, efc, out):
    faiss.omp_set_num_threads(1)
    dim = int(open(f"{data}/{name}.dim").read())
    base = np.fromfile(f"{data}/{name}_base.f32", dtype=np.float32).reshape(-1, dim)
    query = np.fromfile(f"{data}/{name}_query.f32", dtype=np.float32).reshape(-1, dim)
    index = faiss.IndexHNSWFlat(dim, m, faiss.METRIC_INNER_PRODUCT)
    index.hnsw.efConstruction = efc
    rss0 = rss_mib()
    t0 = time.perf_counter()
    index.add(base)
    build = time.perf_counter() - t0
    res = {"set": name, "M": m, "efConstruction": efc, "build_seconds": build,
           "rss_growth_mib": rss_mib() - rss0, "faiss": faiss.__version__, "points": []}
    print(f"build {build:.0f}s")
    for k in (10, 100):
        for ef in EFS:
            if ef < k:
                continue
            index.hnsw.efSearch = ef
            for q in query[:1000]:
                index.search(q[None, :], k)
            ids = np.empty((len(query), k), dtype=np.int32)
            lat = np.empty(len(query))
            start = time.perf_counter()
            for i, q in enumerate(query):
                t = time.perf_counter()
                _, got = index.search(q[None, :], k)
                lat[i] = time.perf_counter() - t
                ids[i] = got[0]
            elapsed = time.perf_counter() - start
            ids.tofile(f"{data}/faiss_{name}_M{m}_k{k}_ef{ef}.i32")
            lat.sort()
            pick = lambda p: float(lat[int(np.ceil(p * len(lat))) - 1] * 1e6)
            point = {"ef": ef, "k": k, "p50_us": pick(.5), "p99_us": pick(.99), "qps": len(query) / elapsed}
            res["points"].append(point)
            print(point)
    with open(out, "w") as f:
        json.dump(res, f, indent=2)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), sys.argv[5])
