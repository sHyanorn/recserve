"""Prepare the R2 datasets (docs/BENCHMARKS.md) as raw little-endian files.

usage: prep.py GLOVE_HDF5 OUT

For each set NAME in (glove, synth) writes NAME.dim, NAME_base.f32,
NAME_query.f32 (L2-normalized float32 rows) and NAME_gt.i32 (the exact top-100
by inner product, from faiss IndexFlatIP, best first).

synth: 1,000,000 base and 10,000 query vectors of 64 dimensions. Each vector is
a center drawn from 1,000 unit-normalized standard normal centers, plus
N(0, 0.1^2) noise per dimension, then L2-normalized. Seed 1.
"""

import sys

import faiss
import h5py
import numpy as np


def normalize(x):
    x = np.ascontiguousarray(x, dtype=np.float32)
    faiss.normalize_L2(x)
    return x


def ground_truth(base, query, k=100):
    index = faiss.IndexFlatIP(base.shape[1])
    index.add(base)
    _, ids = index.search(query, k)
    return ids.astype(np.int32)


def write(out, name, base, query, gt):
    with open(f"{out}/{name}.dim", "w") as f:
        f.write(f"{base.shape[1]}\n")
    base.tofile(f"{out}/{name}_base.f32")
    query.tofile(f"{out}/{name}_query.f32")
    gt.tofile(f"{out}/{name}_gt.i32")
    print(f"{name}: base {base.shape}, query {query.shape}")


def synth():
    rng = np.random.default_rng(1)
    centers = normalize(rng.standard_normal((1000, 64), dtype=np.float32))

    def draw(n):
        x = centers[rng.integers(0, 1000, size=n)] + 0.1 * rng.standard_normal((n, 64), dtype=np.float32)
        return normalize(x)

    return draw(1_000_000), draw(10_000)


def main(glove_path, out):
    with h5py.File(glove_path, "r") as f:
        base, query, hdf_gt = normalize(f["train"][:]), normalize(f["test"][:]), f["neighbors"][:]
    gt = ground_truth(base, query)
    overlap = np.mean([len(set(a) & set(b)) / 100 for a, b in zip(gt, hdf_gt)])
    print(f"glove: overlap of faiss top-100 with the file's neighbors {overlap:.4f}")
    write(out, "glove", base, query, gt)
    base, query = synth()
    write(out, "synth", base, query, ground_truth(base, query))


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
