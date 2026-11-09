"""Verify a bundle directory: checksums, shapes and golden cases.

usage: verify.py BUNDLE_DIR

Recomputes every golden case from the files on disk with the same rules the
service uses (mean user vector, exact dot product, history excluded, ties by
internal ID) and exits non-zero on any mismatch.
"""

import json
import sys

import numpy as np

from build import mean_vector, sha256, top_k

TOL = 1e-4


def main(d):
    with open(f"{d}/manifest.json") as f:
        m = json.load(f)
    for name, want in m["sha256"].items():
        if sha256(f"{d}/{name}") != want:
            sys.exit(f"{name}: checksum mismatch")
    with open(f"{d}/items.ids") as f:
        ids = f.read().splitlines()
    vecs = np.load(f"{d}/items.npy")
    if len(ids) != m["item_count"] or vecs.shape != (m["item_count"] + 1, m["dim"]) or vecs.dtype != np.float32:
        sys.exit(f"shape mismatch: {len(ids)} ids, vectors {vecs.shape} {vecs.dtype}")
    with open(f"{d}/golden.jsonl") as f:
        cases = [json.loads(line) for line in f]
    for n, c in enumerate(cases):
        uv = mean_vector(vecs, c["history"])
        diff = float(np.max(np.abs(uv - np.array(c["user_emb"], dtype=np.float32))))
        if diff > TOL:
            sys.exit(f"golden {n}: user vector differs by {diff}")
        if top_k(vecs, uv, set(c["history"]), 10) != c["top10"]:
            sys.exit(f"golden {n}: top-10 differs")
    print(f"ok: {len(cases)} golden cases, {len(ids)} items")


if __name__ == "__main__":
    main(sys.argv[1])
