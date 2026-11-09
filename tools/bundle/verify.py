"""Verify a bundle directory: checksums, shapes and golden cases.

usage: verify.py BUNDLE_DIR

Recomputes every golden case from the files on disk with the same rules the
service uses (user vector from the last max_seq_len history items, by mean
or by user_encoder.onnx; exact dot product; history excluded; ties by
internal ID) and exits non-zero on any mismatch.
"""

import json
import sys

import numpy as np

from build import encode, mean_vector, session, sha256, top_k

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
    if np.any(vecs[0] != 0):
        sys.exit("padding row 0 is not zero")
    for name in ("cooc_ids.npy", "popular.npy"):
        ids_ = np.load(f"{d}/{name}")
        low = 0 if name == "cooc_ids.npy" else 1
        if ids_.dtype != np.int64 or ids_.min(initial=low) < low or ids_.max(initial=0) > m["item_count"]:
            sys.exit(f"{name}: dtype or ID range")
    if (m["user_encoder"] == "onnx") != ("user_encoder.onnx" in m["sha256"]):
        sys.exit(f"user_encoder {m['user_encoder']!r} and user_encoder.onnx do not match")
    if m["user_encoder"] == "onnx":
        with open(f"{d}/user_encoder.onnx", "rb") as f:
            sess = session(f.read())
    with open(f"{d}/golden.jsonl") as f:
        cases = [json.loads(line) for line in f]
    for n, c in enumerate(cases):
        if not c["history"] or min(c["history"]) < 1 or max(c["history"]) > m["item_count"]:
            sys.exit(f"golden {n}: history IDs out of range")
        if m["user_encoder"] == "onnx":
            uv = encode(sess, c["history"], m["max_seq_len"])
        else:
            uv = mean_vector(vecs, c["history"], m["max_seq_len"])
        diff = float(np.max(np.abs(uv - np.array(c["user_emb"], dtype=np.float32))))
        if diff > TOL:
            sys.exit(f"golden {n}: user vector differs by {diff}")
        if top_k(vecs, uv, set(c["history"]), 10) != c["top10"]:
            sys.exit(f"golden {n}: top-10 differs")
    print(f"ok: {len(cases)} golden cases, {len(ids)} items, user encoder {m['user_encoder']}")


if __name__ == "__main__":
    main(sys.argv[1])
