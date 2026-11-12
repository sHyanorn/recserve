"""Verify a bundle directory: checksums, version, shapes, the ranker and golden cases.

usage: verify.py BUNDLE_DIR

Recomputes every golden case from the files on disk with the same rules the
service uses (user vector from the last max_seq_len history items, by mean
or by user_encoder.onnx; exact dot product; history excluded; ties by
internal ID) and exits non-zero on any mismatch. A ranker must declare the
service's ranker_features and pass its load check (build.check_ranker), and
then every golden case must give the features of its candidates, which are
recomputed from the case's history and user vector (build.features).
"""

import json
import os
import sys

import numpy as np

from build import (FEATURE_TOL, RANKER_FEATURES, check_ranker, encode, features, mean_vector, session, sha256,
                   source_scores, top_k, version_of)

TOL = 1e-4
# Read only when the manifest lists them; one on disk that it does not list
# rejects the bundle, as in the service.
OPTIONAL = ("user_encoder.onnx", "ranker.onnx", "cooc_ids.npy", "cooc_counts.npy", "popular.npy")


def reject(name):
    sys.exit(f"golden.jsonl: {name} is not a number")


def main(d):
    with open(f"{d}/manifest.json") as f:
        m = json.load(f)
    for name, want in m["sha256"].items():
        if sha256(f"{d}/{name}") != want:
            sys.exit(f"{name}: checksum mismatch")
    if m["model_version"] != version_of(m["sha256"]):
        sys.exit(f"model_version {m['model_version']!r}, but the checksums give {version_of(m['sha256'])!r}")
    cutoff = m.get("train_cutoff_ts")
    if type(cutoff) is not int or cutoff <= 0:
        sys.exit(f"train_cutoff_ts {cutoff!r}: a time in milliseconds is required")
    for name in OPTIONAL:
        if name not in m["sha256"] and os.path.exists(f"{d}/{name}"):
            sys.exit(f"{name} is in the directory but not in the manifest")
    with open(f"{d}/items.ids") as f:
        ids = f.read().splitlines()
    vecs = np.load(f"{d}/items.npy")
    if len(ids) != m["item_count"] or vecs.shape != (m["item_count"] + 1, m["dim"]) or vecs.dtype != np.float32:
        sys.exit(f"shape mismatch: {len(ids)} ids, vectors {vecs.shape} {vecs.dtype}")
    if np.any(vecs[0] != 0):
        sys.exit("padding row 0 is not zero")
    for name in ("cooc_ids.npy", "popular.npy"):
        if name not in m["sha256"]:
            continue
        ids_ = np.load(f"{d}/{name}")
        low = 0 if name == "cooc_ids.npy" else 1
        if ids_.dtype != np.int64 or ids_.min(initial=low) < low or ids_.max(initial=0) > m["item_count"]:
            sys.exit(f"{name}: dtype or ID range")
    if m["user_encoder"] not in ("mean", "onnx"):
        sys.exit(f"unsupported user_encoder {m['user_encoder']!r}")
    if (m["user_encoder"] == "onnx") != ("user_encoder.onnx" in m["sha256"]):
        sys.exit(f"user_encoder {m['user_encoder']!r} and user_encoder.onnx do not match")
    if m["user_encoder"] == "onnx":
        with open(f"{d}/user_encoder.onnx", "rb") as f:
            sess = session(f.read())
        # The golden cases use few IDs; the highest one shows that the encoder
        # knows every item. A call takes at most max_seq_len items.
        probe, L = [1, m["item_count"]], m["max_seq_len"]
        for i in range(0, len(probe), L):
            try:
                encode(sess, probe[i : i + L], L)
            except Exception as e:  # ONNX Runtime raises its own error types
                sys.exit(f"user_encoder.onnx: items {probe[i : i + L]}: {e}")
    if "ranker.onnx" in m["sha256"]:
        if m.get("ranker_features") != RANKER_FEATURES:
            sys.exit(f"ranker_features {m.get('ranker_features')}, the service computes {RANKER_FEATURES}")
        with open(f"{d}/ranker.onnx", "rb") as f:
            if msg := check_ranker(session(f.read()), len(RANKER_FEATURES)):
                sys.exit(f"ranker.onnx: {msg}")
    elif "ranker_features" in m:
        sys.exit("ranker_features without ranker.onnx")
    with open(f"{d}/golden.jsonl") as f:
        cases = [json.loads(line, parse_constant=reject) for line in f]
    cooc_ids = np.load(f"{d}/cooc_ids.npy") if "cooc_ids.npy" in m["sha256"] else np.zeros((len(vecs), 1), np.int64)
    popular = np.load(f"{d}/popular.npy") if "popular.npy" in m["sha256"] else np.zeros(0, np.int64)
    index = {raw: i + 1 for i, raw in enumerate(ids)}
    for n, c in enumerate(cases):
        if not c["history"] or min(c["history"]) < 1 or max(c["history"]) > m["item_count"]:
            sys.exit(f"golden {n}: history IDs out of range")
        # Raw IDs, if given, check items.ids, which the internal IDs cannot.
        if c.get("history_raw") is not None and [index.get(raw) for raw in c["history_raw"]] != c["history"]:
            sys.exit(f"golden {n}: history_raw does not map to history through items.ids")
        if m["user_encoder"] == "onnx":
            uv = encode(sess, c["history"], m["max_seq_len"])
        else:
            uv = mean_vector(vecs, c["history"], m["max_seq_len"])
        if not np.isfinite(uv).all() or not np.any(uv):
            sys.exit(f"golden {n}: the user vector is not finite, or zero, which counts as none")
        diff = float(np.max(np.abs(uv - np.array(c["user_emb"], dtype=np.float32))))
        if not diff <= TOL:
            sys.exit(f"golden {n}: user vector differs by {diff}")
        if top_k(vecs, uv, set(c["history"]), 10) != c["top10"]:
            sys.exit(f"golden {n}: top-10 differs")
        if ("candidates" in c or "features" in c) != ("ranker.onnx" in m["sha256"]):
            sys.exit(f"golden {n}: candidates and features go with a ranker, and only with one")
        if "ranker.onnx" in m["sha256"]:
            cands = np.array(c["candidates"], dtype=np.int64)
            want = np.array(c["features"], dtype=np.float32)
            if not len(cands) or cands.min() < 1 or cands.max() > m["item_count"] or want.shape != (len(cands), len(RANKER_FEATURES)):
                sys.exit(f"golden {n}: candidates out of range, or features of the wrong shape")
            uv_case = np.array(c["user_emb"], dtype=np.float32)
            got = features(vecs, *source_scores(cooc_ids, popular, c["history"]), c["history"], uv_case, cands)
            diff = float(np.max(np.abs(got.astype(np.float64) - want)))
            if not diff <= FEATURE_TOL:
                sys.exit(f"golden {n}: features differ by {diff}")
    ranker = "yes" if "ranker.onnx" in m["sha256"] else "no"
    print(f"ok: {len(cases)} golden cases, {len(ids)} items, user encoder {m['user_encoder']}, ranker {ranker}")


if __name__ == "__main__":
    main(sys.argv[1])
