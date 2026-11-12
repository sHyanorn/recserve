"""Verify a bundle directory: checksums, version, shapes, the ranker and golden cases.

usage: verify.py BUNDLE_DIR

Applies the checks of the service's bundle.Load, except the comparison of
train_cutoff_ts with the evaluation split (docs/SPEC.md §8). Recomputes
every golden case from the files on disk with the same rules the service
uses (user vector from the last max_seq_len history items, by mean or by
user_encoder.onnx; exact dot product; history excluded; ties by internal
ID) and exits non-zero on any mismatch. A ranker must declare the service's
ranker_features and pass its load check (build.check_ranker), and then
every golden case must give the features of its candidates, which are
recomputed from the case's history and user vector (build.features).
"""

import json
import math
import os
import sys

import numpy as np

from build import (FEATURE_TOL, RANKER_FEATURES, check_ranker, encode, features, mean_vector, session, sha256,
                   source_scores, top_k, version_of)

TOL = 1e-4
# Read only when the manifest lists them; one on disk that it does not list
# rejects the bundle, as in the service.
OPTIONAL = ("user_encoder.onnx", "ranker.onnx", "cooc_ids.npy", "cooc_counts.npy", "popular.npy")
REQUIRED = ("items.ids", "items.npy", "golden.jsonl")
COOC_WIDTH = 50
# The JSON types of the manifest fields the service reads (train_cutoff_ts is
# checked on its own); as in Go, a field that is missing or null reads as 0,
# "" or false.
MANIFEST_TYPES = {"format_version": int, "model_version": str, "dataset": str, "item_count": int, "dim": int,
                  "metric": str, "normalized": bool, "max_seq_len": int, "pad_id": int, "user_encoder": str,
                  "ranker_features": list, "sha256": dict}
GOLDEN_FIELDS = {"history", "history_raw", "user_emb", "top10", "candidates", "features"}
# The service reads golden.jsonl a line at a time with a buffer of this many
# bytes, which must hold the line and its newline.
MAX_LINE = 1 << 20


def reject(name):
    sys.exit(f"golden.jsonl: {name} is not a number")


def ints(v):
    return type(v) is list and all(type(x) is int for x in v)


def numbers(v):
    return type(v) is list and all(type(x) in (int, float) for x in v)


def load_npy(d, name, descr):
    """Reads a .npy file as the service does: format version 1 to 3, C order,
    dtype descr, and exactly the data bytes its shape needs."""
    fmt = np.lib.format
    with open(f"{d}/{name}", "rb") as f:
        try:
            version = fmt.read_magic(f)
            if version[0] not in (1, 2, 3):
                sys.exit(f"{name}: npy version {version[0]}")
            shape, fortran, dtype = (fmt.read_array_header_1_0 if version[0] == 1 else fmt.read_array_header_2_0)(f)
        except ValueError as e:
            sys.exit(f"{name}: {e}")
        data = f.read()
    if fortran or dtype.str != descr:
        sys.exit(f"{name}: dtype {dtype.str}{', Fortran order' if fortran else ''}, want {descr} in C order")
    if len(data) != math.prod(shape) * dtype.itemsize:
        sys.exit(f"{name}: {len(data)} data bytes for shape {shape}")
    return np.frombuffer(data, dtype).reshape(shape)


def read_manifest(d):
    with open(f"{d}/manifest.json", "rb") as f:
        m = json.load(f, parse_constant=lambda c: sys.exit(f"manifest.json: {c} is not a number"))
    if type(m) is not dict:
        sys.exit("manifest.json is not an object")
    for k, t in MANIFEST_TYPES.items():
        if m.get(k) is not None and type(m[k]) is not t:
            sys.exit(f"manifest: {k} {m[k]!r} is not a JSON {t.__name__}")
    fv, metric, pad, enc = m.get("format_version"), m.get("metric"), m.get("pad_id") or 0, m.get("user_encoder")
    if fv != 1:
        sys.exit(f"format_version {fv!r}, want 1")
    if metric != "dot" or pad != 0 or enc not in ("mean", "onnx"):
        sys.exit(f"unsupported metric {metric!r}, pad_id {pad!r} or user_encoder {enc!r}")
    if not all((m.get(k) or 0) > 0 for k in ("item_count", "dim", "max_seq_len")):
        sys.exit("manifest: item_count, dim and max_seq_len are required")
    m["sha256"] = m.get("sha256") or {}
    if m.get("model_version") != version_of(m["sha256"]):
        sys.exit(f"model_version {m.get('model_version')!r}, but the checksums give {version_of(m['sha256'])!r}")
    return m


def main(d):
    m = read_manifest(d)
    for name, want in m["sha256"].items():
        if name != os.path.basename(name) or name == "manifest.json":
            sys.exit(f"manifest lists {name!r}")
        if sha256(f"{d}/{name}") != want:
            sys.exit(f"{name}: checksum mismatch")
    cutoff = m.get("train_cutoff_ts")
    if type(cutoff) is not int or cutoff <= 0:
        sys.exit(f"train_cutoff_ts {cutoff!r}: a time in milliseconds is required")
    for name in OPTIONAL:
        if name not in m["sha256"] and os.path.exists(f"{d}/{name}"):
            sys.exit(f"{name} is in the directory but not in the manifest")
    for name in REQUIRED:
        if name not in m["sha256"]:
            sys.exit(f"{name} missing from the manifest")
    # Split as the service splits it: at "\n" only, after one final "\n".
    with open(f"{d}/items.ids", "rb") as f:
        ids = [i.decode("utf-8", "surrogateescape") for i in f.read().removesuffix(b"\n").split(b"\n")]
    if len(ids) != m["item_count"]:
        sys.exit(f"items.ids has {len(ids)} lines, item_count is {m['item_count']}")
    index = {}
    for i, raw in enumerate(ids):
        if raw == "":
            sys.exit(f"items.ids line {i + 1} is empty")
        if raw in index:
            sys.exit(f"items.ids repeats {raw!r}")
        index[raw] = i + 1
    vecs = load_npy(d, "items.npy", "<f4")
    if vecs.shape != (m["item_count"] + 1, m["dim"]):
        sys.exit(f"shape mismatch: {len(ids)} ids, vectors {vecs.shape} {vecs.dtype}")
    if not np.isfinite(vecs).all():
        sys.exit("items.npy: non-finite value")
    if np.any(vecs[0] != 0):
        sys.exit("padding row 0 is not zero")
    cooc_ids, popular = np.zeros((len(vecs), 1), np.int64), np.zeros(0, np.int64)
    if "cooc_ids.npy" in m["sha256"]:
        cooc_ids = load_npy(d, "cooc_ids.npy", "<i8")
        if cooc_ids.shape != (m["item_count"] + 1, COOC_WIDTH) or cooc_ids.min() < 0 or cooc_ids.max() > m["item_count"]:
            sys.exit(f"cooc_ids.npy: shape {cooc_ids.shape}, want ({m['item_count'] + 1}, {COOC_WIDTH}), or ID range")
    if "popular.npy" in m["sha256"]:
        popular = load_npy(d, "popular.npy", "<i8")
        if popular.ndim != 1 or popular.min(initial=1) < 1 or popular.max(initial=0) > m["item_count"]:
            sys.exit(f"popular.npy: shape {popular.shape}, want 1-D, or ID range")
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
    ranker = "ranker.onnx" in m["sha256"]
    if ranker:
        if m.get("ranker_features") != RANKER_FEATURES:
            sys.exit(f"ranker_features {m.get('ranker_features')}, the service computes {RANKER_FEATURES}")
        with open(f"{d}/ranker.onnx", "rb") as f:
            if msg := check_ranker(session(f.read()), len(RANKER_FEATURES)):
                sys.exit(f"ranker.onnx: {msg}")
    elif m.get("ranker_features"):
        sys.exit("ranker_features without ranker.onnx")
    # Split as the service's line scanner splits it: at "\n", dropping one
    # "\r" before it; after a final "\n" there is no line.
    with open(f"{d}/golden.jsonl", "rb") as f:
        lines = f.read().split(b"\n")
    if lines[-1] == b"":
        lines.pop()
    cases = []
    for n, line in enumerate(lines):
        if len(line) >= MAX_LINE:
            sys.exit(f"golden {n}: {len(line)} bytes, the service reads lines of at most {MAX_LINE - 1}")
        try:
            cases.append(json.loads(line.removesuffix(b"\r"), parse_constant=reject))
        except ValueError as e:
            sys.exit(f"golden {n}: {e}")
    if not cases:
        sys.exit("golden.jsonl has no cases")
    for n, c in enumerate(cases):
        if type(c) is not dict or set(c) - GOLDEN_FIELDS:
            sys.exit(f"golden {n}: not an object, or fields other than {sorted(GOLDEN_FIELDS)}")
        h, emb, top10 = c.get("history"), c.get("user_emb"), c.get("top10")
        if not (ints(h) and h and numbers(emb) and len(emb) == m["dim"] and ints(top10) and len(top10) == 10):
            sys.exit(f"golden {n}: needs a history, {m['dim']} user_emb values and 10 top10 IDs")
        if min(h) < 1 or max(h) > m["item_count"]:
            sys.exit(f"golden {n}: history IDs out of range")
        # Raw IDs, if given, check items.ids, which the internal IDs cannot.
        raw = c.get("history_raw")
        if raw is not None and (type(raw) is not list or [index.get(r) for r in raw] != h):
            sys.exit(f"golden {n}: history_raw does not map to history through items.ids")
        if m["user_encoder"] == "onnx":
            uv = encode(sess, h, m["max_seq_len"])
        else:
            uv = mean_vector(vecs, h, m["max_seq_len"])
        if uv.shape != (m["dim"],) or not np.isfinite(uv).all() or not np.any(uv):
            sys.exit(f"golden {n}: the user vector is not {m['dim']} finite values, or zero, which counts as none")
        diff = float(np.max(np.abs(uv - np.array(emb, dtype=np.float32))))
        if not diff <= TOL:
            sys.exit(f"golden {n}: user vector differs by {diff}")
        if top_k(vecs, uv, set(h), 10) != top10:
            sys.exit(f"golden {n}: top-10 differs")
        cands, feats = c.get("candidates"), c.get("features")
        if not ranker:
            if cands is not None or feats is not None:
                sys.exit(f"golden {n}: candidates without a ranker")
            continue
        nf = len(RANKER_FEATURES)
        if not (ints(cands) and cands and type(feats) is list and len(feats) == len(cands)
                and all(numbers(row) and len(row) == nf for row in feats)):
            sys.exit(f"golden {n}: a bundle with a ranker needs candidates and {nf} features for each")
        if min(cands) < 1 or max(cands) > m["item_count"]:
            sys.exit(f"golden {n}: candidate IDs out of range")
        got = features(vecs, *source_scores(cooc_ids, popular, h), h, np.array(emb, dtype=np.float32),
                       np.array(cands, dtype=np.int64))
        diff = float(np.max(np.abs(got.astype(np.float64) - np.array(feats, dtype=np.float32))))
        if not diff <= FEATURE_TOL:
            sys.exit(f"golden {n}: features differ by {diff}")
    print(f"ok: {len(cases)} golden cases, {len(ids)} items, user encoder {m['user_encoder']}, ranker {'yes' if ranker else 'no'}")


if __name__ == "__main__":
    main(sys.argv[1])
