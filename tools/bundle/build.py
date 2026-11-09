"""Build a baseline bundle (docs/SPEC.md §8) from the Video_Games split.

usage: build.py DATA OUT

DATA holds the files downloaded by scripts/data.sh. Writes OUT/<version>/
with item vectors from implicit-feedback ALS trained on the train split, a
co-occurrence table, a popularity list, golden cases and manifest.json, and
prints the version. The user encoder is "mean": a user's vector is the mean
of the vectors of their history items.
"""

import csv
import gzip
import hashlib
import json
import os
import sys

# One BLAS thread, as implicit recommends; set before numpy loads.
os.environ["OPENBLAS_NUM_THREADS"] = "1"
from collections import Counter, defaultdict

import implicit
import numpy as np
import scipy.sparse as sp

SEED = 1  # the only source of randomness: ALS initialization and golden sampling
DIM = 64
ALS = {"factors": DIM, "regularization": 0.05, "alpha": 1.0, "iterations": 15}
COOC_WINDOW = 5  # items within this many positions of each other in a user's sequence
COOC_TOP = 50
POPULAR_DAYS = 30
POPULAR_TOP = 500
MAX_SEQ_LEN = 50
GOLDEN_CASES = 20
DATASET = "amazon2023-video_games-5core-timestamp"


def read_split(data, name):
    with gzip.open(f"{data}/Video_Games.{name}.csv.gz", "rt", newline="") as f:
        return [(r["user_id"], r["parent_asin"], int(r["timestamp"])) for r in csv.DictReader(f)]


def sequences(rows, item_ix):
    """Internal item IDs per user, in time order (ties by item ID)."""
    by_user = defaultdict(list)
    for user, item, ts in rows:
        by_user[user].append((ts, item_ix[item]))
    return {u: [i for _, i in sorted(v)] for u, v in by_user.items()}


def mean_vector(vecs, history):
    h = history[-MAX_SEQ_LEN:]
    return vecs[h].astype(np.float64).mean(axis=0).astype(np.float32)


def top_k(vecs, user_vec, exclude, k):
    """Exact dot-product top-k over internal IDs >= 1, excluding `exclude`; ties by ID."""
    scores = vecs.astype(np.float64) @ user_vec.astype(np.float64)
    scores[0] = -np.inf
    scores[list(exclude)] = -np.inf
    order = np.lexsort((np.arange(len(scores)), -scores))
    return [int(i) for i in order[:k]]


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main(data, out):
    train = read_split(data, "train")
    test = read_split(data, "test")
    valid = read_split(data, "valid")
    items = sorted({i for rows in (train, valid, test) for _, i, _ in rows})
    item_ix = {item: n + 1 for n, item in enumerate(items)}  # 0 is padding
    n = len(items) + 1

    users = sorted({u for u, _, _ in train})
    user_ix = {u: n for n, u in enumerate(users)}
    rows = np.array([user_ix[u] for u, _, _ in train])
    cols = np.array([item_ix[i] for _, i, _ in train])
    ui = sp.csr_matrix((np.ones(len(train), dtype=np.float32), (rows, cols)), shape=(len(users), n))
    ui.sum_duplicates()
    ui.data[:] = 1.0
    model = implicit.cpu.als.AlternatingLeastSquares(**ALS, random_state=SEED, num_threads=1)
    model.fit(ui, show_progress=False)
    vecs = np.ascontiguousarray(model.item_factors, dtype=np.float32)
    vecs[0] = 0

    seqs = sequences(train, item_ix)
    pair = defaultdict(Counter)
    for seq in seqs.values():
        for a in range(len(seq)):
            for b in range(a + 1, min(a + 1 + COOC_WINDOW, len(seq))):
                if seq[a] != seq[b]:
                    pair[seq[a]][seq[b]] += 1
                    pair[seq[b]][seq[a]] += 1
    cooc_ids = np.zeros((n, COOC_TOP), dtype=np.int64)
    cooc_counts = np.zeros((n, COOC_TOP), dtype=np.float32)
    for i, c in pair.items():
        top = sorted(c.items(), key=lambda kv: (-kv[1], kv[0]))[:COOC_TOP]
        cooc_ids[i, : len(top)] = [j for j, _ in top]
        cooc_counts[i, : len(top)] = [v for _, v in top]

    cutoff = max(ts for _, _, ts in train)
    recent = Counter(item_ix[i] for _, i, ts in train if ts > cutoff - POPULAR_DAYS * 86_400_000)
    popular = np.array([i for i, _ in sorted(recent.items(), key=lambda kv: (-kv[1], kv[0]))[:POPULAR_TOP]], dtype=np.int64)

    rng = np.random.default_rng(SEED)
    test_users = sorted({u for u, _, _ in test if u in seqs})
    golden = []
    for u in rng.choice(test_users, size=GOLDEN_CASES, replace=False):
        hist = seqs[str(u)][-MAX_SEQ_LEN:]
        uv = mean_vector(vecs, hist)
        golden.append({"history": hist, "user_emb": uv.tolist(), "top10": top_k(vecs, uv, set(hist), 10)})

    version = hashlib.sha256(vecs.tobytes()).hexdigest()[:12]
    d = os.path.join(out, version)
    os.makedirs(d, exist_ok=True)
    with open(f"{d}/items.ids", "w") as f:
        f.writelines(i + "\n" for i in items)
    np.save(f"{d}/items.npy", vecs)
    np.save(f"{d}/cooc_ids.npy", cooc_ids)
    np.save(f"{d}/cooc_counts.npy", cooc_counts)
    np.save(f"{d}/popular.npy", popular)
    with open(f"{d}/golden.jsonl", "w") as f:
        f.writelines(json.dumps(g) + "\n" for g in golden)
    files = sorted(os.listdir(d))
    manifest = {
        "format_version": 1,
        "model_version": version,
        "dataset": DATASET,
        "train_cutoff_ts": cutoff,
        "item_count": len(items),
        "dim": DIM,
        "metric": "dot",
        "normalized": False,
        "max_seq_len": MAX_SEQ_LEN,
        "pad_id": 0,
        "user_encoder": "mean",
        "build": {"seed": SEED, "als": ALS, "implicit": implicit.__version__, "cooc_window": COOC_WINDOW, "popular_days": POPULAR_DAYS},
        "sha256": {name: sha256(f"{d}/{name}") for name in files if name != "manifest.json"},
    }
    with open(f"{d}/manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)
    print(version)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
