"""The ALS grid on the valid split (docs/registration/m8-freshness.md).

For each setting of FACTORS, ALPHAS and REGULARIZATIONS, item vectors from
build.py's ALS on the train split, and the vector reference on the valid
split: a request at each valid event's time, whose history is the user's
train and valid events before it, deduplicated and ordered by time and event
ID as the history store does; the user vector is the fold-in of the last
MAX_SEQ_LEN, as the bundle's ONNX encoder computes it (D058), and every item
but the history's is ranked by u · v, ties to the lower internal ID. A
request whose user vector is zero has an empty reference.

usage: als_grid.py DATA ITEMS_IDS OUT [--category C] [--jobs N]

ITEMS_IDS is a bundle's items.ids, whose lines give the internal IDs. Writes
OUT/als-grid.tsv, a line per setting with its recall@10, recall@20 and
NDCG@10, the selected one marked; the selected setting's rank of each valid
event's item, from 1, 0 outside the top 20, one per line in the valid file's
order, to OUT/valid-ranks.txt.gz; and OUT/als-grid.txt, the selection and
the exact solution's diagnostic. Each setting's line is kept in
OUT/als-grid.partial.tsv as it finishes, and a rerun skips the settings
there; the file is removed once the run ends. It reads the train and valid splits only.
"""

import argparse
import gzip
import os
import time
from collections import defaultdict
from multiprocessing import get_context

os.environ["OPENBLAS_NUM_THREADS"] = "1"
import numpy as np
import scipy.linalg
import scipy.sparse as sp

from build import MAX_SEQ_LEN, SEED, als_vectors, fold_in_matrix, read_split
from fresh import event_id

FACTORS = (64, 128, 256)
ALPHAS = (1.0, 10.0, 40.0, 100.0)
REGULARIZATIONS = (0.01, 0.1, 1.0)
ITERATIONS = 15
K = 20  # items per request, as the quality evaluation asks
BOOTSTRAP = 1000
METRICS = ["recall@10", "recall@20", "ndcg@10"]
COLUMNS = ["factors", "alpha", "regularization", *METRICS, "no_vector", "cond", "fit_s"]


def settings():
    return [(f, a, r) for f in FACTORS for a in ALPHAS for r in REGULARIZATIONS]


class Requests:
    """The valid split's requests: for event k, last holds the internal IDs
    of the user's last MAX_SEQ_LEN events before it, as counts per item;
    seen, every item of the history; target, its item; user, its user."""

    def __init__(self, train, valid, item_ix):
        events = defaultdict(set)
        for u, raw, ts in train + valid:
            events[u].add((ts, event_id(u, raw, ts), item_ix[raw]))
        by_user = {u: sorted(e) for u, e in events.items()}
        ts_of = {u: [e[0] for e in evs] for u, evs in by_user.items()}
        n = len(item_ix) + 1
        last, seen = ([], [], []), ([], [])
        self.target = np.array([item_ix[raw] for _, raw, _ in valid], dtype=np.int64)
        self.user = [u for u, _, _ in valid]
        for k, (u, _, ts) in enumerate(valid):
            j = np.searchsorted(ts_of[u], ts)  # events before ts
            hist = [e[2] for e in by_user[u][:j]]
            ids, counts = np.unique(hist[-MAX_SEQ_LEN:], return_counts=True)
            last[0].extend([k] * len(ids))
            last[1].extend(ids)
            last[2].extend(counts)
            uniq = np.unique(hist)
            seen[0].extend([k] * len(uniq))
            seen[1].extend(uniq)
        shape = (len(valid), n)
        self.last = sp.csr_matrix((np.array(last[2], dtype=np.float64), (last[0], last[1])), shape=shape)
        self.seen = sp.csr_matrix((np.ones(len(seen[0]), dtype=bool), (seen[0], seen[1])), shape=shape)
        self.target_seen = np.asarray(self.seen[np.arange(len(valid)), self.target]).ravel()


def fold_in_vectors(req, vecs, reg):
    """The fold-in user vector of each request, in float32 as the encoder
    gives it."""
    m = fold_in_matrix(vecs, reg)
    return np.asarray((req.last @ vecs.astype(np.float64)) @ m).astype(np.float32)


def exact_vectors(req, vecs, alpha, reg):
    """The user vector ALS's own user step fits for each request: the
    solution of (YᵀY + reg I + (alpha - 1) Σ y yᵀ) u = alpha Σ y, the sums
    over the distinct items of the last MAX_SEQ_LEN, each once as in the
    model's binary matrix; in float32."""
    y = vecs.astype(np.float64)
    gram = y.T @ y + reg * np.eye(y.shape[1])
    out = np.zeros((req.last.shape[0], y.shape[1]), dtype=np.float32)
    for k in range(req.last.shape[0]):
        h = y[req.last.indices[req.last.indptr[k] : req.last.indptr[k + 1]]]
        b = alpha * h.sum(axis=0)
        if np.any(b):
            out[k] = scipy.linalg.cho_solve(scipy.linalg.cho_factor(gram + (alpha - 1) * h.T @ h), b)
    return out


def ranks(req, vecs, users, batch=512):
    """The rank, from 1, of each request's target among every item but the
    history's and padding by u · v, ties to the lower ID; 0 outside the top
    K, for a target in the history and for a zero user vector."""
    v = vecs.astype(np.float64)
    ids = np.arange(len(v))
    out = np.zeros(len(users), dtype=np.int32)
    for lo in range(0, len(users), batch):
        hi = min(lo + batch, len(users))
        s = users[lo:hi].astype(np.float64) @ v.T
        s[:, 0] = -np.inf
        seen = req.seen[lo:hi].tocoo()
        s[seen.row, seen.col] = -np.inf
        t = req.target[lo:hi]
        st = s[np.arange(hi - lo), t][:, None]
        r = 1 + (s > st).sum(axis=1) + ((s == st) & (ids < t[:, None])).sum(axis=1)
        r[req.target_seen[lo:hi] | ~users[lo:hi].any(axis=1)] = 0
        r[r > K] = 0
        out[lo:hi] = r
    return out


def metrics(r):
    hit10 = (r >= 1) & (r <= 10)
    ndcg = np.where(hit10, 1 / np.log2(np.maximum(r, 1) + 1.0), 0.0)
    return {"recall@10": hit10.mean(), "recall@20": ((r >= 1) & (r <= 20)).mean(), "ndcg@10": ndcg.mean()}


def select(rows):
    """The selected setting: the highest recall@10 to four decimals, then
    the highest NDCG@10, then fewer factors, a lower alpha and a higher
    regularization."""
    return min(rows, key=lambda r: (-round(r["recall@10"], 4), -r["ndcg@10"], r["factors"], r["alpha"], -r["regularization"]))


def user_bootstrap(d, users, resamples=BOOTSTRAP, seed=SEED):
    """The mean of d and the 95% nearest-rank percentile interval of its
    mean over resamples of the users, drawn with replacement, each with all
    their events."""
    names, inv = np.unique(users, return_inverse=True)
    sums, counts = np.bincount(inv, weights=d), np.bincount(inv)
    rng = np.random.default_rng(seed)
    means = np.sort([sums[i].sum() / counts[i].sum() for i in (rng.integers(0, len(names), len(names)) for _ in range(resamples))])
    lo, hi = (means[int(np.ceil(p * resamples - 1e-9)) - 1] for p in (0.025, 0.975))
    return d.mean(), lo, hi


def fmt(row, full=True):
    """A setting's line: the values in full, so a rerun reads them back
    exactly, or for the table with metrics to six decimals."""
    def value(c):
        if c in ("factors", "no_vector"):
            return str(row[c])
        return repr(row[c]) if full else f"{row[c]:.6f}" if c in METRICS else f"{row[c]:.4g}"
    return "\t".join(value(c) for c in COLUMNS)


# The worker's inputs, set once per process by init.
_train, _item_ix, _req = None, None, None


def init(train, item_ix, req):
    global _train, _item_ix, _req
    _train, _item_ix, _req = train, item_ix, req


def run(setting):
    f, a, r = setting
    start = time.monotonic()
    vecs = als_vectors(_train, _item_ix, {"factors": f, "regularization": r, "alpha": a, "iterations": ITERATIONS})
    fit = time.monotonic() - start
    users = fold_in_vectors(_req, vecs, r)
    y = vecs.astype(np.float64)
    return {"factors": f, "alpha": a, "regularization": r, **{m: float(v) for m, v in metrics(ranks(_req, vecs, users)).items()},
            "no_vector": int((~users.any(axis=1)).sum()), "cond": float(np.linalg.cond(y.T @ y + r * np.eye(f))), "fit_s": fit}


def read_rows(path):
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return [{c: (int(v) if c in ("factors", "no_vector") else float(v)) for c, v in zip(COLUMNS, line.split("\t"))} for line in f]


def main(data, ids_path, out, category, jobs):
    with open(ids_path) as f:
        item_ix = {raw: n + 1 for n, raw in enumerate(f.read().split("\n")[:-1])}
    train, valid = read_split(data, "train", category), read_split(data, "valid", category)
    if not max(ts for _, _, ts in train) < min(ts for _, _, ts in valid):
        raise SystemExit("the valid split must start after the train split")
    req = Requests(train, valid, item_ix)
    os.makedirs(out, exist_ok=True)
    partial = f"{out}/als-grid.partial.tsv"
    done = {(r["factors"], r["alpha"], r["regularization"]) for r in read_rows(partial)}
    todo = [s for s in settings() if s not in done]
    with get_context("spawn").Pool(jobs, init, (train, item_ix, req)) as pool, open(partial, "a") as f:
        for row in pool.imap_unordered(run, todo):
            f.write(fmt(row) + "\n")
            f.flush()
            print(fmt(row), flush=True)
    rows = sorted(read_rows(partial), key=lambda r: (r["factors"], r["alpha"], r["regularization"]))
    if [(r["factors"], r["alpha"], r["regularization"]) for r in rows] != settings():
        raise SystemExit(f"{partial} does not hold each setting once")
    best = select(rows)
    with open(f"{out}/als-grid.tsv", "w") as f:
        f.write("\t".join(COLUMNS + ["selected"]) + "\n")
        f.writelines(fmt(r, full=False) + "\t" + ("yes" if r is best else "") + "\n" for r in rows)

    f_, a, r = best["factors"], best["alpha"], best["regularization"]
    vecs = als_vectors(train, item_ix, {"factors": f_, "regularization": r, "alpha": a, "iterations": ITERATIONS})
    linear = ranks(req, vecs, fold_in_vectors(req, vecs, r))
    if float(metrics(linear)["recall@10"]) != best["recall@10"]:
        raise SystemExit("the selected setting's refit gives another recall@10")
    with gzip.GzipFile(f"{out}/valid-ranks.txt.gz", "wb", mtime=0) as g:
        g.write("".join(f"{x}\n" for x in linear).encode())
    exact = ranks(req, vecs, exact_vectors(req, vecs, a, r))
    hits = [((x >= 1) & (x <= 10)).astype(np.float64) for x in (exact, linear)]
    d, lo, hi = user_bootstrap(hits[0] - hits[1], req.user)
    with open(f"{out}/als-grid.txt", "w") as f:
        f.write(f"selected: factors {f_}, alpha {a:g}, regularization {r:g}, iterations {ITERATIONS}, seed {SEED}\n")
        f.write(f"valid events: {len(valid)}; recall@10 {best['recall@10']:.4f}, NDCG@10 {best['ndcg@10']:.4f}\n")
        f.write(f"exact user step: recall@10 {metrics(exact)['recall@10']:.4f}; exact - fold-in {d:+.4f} "
                f"[{lo:+.4f}, {hi:+.4f}], 95% interval over {BOOTSTRAP} resamples of {len(set(req.user))} users\n")
    os.remove(partial)
    with open(f"{out}/als-grid.txt") as f:
        print(f.read(), end="")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="The ALS grid on the valid split.")
    p.add_argument("data")
    p.add_argument("items_ids")
    p.add_argument("out")
    p.add_argument("--category", default="Video_Games")
    p.add_argument("--jobs", type=int, default=3)
    a = p.parse_args()
    main(a.data, a.items_ids, a.out, a.category, a.jobs)
