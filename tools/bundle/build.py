"""Build a baseline bundle (docs/SPEC.md §8) from the Video_Games split.

usage: build.py DATA OUT [--encoder mean|onnx] [--ranker]

DATA holds the files downloaded by scripts/data.sh. Writes OUT/<version>/
with item vectors from implicit-feedback ALS trained on the train split, a
co-occurrence table, a popularity list, golden cases and manifest.json, and
prints the version. A user's vector is the mean of the vectors of their last
MAX_SEQ_LEN history items. With --encoder mean (the default) the service
computes it; with --encoder onnx the bundle carries user_encoder.onnx, a
graph that computes the same mean, and the golden user vectors come from
running it with ONNX Runtime. With --ranker the bundle also carries
ranker.onnx, a logistic regression over the six features of SPEC §8 trained
on the valid split (D027), and each golden case carries its candidates and
their features, which the service recomputes.
"""

import argparse
import csv
import gzip
import hashlib
import json
import os
import sys
import tempfile

# One BLAS thread, as implicit recommends; set before numpy loads.
os.environ["OPENBLAS_NUM_THREADS"] = "1"
from collections import Counter, defaultdict

import implicit
import numpy as np
import onnx
import onnxruntime as ort
import scipy.sparse as sp
import sklearn
from onnx import TensorProto, helper, numpy_helper
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score

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
OPSET, IR_VERSION = 17, 8  # IR version 8 is the one that goes with opset 17
PARITY = 1e-5  # largest accepted difference between the ONNX encoder and the float64 mean
# Retrieval as the service runs it (recommend.VectorTop, bundle.CoocSeeds, bundle.PopularTop).
VECTOR_TOP, COOC_SEEDS, POPULAR_CANDIDATES = 200, 10, 100
FEATURE_TOL = 1e-6  # largest accepted difference between the service's features and the golden's
RANKER = {"C": 1.0, "held_out_share": 0.2}  # inverse L2 strength, and the share of users held out


def read_split(data, name):
    with gzip.open(f"{data}/Video_Games.{name}.csv.gz", "rt", newline="") as f:
        return [(r["user_id"], r["parent_asin"], int(r["timestamp"])) for r in csv.DictReader(f)]


def sequences(rows, item_ix):
    """Internal item IDs per user, in time order (ties by item ID)."""
    by_user = defaultdict(list)
    for user, item, ts in rows:
        by_user[user].append((ts, item_ix[item]))
    return {u: [i for _, i in sorted(v)] for u, v in by_user.items()}


def mean_vector(vecs, history, max_len=MAX_SEQ_LEN):
    h = history[-max_len:]
    return vecs[h].astype(np.float64).mean(axis=0).astype(np.float32)


def model(graph):
    """A checked model of graph; the same graph always serializes to the same bytes."""
    m = helper.make_model(graph, opset_imports=[helper.make_opsetid("", OPSET)], producer_name="recserve-bundle")
    m.ir_version = IR_VERSION
    onnx.checker.check_model(m, full_check=True)
    return m


def mean_encoder(vecs):
    """The user encoder of SPEC §8 as a graph: the mean of the rows of vecs
    named in item_seq int64 [B, L], padding (ID 0, a zero row) left out of the
    count; output user_emb float32 [B, D]. A row of only padding gives zeros."""
    nodes = [
        helper.make_node("Gather", ["items", "item_seq"], ["emb"], axis=0),
        helper.make_node("ReduceSum", ["emb", "axis1"], ["sum"], keepdims=0),
        helper.make_node("Greater", ["item_seq", "pad"], ["real"]),
        helper.make_node("Cast", ["real"], ["realf"], to=TensorProto.FLOAT),
        helper.make_node("ReduceSum", ["realf", "axis1"], ["count"], keepdims=1),
        helper.make_node("Max", ["count", "one"], ["divisor"]),
        helper.make_node("Div", ["sum", "divisor"], ["user_emb"]),
    ]
    init = [
        numpy_helper.from_array(vecs, "items"),
        numpy_helper.from_array(np.array([1], dtype=np.int64), "axis1"),
        numpy_helper.from_array(np.array(0, dtype=np.int64), "pad"),
        numpy_helper.from_array(np.array(1, dtype=np.float32), "one"),
    ]
    return model(helper.make_graph(
        nodes, "mean_encoder",
        [helper.make_tensor_value_info("item_seq", TensorProto.INT64, ["B", "L"])],
        [helper.make_tensor_value_info("user_emb", TensorProto.FLOAT, ["B", vecs.shape[1]])],
        init))


# The ranker's input features, in column order (SPEC §8, Ranker features).
RANKER_FEATURES = ["vector_score", "cooc_score", "popular_score", "item_norm", "last_item_cosine", "log_history_len"]
# Rows the service passes to the ranker; rows past the candidates are zero.
RANKER_ROWS = 512


def linear_ranker(w, b):
    """A ranker as a graph: score = features @ w + b, input features float32
    [C, F], output score float32 [C]."""
    w = np.asarray(w, dtype=np.float32)
    nodes = [
        helper.make_node("MatMul", ["features", "w"], ["wx"]),
        helper.make_node("Add", ["wx", "b"], ["score"]),
    ]
    init = [numpy_helper.from_array(w, "w"), numpy_helper.from_array(np.array(b, dtype=np.float32), "b")]
    return model(helper.make_graph(
        nodes, "linear_ranker",
        [helper.make_tensor_value_info("features", TensorProto.FLOAT, ["C", len(w)])],
        [helper.make_tensor_value_info("score", TensorProto.FLOAT, ["C"])],
        init))


# The rows the load check changes, one set at a time from all-zero rows;
# the other rows' scores must not move.
CHECK_ROWS = [[0], [1, RANKER_ROWS // 2, RANKER_ROWS - 1]]


def check_ranker(sess, n_features):
    """Run the service's load check on a ranker: RANKER_ROWS rows of zeros,
    then the same with each set of CHECK_ROWS given fixed values; the scores
    must be finite, one per row, and the rows outside the set must not
    change, bit for bit. Returns an error message or None."""
    def score(x):
        return sess.run(["score"], {"features": x})[0]

    zero = score(np.zeros((RANKER_ROWS, n_features), dtype=np.float32))
    for rows in CHECK_ROWS:
        x = np.zeros((RANKER_ROWS, n_features), dtype=np.float32)
        for k, r in enumerate(rows):
            x[r] = (np.arange(1, n_features + 1, dtype=np.float32) + k) / 4
        got = score(x)
        if zero.shape != (RANKER_ROWS,) or got.shape != (RANKER_ROWS,) or zero.dtype != np.float32:
            return f"ranker output {zero.dtype} {zero.shape}, want float32 ({RANKER_ROWS},)"
        if not (np.isfinite(zero).all() and np.isfinite(got).all()):
            return "ranker output is not finite"
        others = np.setdiff1d(np.arange(RANKER_ROWS), rows)
        if zero[others].tobytes() != got[others].tobytes():
            return f"ranker scores change with rows {rows}"
    return None


def source_scores(cooc_ids, popular, hist):
    """Each item's best co-occurrence and popularity scores for hist, oldest
    first (SPEC §8), as dicts by internal ID."""
    cooc = {}
    for s in range(min(COOC_SEEDS, len(hist))):
        for r, i in enumerate(cooc_ids[hist[-1 - s]]):
            if i == 0:
                break
            cooc[int(i)] = max(cooc.get(int(i), 0.0), 1 / ((s + 1) * (r + 1)))
    pop = {}
    for r, i in enumerate(popular[:POPULAR_CANDIDATES]):
        pop.setdefault(int(i), 1 / (r + 1))
    return cooc, pop


def features(vecs, cooc, pop, hist, uv, ids):
    """The ranker's input for the candidates ids (SPEC §8, in the order of
    RANKER_FEATURES), computed in float64 and rounded to float32; uv is the
    user vector or None."""
    v = vecs[ids].astype(np.float64)
    norm = np.sqrt((v * v).sum(axis=1))
    out = np.zeros((len(ids), len(RANKER_FEATURES)))
    if uv is not None:
        out[:, 0] = v @ uv.astype(np.float64)
    out[:, 1] = [cooc.get(int(i), 0.0) for i in ids]
    out[:, 2] = [pop.get(int(i), 0.0) for i in ids]
    out[:, 3] = norm
    if len(hist):
        last = vecs[hist[-1]].astype(np.float64)
        last_norm = np.sqrt(last @ last)
        ok = (norm > 0) & (last_norm > 0)
        out[ok, 4] = (v[ok] @ last) / (norm[ok] * last_norm)
    out[:, 5] = np.log1p(len(hist))
    return out.astype(np.float32)


def ranker_input(vecs, cooc_ids, popular, hist, uv):
    """The service's ranker candidates for hist, best first, with their
    retrieval scores and features: the union of the vector top VECTOR_TOP
    (zero vectors left out, D028), the co-occurring and the popular items,
    without the history's items; the RANKER_ROWS best by u · v, or without a
    user vector by the best source score, ties to the lower ID."""
    cooc, pop = source_scores(cooc_ids, popular, hist)
    seen = set(hist)
    ids = set(cooc) | set(pop)
    if uv is not None:
        dots = vecs.astype(np.float64) @ uv.astype(np.float64)
        pool = np.flatnonzero(np.any(vecs != 0, axis=1) & ~np.isin(np.arange(len(vecs)), list(seen)))
        ids |= set(pool[np.lexsort((pool, -dots[pool]))[:VECTOR_TOP]].tolist())
    ids = np.array(sorted(ids - seen - {0}), dtype=np.int64)
    if uv is not None:
        score = dots[ids]
    else:
        score = np.array([max(cooc.get(i, 0.0), pop.get(i, 0.0)) for i in ids.tolist()])
    order = np.lexsort((ids, -score))[:RANKER_ROWS]
    return ids[order], score[order], features(vecs, cooc, pop, hist, uv, ids[order])


def recall_at_10(scores, ids, relevant):
    top = ids[np.lexsort((ids, -scores))[:10]]
    return len(relevant & set(top.tolist())) / len(relevant)


def train_ranker(vecs, cooc_ids, popular, seqs, valid, item_ix):
    """Fit the ranker on the valid split: a request at each valid user's
    first valid event, whose history is the user's train events, labels its
    candidates 1 if the user has a valid event on them. The features read
    only tables built from the train split, which ends before the valid
    split starts. Users none of whose valid items are candidates teach a
    ranker nothing and are left out. Returns the weights and bias of a
    logistic regression on the raw features, and what it scored on the
    users held out."""
    relevant = defaultdict(set)
    for user, item, _ in valid:
        relevant[user].add(item_ix[item])
    queries = []
    for user in sorted(u for u in relevant if u in seqs):
        hist = seqs[user]
        uv = mean_vector(vecs, hist)
        ids, retrieval, x = ranker_input(vecs, cooc_ids, popular, hist, uv if np.any(uv) else None)
        y = np.isin(ids, list(relevant[user]))
        if y.any():
            queries.append((x, y, ids, retrieval, relevant[user]))
    held = set(np.random.default_rng(SEED).choice(len(queries), size=int(len(queries) * RANKER["held_out_share"]), replace=False).tolist())
    fit = [q for k, q in enumerate(queries) if k not in held]
    X = np.concatenate([q[0] for q in fit]).astype(np.float64)
    labels = np.concatenate([q[1] for q in fit])
    # Standardized for the solver; the scaling is folded into the weights.
    mean, std = X.mean(axis=0), X.std(axis=0)
    std[std == 0] = 1
    lr = LogisticRegression(C=RANKER["C"], max_iter=1000).fit((X - mean) / std, labels)
    w = lr.coef_[0] / std
    b = float(lr.intercept_[0] - np.sum(lr.coef_[0] * mean / std))
    report = {"ranker": {"auc": [], "recall_at_10": []}, "retrieval": {"auc": [], "recall_at_10": []}}
    for k in sorted(held):
        x, y, ids, retrieval, rel = queries[k]
        for name, scores in (("ranker", x.astype(np.float64) @ w + b), ("retrieval", retrieval)):
            if not y.all():
                report[name]["auc"].append(roc_auc_score(y, scores))
            report[name]["recall_at_10"].append(recall_at_10(scores, ids, rel))
    scores = {name: {k: round(float(np.mean(v)), 4) for k, v in r.items()} for name, r in report.items()}
    stats = {"users": len(queries), "held_out_users": len(held), "rows": len(labels), "positives": int(labels.sum()),
             "weights": [float(v) for v in w], "bias": b, "held_out": scores}
    return w, b, stats


def session(model_bytes):
    """An ONNX Runtime session with one intra-op thread, as the service runs it (D015)."""
    opts = ort.SessionOptions()
    opts.intra_op_num_threads = 1
    return ort.InferenceSession(model_bytes, opts, providers=["CPUExecutionProvider"])


def encode(sess, history, max_len=MAX_SEQ_LEN):
    """Run a user encoder on the last max_len items of history, left-padded
    with 0 to max_len, as the service calls it (B = 1)."""
    h = history[-max_len:]
    seq = np.zeros((1, max_len), dtype=np.int64)
    seq[0, max_len - len(h):] = h
    return sess.run(["user_emb"], {"item_seq": seq})[0][0]


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


def version_of(sums):
    """The version of a bundle whose files have these checksums: it names every file."""
    return hashlib.sha256("".join(f"{name}{sums[name]}" for name in sorted(sums)).encode()).hexdigest()[:12]


def main(data, out, encoder, ranker):
    train = read_split(data, "train")
    test = read_split(data, "test")
    valid = read_split(data, "valid")
    items = sorted({i for rows in (train, valid, test) for _, i, _ in rows})
    item_ix = {item: n + 1 for n, item in enumerate(items)}  # 0 is padding
    n = len(items) + 1

    # ALS sees only train users and items, so the valid and test splits
    # cannot change the model; items without train events get zero vectors.
    users = sorted({u for u, _, _ in train})
    user_ix = {u: n for n, u in enumerate(users)}
    train_items = sorted({item_ix[i] for _, i, _ in train})
    col = {iid: n for n, iid in enumerate(train_items)}
    rows = np.array([user_ix[u] for u, _, _ in train])
    cols = np.array([col[item_ix[i]] for _, i, _ in train])
    ui = sp.csr_matrix((np.ones(len(train), dtype=np.float32), (rows, cols)), shape=(len(users), len(train_items)))
    ui.sum_duplicates()
    ui.data[:] = 1.0
    model = implicit.cpu.als.AlternatingLeastSquares(**ALS, random_state=SEED, num_threads=1)
    model.fit(ui, show_progress=False)
    vecs = np.zeros((n, DIM), dtype=np.float32)
    vecs[train_items] = model.item_factors

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

    enc = mean_encoder(vecs).SerializeToString() if encoder == "onnx" else None
    sess = session(enc) if enc else None

    rank = None
    if ranker:
        # The labels must come after every event the features are built from.
        if not cutoff < min(ts for _, _, ts in valid):
            sys.exit("the valid split must start after the train split")
        w, b, ranker_stats = train_ranker(vecs, cooc_ids, popular, seqs, valid, item_ix)
        rank = linear_ranker(w, b).SerializeToString()
        if msg := check_ranker(session(rank), len(RANKER_FEATURES)):
            sys.exit(f"ranker: {msg}")
        cutoff = max(ts for _, _, ts in valid)  # the labels' events were used in training too

    rng = np.random.default_rng(SEED)
    test_users = sorted({u for u, _, _ in test if u in seqs})
    golden = []
    # Half the cases from users with the longest histories, so the window of the last MAX_SEQ_LEN items is exercised.
    longest = sorted(test_users, key=lambda u: (-len(seqs[u]), u))[: GOLDEN_CASES // 2]
    rest = sorted(set(test_users) - set(longest))
    for u in longest + list(rng.choice(rest, size=GOLDEN_CASES - len(longest), replace=False)):
        hist = seqs[str(u)]
        uv = mean_vector(vecs, hist)
        if sess:
            got = encode(sess, hist)
            diff = float(np.abs(got - uv).max())
            if not np.isfinite(got).all() or not diff <= PARITY:
                sys.exit(f"user encoder differs from the mean by {diff} for user {u}")
            uv = got
        case = {"history": hist, "user_emb": uv.tolist(), "top10": top_k(vecs, uv, set(hist), 10)}
        if rank:
            ids, _, x = ranker_input(vecs, cooc_ids, popular, hist, uv if np.any(uv) else None)
            case |= {"candidates": ids.tolist(), "features": x.tolist()}
        golden.append(case)

    os.makedirs(out, exist_ok=True)
    d = tempfile.mkdtemp(dir=out)
    with open(f"{d}/items.ids", "w") as f:
        f.writelines(i + "\n" for i in items)
    np.save(f"{d}/items.npy", vecs)
    np.save(f"{d}/cooc_ids.npy", cooc_ids)
    np.save(f"{d}/cooc_counts.npy", cooc_counts)
    np.save(f"{d}/popular.npy", popular)
    if enc:
        with open(f"{d}/user_encoder.onnx", "wb") as f:
            f.write(enc)
    if rank:
        with open(f"{d}/ranker.onnx", "wb") as f:
            f.write(rank)
    with open(f"{d}/golden.jsonl", "w") as f:
        f.writelines(json.dumps(g, allow_nan=False) + "\n" for g in golden)
    files = sorted(os.listdir(d))
    sums = {name: sha256(f"{d}/{name}") for name in files}
    # A change of any setting changes a file, and so the version.
    version = version_of(sums)
    build = {"seed": SEED, "als": ALS, "implicit": implicit.__version__, "cooc_window": COOC_WINDOW, "popular_days": POPULAR_DAYS}
    if enc:
        build["user_encoder"] = {"graph": "mean", "opset": OPSET, "onnx": onnx.__version__, "onnxruntime": ort.__version__}
    if rank:
        build["ranker"] = {"model": "logistic_regression", "scikit_learn": sklearn.__version__, **RANKER, **ranker_stats}
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
        "user_encoder": encoder,
        **({"ranker_features": RANKER_FEATURES} if rank else {}),
        "build": build,
        "sha256": sums,
    }
    with open(f"{d}/manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)
    final = os.path.join(out, version)
    if os.path.exists(final):
        sys.exit(f"{final} exists")
    os.chmod(d, 0o755)  # mkdtemp makes it 0700; services read bundles as another user
    os.rename(d, final)
    print(version)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Build a baseline bundle.")
    p.add_argument("data")
    p.add_argument("out")
    p.add_argument("--encoder", choices=["mean", "onnx"], default="mean")
    p.add_argument("--ranker", action="store_true", help="train and add a logistic-regression ranker")
    a = p.parse_args()
    main(a.data, a.out, a.encoder, a.ranker)
