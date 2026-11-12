"""Histories of recserve's test events in recserve's data and in seqrec-lab's.

docs/BENCHMARKS.md, seqrec-lab's bundle against the baseline: for each test
event of recserve's split, the length of its history in recserve (the
user's events before it in the train, valid and test files) and the number
of the same user's events before it in seqrec-lab's data, and the repeated
items in recserve's histories.

seqrec-lab's data is rebuilt as its code at 6aa5d87 builds it, without
pandas: `data/readers.py` read_amazon_2023 (rows with an empty or missing user,
item or timestamp dropped, pandas' default missing values counting as empty),
`data/preprocess.py` build_log (stable sort by user, time and source row,
then the earliest of each repeated (user, item) pair kept) and
`data/splits.py` global_temporal (T the lower 0.9 quantile of the log's
times, T_val that of the times at or before T, the vocabulary the items of
the iterative 5-core of the events at or before T). The script checks these
against seqrec-lab's published figures and exits non-zero if one differs.

    uv run --locked python histories.py DATA ZERO_CORE

DATA is the directory of make data; ZERO_CORE is Video_Games.csv.gz from
https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/benchmark/0core/rating_only/
"""

import csv
import gzip
import hashlib
import sys
from collections import defaultdict

import numpy as np

ZERO_CORE_SHA256 = "30692f9806e33a7b5d8b599b73d7ca33dbfefb0abdfc1578052a101f2e64b3cd"

# seqrec-lab's figures for Video_Games' global temporal split, and the share
# of recserve's test targets in its vocabulary (docs/BENCHMARKS.md), in %.
EXPECTED = {"T": 1649219168790, "T_val": 1618224866975, "vocabulary": 21930, "target_share": 41.8}

# pandas.read_csv's default missing values (pandas 2 and 3), which seqrec-lab's
# reader drops with dropna; "" is the empty field.
PANDAS_NA = {
    "", "#N/A", "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN", "-NaN", "-nan", "1.#IND",
    "1.#QNAN", "<NA>", "N/A", "NA", "NULL", "NaN", "None", "n/a", "nan", "null",
}


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def read_zero_core(path):
    """(user, item, ts) of the rows read_amazon_2023 keeps, in file order."""
    users, items, ts = [], [], []
    dropped = 0
    with gzip.open(path, "rt", encoding="utf-8-sig", newline="") as f:
        for r in csv.DictReader(f):
            u, i, t = r["user_id"], r["parent_asin"], r["timestamp"]
            if None in (u, i, t) or u in PANDAS_NA or i in PANDAS_NA or t in PANDAS_NA:
                dropped += 1
                continue
            users.append(u)
            items.append(i)
            ts.append(int(t))
    return users, items, np.asarray(ts, dtype=np.int64), dropped


def build_log(users, items, ts):
    """build_log: user and item codes into sorted keys and times of the
    deduplicated rows, sorted by (user, ts, source row)."""
    user_keys, user = np.unique(np.asarray(users), return_inverse=True)
    item_keys, item = np.unique(np.asarray(items), return_inverse=True)
    row = np.arange(len(ts))
    order = np.lexsort((row, ts, user))
    pair = user[order].astype(np.int64) * len(item_keys) + item[order]
    _, first = np.unique(pair, return_index=True)  # first position in the sorted order
    keep = order[np.sort(first)]
    return user[keep], item[keep], ts[keep], user_keys, item_keys


def kcore_mask(user, item, k):
    """seqrec-lab's kcore_mask, as written."""
    keep = np.ones(len(user), dtype=bool)
    while True:
        user_count = np.bincount(user[keep], minlength=user.max(initial=-1) + 1)
        item_count = np.bincount(item[keep], minlength=item.max(initial=-1) + 1)
        new = keep & (user_count[user] >= k) & (item_count[item] >= k)
        if new.sum() == keep.sum():
            return keep
        keep = new


def time_quantile(ts, q):
    return int(np.quantile(ts, q, method="lower"))


def read_split(data, name):
    with gzip.open(f"{data}/Video_Games.{name}.csv.gz", "rt", encoding="utf-8", newline="") as f:
        return [(r["user_id"], r["parent_asin"], int(r["timestamp"])) for r in csv.DictReader(f)]


def summary(name, values):
    v = np.asarray(values)
    q = np.percentile(v, [10, 25, 50, 75, 90], method="lower")
    print(f"{name}: median {np.median(v):g}, mean {v.mean():.2f}, "
          f"p10/p25/p50/p75/p90 {'/'.join(str(int(x)) for x in q)}, max {v.max()}")


def main(data, zero_core):
    digest = sha256(zero_core)
    if digest != ZERO_CORE_SHA256:
        sys.exit(f"{zero_core}: sha256 {digest}, want {ZERO_CORE_SHA256}")
    users, items, ts, dropped = read_zero_core(zero_core)
    user, item, ts, user_keys, item_keys = build_log(users, items, ts)
    T = time_quantile(ts, 0.9)
    in_train = ts <= T
    T_val = time_quantile(ts[in_train], 0.9)
    core = kcore_mask(user[in_train], item[in_train], 5)
    vocab = np.zeros(len(item_keys), dtype=bool)
    vocab[np.unique(item[in_train][core])] = True
    print(f"0-core: {len(users) + dropped} rows read, {dropped} dropped, {len(ts)} after deduplication")

    test = read_split(data, "test")
    recserve = defaultdict(list)
    for name in ("train", "valid"):
        for u, i, t in read_split(data, name):
            recserve[u].append((t, i))
    for u, i, t in test:
        recserve[u].append((t, i))

    item_index = {k: n for n, k in enumerate(item_keys)}
    user_index = {k: n for n, k in enumerate(user_keys)}
    test_users = {user_index[u] for u, _, _ in test if u in user_index}
    seqrec = defaultdict(list)  # user code -> times of its rows in the vocabulary
    for n in np.flatnonzero(vocab[item] & np.isin(user, list(test_users))):
        seqrec[user[n]].append(ts[n])
    seqrec = {u: np.sort(np.asarray(t)) for u, t in seqrec.items()}

    rs_len, sq_len, repeats = [], [], []
    in_vocab = absent = 0
    for u, i, t in test:
        hist = [x for tt, x in recserve[u] if tt < t]
        rs_len.append(len(hist))
        repeats.append(len(hist) - len(set(hist)))
        times = seqrec.get(user_index.get(u, -1))
        sq_len.append(0 if times is None else int(np.searchsorted(times, t, side="left")))
        absent += u not in user_index
        in_vocab += i in item_index and vocab[item_index[i]]
    rs_len, sq_len = np.asarray(rs_len), np.asarray(sq_len)

    got = {"T": T, "T_val": T_val, "vocabulary": int(vocab.sum()),
           "target_share": round(100 * in_vocab / len(test), 1)}
    print(f"T {T}, T_val {T_val}, vocabulary {got['vocabulary']} items, "
          f"test targets in it {in_vocab} of {len(test)} ({100 * in_vocab / len(test):.2f}%)")
    print(f"test events: {len(test)}; their users absent from the 0-core log: {absent}")
    summary("recserve history length", rs_len)
    summary("seqrec-lab events before, in its vocabulary", sq_len)
    differ = rs_len != sq_len
    print(f"events where they differ: {differ.sum()} ({100 * differ.mean():.2f}%); "
          f"seqrec-lab longer {(sq_len > rs_len).sum()}, shorter {(sq_len < rs_len).sum()}")
    summary("seqrec-lab minus recserve", sq_len - rs_len)
    print(f"repeated items in recserve's histories: {sum(repeats)}, "
          f"in {sum(r > 0 for r in repeats)} events")
    bad = [f"{k} {got[k]}, want {v}" for k, v in EXPECTED.items() if got[k] != v]
    if bad:
        sys.exit("differs from seqrec-lab's figures: " + "; ".join(bad))


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    main(sys.argv[1], sys.argv[2])
