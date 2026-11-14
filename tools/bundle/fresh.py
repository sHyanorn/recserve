"""The stream state of internal/fresh in Python (docs/DECISIONS.md D059), for
training a ranker on its features (D060) and for the popularity reference.

Every read takes a request time ts and uses only events before it. An event
of user u on item i at time t is paired with the items of u's at most
PAIR_SPAN latest events before t, except those on i, in both items' rows, at
time t; a user's events at one time are ordered by event ID (event_id).

usage: fresh.py DATA ITEMS_IDS OUT [--category C] [--days 1,3,7,14,30]

writes OUT/popularity_<d>d.npy, the rank of each valid event's item under the
popularity reference with a window of d days (every item ranked by its events
in [ts - d days, ts), ties to the lower internal ID, the history's items left
out, 0 for an item in the history), and prints each window's metrics and the
SHA-256 of its ranks, then the window selected by the registration's rule
(docs/registration/m8-freshness.md, 6.2). It reads the train and valid
splits only.
"""

import argparse
import bisect
import csv
import gzip
import hashlib
import os
import struct
from collections import Counter, defaultdict

import numpy as np

PAIR_SPAN = 5
KIND = "rating"  # dataset.Kind
DAY = 86_400_000
NONE = np.iinfo(np.int64).max  # State.first_ts of an item without events


def event_id(user, item, ts, kind=KIND):
    """history.EventID: the first 8 bytes, little-endian, of the SHA-256 of the
    four fields, each after its length as a little-endian uint32."""
    h = hashlib.sha256()
    for s in (user, item, str(ts), kind):
        b = s.encode()
        h.update(struct.pack("<I", len(b)) + b)
    return struct.unpack("<Q", h.digest()[:8])[0]


class State:
    """The state of rows (user, raw item, ts) in the internal IDs of item_ix.
    Rows, and previous items, that item_ix does not know are left out, as
    fresh.Events leaves them out; a row given twice is one event, as the
    history store keeps it."""

    def __init__(self, rows, item_ix):
        self.n = len(item_ix) + 1
        by_user = defaultdict(set)
        for u, raw, ts in rows:
            by_user[u].add((ts, event_id(u, raw, ts), raw))
        times, pairs, events = defaultdict(list), defaultdict(list), []
        for evs in by_user.values():
            evs = sorted(evs)
            ts_of = [e[0] for e in evs]
            for ts, _, raw in evs:
                i = item_ix.get(raw)
                if i is None:
                    continue
                times[i].append(ts)
                events.append((ts, i))
                j = bisect.bisect_left(ts_of, ts)
                for _, _, p in evs[max(0, j - PAIR_SPAN) : j]:
                    q = item_ix.get(p)
                    if q is not None and q != i:
                        pairs[i].append((ts, q))
                        pairs[q].append((ts, i))
        self.times = {i: np.sort(np.array(t, dtype=np.int64)) for i, t in times.items()}
        self.first_ts = np.full(self.n, NONE, dtype=np.int64)  # each item's first event time
        for i, t in self.times.items():
            self.first_ts[i] = t[0]
        events.sort()
        self.ev_ts = np.array([e[0] for e in events], dtype=np.int64)
        self.ev_item = np.array([e[1] for e in events], dtype=np.int64)
        self.pairs = {}
        for i, ps in pairs.items():
            ps.sort()
            self.pairs[i] = (np.array([p[0] for p in ps], dtype=np.int64), np.array([p[1] for p in ps], dtype=np.int64))

    def count(self, item, lo, hi):
        """item's events in [lo, hi)."""
        t = self.times.get(item)
        return 0 if t is None else int(np.searchsorted(t, hi) - np.searchsorted(t, lo))

    def first(self, item, ts):
        """The time of item's first event before ts, None without one."""
        t = self.times.get(item)
        return int(t[0]) if t is not None and t[0] < ts else None

    def counts(self, ts, w):
        """Every item's events in [ts - w, ts), by internal ID."""
        a, b = np.searchsorted(self.ev_ts, ts - w), np.searchsorted(self.ev_ts, ts)
        return np.bincount(self.ev_item[a:b], minlength=self.n)

    def popular(self, ts, w, n=100):
        """The at most n items with the most events in [ts - w, ts), at least
        one each, ties to the lower ID."""
        c = self.counts(ts, w)
        ids = np.flatnonzero(c)
        return ids[np.lexsort((ids, -c[ids]))[:n]].tolist()

    def row(self, item, ts, n=50):
        """The at most n items paired with item most often before ts, ties to
        the lower ID."""
        p = self.pairs.get(item)
        if p is None:
            return []
        c = Counter(p[1][: np.searchsorted(p[0], ts)].tolist())
        return sorted(c, key=lambda j: (-c[j], j))[:n]


def popularity_rank(state, item, ts, w, history):
    """The rank, from 1, of item among all items by their events in
    [ts - w, ts), ties to the lower ID, the history's items left out; 0 for
    an item in the history."""
    if item in history:
        return 0
    c = state.counts(ts, w).astype(np.int64)
    c[0] = -1
    c[list(history)] = -1
    return int(1 + (c > c[item]).sum() + ((c == c[item]) & (np.arange(len(c)) < item)).sum())


def read_split(data, category, split):
    with gzip.open(f"{data}/{category}.{split}.csv.gz", "rt", newline="") as f:
        return [(r["user_id"], r["parent_asin"], int(r["timestamp"])) for r in csv.DictReader(f)]


def main(data, ids_path, out, category, days):
    with open(ids_path) as f:
        item_ix = {raw: n + 1 for n, raw in enumerate(f.read().split("\n")[:-1])}
    train, valid = read_split(data, category, "train"), read_split(data, category, "valid")
    state = State(train + valid, item_ix)
    seen = defaultdict(list)  # each user's (ts, internal ID), for histories
    for u, raw, ts in train + valid:
        seen[u].append((ts, item_ix[raw]))
    for evs in seen.values():
        evs.sort()
    os.makedirs(out, exist_ok=True)
    scores = []
    for d in days:
        ranks = np.zeros(len(valid), dtype=np.int64)
        for k, (u, raw, ts) in enumerate(valid):
            history = {i for t, i in seen[u] if t < ts}
            ranks[k] = popularity_rank(state, item_ix[raw], ts, d * DAY, history)
        path = f"{out}/popularity_{d}d.npy"
        np.save(path, ranks)
        hit = (ranks >= 1) & (ranks <= 10)
        r20 = ((ranks >= 1) & (ranks <= 20)).mean()
        ndcg = np.where(hit, 1 / np.log2(ranks + 1.0), 0).mean()
        with open(path, "rb") as f:
            digest = hashlib.sha256(f.read()).hexdigest()
        print(f"{d}d: recall@10 {hit.mean():.4f} recall@20 {r20:.4f} NDCG@10 {ndcg:.4f} sha256 {digest}")
        scores.append((d, hit.mean(), ndcg))
    print(f"selected: {select_window(scores)}d")


def select_window(scores):
    """The window of (days, recall@10, NDCG@10) with the highest recall@10
    to four decimals, then the highest NDCG@10, then the fewest days."""
    return min(scores, key=lambda s: (-round(s[1], 4), -s[2], s[0]))[0]


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="The popularity reference on the valid split.")
    p.add_argument("data")
    p.add_argument("items_ids")
    p.add_argument("out")
    p.add_argument("--category", default="Video_Games")
    p.add_argument("--days", default="1,3,7,14,30")
    a = p.parse_args()
    main(a.data, a.items_ids, a.out, a.category, [int(x) for x in a.days.split(",")])
