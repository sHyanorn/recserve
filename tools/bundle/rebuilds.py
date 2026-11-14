"""Bundles rebuilt every DAYS days through the test period (docs/registration/m8-freshness.md, section 4).

usage: rebuilds.py DATA OUT --days P --ranker-from B [--category C] [--jobs N]

For T_k = START + k P days, every k >= 0 with T_k not after the latest test
event, builds into OUT the bundle of build.py --encoder onnx --cutoff T_k
--ranker-from B: the category's ALS settings on every event before T_k,
carrying B's ranker and stream window. A rerun skips the times OUT already
holds a bundle for, with B's ranker.
"""

import argparse
import glob
import json
import os
from multiprocessing import get_context

import build

START = 1658002729837  # the test split's start, in both categories (section 2)
DAY = 86_400_000


def times(data, category, days):
    """The rebuild times through the latest test event."""
    last = max(ts for _, _, ts in build.read_split(data, "test", category))
    return list(range(START, last + 1, days * DAY))


def built(out, ranker_from):
    """The rebuild times of the bundles in out that carry the ranker of ranker_from."""
    with open(f"{ranker_from}/manifest.json") as f:
        source = json.load(f)["model_version"]
    done = set()
    for path in glob.glob(f"{out}/*/manifest.json"):
        with open(path) as f:
            b = json.load(f)["build"]
        if b.get("ranker", {}).get("from") == source:
            done.add(b["cutoff"])
    return done


def rebuild(args):
    data, out, category, t, ranker_from = args
    try:
        build.main(data, out, "onnx", False, category, cutoff_at=t, ranker_from=ranker_from)
    except SystemExit as e:  # a worker that exits would leave the pool waiting for its task
        raise RuntimeError(f"rebuild at {t}: {e}") from None


def main(data, out, days, ranker_from, category, jobs):
    todo = [t for t in times(data, category, days) if t not in built(out, ranker_from)]
    with get_context("spawn").Pool(jobs) as pool:
        pool.map(rebuild, [(data, out, category, t, ranker_from) for t in todo], chunksize=1)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Bundles rebuilt through the test period.")
    p.add_argument("data")
    p.add_argument("out")
    p.add_argument("--days", type=int, required=True)
    p.add_argument("--ranker-from", required=True, metavar="B")
    p.add_argument("--category", default="Video_Games")
    p.add_argument("--jobs", type=int, default=os.cpu_count())
    a = p.parse_args()
    main(a.data, a.out, a.days, a.ranker_from, a.category, a.jobs)
