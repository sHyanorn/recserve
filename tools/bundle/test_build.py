"""build.py end to end on small splits: a bundle with the fold-in encoder and a trained ranker, then one
rebuilt at a time in the test period with that ranker, both accepted by verify.py; the rebuild uses the
events before its time and no other. Rankers trained on the stream state, over six features and eight,
and a rebuild with one: their golden inputs are the stream state's."""

import gzip
import json

import build  # first: it limits BLAS to one thread before numpy loads
import fresh
import rebuilds
import verify

import numpy as np
import pytest

CUTOFF = 13_000


def write_splits(d):
    """Splits of 120 users over 40 items: train at times 1 to 9,999, valid 10,000 to 11,999, test from
    12,000; item "early" has test events only before CUTOFF, "late" only after it."""
    rng = np.random.default_rng(7)
    splits = {"train": [], "valid": [], "test": []}
    for u in range(120):
        for name, lo, hi, n in (("train", 1, 10_000, 12), ("valid", 10_000, 12_000, 2), ("test", 12_000, 14_000, 2)):
            for ts in sorted(rng.choice(np.arange(lo, hi), size=n, replace=False)):
                splits[name].append((f"u{u}", f"i{rng.integers(40)}", int(ts)))
    for u in range(0, 120, 3):
        splits["test"] += [(f"u{u}", "early", 12_500 + u), (f"u{u}", "late", CUTOFF + u)]
    for name, rows in splits.items():
        with gzip.open(d / f"T.{name}.csv.gz", "wt") as f:
            f.write("user_id,parent_asin,rating,timestamp\n")
            f.writelines(f"{u},{i},5.0,{ts}\n" for u, i, ts in rows)


def built(out):
    (d,) = out.iterdir()
    verify.main(str(d))
    return d, json.loads((d / "manifest.json").read_text())


def test_rebuild_with_another_bundle_s_ranker(tmp_path, capsys):
    write_splits(tmp_path)
    build.main(str(tmp_path), str(tmp_path / "b1"), "onnx", True, "T")
    b1, m1 = built(tmp_path / "b1")
    build.main(str(tmp_path), str(tmp_path / "r"), "onnx", False, "T", cutoff_at=CUTOFF, ranker_from=str(b1))
    r, mr = built(tmp_path / "r")
    assert m1["train_cutoff_ts"] < 12_000  # the valid split, the ranker's labels
    assert mr["build"]["cutoff"] == CUTOFF and 12_500 <= mr["train_cutoff_ts"] < CUTOFF
    assert mr["build"]["ranker"]["from"] == m1["model_version"] and mr["ranker_features"] == m1["ranker_features"]
    assert (r / "ranker.onnx").read_bytes() == (b1 / "ranker.onnx").read_bytes()
    ids = (r / "items.ids").read_text().split()
    assert ids == (b1 / "items.ids").read_text().split()
    v1, vr = np.load(b1 / "items.npy"), np.load(r / "items.npy")
    early, late = ids.index("early") + 1, ids.index("late") + 1
    assert not v1[early].any() and vr[early].any()  # test events before the cutoff are used
    assert not v1[late].any() and not vr[late].any()  # those at or after it are not
    assert capsys.readouterr().out.count("ok:") == 2


def test_rejects_a_ranker_that_does_not_match_its_manifest(tmp_path):
    write_splits(tmp_path)
    build.main(str(tmp_path), str(tmp_path / "b1"), "onnx", True, "T")
    (b1,) = (tmp_path / "b1").iterdir()
    (b1 / "ranker.onnx").write_bytes((b1 / "ranker.onnx").read_bytes() + b"\0")
    with pytest.raises(SystemExit, match="no ranker"):
        build.main(str(tmp_path), str(tmp_path / "r"), "onnx", False, "T", cutoff_at=CUTOFF, ranker_from=str(b1))


def test_stream_rankers_and_a_rebuild_with_one(tmp_path, capsys):
    write_splits(tmp_path)
    build.main(str(tmp_path), str(tmp_path / "b1"), "onnx", True, "T")
    build.main(str(tmp_path), str(tmp_path / "s6"), "onnx", True, "T", stream_days=1)
    build.main(str(tmp_path), str(tmp_path / "s8"), "onnx", True, "T", stream_days=1, recency=True)
    b1, m1 = built(tmp_path / "b1")
    s6, m6 = built(tmp_path / "s6")
    s8, m8 = built(tmp_path / "s8")
    assert "stream" not in m1
    assert m6["stream"] == m8["stream"] == {"popular_window_ms": 86_400_000}
    assert m6["ranker_features"] == build.RANKER_FEATURES
    assert m8["ranker_features"] == build.RANKER_FEATURES + build.RECENCY_FEATURES
    # B1 with another ranker: the same vectors and tables, and the same events used.
    for name in ("items.npy", "cooc_ids.npy", "popular.npy", "user_encoder.onnx"):
        assert (s8 / name).read_bytes() == (s6 / name).read_bytes() == (b1 / name).read_bytes()
    assert m1["train_cutoff_ts"] == m6["train_cutoff_ts"] == m8["train_cutoff_ts"]
    w1, w6, w8 = (m["build"]["ranker"]["weights"] for m in (m1, m6, m8))
    assert len(w1) == len(w6) == 6 and len(w8) == 8 and w1 != w6

    # Every golden case reads the stream state of the splits' events up to the cutoff, just after it.
    rows = [r for split in ("train", "valid", "test") for r in build.read_split(str(tmp_path), split, "T")]
    ids = (s8 / "items.ids").read_text().split()
    ts = m8["train_cutoff_ts"] + 1
    state = fresh.State([r for r in rows if r[2] < ts], {raw: n + 1 for n, raw in enumerate(ids)})
    for line in (s8 / "golden.jsonl").read_text().splitlines():
        c = json.loads(line)
        assert c["ts"] == ts and c["popular"] == state.popular(ts, 86_400_000)
        assert c["cooc_rows"] == [state.row(i, ts) for i in c["history"][::-1][: build.COOC_SEEDS]]
        assert c["window_counts"] == [state.count(i, ts - 86_400_000, ts) for i in c["candidates"]]
        assert c["first_ts"] == [state.first(i, ts) for i in c["candidates"]]
    assert all("window_counts" in json.loads(line) for line in (s6 / "golden.jsonl").read_text().splitlines())

    build.main(str(tmp_path), str(tmp_path / "r"), "onnx", False, "T", cutoff_at=CUTOFF, ranker_from=str(s8))
    r, mr = built(tmp_path / "r")
    assert mr["stream"] == m8["stream"] and mr["ranker_features"] == m8["ranker_features"]
    assert (r / "ranker.onnx").read_bytes() == (s8 / "ranker.onnx").read_bytes()
    assert {json.loads(line)["ts"] for line in (r / "golden.jsonl").read_text().splitlines()} == {mr["train_cutoff_ts"] + 1}
    assert capsys.readouterr().out.count("ok:") == 4


def test_rebuilds_through_the_test_period_and_resume(tmp_path, monkeypatch, capsys):
    write_splits(tmp_path)
    build.main(str(tmp_path), str(tmp_path / "b1"), "onnx", True, "T")
    (b1,) = (tmp_path / "b1").iterdir()
    monkeypatch.setattr(rebuilds, "START", 12_000)
    monkeypatch.setattr(rebuilds, "DAY", 500)
    out = tmp_path / "r"
    rebuilds.main(str(tmp_path), str(out), 1, str(b1), "T", 2)
    last = max(ts for _, _, ts in build.read_split(str(tmp_path), "test", "T"))
    cutoffs = sorted(json.loads((d / "manifest.json").read_text())["build"]["cutoff"] for d in out.iterdir())
    assert cutoffs == list(range(12_000, last + 1, 500)) and cutoffs[-1] + 500 > last
    rebuilds.main(str(tmp_path), str(out), 1, str(b1), "T", 2)  # nothing left to build
    assert len(list(out.iterdir())) == len(cutoffs)
