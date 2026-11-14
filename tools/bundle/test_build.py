"""build.py end to end on small splits: a bundle with the fold-in encoder and a trained ranker, then one
rebuilt at a time in the test period with that ranker, both accepted by verify.py; the rebuild uses the
events before its time and no other."""

import gzip
import json

import build  # first: it limits BLAS to one thread before numpy loads
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
