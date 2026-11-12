"""verify.py on a hand-written bundle of twelve items, and on each kind of fault it rejects."""

import json

import numpy as np
import pytest

import verify
from build import sha256, version_of

IDS = [f"i{k}" for k in range(1, 13)]
# Item k's vector is (k, 1 - k % 2), so a user vector (x, y) with 0 <= y < x
# ranks items by their ID, highest first; the top 10 are worked out by hand.
VECTORS = np.array([[0, 0]] + [[k, 1 - k % 2] for k in range(1, 13)], dtype=np.float32)
GOLDEN = [
    {"history": [1], "user_emb": [1.0, 0.0], "top10": [12, 11, 10, 9, 8, 7, 6, 5, 4, 3]},
    {"history": [2, 3], "history_raw": ["i2", "i3"], "user_emb": [2.5, 0.5], "top10": [12, 11, 10, 9, 8, 7, 6, 5, 4, 1]},
]
SWAPPED = ["i1", "i3", "i2"] + IDS[3:]  # i2 and i3 swapped


def write_manifest(d, **fields):
    """Writes manifest.json over the files in d, with fields replacing or adding to the defaults."""
    files = ["golden.jsonl", "items.ids", "items.npy"]
    sums = {name: sha256(f"{d}/{name}") for name in files}
    m = {
        "format_version": 1, "model_version": version_of(sums), "dataset": "test", "train_cutoff_ts": 1649219168790,
        "item_count": len(IDS), "dim": 2, "metric": "dot", "normalized": False, "max_seq_len": 3, "pad_id": 0,
        "user_encoder": "mean", "sha256": sums,
    }
    m.update(fields)
    (d / "manifest.json").write_text(json.dumps(m))
    return m


def write_bundle(d, golden=GOLDEN, ids=IDS):
    (d / "items.ids").write_text("".join(f"{i}\n" for i in ids))
    np.save(d / "items.npy", VECTORS)
    (d / "golden.jsonl").write_text("".join(json.dumps(c) + "\n" for c in golden))
    return write_manifest(d)


def rejection(d):
    with pytest.raises(SystemExit) as e:
        verify.main(str(d))
    return str(e.value.code)


def test_accepts(tmp_path, capsys):
    write_bundle(tmp_path)
    verify.main(str(tmp_path))
    assert capsys.readouterr().out == "ok: 2 golden cases, 12 items, user encoder mean, ranker no\n"


def test_null_history_raw_is_absent(tmp_path, capsys):
    write_bundle(tmp_path, golden=[GOLDEN[0], {**GOLDEN[1], "history_raw": None}])
    verify.main(str(tmp_path))
    assert capsys.readouterr().out.startswith("ok: 2 golden cases")


def test_rejects_checksum(tmp_path):
    write_bundle(tmp_path)
    np.save(tmp_path / "items.npy", VECTORS * 2)
    assert rejection(tmp_path) == "items.npy: checksum mismatch"


@pytest.mark.parametrize("cutoff", [None, 0, -1, 1.649e12, "1649219168790", True])
def test_rejects_cutoff(tmp_path, cutoff):
    write_bundle(tmp_path)
    write_manifest(tmp_path, train_cutoff_ts=cutoff)
    assert rejection(tmp_path).startswith(f"train_cutoff_ts {cutoff!r}: a time in milliseconds is required")


def test_rejects_missing_cutoff(tmp_path):
    m = write_bundle(tmp_path)
    del m["train_cutoff_ts"]
    (tmp_path / "manifest.json").write_text(json.dumps(m))
    assert rejection(tmp_path) == "train_cutoff_ts None: a time in milliseconds is required"


# i2 and i3 swapped in items.ids, with internal IDs and vectors unchanged: only
# history_raw shows it. A raw ID outside items.ids does not map either.
@pytest.mark.parametrize("ids, raw", [(SWAPPED, ["i2", "i3"]), (IDS, ["i2", "i13"])])
def test_rejects_history_raw(tmp_path, ids, raw):
    write_bundle(tmp_path, golden=[GOLDEN[0], {**GOLDEN[1], "history_raw": raw}], ids=ids)
    assert rejection(tmp_path) == "golden 1: history_raw does not map to history through items.ids"


def test_swapped_ids_pass_without_history_raw(tmp_path, capsys):
    write_bundle(tmp_path, golden=[GOLDEN[0], {k: v for k, v in GOLDEN[1].items() if k != "history_raw"}], ids=SWAPPED)
    verify.main(str(tmp_path))
    assert capsys.readouterr().out.startswith("ok:")


@pytest.mark.parametrize("top10", [[12, 11, 10, 9, 8, 7, 6, 5, 3, 4], [12, 11, 10, 9, 8, 7, 6, 5, 4], [12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2]])
def test_rejects_top10(tmp_path, top10):
    write_bundle(tmp_path, golden=[{**GOLDEN[0], "top10": top10}, GOLDEN[1]])
    assert rejection(tmp_path) == "golden 0: top-10 differs"


@pytest.mark.parametrize("name", ["popular.npy", "user_encoder.onnx", "ranker.onnx"])
def test_rejects_unlisted_file(tmp_path, name):
    write_bundle(tmp_path)
    (tmp_path / name).write_bytes(b"")
    assert rejection(tmp_path) == f"{name} is in the directory but not in the manifest"
