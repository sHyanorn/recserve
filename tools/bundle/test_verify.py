"""verify.py on a hand-written bundle of twelve items, and on faults it rejects as the service's bundle.Load
does: a checksum, the manifest's fields, train_cutoff_ts, items.ids, the .npy files, history_raw, the golden
cases, an optional file the manifest does not list."""

import json

import numpy as np
import pytest

import verify
from build import mean_vector, sha256, top_k, version_of

IDS = [f"i{k}" for k in range(1, 13)]
# Item k's vector is (k, 1 - k % 2), so a user vector (x, y) with 0 <= y < x
# ranks items by their ID, highest first; the top 10 are worked out by hand.
VECTORS = np.array([[0, 0]] + [[k, 1 - k % 2] for k in range(1, 13)], dtype=np.float32)
GOLDEN = [
    {"history": [1], "user_emb": [1.0, 0.0], "top10": [12, 11, 10, 9, 8, 7, 6, 5, 4, 3]},
    {"history": [2, 3], "history_raw": ["i2", "i3"], "user_emb": [2.5, 0.5], "top10": [12, 11, 10, 9, 8, 7, 6, 5, 4, 1]},
]
SWAPPED = ["i1", "i3", "i2"] + IDS[3:]  # i2 and i3 swapped


def write_manifest(d, files=("golden.jsonl", "items.ids", "items.npy"), **fields):
    """Writes manifest.json listing files in d, with fields replacing or adding to the defaults."""
    sums = {name: sha256(f"{d}/{name}") for name in files}
    m = {
        "format_version": 1, "model_version": version_of(sums), "dataset": "test", "train_cutoff_ts": 1649219168790,
        "item_count": len(IDS), "dim": 2, "metric": "dot", "normalized": False, "max_seq_len": 3, "pad_id": 0,
        "user_encoder": "mean", "sha256": sums,
    }
    m.update(fields)
    (d / "manifest.json").write_text(json.dumps(m))
    return m


def write_bundle(d, golden=GOLDEN, ids=IDS, vectors=VECTORS):
    (d / "items.ids").write_bytes("".join(f"{i}\n" for i in ids).encode())
    np.save(d / "items.npy", vectors)
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


def test_rejects_top10(tmp_path):
    write_bundle(tmp_path, golden=[{**GOLDEN[0], "top10": [12, 11, 10, 9, 8, 7, 6, 5, 3, 4]}, GOLDEN[1]])
    assert rejection(tmp_path) == "golden 0: top-10 differs"


@pytest.mark.parametrize("name", ["popular.npy", "user_encoder.onnx", "ranker.onnx"])
def test_rejects_unlisted_file(tmp_path, name):
    write_bundle(tmp_path)
    (tmp_path / name).write_bytes(b"")
    assert rejection(tmp_path) == f"{name} is in the directory but not in the manifest"


def test_rejects_repeated_id(tmp_path):
    write_bundle(tmp_path, golden=[GOLDEN[0]], ids=IDS[:-1] + ["i11"])
    assert rejection(tmp_path) == "items.ids repeats 'i11'"


def test_rejects_empty_id(tmp_path):
    write_bundle(tmp_path, golden=[GOLDEN[0]], ids=IDS[:2] + [""] + IDS[3:])
    assert rejection(tmp_path) == "items.ids line 3 is empty"


def test_rejects_id_with_carriage_return(tmp_path):
    """The service splits items.ids at "\\n" only, so a "\\r\\n" file gives IDs ending in "\\r"."""
    write_bundle(tmp_path, ids=[f"{i}\r" for i in IDS])
    assert rejection(tmp_path) == "golden 1: history_raw does not map to history through items.ids"


@pytest.mark.parametrize("field, value, message", [
    ("format_version", 2, "format_version 2, want 1"),
    ("format_version", None, "format_version None, want 1"),
    ("metric", "cosine", "unsupported metric 'cosine', pad_id 0 or user_encoder 'mean'"),
    ("pad_id", 1, "unsupported metric 'dot', pad_id 1 or user_encoder 'mean'"),
    ("user_encoder", "sum", "unsupported metric 'dot', pad_id 0 or user_encoder 'sum'"),
    ("max_seq_len", 0, "manifest: item_count, dim and max_seq_len are required"),
    ("dim", 2.0, "manifest: dim 2.0 is not a JSON int"),
    ("normalized", "no", "manifest: normalized 'no' is not a JSON bool"),
])
def test_rejects_manifest_field(tmp_path, field, value, message):
    m = write_bundle(tmp_path)
    if value is None:
        del m[field]
    else:
        m[field] = value
    (tmp_path / "manifest.json").write_text(json.dumps(m))
    assert rejection(tmp_path) == message


def test_missing_pad_id_and_empty_ranker_features_pass(tmp_path, capsys):
    """Go reads a missing pad_id as 0 and an empty ranker_features as none."""
    m = write_bundle(tmp_path)
    del m["pad_id"]
    m["ranker_features"] = []
    (tmp_path / "manifest.json").write_text(json.dumps(m))
    verify.main(str(tmp_path))
    assert capsys.readouterr().out.startswith("ok:")


def test_rejects_unlisted_required_file(tmp_path):
    write_bundle(tmp_path)
    write_manifest(tmp_path, files=("items.ids", "items.npy"))
    assert rejection(tmp_path) == "golden.jsonl missing from the manifest"


def test_rejects_listed_path(tmp_path):
    write_bundle(tmp_path)
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "x").write_bytes(b"")
    write_manifest(tmp_path, files=("golden.jsonl", "items.ids", "items.npy", "sub/x"))
    assert rejection(tmp_path) == "manifest lists 'sub/x'"


def test_rejects_no_golden_cases(tmp_path):
    write_bundle(tmp_path, golden=[])
    assert rejection(tmp_path) == "golden.jsonl has no cases"


@pytest.mark.filterwarnings("ignore:invalid value:RuntimeWarning")
@pytest.mark.parametrize("bad", [np.nan, np.inf, -np.inf])
def test_rejects_non_finite_vector(tmp_path, bad):
    """The golden case is computed on the vectors as they are, with build.top_k, as an exporter would."""
    vecs = VECTORS.copy()
    vecs[12, 1] = bad
    uv = mean_vector(vecs, [1], 3)
    write_bundle(tmp_path, golden=[{"history": [1], "user_emb": uv.tolist(), "top10": top_k(vecs, uv, {1}, 10)}], vectors=vecs)
    assert rejection(tmp_path) == "items.npy: non-finite value"


@pytest.mark.parametrize("array, message", [
    (np.asfortranarray(VECTORS), "items.npy: dtype <f4, Fortran order, want <f4 in C order"),
    (VECTORS.astype(">f4"), "items.npy: dtype >f4, want <f4 in C order"),
    (VECTORS.astype(np.float64), "items.npy: dtype <f8, want <f4 in C order"),
])
def test_rejects_npy_layout(tmp_path, array, message):
    write_bundle(tmp_path, vectors=array)
    assert rejection(tmp_path) == message


def test_rejects_npy_trailing_bytes(tmp_path):
    write_bundle(tmp_path)
    with open(tmp_path / "items.npy", "ab") as f:
        f.write(bytes(4))
    write_manifest(tmp_path)
    assert rejection(tmp_path) == "items.npy: 108 data bytes for shape (13, 2)"


def write_sources(d, cooc, popular):
    write_bundle(d)
    np.save(d / "cooc_ids.npy", cooc)
    np.save(d / "popular.npy", popular)
    write_manifest(d, files=("golden.jsonl", "items.ids", "items.npy", "cooc_ids.npy", "popular.npy"))


COOC = np.zeros((13, 50), np.int64)
POPULAR = np.array([12, 1, 5], np.int64)


def test_accepts_sources(tmp_path, capsys):
    write_sources(tmp_path, COOC, POPULAR)
    verify.main(str(tmp_path))
    assert capsys.readouterr().out.startswith("ok:")


@pytest.mark.parametrize("cooc, popular, message", [
    (COOC[:, :49], POPULAR, "cooc_ids.npy: shape (13, 49), want (13, 50), or ID range"),
    (COOC[:12], POPULAR, "cooc_ids.npy: shape (12, 50), want (13, 50), or ID range"),
    (COOC.ravel(), POPULAR, "cooc_ids.npy: shape (650,), want (13, 50), or ID range"),
    (COOC + 13, POPULAR, "cooc_ids.npy: shape (13, 50), want (13, 50), or ID range"),
    (COOC.astype(np.int32), POPULAR, "cooc_ids.npy: dtype <i4, want <i8 in C order"),
    (COOC, POPULAR[None, :], "popular.npy: shape (1, 3), want 1-D, or ID range"),
    (COOC, np.array([12, 0], np.int64), "popular.npy: shape (2,), want 1-D, or ID range"),
    (COOC, POPULAR.astype(np.float32), "popular.npy: dtype <f4, want <i8 in C order"),
])
def test_rejects_sources(tmp_path, cooc, popular, message):
    write_sources(tmp_path, cooc, popular)
    assert rejection(tmp_path) == message


@pytest.mark.parametrize("case, message", [
    ({**GOLDEN[0], "note": ""}, "golden 0: not an object, or fields other than"),
    ({**GOLDEN[0], "top10": GOLDEN[0]["top10"][:9]}, "golden 0: needs a history, 2 user_emb values and 10 top10 IDs"),
    ({**GOLDEN[0], "top10": [float(i) for i in GOLDEN[0]["top10"]]}, "golden 0: needs a history"),
    ({**GOLDEN[0], "user_emb": [1.0]}, "golden 0: needs a history"),
    ({**GOLDEN[0], "user_emb": [True, False]}, "golden 0: needs a history"),
    ({**GOLDEN[0], "candidates": []}, "golden 0: candidates without a ranker"),
])
def test_rejects_golden_case(tmp_path, case, message):
    write_bundle(tmp_path, golden=[case])
    assert rejection(tmp_path).startswith(message)


def test_null_candidates_are_absent(tmp_path, capsys):
    write_bundle(tmp_path, golden=[{**GOLDEN[0], "candidates": None, "features": None}])
    verify.main(str(tmp_path))
    assert capsys.readouterr().out.startswith("ok: 1 golden cases")


@pytest.mark.parametrize("pad, ok", [(0, True), (1, False)])
def test_golden_line_limit(tmp_path, capsys, pad, ok):
    """The service reads lines of at most 2^20 - 1 bytes; JSON allows the trailing spaces."""
    line = json.dumps(GOLDEN[0])
    write_bundle(tmp_path, golden=[])
    (tmp_path / "golden.jsonl").write_text(line + " " * (verify.MAX_LINE - 1 - len(line) + pad) + "\n")
    write_manifest(tmp_path)
    if ok:
        verify.main(str(tmp_path))
        assert capsys.readouterr().out.startswith("ok: 1 golden cases")
    else:
        assert rejection(tmp_path) == "golden 0: 1048576 bytes, the service reads lines of at most 1048575"
