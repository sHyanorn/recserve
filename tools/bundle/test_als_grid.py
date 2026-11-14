"""als_grid.py on small data: the rank against build.py's exact top k, the history the store would give,
the fold-in against build.py's, the exact solution against implicit's own user step, the selection rule,
and a run that resumes from its partial file."""

import gzip
from types import SimpleNamespace

import als_grid  # first: it limits BLAS to one thread before numpy loads
from als_grid import Requests, exact_vectors, fold_in_vectors, ranks, select
from build import fold_in, fold_in_matrix, top_k
from fresh import event_id

import implicit
import numpy as np
import pytest
import scipy.sparse as sp


def test_ranks_are_the_exact_top_k():
    rng = np.random.default_rng(3)
    vecs = rng.integers(-1, 3, size=(31, 3)).astype(np.float32)  # many ties
    vecs[0] = 0
    vecs[[5, 9]] = 0  # items without vectors still rank, at 0
    users = rng.integers(-2, 3, size=(200, 3)).astype(np.float32)
    users[:10] = 0
    target = rng.integers(1, 31, size=200)
    seen = [set(rng.choice(np.arange(1, 31), size=rng.integers(0, 6), replace=False).tolist()) for _ in range(200)]
    seen[10].add(int(target[10]))
    rows = [k for k, s in enumerate(seen) for _ in s]
    cols = [i for s in seen for i in s]
    m = sp.csr_matrix((np.ones(len(rows), dtype=bool), (rows, cols)), shape=(200, 31))
    req = SimpleNamespace(seen=m, target=target, target_seen=np.array([t in s for t, s in zip(target, seen)]))
    got = ranks(req, vecs, users, batch=64)
    for k in range(200):
        top = [] if not users[k].any() else top_k(vecs, users[k], seen[k], als_grid.K)
        want = top.index(target[k]) + 1 if target[k] in top and target[k] not in seen[k] else 0
        assert got[k] == want, k
    assert (got[:11] == 0).all() and (got > 0).sum() > 50 and (got == 0).sum() > 30


def test_history_is_the_store_s(monkeypatch):
    monkeypatch.setattr(als_grid, "MAX_SEQ_LEN", 2)
    ix = {f"i{k}": k for k in range(1, 6)}
    train = [("u", "i1", 1), ("u", "i1", 1), ("u", "i2", 2), ("u", "i3", 2), ("u", "i4", 2)]
    valid = [("u", "i5", 2), ("u", "i2", 3), ("v", "i1", 3)]
    req = Requests(train, valid, ix)
    # At 2 only the event at 1, stored once; at 3 the last two of the four at 2, by event ID.
    assert req.last[0].toarray().tolist() == [[0, 1, 0, 0, 0, 0]]
    last = sorted((event_id("u", f"i{k}", 2), k) for k in range(2, 6))[-2:]
    assert sorted(req.last[1].indices.tolist()) == sorted(k for _, k in last)
    assert req.seen[1].indices.tolist() == [1, 2, 3, 4, 5]
    assert req.target_seen.tolist() == [False, True, False]
    assert req.last[2].nnz == 0 and req.target.tolist() == [5, 2, 1]


def test_fold_in_vectors_are_build_s():
    rng = np.random.default_rng(4)
    vecs = rng.normal(size=(6, 3)).astype(np.float32)
    vecs[0] = 0
    ix = {f"i{k}": k for k in range(1, 6)}
    train = [("u", "i1", 1), ("u", "i3", 2), ("u", "i1", 3), ("w", "i2", 1)]
    req = Requests(train, [("u", "i4", 4), ("w", "i5", 2)], ix)
    m = fold_in_matrix(vecs, 0.3)
    got = fold_in_vectors(req, vecs, 0.3)
    np.testing.assert_array_equal(got[0], fold_in(m, vecs, [1, 3, 1]))  # i1 twice
    np.testing.assert_array_equal(got[1], fold_in(m, vecs, [2]))


def test_exact_vectors_are_implicit_s_user_step():
    rng = np.random.default_rng(5)
    ui = sp.csr_matrix((rng.random((40, 30)) < 0.2).astype(np.float32))
    model = implicit.cpu.als.AlternatingLeastSquares(factors=4, regularization=0.1, alpha=10.0, iterations=5, random_state=1, num_threads=1)
    model.fit(ui, show_progress=False)
    vecs = np.vstack([np.zeros((1, 4), dtype=np.float32), model.item_factors])
    hists = [[3, 7, 7, 20], [1], [5, 6, 8, 9, 30]]  # item IDs from 1; 7 twice counts once
    last = sp.csr_matrix([[h.count(i) for i in range(31)] for h in hists], dtype=np.float64)
    req = SimpleNamespace(last=last)
    got = exact_vectors(req, vecs, 10.0, 0.1)
    for k, h in enumerate(hists):
        row = sp.csr_matrix((np.ones(len(set(h)), dtype=np.float32), ([0] * len(set(h)), sorted(i - 1 for i in set(h)))), shape=(1, 30))
        np.testing.assert_allclose(got[k], model.recalculate_user(0, row), rtol=1e-4, atol=1e-6)
    # With alpha 1 and no item twice, the fold-in is the exact solution (D058).
    req = SimpleNamespace(last=last[[1, 2]])
    np.testing.assert_allclose(exact_vectors(req, vecs, 1.0, 0.1), fold_in_vectors(req, vecs, 0.1), rtol=1e-5, atol=1e-7)


def test_selection_rule():
    def row(f, a, r, r10, ndcg):
        return {"factors": f, "alpha": a, "regularization": r, "recall@10": r10, "ndcg@10": ndcg}

    rows = [row(64, 1, 0.1, 0.02341, 0.010), row(128, 1, 0.1, 0.02338, 0.011), row(64, 10, 0.1, 0.0233, 0.02)]
    assert select(rows) is rows[1]  # 0.0234 both: the higher NDCG@10
    rows = [row(128, 1, 1, 0.0234, 0.01), row(64, 10, 0.01, 0.0234, 0.01), row(64, 10, 0.1, 0.0234, 0.01)]
    assert select(rows) is rows[2]  # fewer factors, then the lower alpha, then the higher regularization
    rows.append(row(64, 1, 0.01, 0.0234, 0.01))
    assert select(rows) is rows[3]


def write_split(d, name, rows):
    with gzip.open(d / f"T.{name}.csv.gz", "wt") as f:
        f.write("user_id,parent_asin,rating,timestamp\n")
        f.writelines(f"{u},{i},5.0,{ts}\n" for u, i, ts in rows)


@pytest.fixture
def data(tmp_path, monkeypatch):
    monkeypatch.setattr(als_grid, "FACTORS", (2,))
    monkeypatch.setattr(als_grid, "ALPHAS", (1.0, 10.0))
    monkeypatch.setattr(als_grid, "REGULARIZATIONS", (0.1,))
    rng = np.random.default_rng(6)
    write_split(tmp_path, "train", [(f"u{rng.integers(30)}", f"i{rng.integers(12)}", ts) for ts in range(1, 400)])
    valid = [(f"u{rng.integers(30)}", f"i{rng.integers(12)}", ts) for ts in range(400, 460)]
    write_split(tmp_path, "valid", valid)
    (tmp_path / "items.ids").write_text("".join(f"i{k}\n" for k in range(12)))
    return tmp_path, len(valid)


def test_run_writes_the_grid_and_resumes(data):
    d, n_valid = data
    out = d / "out"
    out.mkdir()
    # A setting already in the partial file is not run again.
    (out / "als-grid.partial.tsv").write_text("2\t10.0\t0.1\t0.0\t0.0\t0.0\t0\t1.0\t0.5\n")
    als_grid.main(str(d), str(d / "items.ids"), str(out), "T", 1)
    lines = (out / "als-grid.tsv").read_text().splitlines()
    assert lines[0].split("\t") == als_grid.COLUMNS + ["selected"]
    assert [ln.split("\t")[:3] for ln in lines[1:]] == [["2", "1", "0.1"], ["2", "10", "0.1"]]
    assert lines[2].split("\t")[3:6] == ["0.000000"] * 3 and lines[2].endswith("\t")
    assert lines[1].endswith("\tyes")  # above the 0 kept, or tied and with the lower alpha
    with gzip.open(out / "valid-ranks.txt.gz", "rt") as f:
        r = [int(x) for x in f.read().split()]
    assert len(r) == n_valid and all(0 <= x <= als_grid.K for x in r)
    assert not (out / "als-grid.partial.tsv").exists()
    assert (out / "als-grid.txt").read_text().startswith("selected: factors 2, alpha 1, regularization 0.1")


def test_run_stops_when_the_refit_differs(data):
    d, _ = data
    out = d / "out"
    out.mkdir()
    (out / "als-grid.partial.tsv").write_text("2\t10.0\t0.1\t1.0\t1.0\t1.0\t0\t1.0\t0.5\n")
    with pytest.raises(SystemExit, match="refit"):
        als_grid.main(str(d), str(d / "items.ids"), str(out), "T", 1)
