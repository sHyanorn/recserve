"""Write the ONNX models and the ranker golden the Go tests use, and their SHA256SUMS.

usage: testdata.py DIR   (DIR is internal/onnx/onnxtest/testdata)

mean.onnx is build.py's mean encoder over 12 items of 2 dimensions, item i
being (cos i, sin i) / i (onnxtest.MeanVectors); zero.onnx is the same graph
over zero vectors, so its output is always zero. fold_in.onnx is build.py's
fold-in encoder over the same items with regularization FOLD_IN_REG
(onnxtest.FoldIn). slow.onnx applies Sin 300
times to a float32 vector x of any length, giving y; a run on a long vector
lasts long enough to be terminated.

The rankers take features float32 [C, 6]. linear_ranker.onnx is build.py's
linear ranker with LINEAR_W and LINEAR_B (onnxtest.LinearW, LinearB);
cross_row_ranker.onnx subtracts the mean score of all rows from each, so a
row's score depends on the others; next_row_ranker.onnx adds the next row's
score to each, which a change of row 0 alone does not show; column_ranker.onnx outputs [C, 1];
slow_ranker.onnx applies Sin 300 times to the features and sums each row.

ranker_golden.jsonl is golden.jsonl for bundletest's bundle with a ranker:
its two cases, histories [1] and [2, 5], over mean.onnx's items, item 1
co-occurring with 3 then 2 and the popular items 3 then 1, with their
candidates and features as build.py computes them, for the service to
recompute.

stream_ranker.onnx is the linear ranker over features float32 [C, 8], the
six and the recency features, with STREAM_W and LINEAR_B, and
stream_golden.jsonl the same two cases for a bundle with a stream window
of STREAM_DAYS days (D060): their sources, features and stream inputs come
from fresh.State over STREAM_EVENTS at STREAM_TS, which differ from the
bundle's own tables. The same inputs give the same bytes.
"""

import hashlib
import json
import os
import sys

import numpy as np
from onnx import TensorProto, helper, numpy_helper

from build import (COOC_SEEDS, RANKER_FEATURES, fold_in_encoder, fold_in_matrix, linear_ranker, mean_encoder, mean_vector,
                   model, ranker_input, streamed, tables, top_k)
from fresh import DAY, State

MEAN_ITEMS = 12
FOLD_IN_REG = 0.5
SIN_NODES = 300
LINEAR_W = [1, 2, -1, 0.5, 0.25, -0.5]
LINEAR_B = 0.125
STREAM_W = LINEAR_W + [0.75, -0.25]
STREAM_DAYS = 7
STREAM_TS = 10 * DAY
# (user, item, ts): the event at STREAM_TS is not before it.
STREAM_EVENTS = [("a", "item1", DAY), ("a", "item3", 2 * DAY), ("a", "item2", 3 * DAY), ("b", "item3", 3 * DAY),
                 ("e", "item1", 4 * DAY), ("e", "item7", 4 * DAY + 1), ("b", "item1", 5 * DAY), ("c", "item4", 6 * DAY),
                 ("c", "item3", 9 * DAY + 7), ("d", "item6", STREAM_TS)]


def mean_vectors():
    i = np.arange(1, MEAN_ITEMS + 1, dtype=np.float64)
    vecs = np.zeros((MEAN_ITEMS + 1, 2), dtype=np.float32)
    vecs[1:, 0] = np.cos(i) / i
    vecs[1:, 1] = np.sin(i) / i
    return vecs


def slow():
    names = ["x"] + [f"h{n}" for n in range(1, SIN_NODES)] + ["y"]
    nodes = [helper.make_node("Sin", [a], [b]) for a, b in zip(names, names[1:])]
    return model(helper.make_graph(
        nodes, "slow",
        [helper.make_tensor_value_info("x", TensorProto.FLOAT, ["N"])],
        [helper.make_tensor_value_info("y", TensorProto.FLOAT, ["N"])]))


def with_nodes(m, name, nodes, out_shape, init=()):
    """m's graph with nodes appended after its output, which they consume as
    "linear" and replace with a new "score" of out_shape."""
    g = m.graph
    body = [helper.make_node(n.op_type, n.input, ["linear" if o == "score" else o for o in n.output], **{
        a.name: helper.get_attribute_value(a) for a in n.attribute}) for n in g.node]
    return model(helper.make_graph(
        body + nodes, name, list(g.input),
        [helper.make_tensor_value_info("score", TensorProto.FLOAT, out_shape)],
        list(g.initializer) + list(init)))


def cross_row_ranker():
    return with_nodes(linear_ranker(LINEAR_W, LINEAR_B), "cross_row_ranker", [
        helper.make_node("ReduceMean", ["linear"], ["mean"], axes=[0], keepdims=0),
        helper.make_node("Sub", ["linear", "mean"], ["score"]),
    ], ["C"])


def next_row_ranker():
    """Each row's score adds the next row's: changing row 0 alone moves no
    other row, so only a check that changes later rows sees it."""
    return with_nodes(linear_ranker(LINEAR_W, LINEAR_B), "next_row_ranker", [
        helper.make_node("Slice", ["linear", "one", "end"], ["next"]),
        helper.make_node("Pad", ["next", "pad"], ["padded"]),
        helper.make_node("Add", ["linear", "padded"], ["score"]),
    ], ["C"], [numpy_helper.from_array(np.array(v, dtype=np.int64), n)
               for n, v in (("one", [1]), ("end", [np.iinfo(np.int64).max]), ("pad", [0, 1]))])


def column_ranker():
    return with_nodes(linear_ranker(LINEAR_W, LINEAR_B), "column_ranker", [
        helper.make_node("Unsqueeze", ["linear", "axis1"], ["score"]),
    ], ["C", 1], [numpy_helper.from_array(np.array([1], dtype=np.int64), "axis1")])


def slow_ranker():
    f = len(RANKER_FEATURES)
    names = ["features"] + [f"h{n}" for n in range(1, SIN_NODES + 1)]
    nodes = [helper.make_node("Sin", [a], [b]) for a, b in zip(names, names[1:])]
    nodes.append(helper.make_node("ReduceSum", [names[-1], "axis1"], ["score"], keepdims=0))
    return model(helper.make_graph(
        nodes, "slow_ranker",
        [helper.make_tensor_value_info("features", TensorProto.FLOAT, ["C", f])],
        [helper.make_tensor_value_info("score", TensorProto.FLOAT, ["C"])],
        [numpy_helper.from_array(np.array([1], dtype=np.int64), "axis1")]))


def ranker_golden(stream=False):
    vecs = mean_vectors()
    cooc_ids = np.zeros((MEAN_ITEMS + 1, 50), dtype=np.int64)
    cooc_ids[1, :2] = [3, 2]
    src = tables(cooc_ids, [3, 1])
    if stream:
        state = State(STREAM_EVENTS, {f"item{k}": k for k in range(1, MEAN_ITEMS + 1)})
        src = streamed(state, STREAM_TS, STREAM_DAYS * DAY, recency=True)
    lines = []
    for hist in ([1], [2, 5]):
        uv = mean_vector(vecs, hist)
        ids, _, x = ranker_input(vecs, src, hist, uv)
        case = {"history": hist, "user_emb": uv.tolist(), "top10": top_k(vecs, uv, set(hist), 10),
                "candidates": ids.tolist(), "features": x.tolist()}
        if stream:
            _, counts, first = src.recency
            case |= {"ts": STREAM_TS, "cooc_rows": [src.row(i) for i in hist[::-1][:COOC_SEEDS]], "popular": src.popular,
                     "window_counts": counts[ids].tolist(), "first_ts": [int(t) if t < STREAM_TS else None for t in first[ids]]}
        lines.append(json.dumps(case) + "\n")
    return "".join(lines).encode()


def main(d):
    models = {
        "mean.onnx": mean_encoder(mean_vectors()),
        "zero.onnx": mean_encoder(np.zeros((MEAN_ITEMS + 1, 2), dtype=np.float32)),
        "fold_in.onnx": fold_in_encoder(mean_vectors(), fold_in_matrix(mean_vectors(), FOLD_IN_REG)),
        "slow.onnx": slow(),
        "linear_ranker.onnx": linear_ranker(LINEAR_W, LINEAR_B),
        "cross_row_ranker.onnx": cross_row_ranker(),
        "next_row_ranker.onnx": next_row_ranker(),
        "column_ranker.onnx": column_ranker(),
        "slow_ranker.onnx": slow_ranker(),
        "stream_ranker.onnx": linear_ranker(STREAM_W, LINEAR_B),
    }
    files = {name: m.SerializeToString() for name, m in models.items()} | {
        "ranker_golden.jsonl": ranker_golden(), "stream_golden.jsonl": ranker_golden(stream=True)}
    sums = []
    for name, data in files.items():
        with open(os.path.join(d, name), "wb") as f:
            f.write(data)
        sums.append(f"{hashlib.sha256(data).hexdigest()}  {name}\n")
    with open(os.path.join(d, "SHA256SUMS"), "w") as f:
        f.writelines(sums)


if __name__ == "__main__":
    main(sys.argv[1])
