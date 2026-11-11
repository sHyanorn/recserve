"""Write the ONNX models and the ranker golden the Go tests use, and their SHA256SUMS.

usage: testdata.py DIR   (DIR is internal/onnx/onnxtest/testdata)

mean.onnx is build.py's mean encoder over 12 items of 2 dimensions, item i
being (cos i, sin i) / i (onnxtest.MeanVectors); zero.onnx is the same graph
over zero vectors, so its output is always zero. slow.onnx applies Sin 300
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
recompute. The same inputs give the same bytes.
"""

import hashlib
import json
import os
import sys

import numpy as np
from onnx import TensorProto, helper, numpy_helper

from build import RANKER_FEATURES, linear_ranker, mean_encoder, mean_vector, model, ranker_input, top_k

MEAN_ITEMS = 12
SIN_NODES = 300
LINEAR_W = [1, 2, -1, 0.5, 0.25, -0.5]
LINEAR_B = 0.125


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


def ranker_golden():
    vecs = mean_vectors()
    cooc_ids = np.zeros((MEAN_ITEMS + 1, 50), dtype=np.int64)
    cooc_ids[1, :2] = [3, 2]
    popular = np.array([3, 1], dtype=np.int64)
    lines = []
    for hist in ([1], [2, 5]):
        uv = mean_vector(vecs, hist)
        ids, _, x = ranker_input(vecs, cooc_ids, popular, hist, uv)
        case = {"history": hist, "user_emb": uv.tolist(), "top10": top_k(vecs, uv, set(hist), 10),
                "candidates": ids.tolist(), "features": x.tolist()}
        lines.append(json.dumps(case) + "\n")
    return "".join(lines).encode()


def main(d):
    models = {
        "mean.onnx": mean_encoder(mean_vectors()),
        "zero.onnx": mean_encoder(np.zeros((MEAN_ITEMS + 1, 2), dtype=np.float32)),
        "slow.onnx": slow(),
        "linear_ranker.onnx": linear_ranker(LINEAR_W, LINEAR_B),
        "cross_row_ranker.onnx": cross_row_ranker(),
        "next_row_ranker.onnx": next_row_ranker(),
        "column_ranker.onnx": column_ranker(),
        "slow_ranker.onnx": slow_ranker(),
    }
    files = {name: m.SerializeToString() for name, m in models.items()} | {"ranker_golden.jsonl": ranker_golden()}
    sums = []
    for name, data in files.items():
        with open(os.path.join(d, name), "wb") as f:
            f.write(data)
        sums.append(f"{hashlib.sha256(data).hexdigest()}  {name}\n")
    with open(os.path.join(d, "SHA256SUMS"), "w") as f:
        f.writelines(sums)


if __name__ == "__main__":
    main(sys.argv[1])
