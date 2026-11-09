"""Write the ONNX models the Go tests use, and their SHA256SUMS.

usage: testdata.py DIR   (DIR is internal/onnx/onnxtest/testdata)

mean.onnx is build.py's mean encoder over 12 items of 2 dimensions, item i
being (cos i, sin i) / i (onnxtest.MeanVectors); zero.onnx is the same graph
over zero vectors, so its output is always zero. slow.onnx applies Sin 300
times to a float32 vector x of any length, giving y; a run on a long vector
lasts long enough to be terminated. The same inputs give the same bytes.
"""

import hashlib
import os
import sys

import numpy as np
from onnx import TensorProto, helper

from build import mean_encoder, model

MEAN_ITEMS = 12
SIN_NODES = 300


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


def main(d):
    models = {
        "mean.onnx": mean_encoder(mean_vectors()),
        "zero.onnx": mean_encoder(np.zeros((MEAN_ITEMS + 1, 2), dtype=np.float32)),
        "slow.onnx": slow(),
    }
    sums = []
    for name, m in models.items():
        data = m.SerializeToString()
        with open(os.path.join(d, name), "wb") as f:
            f.write(data)
        sums.append(f"{hashlib.sha256(data).hexdigest()}  {name}\n")
    with open(os.path.join(d, "SHA256SUMS"), "w") as f:
        f.writelines(sums)


if __name__ == "__main__":
    main(sys.argv[1])
