"""Export the R1 models (docs/BENCHMARKS.md) with random weights, plus parity cases.

Writes to OUT: enc_l50.onnx, enc_l200.onnx, ranker.onnx, the ranker weights as
.npy for the pure-Go reference, and per model 100 seeded inputs with the
outputs of onnxruntime in Python.
"""

import os
import sys

import numpy as np
import onnxruntime as ort
import torch
from torch import nn

ITEMS, DIM, HEADS, FF, LAYERS = 140_000, 64, 2, 256, 2
CANDIDATES, FEATS, CASES = 500, 8, 100


class Encoder(nn.Module):
    def __init__(self, max_len):
        super().__init__()
        self.item = nn.Embedding(ITEMS, DIM, padding_idx=0)
        self.pos = nn.Embedding(max_len, DIM)
        layer = nn.TransformerEncoderLayer(DIM, HEADS, FF, dropout=0.0, batch_first=True)
        self.blocks = nn.TransformerEncoder(layer, LAYERS, enable_nested_tensor=False)
        self.norm = nn.LayerNorm(DIM)

    def forward(self, item_seq):
        length = item_seq.shape[1]
        pos = torch.arange(length, device=item_seq.device).unsqueeze(0)
        x = self.item(item_seq) + self.pos(pos)
        causal = torch.triu(torch.full((length, length), float("-inf")), diagonal=1)
        x = self.blocks(x, mask=causal)
        return self.norm(x[:, -1, :])


class Ranker(nn.Module):
    def __init__(self):
        super().__init__()
        self.mlp = nn.Sequential(
            nn.Linear(2 * DIM + FEATS, 128), nn.ReLU(),
            nn.Linear(128, 64), nn.ReLU(),
            nn.Linear(64, 1),
        )

    def forward(self, user, items, feats):
        u = user.unsqueeze(1).expand(-1, items.shape[1], -1)
        return self.mlp(torch.cat([u, items, feats], dim=-1)).squeeze(-1)


def export(model, args, path, names, outputs, axes):
    torch.onnx.export(model, args, path, input_names=names, output_names=outputs,
                      dynamic_axes=axes, opset_version=17, dynamo=False)


def sequences(rng, length):
    seq = rng.integers(1, ITEMS, size=(CASES, length), dtype=np.int64)
    for i, pad in enumerate(rng.integers(0, length, size=CASES)):
        seq[i, :pad] = 0  # left padding, at least one real item remains
    return seq


def main(out):
    os.makedirs(out, exist_ok=True)
    torch.manual_seed(0)
    rng = np.random.default_rng(0)
    for length in (50, 200):
        enc = Encoder(length).eval()
        path = f"{out}/enc_l{length}.onnx"
        export(enc, (torch.ones(1, length, dtype=torch.int64),), path, ["item_seq"], ["user_emb"],
               {"item_seq": {0: "B", 1: "L"}, "user_emb": {0: "B"}})
        seq = sequences(rng, length)
        sess = ort.InferenceSession(path, providers=["CPUExecutionProvider"])
        got = np.concatenate([sess.run(None, {"item_seq": seq[i:i + 1]})[0] for i in range(CASES)])
        with torch.no_grad():
            want = enc(torch.from_numpy(seq)).numpy()
        print(f"enc_l{length}: python ort vs torch max abs diff {np.abs(got - want).max():.3g}")
        np.save(f"{out}/enc_l{length}_in.npy", seq)
        np.save(f"{out}/enc_l{length}_out.npy", got.astype(np.float32))

    ranker = Ranker().eval()
    path = f"{out}/ranker.onnx"
    export(ranker, (torch.zeros(1, DIM), torch.zeros(1, CANDIDATES, DIM), torch.zeros(1, CANDIDATES, FEATS)),
           path, ["user", "items", "feats"], ["scores"],
           {"user": {0: "B"}, "items": {0: "B", 1: "C"}, "feats": {0: "B", 1: "C"}, "scores": {0: "B", 1: "C"}})
    user = rng.standard_normal((CASES, DIM), dtype=np.float32)
    items = rng.standard_normal((CASES, CANDIDATES, DIM), dtype=np.float32)
    feats = rng.standard_normal((CASES, CANDIDATES, FEATS), dtype=np.float32)
    sess = ort.InferenceSession(path, providers=["CPUExecutionProvider"])
    got = np.concatenate([sess.run(None, {"user": user[i:i + 1], "items": items[i:i + 1],
                                          "feats": feats[i:i + 1]})[0] for i in range(CASES)])
    with torch.no_grad():
        want = ranker(torch.from_numpy(user), torch.from_numpy(items), torch.from_numpy(feats)).numpy()
    print(f"ranker: python ort vs torch max abs diff {np.abs(got - want).max():.3g}")
    for name, a in (("user", user), ("items", items), ("feats", feats), ("out", got)):
        np.save(f"{out}/ranker_{name}.npy", a.astype(np.float32))
    for i, layer in enumerate(m for m in ranker.mlp if isinstance(m, nn.Linear)):
        np.save(f"{out}/ranker_w{i}.npy", layer.weight.detach().numpy())  # [out, in]
        np.save(f"{out}/ranker_b{i}.npy", layer.bias.detach().numpy())
    print("onnxruntime", ort.__version__, "torch", torch.__version__)


if __name__ == "__main__":
    main(sys.argv[1])
