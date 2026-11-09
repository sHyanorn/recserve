# M0 risk checks

Code for the two checks defined in [docs/BENCHMARKS.md](../../docs/BENCHMARKS.md#m0-risk-checks). Results are in [`bench-results/m0`](../../bench-results/m0). This is a separate Go module, so its dependencies stay out of the service.

Inputs are downloaded or generated under a data directory outside the repository (`DATA` below; the flags default to `/home/user/data/m0`). Run the shared step from that directory. Nothing there is committed.

```bash
# Shared: ONNX Runtime 1.31.1, glove-100-angular, a Python environment
curl -LO https://github.com/microsoft/onnxruntime/releases/download/v1.31.1/onnxruntime-linux-x64-1.31.1.tgz && tar xzf onnxruntime-linux-x64-1.31.1.tgz
curl -LO https://ann-benchmarks.com/glove-100-angular.hdf5
python3 -m venv venv && venv/bin/pip install --index-url https://download.pytorch.org/whl/cpu torch==2.15.0
venv/bin/pip install onnx onnxruntime==1.31.1 faiss-cpu==1.15.1 h5py numpy

# Noise reference, once per session on an idle machine
go run ./cmd/noise -out ../../bench-results/m0/noise-reference.json

# R1: ONNX Runtime from Go
DATA/venv/bin/python -I onnx/export.py DATA/onnx
go run ./onnx -mode parity -dir DATA/onnx -lib .../libonnxruntime.so
go run ./onnx -mode latency -dir DATA/onnx -lib .../libonnxruntime.so -ref REF -out ../../bench-results/m0/onnx/latency.json
docker build -f onnx/Dockerfile -t recserve-m0-onnx . && docker run --rm -v DATA/onnx:/models:ro recserve-m0-onnx -mode parity

# R2: pure-Go HNSW
DATA/venv/bin/python -I ann/prep.py DATA/glove-100-angular.hdf5 DATA/ann
# coder/hnsw, exact search (once per set), faiss with noise baselines, faiss recall
DATA=DATA/ann PY=DATA/venv/bin/python ann/run.sh synth 16 200 REF
DATA=DATA/ann PY=DATA/venv/bin/python ann/run.sh glove 16 200 REF
# and with M 32, efConstruction 400
```
