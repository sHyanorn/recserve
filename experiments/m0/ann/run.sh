#!/usr/bin/env bash
# Runs R2 for one dataset and one (M, efConstruction) setting.
# usage: ann/run.sh SET M EFC REF   (from experiments/m0; DATA, PY and OUT may be overridden)
set -euo pipefail
set_=$1 m=$2 efc=$3 ref=$4
DATA=${DATA:-/home/user/data/m0/ann}
PY=${PY:-/home/user/venv/bin/python}
OUT=${OUT:-../../bench-results/m0/ann}
mkdir -p "$OUT"
go build -o /tmp/r2 ./ann
go build -o /tmp/noise ./cmd/noise
/tmp/r2 -data "$DATA" -set "$set_" -index hnsw -M "$m" -efc "$efc" -ref "$ref" -out "$OUT/$set_-hnsw-M$m.json"
if [ ! -f "$OUT/$set_-exact.json" ]; then
  /tmp/r2 -data "$DATA" -set "$set_" -index exact -ref "$ref" -out "$OUT/$set_-exact.json"
fi
/tmp/noise -n 1 -out "$OUT/$set_-faiss-M$m-noise-before.json"
"$PY" -I ann/faiss_ref.py "$DATA" "$set_" "$m" "$efc" "$OUT/$set_-faiss-M$m.json"
/tmp/noise -n 1 -out "$OUT/$set_-faiss-M$m-noise-after.json"
/tmp/r2 -data "$DATA" -set "$set_" -index faiss -M "$m" -efc "$efc" -out "$OUT/$set_-faiss-M$m-recall.json"
