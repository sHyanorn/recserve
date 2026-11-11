#!/usr/bin/env bash
# Rollouts with SPEC §5's budgets of docs/BENCHMARKS.md (D047):
#   scripts/bench-kind.sh [RUNS]
# Each run on a new cluster (scripts/kind-up.sh): the gateways' stage budgets
# set back to the binary's defaults, SPEC §5's, in place of kind's 100 ms;
# then scripts/kind-check.sh's rollouts and swap to VERSION under its load,
# keeping loadgen's report and samples and the phases' start times. Reported,
# not judged (D022). Results in OUT (bench-results/m6/kind).
set -euo pipefail
cd "$(dirname "$0")/.."
RUNS=${1:-3}
OUT=${OUT:-bench-results/m6/kind}
mkdir -p "$OUT/raw"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
for n in $(seq 1 "$RUNS"); do
	kind delete cluster --name recserve >/dev/null 2>&1 || true
	scripts/kind-up.sh "$DATA"
	kubectl kustomize deploy/kind | sed '/- -budget-[a-z]*=100ms$/d' | kubectl apply -f - >/dev/null
	kubectl -n recserve rollout status deploy/gateway --timeout=5m
	rm -rf "${tmp:?}"/*
	OUT=$tmp scripts/kind-check.sh "$DATA" "$VERSION" | tee "$OUT/run$n.check.txt" || true
	mv "$tmp/samples.csv.gz" "$OUT/raw/run$n.csv.gz"
	mv "$tmp/report.json" "$OUT/run$n.json"
	mv "$tmp/marks" "$OUT/run$n.marks"
done
kind delete cluster --name recserve
bin/benchsum phases -dir "$OUT" | tee "$OUT/kind.md"
