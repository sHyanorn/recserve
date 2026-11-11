#!/usr/bin/env bash
# Zero-error check in kind (make kind-check, after make kind-up): loadgen offers
# the test split's requests at RATE per second through the gateways' NodePort
# while the gateways restart one by one, then the replicas, then the gateways
# swap to bundle VERSION (scripts/kind-swap.sh). Every response must be a
# recommendation with nothing degraded, and both versions must have answered.
# With OUT set, loadgen's report and samples and the phases' start times
# ("<phase> <unix ms>") stay in OUT.
# usage: scripts/kind-check.sh DATA VERSION [RATE]
set -euo pipefail
data=$(cd "${1:?usage: scripts/kind-check.sh DATA VERSION [RATE]}" && pwd)
version=${2:?usage: scripts/kind-check.sh DATA VERSION [RATE]}
rate=${3:-100}
cd "$(dirname "$0")/.."
k() { kubectl -n recserve "$@"; }
from=$(k get deploy gateway -o jsonpath='{.spec.template.spec.containers[0].env[0].value}')
[ "$from" != "$version" ] || { echo "the gateways already serve $version" >&2; exit 1; }
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
out=${OUT:-$tmp}
mkdir -p "$out"

# loadgen opens its connections on gateways that answer /readyz, so none may
# be stopping when it starts.
k rollout status deploy/gateway --timeout=5m
while k get pods -l app.kubernetes.io/name=gateway -o jsonpath='{.items[*].metadata.deletionTimestamp}' | grep -q .; do sleep 1; done

# Long enough for the three phases, which take about a minute on one node;
# the check fails if they outlast it.
go build -o "$tmp/loadgen" ./cmd/loadgen
"$tmp/loadgen" -addr 127.0.0.1:30080 -split "$data/Video_Games.test.csv.gz" -rate "$rate" \
  -warmup 5s -duration 150s -samples "$out/samples.csv.gz" -out "$out/report.json" > "$out/loadgen.log" 2>&1 &
loadgen=$!
sleep 10 # past the warmup

# step PHASE TEXT: the phase's start, logged and marked.
step() { printf '%s %s\n' "$(date -u +%T)" "$2"; echo "$1 $(date +%s%3N)" >>"$out/marks"; }
step gateways "rolling restart of the gateways"
k rollout restart deploy/gateway
k rollout status deploy/gateway --timeout=5m
step replicas "rolling restart of the replicas"
k rollout restart statefulset/annshard
k rollout status statefulset/annshard --timeout=10m
step swap "swap from $from to $version"
scripts/kind-swap.sh "$version"
step after "done; waiting for loadgen"
kill -0 "$loadgen" 2>/dev/null || { cat "$out/loadgen.log" >&2; echo "FAIL: the run ended before the last phase" >&2; exit 1; }
wait "$loadgen" || true

report=$out/report.json
[ -f "$report" ] || { cat "$out/loadgen.log" >&2; echo "FAIL: loadgen wrote no report" >&2; exit 1; }
jq -c '{valid, problems, outcomes, versions}' "$report"
jq -e --arg from "$from" --arg to "$version" '
  .valid
  and (.outcomes | keys == ["recommend_ok"])
  and (.versions | has($from) and has($to))' "$report" >/dev/null ||
  { echo "FAIL: errors, degraded responses or a missing version" >&2; exit 1; }
echo "PASS"
