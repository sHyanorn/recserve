#!/usr/bin/env bash
# Hedge delay of docs/BENCHMARKS.md (Hedge delay, D041):
#   scripts/bench-hedge.sh RATE
# RATE is half the headline rate; replay, cache off. Three runs with hedging
# off give each shard's p95 of a call to a replica and so the default delay;
# three runs with that delay give hedges per search and p99. Results in OUT
# (bench-results/m6/hedge).
cd "$(dirname "$0")/.."
RATE=$1
OUT=${OUT:-bench-results/m6/hedge}
source scripts/bench-lib.sh
export CACHE_SIZE=0 HEDGE_DELAY=0
env_record
# Runs already summarized are kept, so a cut-off series resumes.
for n in 1 2 3; do [[ -s $OUT/delay0-run$n.summary.json ]] || one_run "$OUT/delay0-run$n" "$RATE"; done
d=$(bin/benchsum hedge -dir "$OUT" | sed -n 's/^default_ms=//p')
log "default hedge delay: $d ms"
export HEDGE_DELAY=${d}ms
for n in 1 2 3; do [[ -s $OUT/default-run$n.summary.json ]] || one_run "$OUT/default-run$n" "$RATE"; done
bin/benchsum hedge -dir "$OUT" | tee "$OUT/hedge.md"
down
