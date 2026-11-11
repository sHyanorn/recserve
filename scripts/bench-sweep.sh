#!/usr/bin/env bash
# Load sweep of docs/BENCHMARKS.md (Load tests and its M6 additions):
#   scripts/bench-sweep.sh SCENARIO CACHE [RATE...]
# SCENARIO is replay, writes or repeats, CACHE on or off. Each rate runs
# three times on fresh services, each run bracketed by noise baselines and
# rerun at most twice while one is flagged; the sweep stops at the first
# rate with an invalid run or a median p99 over 100 ms. Without rates it
# runs the grid 100, 200, 400, 800, then steps of 400, up to the first rate
# that misses the headline condition (D054), and for replay with the cache
# off then bisects to 50 requests/s below that rate (benchsum next). Results
# in OUT (bench-results/m6/sweep-...).
cd "$(dirname "$0")/.."
SCENARIO=$1 CACHE=$2
shift 2
OUT=${OUT:-bench-results/m6/sweep-$SCENARIO-cache-$CACHE}
source scripts/bench-lib.sh
case $SCENARIO in
replay) ARGS=() ;;
writes) ARGS=(-write-share 0.1) ;;
repeats) ARGS=(-repeat 0.3 -seed 1) ;;
*) echo "unknown scenario $SCENARIO" >&2; exit 2 ;;
esac
case $CACHE in
on) export CACHE_SIZE=13000 ;;
off) export CACHE_SIZE=0 ;;
*) echo "CACHE is on or off" >&2; exit 2 ;;
esac
env_record

# run_rate RATE: three runs; those already done are kept, so a sweep that
# was cut off resumes where it stopped.
run_rate() {
	local rate=$1 n
	for n in 1 2 3; do
		[[ -s $OUT/$rate-run$n.summary.json ]] || one_run "$OUT/$rate-run$n" "$rate" "${ARGS[@]}"
	done
	log "rate $rate: $(bin/benchsum rate -dir "$OUT" -rate "$rate")"
}

if (($# > 0)); then
	for rate in "$@"; do
		run_rate "$rate"
		[[ $(bin/benchsum rate -dir "$OUT" -rate "$rate") == *stop=true* ]] && break
	done
else
	bisect=false
	[[ $SCENARIO == replay && $CACHE == off ]] && bisect=true
	while next=$(bin/benchsum next -dir "$OUT" -bisect="$bisect") && [[ $next != done* ]]; do
		run_rate "$next"
	done
	log "$next"
fi
bin/benchsum sweep -dir "$OUT" | tee "$OUT/sweep.md"
down
