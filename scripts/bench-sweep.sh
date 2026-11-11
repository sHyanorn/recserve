#!/usr/bin/env bash
# Load sweep of docs/BENCHMARKS.md (Load tests and its M6 additions):
#   scripts/bench-sweep.sh SCENARIO CACHE [RATE...]
# SCENARIO is replay, writes or repeats, CACHE on or off. Each rate runs
# three times on fresh services, each run bracketed by noise baselines and
# rerun at most twice while one is flagged; the sweep stops at the first
# rate with an invalid run or a median p99 over 100 ms. Without rates it
# runs the grid 100, 200, 400, 800, then steps of 400, and for replay with
# the cache off then bisects to 50 requests/s below the first rate that
# misses the headline condition. Results in OUT (bench-results/m6/sweep-...).
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

# run_rate RATE: three runs; sets MEETS and STOP from benchsum's verdict.
run_rate() {
	local rate=$1 n
	for n in 1 2 3; do
		one_run "$OUT/$rate-run$n" "$rate" "${ARGS[@]}"
	done
	local v
	v=$(bin/benchsum rate -dir "$OUT" -rate "$rate")
	log "rate $rate: $v"
	[[ $v == *meets=true* ]] && MEETS=1 || MEETS=0
	[[ $v == *stop=true* ]] && STOP=1 || STOP=0
}

if (($# > 0)); then
	for rate in "$@"; do
		run_rate "$rate"
		((STOP)) && break
	done
else
	last=0 first_miss=0 rate=100
	while :; do
		run_rate "$rate"
		if ((MEETS)) && ((first_miss == 0)); then last=$rate; elif ((first_miss == 0)); then first_miss=$rate; fi
		((STOP)) && break
		if ((rate < 800)); then rate=$((rate * 2)); else rate=$((rate + 400)); fi
	done
	if [[ $SCENARIO == replay && $CACHE == off ]]; then
		lo=$last hi=$first_miss
		((hi == 0)) && hi=$rate
		if ((lo == 0)); then run_rate 50; else
			while ((hi - lo > 50)); do
				mid=$(((lo + hi) / 2 / 50 * 50))
				((mid == lo)) && mid=$((lo + 50))
				run_rate "$mid"
				if ((MEETS)); then lo=$mid; else hi=$mid; fi
			done
			log "headline: $lo requests/s"
		fi
	fi
fi
bin/benchsum sweep -dir "$OUT" | tee "$OUT/sweep.md"
down
