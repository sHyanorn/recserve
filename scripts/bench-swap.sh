#!/usr/bin/env bash
# Bundle swaps under load of docs/BENCHMARKS.md (D031):
#   scripts/bench-swap.sh RATE [RUNS]
# RATE is half the headline rate; replay, cache off, 10 s warmup and 150 s
# measured. Every 15 s from 15 s on, the admin API swaps to the other of
# VERSION and OTHER (5de55036a382), nine swaps; each swap's request and
# answer times, its answer and the containers' CPU time before and after it
# go to PREFIX.swaps and PREFIX.swapN.cpu0/1, the gateway's GC trace to
# PREFIX.gctrace. RUNS (3) runs as configured, as many with GOGC=off. Results in
# OUT (bench-results/m6/swap).
cd "$(dirname "$0")/.."
RATE=$1 RUNS=${2:-3} OTHER=${OTHER:-5de55036a382}
OUT=${OUT:-bench-results/m6/swap}
source scripts/bench-lib.sh
export CACHE_SIZE=0 GODEBUG=gctrace=1
env_record


# attempt PREFIX: one run's files.
attempt() {
	local p=$1 t0 k v admin s e a
	rm -f "$p".*
	fresh
	admin=$(ip_of gateway):8081
	noise "$p" before
	loadgen "$p" "$RATE" 10s 150s
	t0=$(started "$p")
	for k in $(seq 1 9); do
		((k % 2)) && v=$OTHER || v=$VERSION
		at $((t0 + 10000 + 15000 * k))
		cpu_snapshot "$p.swap$k.cpu0"
		s=$(now_ms)
		a=$(curl -sf -X POST "http://$admin/v1/admin/swap" -H 'Content-Type: application/json' -d "{\"bundle\":\"/data/bundle/$v\"}")
		e=$(now_ms)
		cpu_snapshot "$p.swap$k.cpu1"
		echo "$s $e $a" >>"$p.swaps"
	done
	wait "$LOADGEN" || true
	noise "$p" after
	docker inspect -f '{{.State.StartedAt}}' recserve-gateway-1 >"$p.started"
	docker logs recserve-gateway-1 2>&1 | grep '^gc ' >"$p.gctrace" || true
}

for gogc in "" off; do
	export GOGC=$gogc
	for n in $(seq 1 "$RUNS"); do
		p=$OUT/gc${gogc:+-$gogc}-run$n
		for try in 1 2 3; do
			log "$(basename "$p") attempt $try"
			attempt "$p"
			echo "$try" >"$p.attempt"
			noisy "$p" || break
		done
		bin/benchsum swap -prefix "$p" -samples "$OUT/raw/$(basename "$p").csv.gz"
	done
done
bin/benchsum swaps -dir "$OUT" | tee "$OUT/swaps.md"
down
