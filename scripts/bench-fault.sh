#!/usr/bin/env bash
# Fault runs of docs/BENCHMARKS.md (Faults, setup and pass criteria (M6), as
# amended for M7) and the runs of Valkey circuit breaker openings (M7):
#   scripts/bench-fault.sh FAULT RATE [RUNS]
# FAULT is F1 to F6, with the cache off, or none: nothing injected and the
# cache on. RATE is half the headline rate; RUNS 3, or 5 with none. Each
# run: toxiproxy between the gateway and the replicas and Valkey, fresh
# services, 270 s of load from the first request's due time, the fault
# injected at 90 s and removed at 150 s; the gateway's metrics at 30, 90,
# 150 s and the end; from the first request's due time to the end of the
# load, a read each second of the gateway's metrics of the criteria and the
# containers' CPU time (OUT/raw/*.reads); the changes of state its Valkey
# circuit breaker logged, with their time. An invalid run is rerun at most
# twice. Results in OUT (bench-results/m7/faults; with none,
# bench-results/m7/breaker).
cd "$(dirname "$0")/.."
FAULT=$1 RATE=$2 RUNS=${3:-3}
ARGS=()
case $FAULT in
F1 | F2 | F3 | F5) ;;
F4) ARGS=(-write-share 0.1) ;;
F6) export ANNSHARD1_IMAGE=recserve/faultshard:${TAG:-dev} ;;
none) RUNS=${3:-5} ;;
*) echo "FAULT is F1 to F6 or none" >&2; exit 2 ;;
esac
if [[ $FAULT == none ]]; then
	OUT=${OUT:-bench-results/m7/breaker}
	unset CACHE_SIZE
else
	OUT=${OUT:-bench-results/m7/faults}
	export CACHE_SIZE=0
fi
unset HEDGE_DELAY BUDGET_HISTORY # the commit's defaults
source scripts/bench-lib.sh
export SHARD_ADDRS="toxiproxy:19090,toxiproxy:19091;toxiproxy:19092,toxiproxy:19093" VALKEY_ADDR=toxiproxy:16379
EXTRA_CONTAINERS=toxiproxy
env_record

# toxi METHOD PATH [BODY]: a call to toxiproxy's API.
toxi() { curl -sf -X "$1" "http://$(ip_of toxiproxy):8474$2" ${3:+-H 'Content-Type: application/json' -d "$3"} >/dev/null; }

inject() {
	local r
	case $FAULT in
	F1) docker kill recserve-annshard-1-1 ;;
	F2) for r in annshard-2 annshard-3; do toxi POST "/proxies/$r/toxics" '{"name":"latency","type":"latency","stream":"downstream","attributes":{"latency":50}}'; done ;;
	F3) toxi POST /proxies/valkey '{"enabled":false}' ;;
	F4) docker pause recserve-ingest-1 ;;
	F5) docker pause recserve-annshard-1-1 ;;
	F6) docker kill -s USR1 recserve-annshard-1-1 ;;
	esac >/dev/null
}

remove() {
	local r
	case $FAULT in
	F1) docker start recserve-annshard-1-1 ;;
	F2) for r in annshard-2 annshard-3; do toxi DELETE "/proxies/$r/toxics/latency"; done ;;
	F3) toxi POST /proxies/valkey '{"enabled":true}' ;;
	F4) docker unpause recserve-ingest-1 ;;
	F5) docker unpause recserve-annshard-1-1 ;;
	F6) docker kill -s USR2 recserve-annshard-1-1 ;;
	esac >/dev/null
}

# metrics PREFIX TAG: the gateway's /metrics.
metrics() { curl -sf "http://$GATEWAY:8080/metrics" >"$1.$2.metrics"; }

# reads PREFIX: the run's per-second reads, written by poll.
reads() { echo "$OUT/raw/$(basename "$1").reads"; }

# The gateway's metrics that poll reads: the replicas' calls and ejections,
# the Valkey calls, the breaker's state and the GC pause time.
POLLED='recserve_shard_calls_total|recserve_shard_ejections_total|recserve_valkey_calls_total|recserve_valkey_breaker_state|go_gc_duration_seconds_sum'

# poll PREFIX T0: a read each second from T0, the first request's due time
# in Unix ms, to the end of the load at T0 + 270 s: a line "read <unix ms>",
# the gateway's /metrics lines of POLLED (none if it does not answer), and
# a line "cpu <container> <ns>" for each container.
poll() {
	local k
	for ((k = 0; k <= 270; k++)); do
		at $(($2 + k * 1000))
		echo "read $(now_ms)"
		curl -sf -m 0.5 "http://$GATEWAY:8080/metrics" | grep -E "^($POLLED)[{ ]" || true
		cpu_times | sed 's/^/cpu /'
	done >"$(reads "$1")"
}

# lag PREFIX: each second, "<unix ms> <events the gateway accepted> <records
# ingest applied>"; no line while ingest does not answer.
lag() {
	local in a b
	in=$(ip_of ingest)
	while :; do
		a=$(curl -sf -m 0.5 "http://$GATEWAY:8080/metrics" | awk '/^recserve_http_requests_total\{/ && /route="events"/ && /code="202"/ {print $2}') || true
		b=$(curl -sf -m 0.5 "http://$in:8082/metrics" | awk '/^recserve_ingest_records_total\{/ && /result="applied"/ {print $2}') || true
		[[ -n $a && -n $b ]] && echo "$(now_ms) $a $b"
		sleep 1
	done >"$1.lag"
}

# attempt PREFIX: one run's files. The gateway's whole log goes to OUT/raw,
# its breaker's lines, JSON with their time, to PREFIX.breaker.
attempt() {
	local p=$1 t0 lagpid= pollpid gwlog
	gwlog=$OUT/raw/$(basename "$p").gateway.log
	rm -f "$p".*
	[[ $FAULT == F4 ]] && "${COMPOSE[@]}" up -d --force-recreate --wait kafka >/dev/null 2>&1 # no events of an earlier run
	"${COMPOSE[@]}" up -d --force-recreate toxiproxy >/dev/null 2>&1
	until toxi GET /proxies; do sleep 0.2; done
	fresh
	GATEWAY=$(ip_of gateway)
	noise "$p" before
	loadgen "$p" "$RATE" 0s 270s "${ARGS[@]}"
	t0=$(started "$p")
	poll "$p" "$t0" &
	pollpid=$!
	if [[ $FAULT == F4 ]]; then
		lag "$p" &
		lagpid=$!
	fi
	at $((t0 + 30000)) && metrics "$p" b0
	at $((t0 + 90000)) && echo "inject $(now_ms)" >"$p.marks" && inject && metrics "$p" b1
	at $((t0 + 150000)) && echo "remove $(now_ms)" >>"$p.marks" && remove && metrics "$p" b2
	wait "$LOADGEN" || true
	wait "$pollpid"
	metrics "$p" b3
	docker logs recserve-gateway-1 >"$gwlog" 2>&1 || true
	grep '"valkey circuit breaker"' "$gwlog" >"$p.breaker" || true
	noise "$p" after
	if [[ -n $lagpid ]]; then
		at $((t0 + 300000)) # the lag file shows the drain; the histories need it done
		kill "$lagpid"
		bin/benchsum histories -valkey "$(ip_of valkey):6379" -preload "$DATA/Video_Games.train.csv.gz,$DATA/Video_Games.valid.csv.gz" \
			-split "$DATA/Video_Games.test.csv.gz" -samples "$(samples "$p")" >"$p.histories"
	fi
}

for n in $(seq 1 "$RUNS"); do
	p=$OUT/$FAULT-run$n
	for try in 1 2 3; do
		log "$FAULT run $n attempt $try"
		attempt "$p"
		echo "$try" >"$p.attempt"
		v=$(bin/benchsum fault -prefix "$p" -samples "$(samples "$p")" -reads "$(reads "$p")" -fault "$FAULT")
		log "$v"
		[[ $v == *"valid true"* ]] && break
	done
done
bin/benchsum faults -dir "$OUT" | tee "$OUT/faults.md"
bin/benchsum breakers -dir "$OUT" | tee "$OUT/breaker.md"
down
