#!/usr/bin/env bash
# Cross-check of docs/BENCHMARKS.md (Load tests, M6): at RATE, half the
# headline rate, replay with the cache off, three loadgen runs and three
# vegeta runs alternate, each on fresh services with noise baselines.
# vegeta (v12.12.0, go install github.com/tsenart/vegeta/v12@v12.12.0) sends
# the same requests over up to 64 connections for 10 s, then 60 s measured,
# and times each from its send, not its due time.
#   scripts/bench-vegeta.sh RATE
# Results in OUT (bench-results/m6/vegeta).
cd "$(dirname "$0")/.."
RATE=$1
OUT=${OUT:-bench-results/m6/vegeta}
source scripts/bench-lib.sh
export CACHE_SIZE=0
command -v vegeta >/dev/null || { echo "vegeta is not installed" >&2; exit 1; }
env_record
echo "vegeta: $(go version -m "$(command -v vegeta)" | awk '$1 == "mod" {print $3}')" >>"$OUT/env.txt"

# targets ADDR: the test split in time order as vegeta's JSON targets.
targets() {
	zcat "$DATA/Video_Games.test.csv.gz" | tail -n +2 | sort -s -t, -k4,4n |
		jq -Rc --arg url "http://$1/v1/recommend" 'split(",") |
			{method: "POST", url: $url, header: {"Content-Type": ["application/json"]},
			 body: ({user_id: .[0], k: 20, ts: (.[3] | tonumber)} | tojson | @base64)}'
}

# vegeta_run PREFIX: as one_run, with vegeta.
vegeta_run() {
	local p=$1 attempt
	for attempt in 1 2 3; do
		log "$(basename "$p") attempt $attempt"
		fresh
		targets "$(ip_of gateway):8080" >"$OUT/raw/targets.json"
		noise "$p" before
		taskset -c 0 vegeta attack -format json -targets "$OUT/raw/targets.json" -rate "$RATE/1s" -duration 10s -max-connections 64 >/dev/null
		taskset -c 0 vegeta attack -format json -targets "$OUT/raw/targets.json" -rate "$RATE/1s" -duration 60s -max-connections 64 >"$OUT/raw/$(basename "$p").bin"
		noise "$p" after
		echo "$attempt" >"$p.attempt"
		noisy "$p" || break
	done
	vegeta report -type json "$OUT/raw/$(basename "$p").bin" >"$p.json"
}

for n in 1 2 3; do
	one_run "$OUT/loadgen-run$n" "$RATE"
	vegeta_run "$OUT/vegeta-run$n"
done
{
	echo "| Run | loadgen p99 ms | vegeta p99 ms | vegeta success | vegeta status codes |"
	echo "|---|---|---|---|---|"
	for n in 1 2 3; do
		echo "| $n | $(jq .p99_ms "$OUT/loadgen-run$n.summary.json") | $(jq '.latencies["99th"] / 1e6' "$OUT/vegeta-run$n.json") | $(jq .success "$OUT/vegeta-run$n.json") | $(jq -c .status_codes "$OUT/vegeta-run$n.json") |"
	done
} | tee "$OUT/vegeta.md"
down
