#!/usr/bin/env bash
# CPU profiles of docs/BENCHMARKS.md (Profiles and an optimization): replay,
# cache off, 400 requests/s; 30 s profiles of the gateway and of annshard-0
# while each container's CPU per request is measured. The profiles' top
# functions go to OUT/profile-*.txt, the profiles to OUT/raw.
cd "$(dirname "$0")/.."
OUT=${OUT:-bench-results/m6/profile}
RATE=${RATE:-400}
source scripts/bench-lib.sh
export CACHE_SIZE=0
env_record
p=$OUT/$RATE
fresh
noise "$p" before
loadgen "$p" "$RATE" 10s 40s
sleep 10
snapshot "$p" at0
curl -sf "http://$(ip_of gateway):8081/debug/pprof/profile?seconds=30" -o "$OUT/raw/gateway.pprof" &
curl -sf "http://$(ip_of annshard-0):6060/debug/pprof/profile?seconds=30" -o "$OUT/raw/annshard-0.pprof"
wait %1
wait "$LOADGEN" || true
snapshot "$p" at1
noise "$p" after
bin/benchsum run -prefix "$p" -samples "$OUT/raw/$(basename "$p").csv.gz"
for c in gateway annshard-0; do
	go tool pprof -top -nodecount=25 "$OUT/raw/$c.pprof" 2>/dev/null >"$OUT/profile-$c.txt"
done
down
