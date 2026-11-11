# Shared by the benchmark scripts (docs/BENCHMARKS.md, M6; D052). Sourced,
# with DATA, VERSION, OUT and REF set: the data directory, the bundle
# version, the result directory, and the session's noise reference in ns
# (make bench-ref). Raw samples go to OUT/raw, which git ignores.
set -euo pipefail

REF=${REF:-$(sed -n 's/^ *"median_ns": \([0-9.]*\),*$/\1/p' bench-results/m6/noise-reference.json)}
COMPOSE=(docker compose -f deploy/compose.yaml -f deploy/bench.compose.yaml)
SERVICES=(ingest annshard-0 annshard-1 annshard-2 annshard-3 gateway)
CONTAINERS=(gateway annshard-0 annshard-1 annshard-2 annshard-3 valkey kafka ingest)
export DATA VERSION
mkdir -p "$OUT/raw"

log() { echo "$(date -u +%H:%M:%S) $*" >&2; }
now_ms() { date +%s%3N; }

ip_of() { docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "recserve-$1-1"; }

# cpu_snapshot FILE: each container's CPU time in ns, from its cgroup.
cpu_snapshot() {
	local c id f
	for c in "${CONTAINERS[@]}" ${EXTRA_CONTAINERS:-}; do
		id=$(docker inspect -f '{{.Id}}' "recserve-$c-1" 2>/dev/null) || continue
		for f in /sys/fs/cgroup/cpuacct/docker/$id/cpuacct.usage /sys/fs/cgroup/system.slice/docker-$id.scope/cpu.stat /sys/fs/cgroup/docker/$id/cpu.stat; do
			[[ -r $f ]] || continue
			if [[ $f == *cpu.stat ]]; then
				echo "$c $(awk '$1 == "usage_usec" {print $2 * 1000}' "$f")"
			else
				echo "$c $(cat "$f")"
			fi
			break
		done
	done >"$1"
}

# snapshot PREFIX TAG: the gateway's metrics and the containers' CPU time.
snapshot() {
	curl -sf "http://$(ip_of gateway):8080/metrics" >"$1.$2.metrics"
	cpu_snapshot "$1.$2.cpu"
}

# noise PREFIX SIDE: a baseline on CPU 1, flagged against REF.
noise() { taskset -c 1 bin/noisebase -n 1 -cpu 1 -ref "$REF" -out "$1.noise-$2.json" -q >/dev/null; }

noisy() { grep -q '"noisy": true' "$1.noise-before.json" "$1.noise-after.json"; }

# fresh [SERVICE...]: Valkey emptied, the services recreated, the gateway
# ready (it preloads the train and valid splits first).
fresh() {
	"${COMPOSE[@]}" up -d --wait kafka valkey >/dev/null
	"${COMPOSE[@]}" exec -T valkey valkey-cli FLUSHALL >/dev/null
	"${COMPOSE[@]}" up -d --force-recreate "$@" "${SERVICES[@]}" >/dev/null 2>&1
	local ip deadline=$((SECONDS + 300))
	until ip=$(ip_of gateway) && [[ -n $ip ]] && curl -sf "http://$ip:8080/readyz" >/dev/null; do
		((SECONDS < deadline)) || { log "the gateway is not ready after 300 s"; "${COMPOSE[@]}" logs --tail 20 gateway >&2; return 1; }
		sleep 1
	done
}

down() { "${COMPOSE[@]}" --profile app --profile faults down -v >/dev/null 2>&1 || true; }

# loadgen PREFIX RATE WARMUP DURATION [ARGS...]: loadgen on CPU 0 against
# the gateway, in the background; its pid in LOADGEN.
loadgen() {
	local prefix=$1 rate=$2 warmup=$3 duration=$4
	shift 4
	taskset -c 0 bin/loadgen -addr "$(ip_of gateway):8080" -split "$DATA/Video_Games.test.csv.gz" -rate "$rate" \
		-warmup "$warmup" -duration "$duration" -samples "$OUT/raw/$(basename "$prefix").csv.gz" -out "$prefix.json" "$@" \
		>"$prefix.loadgen.txt" 2>&1 &
	LOADGEN=$!
}

# one_run PREFIX RATE [LOADGEN ARGS...]: a sweep run on fresh services, 10 s
# warmup and 60 s measured, bracketed by noise baselines and rerun at most
# twice while one is flagged; then its summary.
one_run() {
	local p=$1 rate=$2 attempt
	shift 2
	for attempt in 1 2 3; do
		log "$(basename "$p") attempt $attempt"
		fresh
		noise "$p" before
		loadgen "$p" "$rate" 10s 60s "$@"
		sleep 10
		snapshot "$p" at0
		wait "$LOADGEN" || true
		snapshot "$p" at1
		noise "$p" after
		echo "$attempt" >"$p.attempt"
		noisy "$p" || break
	done
	bin/benchsum run -prefix "$p" -samples "$OUT/raw/$(basename "$p").csv.gz"
}

# env_record: the environment of docs/BENCHMARKS.md, once per result directory.
env_record() {
	[[ -s $OUT/env.txt ]] && return
	{
		echo "date: $(date -u +%FT%TZ)"
		echo "commit: $(git rev-parse HEAD) (uncommitted changes: $([[ -n $(git status --porcelain -- cmd internal deploy scripts) ]] && echo yes || echo no))"
		echo "cpu: $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs), $(nproc) vCPU"
		echo "memory: $(grep MemTotal /proc/meminfo | xargs)"
		echo "kernel: $(uname -r)"
		echo "go: $(go version | cut -d' ' -f3)"
		echo "docker: $(docker version -f '{{.Server.Version}}'), cgroup $(docker info -f '{{.CgroupVersion}}')"
		echo "onnxruntime: 1.31.1 (gateway image)"
		echo "bundle: $VERSION"
		echo "cpus: loadgen 0, noise baseline 1, containers 1-3"
		echo "noise reference ns: $REF"
		echo "noisebase sha256: $(sha256sum bin/noisebase | cut -d' ' -f1)"
		for i in gateway annshard ingest faultshard; do echo "image recserve/$i:${TAG:-dev}: $(docker image inspect -f '{{.Id}}' "recserve/$i:${TAG:-dev}" 2>/dev/null)"; done
	} >"$OUT/env.txt"
}
