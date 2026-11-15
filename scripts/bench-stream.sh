#!/usr/bin/env bash
# The stream state's cost in serving, docs/BENCHMARKS.md, Stream state in
# serving (D062):
#   scripts/bench-stream.sh STEP
# STEP is gate, micro, phase1, swap, profile or phase2, in that order, run
# by make bench-stream, which builds the images and the binaries first;
# DATA holds the split files and the bundles of both groups and of the
# swap; gate and micro need RECSERVE_ORT_LIB. A step that measures needs a
# tree without changes to the code, and a phase's runs come from one
# commit. Each step resumes from what it has written, within the section's
# rules for restarts: a boot is /proc/sys/kernel/random/boot_id, each boot
# takes its own noise reference and adds itself to the environment record,
# and runs of an earlier boot that a step cannot keep move to
# OUT/set-aside/<the boot ID's first 8 characters>/. Results in
# bench-results/m8/serving.
set -Eeuo pipefail
trap 'echo "$(date -u +%H:%M:%S) failed at line $LINENO: $BASH_COMMAND" >&2' ERR
cd "$(dirname "$0")/.."
STEP=${1:?STEP}
SERVING=bench-results/m8/serving
OFF=a8d9c526fb03 ON=74ad6c83bf3c S6=d6d2691322ca
BOOT=$(cat /proc/sys/kernel/random/boot_id)
B8=${BOOT:0:8}
case $STEP in
gate | micro | phase1 | phase2 | swap | profile) OUT=$SERVING/$STEP ;;
*) echo "unknown step $STEP" >&2; exit 2 ;;
esac
# The gateway's flags and the reference come from this script alone.
unset SHARD_ADDRS VALKEY_ADDR HEDGE_DELAY BUDGET_HISTORY GOGC GODEBUG ANNSHARD1_IMAGE REF
COMMIT=$(git rev-parse HEAD)
if [[ -n $(git status --porcelain -- cmd internal deploy scripts Dockerfile go.mod go.sum) ]]; then
	[[ $STEP == gate ]] || { echo "the code differs from $COMMIT: commit it first" >&2; exit 1; }
	COMMIT=$COMMIT-dirty
fi

# A reference and its baselines come from one build of noisebase (Noise
# baseline), taken once per boot on the idle machine.
REF_FILE=$SERVING/noise-reference-$B8.json
mkdir -p "$SERVING"
if [[ -s $REF_FILE ]] && ! grep -q "\"executable_sha256\": \"$(sha256sum bin/noisebase | cut -d' ' -f1)\"" "$REF_FILE"; then
	echo "bin/noisebase is not the build that took $REF_FILE" >&2
	exit 1
fi
if [[ ! -s $REF_FILE ]]; then
	docker compose -f deploy/compose.yaml -f deploy/bench.compose.yaml --profile app --profile faults down -v >/dev/null 2>&1 || true
	taskset -c 1 bin/noisebase -n 10 -cpu 1 -out "$REF_FILE"
fi
export REF_FILE CACHE_SIZE=0
source scripts/bench-lib.sh

for v in $OFF $ON $S6; do
	grep -q "\"model_version\": \"$v\"" "$DATA/bundle/$v/manifest.json" || { echo "bundle $v is not in $DATA/bundle" >&2; exit 1; }
done

# boot_record adds this boot, commit and set of images to the environment
# record, once.
boot_record() {
	local images i
	images=$(for i in gateway annshard ingest; do docker image inspect -f '{{.Id}}' "recserve/$i:${TAG:-dev}" || echo none; done | xargs)
	local key="key: $BOOT $COMMIT $images"
	grep -qxF "$key" "$SERVING/env.txt" 2>/dev/null && return
	{
		echo "$key"
		echo "boot: $BOOT"
		echo "date: $(date -u +%FT%TZ)"
		echo "commit: $COMMIT"
		echo "cpu: $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs), $(nproc) vCPU"
		echo "memory: $(grep MemTotal /proc/meminfo | xargs)"
		echo "kernel: $(uname -r)"
		echo "go: $(go version | cut -d' ' -f3)"
		echo "docker: $(docker version -f '{{.Server.Version}}'), cgroup $(docker info -f '{{.CgroupVersion}}')"
		echo "bundles: off $OFF, on $ON, swap $S6"
		echo "cpus: loadgen 0, noise baseline 1, containers 1-3"
		echo "noise reference ns: $REF"
		echo "noisebase sha256: $(sha256sum bin/noisebase | cut -d' ' -f1)"
		for i in gateway annshard ingest; do echo "image recserve/$i:${TAG:-dev}: $(docker image inspect -f '{{.Id}}' "recserve/$i:${TAG:-dev}")"; done
		echo
	} >>"$SERVING/env.txt"
}
boot_record

version_of() { case $1 in off) echo $OFF ;; on) echo $ON ;; esac; }

# runs_of GLOB: the run prefixes of OUT whose names match GLOB.
runs_of() {
	local f
	for f in "$OUT"/$1.boot; do [[ -e $f ]] && echo "${f%.boot}"; done
	return 0
}

# summaries DIR GLOB: the number of run summaries in DIR whose names match GLOB.
summaries() {
	local f n=0
	for f in "$1"/$2.summary.json; do [[ -e $f ]] && n=$((n + 1)); done
	echo "$n"
}

# set_aside_runs PREFIX...: each run's files, samples and profile to
# OUT/set-aside/<its boot's first 8 characters>/.
set_aside_runs() {
	local p b d f
	for p in "$@"; do
		b=$(cat "$p.boot")
		d=$OUT/set-aside/${b:0:8}
		mkdir -p "$d/raw"
		for f in "$p".*; do mv "$f" "$d/"; done
		for f in "$OUT/raw/$(basename "$p")".*; do [[ -e $f ]] && mv "$f" "$d/raw/"; done
	done
	return 0
}

# other_boot PREFIX...: whether a run ran on another boot.
other_boot() {
	local p
	for p in "$@"; do [[ $(cat "$p.boot") != "$BOOT" ]] && return 0; done
	return 1
}

# one_commit PREFIX...: stops unless every run was built from this commit.
one_commit() {
	local p
	for p in "$@"; do
		[[ ! -e $p.commit || $(cat "$p.commit") == "$COMMIT" ]] || { echo "$p was built from $(cat "$p.commit"), not $COMMIT" >&2; exit 1; }
	done
}

# restarts: each container's restart count, -1 if it is not found.
restarts() {
	local c n
	for c in "${CONTAINERS[@]}"; do
		n=$(docker inspect -f '{{.RestartCount}}' "recserve-$c-1" 2>/dev/null) || n=-1
		echo "$c $n"
	done
}

hwm() {
	local pid
	pid=$(docker inspect -f '{{.State.Pid}}' recserve-gateway-1) && grep VmHWM "/proc/$pid/status"
}

# scrape FILE: the gateway's metrics; an empty file if it does not answer.
scrape() { curl -sf "http://$(ip_of gateway):8080/metrics" >"$1" || : >"$1"; }

# take PREFIX TAG: the gateway's metrics and the containers' CPU time.
take() {
	scrape "$1.$2.metrics"
	cpu_times >"$1.$2.cpu"
}

# drain waits until ingest has applied every event the running gateway
# accepted (its 202 answers to /v1/events, one event each), so that the
# next run empties Valkey of them all.
drain() {
	local gw ing want got deadline=$((SECONDS + 120))
	gw=$(ip_of gateway 2>/dev/null) && ing=$(ip_of ingest 2>/dev/null) && [[ -n $gw && -n $ing ]] || return 0
	curl -sf "http://$gw:8080/metrics" >"$OUT/drain.gw.metrics" || { log "no gateway to drain"; return 0; }
	want=$(bin/benchsum value -after "$OUT/drain.gw.metrics" -name recserve_http_requests_total -label route=events,code=202 2>/dev/null) || want=0
	while :; do
		curl -sf "http://$ing:8082/metrics" >"$OUT/drain.ingest.metrics" || : >"$OUT/drain.ingest.metrics"
		got=$(bin/benchsum value -after "$OUT/drain.ingest.metrics" -name recserve_ingest_records_total 2>/dev/null) || got=-1
		awk -v g="$got" -v w="$want" 'BEGIN { exit !(g + 0 >= w + 0) }' && break
		((SECONDS < deadline)) || { log "ingest applied $got of $want events after 120 s"; return 1; }
		sleep 0.5
	done
	rm -f "$OUT"/drain.*.metrics
}

# start PREFIX: a run's first files, fresh services, the ready files.
start() {
	local p=$1
	rm -f "$p".* "$OUT/raw/$(basename "$p")".*
	echo "$BOOT" >"$p.boot"
	echo "$COMMIT" >"$p.commit"
	drain
	fresh
	restarts >"$p.restarts0"
	docker inspect -f '{{.Image}}' recserve-gateway-1 >"$p.image"
	docker inspect -f '{{json .Args}}' recserve-gateway-1 >"$p.args"
	docker inspect -f '{{.State.StartedAt}}' recserve-gateway-1 >"$p.started"
	docker logs recserve-gateway-1 2>&1 | grep -m1 '"msg":"serving"' >"$p.serving" || true
}

# stream_run PREFIX GROUP RATE [WARMUP DURATION]: a run of the stream
# scenario on fresh services serving GROUP's bundle, bracketed by noise
# baselines and rerun at most twice while one is flagged; then its
# summary. Times count from the first measured request. With PROFILE set,
# a CPU profile of the gateway is taken from 20 s, into OUT/raw.
stream_run() {
	local p=$1 group=$2 rate=$3 warmup=${4:-10} duration=${5:-60} attempt t0 prof
	VERSION=$(version_of "$group")
	for attempt in 1 2 3; do
		log "$(basename "$p") attempt $attempt"
		start "$p"
		noise "$p" before
		scrape "$p.ready.metrics"
		loadgen "$p" "$rate" "${warmup}s" "${duration}s" -stream
		t0=$(($(started "$p") + warmup * 1000))
		prof=
		if [[ -n ${PROFILE:-} ]]; then
			(at $((t0 + 20000)) && curl -sf "http://$(ip_of gateway):8081/debug/pprof/profile?seconds=30" -o "$OUT/raw/$(basename "$p").pprof") &
			prof=$!
		fi
		at "$t0"
		take "$p" at0
		at $((t0 + duration * 1000))
		scrape "$p.end.metrics"
		wait "$LOADGEN" || true
		take "$p" at1
		hwm >"$p.hwm" || true
		restarts >"$p.restarts1"
		[[ -z $prof ]] || wait "$prof" || log "no profile"
		noise "$p" after
		echo "$attempt" >"$p.attempt"
		noisy "$p" || break
	done
	bin/benchsum stream -prefix "$p" -samples "$(samples "$p")" -group "$group" -version "$VERSION"
}

# rate_runs RATE GROUP...: the runs of GROUP... at RATE in that order, as
# RATE-GROUP-N; those with a summary are kept.
rate_runs() {
	local rate=$1 g n
	shift
	declare -A count=()
	for g in "$@"; do
		n=$((${count[$g]:-0} + 1))
		count[$g]=$n
		[[ -s $OUT/$rate-$g-$n.summary.json ]] || stream_run "$OUT/$rate-$g-$n" "$g" "$rate"
	done
}

# set_aside_tables: the tables of the runs set aside, each apart.
set_aside_tables() {
	local d
	for d in "$OUT"/set-aside/*/; do
		[[ -e $d ]] || continue
		(($(summaries "${d%/}" "*") > 0)) && bin/benchsum streamtable -dir "${d%/}" >"${d}table.md"
	done
	return 0
}

case $STEP in
gate)
	# A new topic, then the comparisons of TestRealDataStreamGateway.
	down
	VERSION=$ON
	fresh
	status=0
	RECSERVE_DATA=$DATA RECSERVE_BUNDLE=$DATA/bundle/$ON RECSERVE_REQUIRE_ORT=1 \
		RECSERVE_GATEWAY=$(ip_of gateway):8080 RECSERVE_GATEWAY_ADMIN=$(ip_of gateway):8081 RECSERVE_INGEST=$(ip_of ingest):8082 \
		RECSERVE_VIA=/data/bundle/$OFF go test -count=1 -timeout 30m -run '^TestRealDataStreamGateway$' -v ./internal/recommend/ | tee "$OUT/gate-$B8.txt" || status=$?
	{ echo "commit: $COMMIT"; echo "boot: $BOOT"; } >>"$OUT/gate-$B8.txt"
	down
	exit "$status"
	;;
micro)
	# Each benchmark's ten counts on one boot; a complete file stands.
	f=$OUT/micro.txt
	if [[ ! -s $f ]]; then
		go test -c -o "$OUT/raw/recommend.test" ./internal/recommend/
		{ echo "boot: $BOOT"; echo "commit: $COMMIT"; } >"$f.part"
		for b in 'BenchmarkRealStreamTake$ 34510x' 'BenchmarkRealStreamReads$ 1s' 'BenchmarkRealStreamPipeline$ 3451x'; do
			RECSERVE_DATA=$DATA RECSERVE_BUNDLE=$DATA/bundle/$OFF RECSERVE_BUNDLE_S8=$DATA/bundle/$ON RECSERVE_REQUIRE_ORT=1 \
				taskset -c 1 "$OUT/raw/recommend.test" -test.run '^$' -test.bench "^${b% *}" -test.benchtime "${b#* }" -test.count 10 -test.cpu 1 -test.timeout 0 \
				>>"$f.part" || { tail -n 20 "$f.part" >&2; exit 1; }
		done
		if grep -q -e '--- SKIP' -e '--- FAIL' "$f.part"; then
			grep -e '--- SKIP' -e '--- FAIL' -A3 "$f.part" >&2
			exit 1
		fi
		mv "$f.part" "$f"
	fi
	bin/benchsum bench -file "$f" | tee "$OUT/micro.md"
	;;
phase1)
	# A rate is complete with the summaries of its six runs; one cut off
	# by a restart runs again in full, its earlier runs set aside.
	down
	mapfile -t runs < <(runs_of "*")
	one_commit "${runs[@]}"
	for rate in 100 200 400 800; do
		mapfile -t runs < <(runs_of "$rate-*")
		if (($(summaries "$OUT" "$rate-*") < 6)) && ((${#runs[@]} > 0)) && other_boot "${runs[@]}"; then
			log "rate $rate: runs of another boot set aside"
			set_aside_runs "${runs[@]}"
		fi
		rate_runs "$rate" off on on off off on
	done
	set_aside_tables
	bin/benchsum streamtable -dir "$OUT" | tee "$OUT/table.md"
	down
	;;
phase2)
	# Phase 2 runs on one boot: unless it is complete, runs of another boot
	# move aside and it starts again; phase 1's four rates count as its
	# first only if all ran on this boot and from this commit.
	if [[ ! -s $OUT/done ]]; then
		mapfile -t runs < <(runs_of "*")
		if ((${#runs[@]} > 0)) && other_boot "${runs[@]}"; then
			log "phase 2: runs of another boot set aside; it starts again"
			set_aside_runs "${runs[@]}"
			mapfile -t runs < <(runs_of "*")
		fi
		one_commit "${runs[@]}"
		dirs=$OUT
		mapfile -t p1 < <(OUT=$SERVING/phase1 runs_of "*")
		if (($(summaries "$SERVING/phase1" "*") == 24)) && ! other_boot "${p1[@]}" && (one_commit "${p1[@]}" 2>/dev/null); then
			dirs=$SERVING/phase1,$OUT
		fi
		down
		while :; do
			off=$(bin/benchsum streamnext -dir "$dirs" -group off -boot "$BOOT")
			on=$(bin/benchsum streamnext -dir "$dirs" -group on -boot "$BOOT")
			log "next: off $off, on $on"
			[[ $off == done* && $on == done* ]] && break
			if [[ $off == "$on" ]]; then
				rate_runs "$off" off on on off off on
			else
				[[ $off == done* ]] || rate_runs "$off" off off off
				[[ $on == done* ]] || rate_runs "$on" on on on
			fi
		done
		{ echo "dirs: $dirs"; echo "off: $off"; echo "on: $on"; } >"$OUT/done"
		down
	fi
	set_aside_tables
	{
		cat "$OUT/done"
		echo
		bin/benchsum streamtable -readings=false -dir "$(sed -n 's/^dirs: //p' "$OUT/done")"
	} | tee "$OUT/table.md"
	;;
swap)
	# One run; a run cut off starts again in full, a complete one stands.
	p=$OUT/swap
	if [[ ! -s $p.swap.json ]]; then
		down
		VERSION=$ON
		start "$p"
		noise "$p" before
		loadgen "$p" 200 10s 150s -stream
		t0=$(($(started "$p") + 10000))
		admin=$(ip_of gateway):8081
		k=0
		for v in $S6 $OFF $ON; do
			k=$((k + 1))
			at $((t0 + 40000 * k))
			scrape "$p.swap$k.before.metrics"
			s=$(now_ms)
			a=$(curl -s -X POST "http://$admin/v1/admin/swap" -H 'Content-Type: application/json' -d "{\"bundle\":\"/data/bundle/$v\"}" | tr -d '\n') || a="no answer"
			e=$(now_ms)
			scrape "$p.swap$k.after.metrics"
			echo "$s $e $a" >>"$p.swaps"
		done
		wait "$LOADGEN" || true
		hwm >"$p.hwm" || true
		restarts >"$p.restarts1"
		noise "$p" after
		down
	fi
	bin/benchsum streamswap -prefix "$p" -samples "$(samples "$p")" | tee "$OUT/swap.md"
	;;
profile)
	# One run of each group; a run cut off starts again, a complete one stands.
	export PROFILE=1
	down
	for g in off on; do
		p=$OUT/400-$g-1
		[[ -s $p.summary.json ]] || stream_run "$p" "$g" 400
		go tool pprof -top -nodecount 30 "$OUT/raw/400-$g-1.pprof" >"$p.top.txt"
		go tool pprof -top -cum -nodecount 30 "$OUT/raw/400-$g-1.pprof" >"$p.cum.txt"
	done
	bin/benchsum streamtable -readings=false -dir "$OUT" | tee "$OUT/table.md"
	down
	;;
esac
