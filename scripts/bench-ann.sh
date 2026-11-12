#!/usr/bin/env bash
# ANN of docs/BENCHMARKS.md (ANN in M6, and annbench's profile in Profiles
# and an optimization):
#   scripts/bench-ann.sh profile|table
# profile: a glove M = 16 graph of its own, then annbench's query at ef 400,
#   k 100 with a CPU profile; its top functions go to
#   OUT/profile-annbench.txt. The graph stays for make bench-compare's runs.
# table: annbench as in M2, one thread, recall@100 and p99 at ef 120, 200,
#   400 and 800, on synth with M = 16, glove with M = 16 and 32, and glove
#   with M = 32 on two shards; faiss on the same sets and settings.
# ANN_DATA holds prep.py's files (experiments/m0/ann), made here from
# glove-100-angular.hdf5 if missing, and the graphs; PY is a Python with
# faiss-cpu 1.15.1, h5py and numpy. A result already written is kept, so a
# step that was cut off resumes. Results in OUT (bench-results/m6/ann).
set -euo pipefail
cd "$(dirname "$0")/.."
STEP=$1
OUT=${OUT:-bench-results/m6/ann}
ANN_DATA=${ANN_DATA:-$DATA/ann}
PY=${PY:-python3}
REF=${REF:-$(sed -n 's/^ *"median_ns": \([0-9.]*\),*$/\1/p' bench-results/m7/noise-reference.json)}
EFS=120,200,400,800
mkdir -p "$OUT/raw" "$ANN_DATA/graphs"

if [[ ! -s $ANN_DATA/glove_gt.i32 ]]; then
	[[ -s $ANN_DATA/glove-100-angular.hdf5 ]] || curl -sfLo "$ANN_DATA/glove-100-angular.hdf5" https://ann-benchmarks.com/glove-100-angular.hdf5
	"$PY" -I experiments/m0/ann/prep.py "$ANN_DATA/glove-100-angular.hdf5" "$ANN_DATA"
fi
ann() { bin/annbench -data "$ANN_DATA" -noise bin/noisebase -ref "$REF" "$@"; }
[[ -s $OUT/env.txt ]] || {
	echo "date: $(date -u +%FT%TZ)"
	echo "commit: $(git rev-parse HEAD) (uncommitted changes: $([[ -n $(git status --porcelain -- cmd internal scripts experiments) ]] && echo yes || echo no))"
	echo "cpu: $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs), $(nproc) vCPU"
	echo "memory: $(grep MemTotal /proc/meminfo | xargs)"
	echo "kernel: $(uname -r)"
	echo "go: $(go version | cut -d' ' -f3)"
	echo "python: $("$PY" -m pip freeze | grep -E '^(faiss-cpu|numpy|h5py)==' | paste -sd' ')"
	echo "cpus: annbench and faiss not pinned, noise baselines on CPU 0"
	echo "noise reference ns: $REF"
	echo "noisebase sha256: $(sha256sum bin/noisebase | cut -d' ' -f1)"
} >"$OUT/env.txt"

# graph NAME SET M EFC: the graph NAME, built once with 4 goroutines.
graph() {
	local g=$ANN_DATA/graphs/$1.bin
	[[ -s $g ]] || ann -mode build -set "$2" -M "$3" -efc "$4" -workers 4 -graph "$g" -out "$OUT/$1-build.json"
}

# query SET M: recall and latency on the graph.
query() {
	[[ -s $OUT/$1-M$2.json ]] || ann -mode query -set "$1" -graph "$ANN_DATA/graphs/$1-M$2.bin" -ef "$EFS" -k 100 -out "$OUT/$1-M$2.json"
}

# faiss SET M EFC: faiss_ref.py between noise baselines, then its recall.
faiss() {
	local p=$OUT/$1-faiss-M$2
	if [[ ! -s $p.json ]]; then
		bin/noisebase -n 1 -ref "$REF" -out "$p-noise-before.json" -q >/dev/null
		"$PY" -I experiments/m0/ann/faiss_ref.py "$ANN_DATA" "$1" "$2" "$3" "$p.json"
		bin/noisebase -n 1 -ref "$REF" -out "$p-noise-after.json" -q >/dev/null
	fi
	[[ -s $p-recall.json ]] || bin/m0-ann -data "$ANN_DATA" -set "$1" -index faiss -M "$2" -efc "$3" -out "$p-recall.json"
}

case $STEP in
profile)
	graph profile-glove-M16 glove 16 200
	if [[ ! -s $OUT/profile-annbench.txt ]]; then
		ann -mode query -set glove -graph "$ANN_DATA/graphs/profile-glove-M16.bin" -ef 400 -k 100 -cpuprofile "$OUT/raw/annbench.pprof" -out "$OUT/profile-query.json"
		go tool pprof -top -nodecount=25 "$OUT/raw/annbench.pprof" 2>/dev/null >"$OUT/profile-annbench.txt"
	fi
	;;
table)
	(cd experiments/m0 && go build -o ../../bin/m0-ann ./ann)
	for s in synth glove; do
		[[ -s $OUT/$s-checkgt.json ]] || ann -mode checkgt -set "$s" -out "$OUT/$s-checkgt.json"
	done
	graph synth-M16 synth 16 200 && query synth 16
	graph glove-M16 glove 16 200 && query glove 16
	graph glove-M32 glove 32 400 && query glove 32
	[[ -s $OUT/glove-M32-2shards.json ]] || ann -mode shards -set glove -M 32 -efc 400 -shards 2 -graph "$ANN_DATA/graphs/glove-M32-2shards.bin" \
		-ef "$EFS" -k 100 -out "$OUT/glove-M32-2shards.json"
	faiss synth 16 200
	faiss glove 16 200
	faiss glove 32 400
	;;
*) echo "STEP is profile or table" >&2; exit 2 ;;
esac
