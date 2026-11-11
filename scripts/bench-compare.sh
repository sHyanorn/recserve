#!/usr/bin/env bash
# Before and after an optimization (docs/BENCHMARKS.md, Profiles and an
# optimization): scripts/bench-compare.sh BEFORE, a commit; after is the
# working tree's HEAD. Builds vec's test binary, annbench and the images of
# both, then alternates before and after, three runs each: the vec benchmark
# MICRO (Dot) pinned to CPU 1 (its file copied into BEFORE); with ANN_GRAPH,
# a glove graph in ANN_DATA (DATA/ann), annbench's query on it at ef 120,
# 200, 400 and 800; and the service at RATES (200 400), replay, cache off.
# Images are tagged before and after; DOCKER_BUILD_FLAGS goes to docker
# build. A run already written is kept, so a comparison that was cut off
# resumes.
cd "$(dirname "$0")/.."
BEFORE=$1
MICRO=${MICRO:-Dot}
RATES=${RATES-200 400} # empty: no service runs
ANN_DATA=${ANN_DATA:-$DATA/ann}
OUT=${OUT:-bench-results/m6/optimization}
source scripts/bench-lib.sh
export CACHE_SIZE=0
wt=$(mktemp -d)
git worktree add --detach "$wt" "$BEFORE" >/dev/null
trap 'git worktree remove --force "$wt"' EXIT
cp internal/vec/dot_bench_test.go "$wt/internal/vec/"
(cd "$wt" && go test -c -o "$OLDPWD/$OUT/raw/vec.before.test" ./internal/vec && go build -o "$OLDPWD/$OUT/raw/annbench.before" ./cmd/annbench)
go test -c -o "$OUT/raw/vec.after.test" ./internal/vec
go build -o "$OUT/raw/annbench.after" ./cmd/annbench
if [[ -n $RATES ]]; then
	(cd "$wt" && make images TAG=before DOCKER_BUILD_FLAGS="${DOCKER_BUILD_FLAGS:-}" >/dev/null)
	make images TAG=after DOCKER_BUILD_FLAGS="${DOCKER_BUILD_FLAGS:-}" >/dev/null
fi
echo "before: $(git rev-parse "$BEFORE"), after: $(git rev-parse HEAD)" >"$OUT/commits.txt"
TAG=after env_record
for n in 1 2 3; do
	for side in before after; do
		p=$OUT/micro-$side-$n
		[[ -s $p.txt ]] && continue
		noise "$p" before
		taskset -c 1 "$OUT/raw/vec.$side.test" -test.run '^$' -test.bench "^Benchmark$MICRO\$" -test.benchtime 2s >"$p.txt"
		noise "$p" after
	done
done
if [[ -n ${ANN_GRAPH:-} ]]; then
	for n in 1 2 3; do
		for side in before after; do
			p=$OUT/ann-$side-$n
			[[ -s $p.json ]] || "$OUT/raw/annbench.$side" -mode query -data "$ANN_DATA" -set glove -graph "$ANN_GRAPH" \
				-ef 120,200,400,800 -k 100 -noise bin/noisebase -ref "$REF" -out "$p.json"
		done
	done
fi
for rate in $RATES; do
	for n in 1 2 3; do
		for side in before after; do
			export TAG=$side
			[[ -s $OUT/$rate-$side-run$n.summary.json ]] || one_run "$OUT/$rate-$side-run$n" "$rate"
		done
	done
done
bin/benchsum compare -dir "$OUT" | tee "$OUT/compare.md"
down
