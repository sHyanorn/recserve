#!/usr/bin/env bash
# Before and after an optimization (docs/BENCHMARKS.md, Profiles and an
# optimization): scripts/bench-compare.sh BEFORE, a commit; after is the
# working tree's HEAD. Builds vec's test binary and the images of both,
# then alternates before and after, three runs each: BenchmarkDot pinned to
# CPU 1, and the service at RATES (200 400), replay, cache off. Images are
# tagged before and after; DOCKER_BUILD_FLAGS goes to docker build.
cd "$(dirname "$0")/.."
BEFORE=$1
RATES=${RATES:-200 400}
OUT=${OUT:-bench-results/m6/optimization}
source scripts/bench-lib.sh
export CACHE_SIZE=0
wt=$(mktemp -d)
git worktree add --detach "$wt" "$BEFORE" >/dev/null
trap 'git worktree remove --force "$wt"' EXIT
(cd "$wt" && go test -c -o "$OLDPWD/$OUT/raw/vec.before.test" ./internal/vec && make images TAG=before DOCKER_BUILD_FLAGS="${DOCKER_BUILD_FLAGS:-}" >/dev/null)
go test -c -o "$OUT/raw/vec.after.test" ./internal/vec
make images TAG=after DOCKER_BUILD_FLAGS="${DOCKER_BUILD_FLAGS:-}" >/dev/null
echo "before: $(git rev-parse "$BEFORE"), after: $(git rev-parse HEAD)" >"$OUT/commits.txt"
TAG=after env_record
for n in 1 2 3; do
	for side in before after; do
		p=$OUT/micro-$side-$n
		noise "$p" before
		taskset -c 1 "$OUT/raw/vec.$side.test" -test.run '^$' -test.bench BenchmarkDot -test.benchtime 2s >"$p.txt"
		noise "$p" after
	done
done
for rate in $RATES; do
	for n in 1 2 3; do
		for side in before after; do
			export TAG=$side
			one_run "$OUT/$rate-$side-run$n" "$rate"
		done
	done
done
bin/benchsum compare -dir "$OUT" | tee "$OUT/compare.md"
down
