#!/usr/bin/env bash
# Downloads a category of Amazon Reviews 2023 (5-core, timestamp split) into
# DIR, Video_Games and its item metadata by default, and verifies the files
# against scripts/data.sha256, which must list each of them before any is
# downloaded.
# usage: scripts/data.sh DIR [CATEGORY]
set -euo pipefail
dir=${1:?usage: scripts/data.sh DIR [CATEGORY]}
cat=${2:-Video_Games}
base=https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023
sums=$(cd "$(dirname "$0")" && pwd)/data.sha256
files=("$cat.train.csv.gz" "$cat.valid.csv.gz" "$cat.test.csv.gz")
if [ "$cat" = Video_Games ]; then files+=(meta_Video_Games.jsonl.gz); fi
for f in "${files[@]}"; do
	awk -v f="$f" '$2 == f { n++ } END { exit n != 1 }' "$sums" || { echo "data.sha256 has no checksum for $f" >&2; exit 1; }
done
mkdir -p "$dir"
cd "$dir"
get() { [ -f "$2" ] || { curl -fsSL -o "$2.part" "$base/$1/$2" && mv "$2.part" "$2"; }; }
for f in "${files[@]}"; do
	case $f in
	meta_*) get raw/meta_categories "$f" ;;
	*) get benchmark/5core/timestamp "$f" ;;
	esac
done
awk -v list="${files[*]}" 'BEGIN { split(list, f, " "); for (i in f) want[f[i]] } $2 in want' "$sums" | sha256sum -c
