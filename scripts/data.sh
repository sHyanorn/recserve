#!/usr/bin/env bash
# Downloads Amazon Reviews 2023 Video_Games (5-core, timestamp split) and its
# item metadata into DIR, and verifies them against scripts/data.sha256.
# usage: scripts/data.sh DIR
set -euo pipefail
dir=${1:?usage: scripts/data.sh DIR}
base=https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023
sums=$(cd "$(dirname "$0")" && pwd)/data.sha256
mkdir -p "$dir"
cd "$dir"
get() { [ -f "$2" ] || { curl -fsSL -o "$2.part" "$base/$1/$2" && mv "$2.part" "$2"; }; }
for s in train valid test; do get benchmark/5core/timestamp "Video_Games.$s.csv.gz"; done
get raw/meta_categories meta_Video_Games.jsonl.gz
sha256sum -c "$sums"
