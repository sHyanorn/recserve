#!/usr/bin/env bash
# Creates the kind cluster "recserve" (deploy/kind/cluster.yaml) with DATA, the
# directory of make data and make bundle, mounted at /data, and loads the images
# of make images and metrics-server's into it.
# usage: scripts/kind-cluster.sh DATA
set -euo pipefail
data=$(cd "${1:?usage: scripts/kind-cluster.sh DATA}" && pwd)
cd "$(dirname "$0")/.."
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

sed "s|hostPath: DATA$|hostPath: $data|" deploy/kind/cluster.yaml > "$tmp/cluster.yaml"
kind create cluster --config "$tmp/cluster.yaml"
for image in recserve/gateway:dev recserve/annshard:dev; do
  kind load docker-image --name recserve "$image"
done
# Loaded rather than pulled by the node. A multi-platform image is saved for
# this platform alone: its archive then holds every layer it names.
ms=registry.k8s.io/metrics-server/metrics-server:v0.9.1
docker pull -q "$ms" >/dev/null
docker save --platform linux/amd64 -o "$tmp/ms.tar" "$ms"
kind load image-archive --name recserve "$tmp/ms.tar"
