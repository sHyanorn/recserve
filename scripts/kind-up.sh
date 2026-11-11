#!/usr/bin/env bash
# Creates the kind cluster (scripts/kind-cluster.sh DATA), deploys
# metrics-server and deploy/kind, and reports how long the Pods took to be
# ready and how often the gateways restarted on the way.
# usage: scripts/kind-up.sh DATA
set -euo pipefail
cd "$(dirname "$0")/.."
scripts/kind-cluster.sh "${1:?usage: scripts/kind-up.sh DATA}"
# Loaded rather than pulled by the node. The multi-platform image is saved for
# linux/amd64 alone, the platform of the images here, so that its archive holds
# every layer it names.
ms=registry.k8s.io/metrics-server/metrics-server:v0.9.1
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
for try in 1 2 3; do
  docker pull -q "$ms" >/dev/null && break
  [ $try = 3 ] && exit 1
  sleep 5
done
docker save --platform linux/amd64 -o "$tmp/ms.tar" "$ms"
kind load image-archive --name recserve "$tmp/ms.tar"
kubectl apply -k deploy/kind/metrics-server

start=$(date +%s.%N)
kubectl apply -k deploy/kind
kubectl -n recserve rollout status deployment/gateway --timeout=10m
ready() { [ "$(kubectl -n recserve get statefulset annshard -o jsonpath='{.status.readyReplicas}')" = 4 ]; }
for _ in $(seq 3000); do ready && break; sleep 0.2; done
ready || { echo "the replicas are not ready after 10 minutes" >&2; exit 1; }
restarts=$(kubectl -n recserve get pods -l app.kubernetes.io/name=gateway \
  -o jsonpath='{range .items[*]}{.status.containerStatuses[0].restartCount}{"\n"}{end}' | paste -sd+ | bc)
printf 'ready after %.1f s, gateway restarts %s\n' "$(echo "$(date +%s.%N) - $start" | bc)" "$restarts"
