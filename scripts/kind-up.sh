#!/usr/bin/env bash
# Creates the kind cluster (scripts/kind-cluster.sh DATA), deploys
# metrics-server and deploy/kind, and reports how long the Pods took to be
# ready and how often the gateways restarted on the way.
# usage: scripts/kind-up.sh DATA
set -euo pipefail
cd "$(dirname "$0")/.."
scripts/kind-cluster.sh "${1:?usage: scripts/kind-up.sh DATA}"
kubectl apply -k deploy/kind/metrics-server

start=$(date +%s.%N)
kubectl apply -k deploy/kind
kubectl -n recserve rollout status deployment/gateway --timeout=10m
until [ "$(kubectl -n recserve get statefulset annshard -o jsonpath='{.status.readyReplicas}')" = 4 ]; do sleep 0.2; done
restarts=$(kubectl -n recserve get pods -l app.kubernetes.io/name=gateway \
  -o jsonpath='{range .items[*]}{.status.containerStatuses[0].restartCount}{"\n"}{end}' | paste -sd+ | bc)
printf 'ready after %.1f s, gateway restarts %s\n' "$(echo "$(date +%s.%N) - $start" | bc)" "$restarts"
