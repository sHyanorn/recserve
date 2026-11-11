#!/usr/bin/env bash
# Swaps the gateways in kind to bundle VERSION, whose directory must already be
# on the shared volume (DATA/bundle/VERSION): a rollout of the gateway
# Deployment to BUNDLE_VERSION=VERSION (D048). It starts only once every gateway
# Pod, terminating ones included, runs the version of the Deployment, and returns
# once none runs another, so at most two versions are ever in use.
# usage: scripts/kind-swap.sh VERSION
set -euo pipefail
version=${1:?usage: scripts/kind-swap.sh VERSION}
k() { kubectl -n recserve "$@"; }
pods_by_version() {
  k get pods -l app.kubernetes.io/name=gateway -o jsonpath='{range .items[*]}{.metadata.name} {.spec.containers[0].env[?(@.name=="BUNDLE_VERSION")].value}{"\n"}{end}'
}

docker exec recserve-control-plane test -f "/data/bundle/$version/manifest.json" ||
  { echo "no bundle $version on the volume" >&2; exit 1; }
current=$(k get deploy gateway -o jsonpath='{.spec.template.spec.containers[0].env[0].value}')
if pods_by_version | awk -v v="$current" '$2 != v { found = 1 } END { exit !found }'; then
  echo "a swap to $current is still in progress" >&2
  exit 1
fi
# The test makes the change conditional on the version just read, so of two
# swaps started at once one fails here.
k patch deploy gateway --type json -p "[
  {\"op\": \"test\", \"path\": \"/spec/template/spec/containers/0/env/0\", \"value\": {\"name\": \"BUNDLE_VERSION\", \"value\": \"$current\"}},
  {\"op\": \"replace\", \"path\": \"/spec/template/spec/containers/0/env/0/value\", \"value\": \"$version\"}]"
k rollout status deploy/gateway --timeout=10m
# The replaced Pods go on serving through their shutdown delay and drain.
old=$(pods_by_version | awk -v v="$version" '$2 != v { print "pod/" $1 }')
[ -z "$old" ] || k wait --for=delete $old --timeout=2m
