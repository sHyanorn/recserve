#!/usr/bin/env bash
# Checks in the kind cluster that the NetworkPolicy is enforced (D040, D047):
# a gateway Pod reaches a replica's port, through an ephemeral container in its
# network namespace; a Pod of the namespace with other labels does not, nor a
# Pod with the gateway's labels in another namespace. Runs on a cluster made by
# scripts/kind-cluster.sh; the Pods need only be running, not ready. Fails
# where the network plugin does not enforce NetworkPolicy.
# usage: scripts/kind-netpol.sh
set -euo pipefail
cd "$(dirname "$0")/.."
image=recserve/netpol-client:busybox-1.37
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
docker pull -q docker.io/library/busybox:1.37@sha256:a7dbc319b1438115a207e11930dc4bb3e643dcb3ec0bdd694bb36cd8fad289ec >/dev/null
docker tag docker.io/library/busybox:1.37@sha256:a7dbc319b1438115a207e11930dc4bb3e643dcb3ec0bdd694bb36cd8fad289ec "$image"
docker save --platform linux/amd64 -o "$tmp/client.tar" "$image"
kind load image-archive --name recserve "$tmp/client.tar"

kubectl apply -k deploy/kind >/dev/null
# The replica's name resolves once its address is in the Service's EndpointSlice.
for _ in $(seq 180); do
  kubectl -n recserve get endpointslice -l kubernetes.io/service-name=annshard \
    -o jsonpath='{.items[*].endpoints[*].targetRef.name}' | grep -qw annshard-0 && break
  sleep 1
done
kubectl -n recserve wait --for=jsonpath='{.status.phase}'=Running \
  "$(kubectl -n recserve get pods -l app.kubernetes.io/name=gateway -o name | head -1)" --timeout=3m >/dev/null
replica=annshard-0.annshard.recserve.svc.cluster.local
# What the namespace's Pod Security level asks of a container.
sc='{"runAsNonRoot": true, "runAsUser": 65534, "allowPrivilegeEscalation": false, "capabilities": {"drop": ["ALL"]}, "seccompProfile": {"type": "RuntimeDefault"}}'

# from_gateway: whether a gateway Pod opens a TCP connection to the replica.
from_gateway() {
  local pod name=netpol-$RANDOM code=
  pod=$(kubectl -n recserve get pods -l app.kubernetes.io/name=gateway -o jsonpath='{.items[0].metadata.name}')
  echo '{"securityContext": {"runAsUser": 65534}}' > "$tmp/custom.json"
  kubectl -n recserve debug "$pod" -c "$name" --image="$image" --profile=restricted --custom="$tmp/custom.json" -- nc -z -w 5 "$replica" 9090 >/dev/null
  for _ in $(seq 60); do
    code=$(kubectl -n recserve get pod "$pod" -o jsonpath="{.status.ephemeralContainerStatuses[?(@.name==\"$name\")].state.terminated.exitCode}")
    [ -z "$code" ] || break
    sleep 0.5
  done
  [ "$code" = 0 ]
}

# from_pod NAMESPACE LABELS: whether a Pod there with those labels does.
from_pod() {
  local name=netpol-$RANDOM phase=
  kubectl -n "$1" run "$name" --image="$image" --restart=Never --overrides="{
    \"metadata\": {\"labels\": $2},
    \"spec\": {\"containers\": [{\"name\": \"c\", \"image\": \"$image\", \"command\": [\"nc\", \"-z\", \"-w\", \"5\", \"$replica\", \"9090\"],
      \"securityContext\": $sc}]}}" >/dev/null
  for _ in $(seq 60); do
    phase=$(kubectl -n "$1" get pod "$name" -o jsonpath='{.status.phase}')
    case $phase in Succeeded | Failed) break ;; esac
    sleep 0.5
  done
  kubectl -n "$1" delete pod "$name" --wait=false >/dev/null
  case $phase in
    Succeeded) return 0 ;;
    Failed) return 1 ;;
    *) echo "FAIL: the client Pod in $1 did not run ($phase)" >&2; exit 1 ;;
  esac
}

fail=0
from_gateway || { echo "FAIL: a gateway Pod cannot reach the replicas" >&2; fail=1; }
from_pod recserve '{"app.kubernetes.io/name": "other"}' && { echo "FAIL: a Pod without the gateway's labels reaches the replicas" >&2; fail=1; }
from_pod default '{"app.kubernetes.io/name": "gateway"}' && { echo "FAIL: a Pod of another namespace reaches the replicas" >&2; fail=1; }
[ $fail = 0 ] && echo PASS
exit $fail
