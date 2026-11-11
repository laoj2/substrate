#!/usr/bin/env bash
# Copyright 2026 The Agent Substrate Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# EXPERIMENT ONLY. Shapes a fake-data-plane cluster for one schedsim scenario:
# the WorkerPools and the sched-* ActorTemplates. Pools of other scenarios are
# deleted. Needs KUBECONFIG and KO_DOCKER_REPO pointing at the target cluster.
#
# usage: kind-setup.sh a-homogeneous|b-mixed|c-hetero-actors

set -o errexit -o nounset -o pipefail

scenario="$1"
ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"
NS=benchmark-workloads
KUBECTL_ATE="${KUBECTL_ATE:-/tmp/schedsim-kubectl-ate}"
go build -o "${KUBECTL_ATE}" ./cmd/kubectl-ate

pool() { # name replicas cpu memory slots
  cat <<EOF
---
apiVersion: ate.dev/v1
kind: WorkerPool
metadata:
  name: $1
  namespace: ${NS}
  labels:
    workload: benchmark-worker
  annotations:
    ate.dev/fake-max-actors: "$5"
spec:
  replicas: $2
  sandboxClasses:
  - name: gvisor
  workerImage: ko://go.ate.dev/substrate/cmd/ateworker-gvisor
  template:
    labels:
      workload: benchmark-worker
    resources: {requests: {cpu: "$3", memory: "$4"}, limits: {cpu: "$3", memory: "$4"}}
EOF
}

template() { # name cpuMillicores memoryBytes
  cat <<EOF
metadata:
  atespace: ${NS}
  name: $1
workerSelector:
  matchLabels:
    workload: benchmark-worker
containers:
- name: glutton
  image: ko://go.ate.dev/substrate/cmd/benchmarking/glutton
  command: ["/ko-app/glutton", "--grpc-listen-addr=:80", "--mode=http"]
resources:
  limits:
    cpuMillicores: "$2"
    memoryBytes: "$3"
snapshotConfig:
  preferredFidelity: SNAPSHOT_FIDELITY_MEMORY
EOF
}

case "${scenario}" in
  a-homogeneous)
    pools="$(pool pool 40 4 8Gi 16)"
    want="pool"
    ;;
  b-mixed)
    pools="$(pool big 6 8 16Gi 32; pool small 20 2 4Gi 8)"
    want="big small"
    ;;
  c-hetero-actors)
    pools="$(pool pool 40 4 8Gi 32)"
    want="pool"
    ;;
  *)
    echo "unknown scenario ${scenario}" >&2
    exit 1
    ;;
esac

kubectl create namespace "${NS}" --dry-run=client -o yaml | kubectl apply -f -

# A pool's capacity is read when its Workers are made, so recreate pools
# rather than patch them.
for p in $(kubectl get workerpool -n "${NS}" -o name); do
  echo "deleting ${p}"
  kubectl delete -n "${NS}" "${p}" --wait=true
done
echo "${pools}" | hack/run-tool.sh ko apply -f -
for p in ${want}; do
  replicas="$(kubectl get workerpool "${p}" -n "${NS}" -o jsonpath='{.spec.replicas}')"
  kubectl wait --for=jsonpath='{.status.readyReplicas}'="${replicas}" "workerpool/${p}" -n "${NS}" --timeout=300s
done

"${KUBECTL_ATE}" create atespace "${NS}" >/dev/null 2>&1 || true
for spec in "sched-250m 250 536870912" "sched-125m 125 268435456" "sched-1000m 1000 2147483648"; do
  read -r name cpu mem <<<"${spec}"
  if "${KUBECTL_ATE}" get actor-template "${name}" -a "${NS}" >/dev/null 2>&1; then
    continue
  fi
  template "${name}" "${cpu}" "${mem}" | hack/run-tool.sh ko resolve -f - | "${KUBECTL_ATE}" create actor-template -f -
done
for name in sched-250m sched-125m sched-1000m; do
  for _ in $(seq 60); do
    tag="$("${KUBECTL_ATE}" get actor-template "${name}" -a "${NS}" -o json | jq -r '.status.goldenSnapshotStatus.goldenTag.name // empty')"
    [[ -n "${tag}" ]] && break
    sleep 5
  done
  [[ -n "${tag}" ]] || { echo "template ${name} has no golden snapshot" >&2; exit 1; }
done
echo "scenario ${scenario} ready"
