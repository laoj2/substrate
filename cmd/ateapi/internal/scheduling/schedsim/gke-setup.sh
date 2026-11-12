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

# EXPERIMENT ONLY. Creates the WorkerPool, the sched-250m actor template and
# the actor population for one scenario, on the cluster of the current kubectl
# context.
#
# usage: gke-setup.sh SCENARIO ATESPACE REPLICAS WORKER_CPU WORKER_MEMORY [FAKE_MAX_ACTORS]
#   fake data plane:  gke-setup.sh f-kind200 cell-1 200 4 8Gi 16
#   real data plane:  gke-setup.sh g-gke8 cell-1 24 2 4Gi

set -o errexit -o nounset -o pipefail

scenario="$1" atespace="$2" replicas="$3" cpu="$4" memory="$5" fake_max="${6:-}"
: "${KO_DOCKER_REPO:?set KO_DOCKER_REPO}"
NS=benchmark-workloads
BIN="${BIN:-bin/schedsim}"
KUBECTL_ATE="${KUBECTL_ATE:-bin/kubectl-ate}"
KUBECONFIG_PATH="${KUBECONFIG:-${HOME}/.kube/config}"

echo "target context: $(kubectl config current-context)"
go build -o "${KUBECTL_ATE}" ./cmd/kubectl-ate
go build -o "${BIN}" ./cmd/ateapi/internal/scheduling/schedsim

annotations=""
if [[ -n "${fake_max}" ]]; then
  annotations="  annotations:
    ate.dev/fake-max-actors: \"${fake_max}\""
fi

kubectl create namespace "${NS}" --dry-run=client -o yaml | kubectl apply -f -

# A pool's capacity is read when its Workers are made, so recreate the pool
# instead of patching it.
if kubectl get workerpool pool -n "${NS}" >/dev/null 2>&1; then
  kubectl delete workerpool pool -n "${NS}" --wait=true --timeout=600s
fi
cat <<YAML | hack/run-tool.sh ko apply -f -
apiVersion: ate.dev/v1
kind: WorkerPool
metadata:
  name: pool
  namespace: ${NS}
  labels:
    workload: benchmark-worker
${annotations}
spec:
  replicas: ${replicas}
  sandboxClasses:
  - name: gvisor
  workerImage: ko://go.ate.dev/substrate/cmd/ateworker-gvisor
  template:
    labels:
      workload: benchmark-worker
    resources: {requests: {cpu: "${cpu}", memory: "${memory}"}, limits: {cpu: "${cpu}", memory: "${memory}"}}
YAML
kubectl wait --for=jsonpath='{.status.readyReplicas}'="${replicas}" workerpool/pool -n "${NS}" --timeout=900s

"${KUBECTL_ATE}" create atespace "${NS}" >/dev/null 2>&1 || true
if ! "${KUBECTL_ATE}" get actor-template sched-250m -a "${NS}" >/dev/null 2>&1; then
  cat <<YAML | hack/run-tool.sh ko resolve -f - | "${KUBECTL_ATE}" create actor-template -f -
metadata:
  atespace: ${NS}
  name: sched-250m
workerSelector:
  matchLabels:
    workload: benchmark-worker
containers:
- name: glutton
  image: ko://go.ate.dev/substrate/cmd/benchmarking/glutton
  # As in benchmarking/workloads/manifests/glutton-template.yaml.tmpl:
  # glutton exits without --data-dir, and the wakeup probe holds ResumeActor
  # until the actor answers.
  command: ["/ko-app/glutton", "--grpc-listen-addr=:80", "--data-dir=/var/lib/glutton", "--mode=http"]
  wakeupProbe:
    httpGet:
      path: /readyz
      port: 80
resources:
  limits:
    cpuMillicores: "250"
    memoryBytes: "536870912"
snapshotConfig:
  preferredFidelity: SNAPSHOT_FIDELITY_MEMORY
YAML
fi
tag=""
for _ in $(seq 60); do
  tag="$("${KUBECTL_ATE}" get actor-template sched-250m -a "${NS}" -o json | jq -r '.status.goldenSnapshotStatus.goldenTag.name // empty')"
  [[ -n "${tag}" ]] && break
  sleep 5
done
[[ -n "${tag}" ]] || { echo "template sched-250m has no golden snapshot" >&2; exit 1; }

"${BIN}" -kind -setup -kubeconfig "${KUBECONFIG_PATH}" -scenario "${scenario}" -atespace "${atespace}"
echo "setup done: scenario ${scenario}, atespace ${atespace}"
