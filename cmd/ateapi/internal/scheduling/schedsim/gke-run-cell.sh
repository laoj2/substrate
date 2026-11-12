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

# EXPERIMENT ONLY. Runs one cell on the cluster of the current kubectl
# context: sets the policy on ate-api-server, follows its logs for the whole
# run so log rotation cannot drop retry, detach or crash lines, drives the
# pattern, and appends to OUT/retries.csv.
#
# usage: gke-run-cell.sh SCENARIO ATESPACE PATTERN POLICY SEED
#   gke-run-cell.sh g-gke8 cell-1 ramp-down prefixsat:1:0.2 1

set -o errexit -o nounset -o pipefail

scenario="$1" atespace="$2" pattern="$3" policy="$4" seed="$5"
OUT="${OUT:-cmd/ateapi/internal/scheduling/schedsim/gke-results/phase-b}"
BIN="${BIN:-bin/schedsim}"
KUBECONFIG_PATH="${KUBECONFIG:-${HOME}/.kube/config}"
mkdir -p "${OUT}"

echo "target context: $(kubectl config current-context)"
current="$(kubectl get deploy ate-api-server -n ate-system \
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="ATE_API_SCHEDULING_POLICY")].value}')"
changed=no
if [[ "${current}" != "${policy}" ]]; then
  kubectl set env deploy/ate-api-server -n ate-system -c ate-api-server "ATE_API_SCHEDULING_POLICY=${policy}"
  changed=yes
fi
kubectl rollout status deploy/ate-api-server -n ate-system --timeout=300s

# Old pods linger while they shut down. Wait so nothing dials or follows them.
want="$(kubectl get deploy ate-api-server -n ate-system -o jsonpath='{.spec.replicas}')"
until [[ "$(kubectl get pods -n ate-system -l app=ate-api-server --no-headers | wc -l | tr -d ' ')" == "${want}" ]]; do
  sleep 2
done
sleep 3

# objectstore-plugin's exchange client keeps a channel to the headless
# svc/api. After the rollout it holds the old pods' IPs, and the first snapshot
# transfer on each node waits out grpc-go's 20s minConnectTimeout before it
# re-resolves. Restart atelet, which carries the plugin, so no cell pays that.
if [[ "${changed}" == "yes" ]]; then
  kubectl rollout restart ds/atelet -n ate-system
  kubectl rollout status ds/atelet -n ate-system --timeout=900s
  pool_want="$(kubectl get workerpool pool -n benchmark-workloads -o jsonpath='{.spec.replicas}')"
  kubectl wait --for=jsonpath='{.status.readyReplicas}'="${pool_want}" workerpool/pool -n benchmark-workloads --timeout=600s
fi

# After a change every new pod must have logged the policy at startup. An
# unchanged pod is not checked: its startup lines may have rotated away.
if [[ "${changed}" == "yes" ]]; then
  for pod in $(kubectl get pods -n ate-system -l app=ate-api-server -o name); do
    line="$(kubectl logs -n ate-system "${pod}" -c ate-api-server --tail=-1 | grep '"msg":"scheduling policy"' || true)"
    [[ -n "${line}" ]] || { echo "${pod} did not log its scheduling policy" >&2; exit 1; }
    echo "${line}"
  done
fi

tag="${pattern}_${policy//:/-}_${seed}_${atespace}"
pids=()
for pod in $(kubectl get pods -n ate-system -l app=ate-api-server -o name); do
  kubectl logs -f -n ate-system "${pod}" -c ate-api-server --since=1s \
    > >(grep --line-buffered -e 'schedexp assignment retry' -e 'Detached actor from its full node' -e '"Actor crashed"' \
      >>"${OUT}/events_${tag}_${pod##*/}.log") 2>/dev/null &
  pids+=("$!")
done

restarts() {
  kubectl get pods -n ate-system -o jsonpath='{range .items[*]}{range .status.containerStatuses[*]}{.restartCount}{"\n"}{end}{end}' \
    | awk '{n += $1} END {print n + 0}'
}
restarts_before="$(restarts)"
t_start="$(date +%s)"

"${BIN}" -kind -kubeconfig "${KUBECONFIG_PATH}" -scenario "${scenario}" -pattern "${pattern}" \
  -policy "${policy}" -atespace "${atespace}" -seed "${seed}" -out "${OUT}" 2>&1 | tee -a "${OUT}/driver.log"

t_end="$(date +%s)"
# A follower that ended early means the counts are short.
stream_ok=yes
for p in "${pids[@]}"; do
  if ! kill -0 "${p}" 2>/dev/null; then
    stream_ok=no
  fi
  kill "${p}" 2>/dev/null || true
done
sleep 1
restarted=$(( $(restarts) - restarts_before ))

count() { cat "${OUT}"/events_"${tag}"_*.log 2>/dev/null | grep -c -e "$1" || true; }
if [[ ! -f "${OUT}/retries.csv" ]]; then
  echo "scenario,pattern,policy,seed,atespace,filled_up,version_conflict,detaches,crashes_log,stream_ok,ate_system_restarts,t_start,t_end" >"${OUT}/retries.csv"
fi
echo "${scenario},${pattern},${policy},${seed},${atespace},$(count '"reason":"filled_up"'),$(count '"reason":"version_conflict"'),$(count 'Detached actor from its full node'),$(count '"Actor crashed"'),${stream_ok},${restarted},${t_start},${t_end}" >>"${OUT}/retries.csv"
tail -1 "${OUT}/retries.csv"
