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

# EXPERIMENT ONLY. Runs one schedsim kind cell: sets the scheduling policy on
# ate-api-server, waits for the rollout, drives the pattern, and counts the
# bind retries ate-api-server logged during the run.
#
# usage: kind-run.sh SCENARIO PATTERN POLICY SEED
#   POLICY is ATE_API_SCHEDULING_POLICY, e.g. p2c, topk:3, bestofd:3, firstfit:2

set -o errexit -o nounset -o pipefail

scenario="$1" pattern="$2" policy="$3" seed="$4"
: "${KUBECONFIG:?point KUBECONFIG at the experiment cluster}"
OUT="${OUT:-/tmp/schedkind}"
BIN="${BIN:-/tmp/schedsim-bin}"
case "${scenario}" in
  a-*) atespace=schedexp-a ;;
  b-*) atespace=schedexp-b ;;
  c-*) atespace=schedexp-c ;;
esac
mkdir -p "${OUT}"

current="$(kubectl get deploy ate-api-server -n ate-system \
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="ATE_API_SCHEDULING_POLICY")].value}')"
if [[ "${current}" != "${policy}" ]]; then
  kubectl set env deploy/ate-api-server -n ate-system -c ate-api-server "ATE_API_SCHEDULING_POLICY=${policy}"
fi
kubectl rollout status deploy/ate-api-server -n ate-system --timeout=300s

start="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
"${BIN}" -kind -kubeconfig "${KUBECONFIG}" -scenario "${scenario}" -pattern "${pattern}" \
  -policy "${policy}" -atespace "${atespace}" -seed "${seed}" -out "${OUT}" 2>&1 \
  | grep -v 'no --token-file given' | tee -a "${OUT}/driver.log"

retries() {
  kubectl logs -n ate-system -l app=ate-api-server -c ate-api-server --since-time="${start}" \
    --tail=-1 --max-log-requests=10 | grep "schedexp assignment retry" | grep -c "\"reason\":\"$1\"" || true
}
if [[ ! -f "${OUT}/retries.csv" ]]; then
  echo "scenario,pattern,policy,seed,filled_up,version_conflict" >"${OUT}/retries.csv"
fi
echo "${scenario},${pattern},${policy},${seed},$(retries filled_up),$(retries version_conflict)" >>"${OUT}/retries.csv"
tail -1 "${OUT}/retries.csv"
