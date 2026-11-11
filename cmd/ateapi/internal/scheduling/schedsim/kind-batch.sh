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

# EXPERIMENT ONLY. Runs the kind matrix in priority order: the drain and ramp
# patterns across every scenario first, then the rest. Policies are shuffled
# within each pattern so drift does not favor one.
#
# usage: kind-batch.sh

set -o errexit -o nounset -o pipefail

: "${KUBECONFIG:?point KUBECONFIG at the experiment cluster}"
HERE="$(cd "$(dirname "$0")" && pwd)"
SCENARIOS="${SCENARIOS:-a-homogeneous c-hetero-actors b-mixed}"
POLICIES="${POLICIES:-p2c bestofd:3 mostallocated}"
PHASES="${PHASES:-ramp-down,ramp burst,square-wave,constant}"
SEED="${SEED:-1}"

setup() {
  local scenario="$1" atespace
  case "${scenario}" in
    a-*) atespace=schedexp-a ;;
    b-*) atespace=schedexp-b ;;
    c-*) atespace=schedexp-c ;;
  esac
  "${HERE}/kind-setup.sh" "${scenario}" 2>&1 | grep -v 'no --token-file given'
  "${BIN:-/tmp/schedsim-bin}" -kind -setup -kubeconfig "${KUBECONFIG}" -scenario "${scenario}" \
    -atespace "${atespace}" 2>&1 | grep -v 'no --token-file given' | tail -3
}

for phase in ${PHASES}; do
  for scenario in ${SCENARIOS}; do
    setup "${scenario}"
    for pattern in ${phase//,/ }; do
      for policy in $(printf '%s\n' ${POLICIES} | sort -R); do
        if grep -q "^${scenario},${pattern},${policy},${SEED}," "${OUT:-/tmp/schedkind}/runs.csv" 2>/dev/null; then
          continue
        fi
        echo "=== $(date -u +%H:%M:%S) ${scenario} ${pattern} ${policy}"
        "${HERE}/kind-run.sh" "${scenario}" "${pattern}" "${policy}" "${SEED}" | grep -E '^(20|a-|b-|c-)' | grep -vE 'reset round|connected' || echo "run failed"
      done
    done
  done
done
echo "batch done"
