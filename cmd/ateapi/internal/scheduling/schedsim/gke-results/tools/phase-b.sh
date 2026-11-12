#!/usr/bin/env bash
# Phase B orchestrator: two shuffled passes over ramp-down/burst x 3 policies.
# Each run gets a fresh WorkerPool and a fresh atespace population.
# usage: [SCENARIO=g-gke8] [PREFIX=cell] [OUT=dir] phase-b.sh [START_INDEX]
set -o nounset -o pipefail

G="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(git -C "${G}" rev-parse --show-toplevel)"
SCENARIO="${SCENARIO:-g-gke8}"
PREFIX="${PREFIX:-cell}"
export OUT="${OUT:-${G}/phase-b}"
mkdir -p "${OUT}"
cd "${REPO}"
# shellcheck disable=SC1091
source /usr/local/google/home/luizaoj/go/src/substrate-gke/.ate-dev-env.sh
# Pin every kubectl, kubectl-ate and driver call to sched-exp, so a context
# switch elsewhere cannot redirect a cell to another cluster.
export KUBECONFIG="${G}/kubeconfig-sched-exp"
[[ "$(kubectl config current-context)" == "${KUBECTL_CONTEXT}" ]] || { echo "kubeconfig is not ${KUBECTL_CONTEXT}" >&2; exit 1; }

start="${1:-1}"
# CELLS_FILE lists "idx pattern policy seed" lines to run instead of the two passes.
# Fixed shuffle so a restart resumes the same order.
if [[ -n "${CELLS_FILE:-}" ]]; then
  mapfile -t cells <"${CELLS_FILE}"
else
mapfile -t cells < <(python3 - <<'EOF'
import itertools, random
combos = list(itertools.product(["ramp-down", "burst"], ["p2c", "bestofd:3", "prefixsat:1:0.2"]))
out = []
for seed in (1, 2):
    c = combos[:]
    random.Random(100 + seed).shuffle(c)
    out += [f"{p} {pol} {seed}" for p, pol in c]
for i, x in enumerate(out, 1):
    print(f"{i:02d} {x}")
EOF
)
fi
printf '%s\n' "${cells[@]}" >> "${OUT}/order.txt"

for line in "${cells[@]}"; do
  read -r idx pattern policy seed <<<"${line}"
  (( 10#${idx} < start )) && continue
  atespace="${PREFIX}-${idx}"
  echo "=== $(date -Is) ${SCENARIO} cell ${idx}: ${pattern} ${policy} seed ${seed} atespace ${atespace}" | tee -a "${OUT}/phase-b.log"
  if ! cmd/ateapi/internal/scheduling/schedsim/gke-setup.sh "${SCENARIO}" "${atespace}" 24 2 4160Mi 2>&1 \
      | grep -v -e "pushed blob" -e "existing blob" -e "existing manifest" -e "token-file" >>"${OUT}/setup.log"; then
    echo "setup failed for cell ${idx}" | tee -a "${OUT}/phase-b.log"
    continue
  fi
  if ! cmd/ateapi/internal/scheduling/schedsim/gke-run-cell.sh "${SCENARIO}" "${atespace}" "${pattern}" "${policy}" "${seed}" 2>&1 \
      | grep -v "token-file" >>"${OUT}/cells.log"; then
    echo "run failed for cell ${idx}" | tee -a "${OUT}/phase-b.log"
  fi
  tail -1 "${OUT}/retries.csv" | tee -a "${OUT}/phase-b.log"
  tail -1 "${OUT}/runs.csv" | tee -a "${OUT}/phase-b.log"
  kubectl port-forward -n benchmarking svc/prometheus 19090:9090 >/dev/null 2>&1 &
  pf=$!
  for _ in $(seq 30); do curl -sf localhost:19090/-/ready >/dev/null && break; sleep 1; done
  python3 "${G}/tools/collect_metrics.py" "${OUT}" >>"${OUT}/phase-b.log" 2>&1 || echo "metrics failed for cell ${idx}" | tee -a "${OUT}/phase-b.log"
  kill "${pf}" 2>/dev/null || true
done
echo "=== $(date -Is) phase B done" | tee -a "${OUT}/phase-b.log"
