#!/usr/bin/env bash
set -euo pipefail

# Cluster-free readiness gate for the 100-agent plan.  The controller is run
# with --plan, which validates corpus selection and backend mapping without
# invoking oc or creating any Kubernetes resource.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CORPUS="${1:-$ROOT/data/replay.json}"
REQUIRE_TRACES="${REQUIRE_TRACES:-100}"
if [ ! -f "$CORPUS" ]; then
  echo "[local-readiness] corpus not found: $CORPUS" >&2
  echo "[local-readiness] build one with: python -m trace_replay_sim ingest --out data/replay.json --limit 400" >&2
  echo "[local-readiness] the gate needs $REQUIRE_TRACES traces across all four workloads; lower REQUIRE_TRACES for a smaller corpus" >&2
  exit 1
fi
TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/trace-replay-readiness.XXXXXX")"
trap 'rm -rf "$TMP_ROOT"' EXIT

cd "$ROOT"
echo "[local-readiness] syntax"
python3 -m py_compile scripts/run_experiment.py scripts/domain_backend.py scripts/appworld_service.py src/trace_replay_sim/*.py
bash -n scripts/run_experiment.sh scripts/deploy_workload_backends.sh

echo "[local-readiness] tests"
pytest -q

echo "[local-readiness] 100-agent plan"
scripts/run_experiment.sh \
  --corpus "$CORPUS" \
  --out "$TMP_ROOT/plan-100" \
  --agents 100 \
  --warm-pool-size 100 \
  --traces-per-workload 25 \
  --workload-plan personal_assistant=25,software_engineering_qa=25,research_deep_search=25,customer_support_ops=25 \
  --nodes "${TRACE_REPLAY_ALLOWED_NODES:-worker-a,worker-b}" \
  --workspace-access-mode ReadWriteOnce \
  --require-traces "$REQUIRE_TRACES" \
  --plan

echo "[local-readiness] PASS: code, tests, and the ${REQUIRE_TRACES}-trace production plan are valid without cluster access"
