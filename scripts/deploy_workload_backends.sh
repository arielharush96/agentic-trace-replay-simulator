#!/usr/bin/env bash
set -euo pipefail

# Deploy only the workload-specific backend services. This is intentionally a
# separate, explicit gate so backend readiness can be checked before creating
# OpenClaw agents, OpenShell workspaces, or sandboxes.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
NAMESPACE="${NAMESPACE:-trace-replay}"
SYSTEM_NAMESPACE="${SYSTEM_NAMESPACE:-trace-replay}"
OPENSHELL_NAMESPACE="${OPENSHELL_NAMESPACE:-openshell-tracesim}"
NODE="${NODE:?set NODE to a schedulable worker node}"
KUBECONFIG_PATH="${KUBECONFIG:-$HOME/.kube/config}"
WORKLOAD_PLAN="${WORKLOAD_PLAN:-personal_assistant=5,software_engineering_qa=5,research_deep_search=5,customer_support_ops=5}"
AGENTS="${AGENTS:-20}"
BACKEND_MANIFEST="${BACKEND_MANIFEST:-}"

usage() {
  cat <<'EOF'
Usage: scripts/deploy_workload_backends.sh [--execute]

Deploys and waits for the selected workload backends only. It does not create
OpenClaw agents, OpenShell workspaces, sandboxes, Jobs, or mock-LLM services.

Environment overrides:
  WORKLOAD_PLAN, AGENTS, NODE, KUBECONFIG, NAMESPACE, SYSTEM_NAMESPACE,
  OPENSHELL_NAMESPACE, BACKEND_MANIFEST
EOF
}

EXECUTE=0
while (($#)); do
  case "$1" in
    --execute) EXECUTE=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "ERROR: unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if (( ! EXECUTE )); then
  echo "ERROR: pass --execute explicitly; this command changes only trace-replay-owned backend resources" >&2
  exit 2
fi

export KUBECONFIG="$KUBECONFIG_PATH"
export PYTHONPATH="$ROOT/src${PYTHONPATH:+:$PYTHONPATH}"

ARGS=(
  --agents "$AGENTS"
  --workload-plan "$WORKLOAD_PLAN"
  --namespace "$NAMESPACE"
  --system-namespace "$SYSTEM_NAMESPACE"
  --openshell-namespace "$OPENSHELL_NAMESPACE"
  --node "$NODE"
  --kubeconfig "$KUBECONFIG_PATH"
  --backends-only
  --execute
)
if [[ -n "$BACKEND_MANIFEST" ]]; then
  ARGS+=(--backend-manifest "$BACKEND_MANIFEST")
fi

exec "$PYTHON_BIN" "$ROOT/scripts/run_experiment.py" "${ARGS[@]}"
