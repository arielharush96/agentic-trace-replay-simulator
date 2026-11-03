#!/usr/bin/env bash
set -euo pipefail

# The controller fans out one trace job per deployed agent.  macOS commonly
# starts shells with a soft descriptor limit of 256, which is too low for a
# 100-agent run because each concurrent oc/collection operation owns multiple
# pipes and sockets.  Raise only this controller process's soft limit; do not
# change any cluster or system-wide setting.
ulimit -n 8192 2>/dev/null || true

# Main operator-facing entrypoint. The Python controller is deliberately kept
# behind this shell interface because deployment, kubeconfig, logging and
# lifecycle control belong in the shell; Python is used for JSON/OTel analysis.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
CORPUS=""
OUT=""
AGENTS=20
WARM_POOL_SIZE=10
TRACES_PER_WORKLOAD=5
MAX_TRACES=""
TRACE_OFFSET=0
REQUIRE_TRACES=""
SESSION_ID=""
SESSION_IDS=""
AGENT_OFFSET=0
AGENT_INDICES=""
NAMESPACE="trace-replay"
SYSTEM_NAMESPACE="trace-replay"
OPENSHELL_NAMESPACE="openshell-tracesim"
NODE="${NODE:-${TRACE_REPLAY_NODE:-}}"
NODES=""
KUBECONFIG_PATH="${KUBECONFIG:-$HOME/.kube/config}"
MODE=""
BACKENDS_ONLY=0
APPWORLD=0
APPWORLD_TASK_MAP=""
WORKLOAD_PLAN=""
BACKEND_MANIFEST=""
SWE_TASK_MANIFEST=""
REUSE_AGENTS=0
REUSE_EXISTING_AGENT_PREFIX=""
REUSE_EXISTING_WORKSPACE_PREFIX=""
RESUME_EXISTING_RUN=0
RESUME_PREFERRED_NODE=""
DEPLOYMENT_ONLY=0
ALLOW_CROSS_NODE=0
RETRIES=0
EXECUTION_WORKERS=""
JOB_DISPATCH_INTERVAL="2"
CLUSTER_API_WORKERS="6"
CLUSTER_RETRY_TIMEOUT="1800"
CLUSTER_RETRY_INTERVAL="10"
WORKSPACE_PVC="${TRACE_REPLAY_WORKSPACE_PVC:-}"
WORKSPACE_PVCS="${TRACE_REPLAY_WORKSPACE_PVCS:-}"
WORKSPACE_STORAGE_CLASS="${TRACE_REPLAY_WORKSPACE_STORAGE_CLASS:-rook-cephfs-fast}"
WORKSPACE_ACCESS_MODE="${TRACE_REPLAY_WORKSPACE_ACCESS_MODE:-ReadWriteMany}"
OPENSHELL_GATEWAY_ENDPOINT="${TRACE_REPLAY_OPENSHELL_GATEWAY_ENDPOINT:-http://openshell.openshell-tracesim.svc.cluster.local:8080}"
OPENSHELL_CLIENT_SECRET="${TRACE_REPLAY_OPENSHELL_CLIENT_SECRET:-openshell-client-tls}"

usage() {
  sed -n '1,45p' "$0"
}

die() { echo "ERROR: $*" >&2; exit 2; }

while (($#)); do
  case "$1" in
    --corpus) CORPUS="$2"; shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    --agents) AGENTS="$2"; shift 2 ;;
    --warm-pool-size) WARM_POOL_SIZE="$2"; shift 2 ;;
    --traces-per-workload) TRACES_PER_WORKLOAD="$2"; shift 2 ;;
    --max-traces) MAX_TRACES="$2"; shift 2 ;;
    --trace-offset) TRACE_OFFSET="$2"; shift 2 ;;
    --require-traces) REQUIRE_TRACES="$2"; shift 2 ;;
    --session-id) SESSION_ID="$2"; shift 2 ;;
    --session-ids) SESSION_IDS="$2"; shift 2 ;;
    --agent-offset) AGENT_OFFSET="$2"; shift 2 ;;
    --agent-indices) AGENT_INDICES="$2"; shift 2 ;;
    --namespace) NAMESPACE="$2"; shift 2 ;;
    --system-namespace) SYSTEM_NAMESPACE="$2"; shift 2 ;;
    --openshell-namespace) OPENSHELL_NAMESPACE="$2"; shift 2 ;;
    --openshell-gateway-endpoint) OPENSHELL_GATEWAY_ENDPOINT="$2"; shift 2 ;;
    --openshell-client-secret) OPENSHELL_CLIENT_SECRET="$2"; shift 2 ;;
    --node) NODE="$2"; shift 2 ;;
    --nodes) NODES="$2"; shift 2 ;;
    --kubeconfig) KUBECONFIG_PATH="$2"; shift 2 ;;
    --plan) MODE="plan"; shift ;;
    --execute) MODE="execute"; shift ;;
    --backends-only) BACKENDS_ONLY=1; MODE="execute"; shift ;;
    --appworld) APPWORLD=1; shift ;;
    --appworld-task-map) APPWORLD_TASK_MAP="$2"; shift 2 ;;
    --workload-plan) WORKLOAD_PLAN="$2"; shift 2 ;;
    --backend-manifest) BACKEND_MANIFEST="$2"; shift 2 ;;
    --swe-task-manifest) SWE_TASK_MANIFEST="$2"; shift 2 ;;
    --appworld-endpoint) APPWORLD_ENDPOINT="$2"; shift 2 ;;
    --reuse-agents) REUSE_AGENTS=1; shift ;;
    --reuse-existing-agent-prefix) REUSE_EXISTING_AGENT_PREFIX="$2"; shift 2 ;;
    --reuse-existing-workspace-prefix) REUSE_EXISTING_WORKSPACE_PREFIX="$2"; shift 2 ;;
    --resume-existing-run) RESUME_EXISTING_RUN=1; shift ;;
    --resume-preferred-node) RESUME_PREFERRED_NODE="$2"; shift 2 ;;
    --deployment-only) DEPLOYMENT_ONLY=1; shift ;;
    --allow-cross-node) ALLOW_CROSS_NODE=1; shift ;;
    --execution-workers) EXECUTION_WORKERS="$2"; shift 2 ;;
    --job-dispatch-interval) JOB_DISPATCH_INTERVAL="$2"; shift 2 ;;
    --cluster-api-workers) CLUSTER_API_WORKERS="$2"; shift 2 ;;
    --cluster-retry-timeout) CLUSTER_RETRY_TIMEOUT="$2"; shift 2 ;;
    --cluster-retry-interval) CLUSTER_RETRY_INTERVAL="$2"; shift 2 ;;
    --workspace-pvc) WORKSPACE_PVC="$2"; shift 2 ;;
    --workspace-pvcs) WORKSPACE_PVCS="$2"; shift 2 ;;
    --workspace-storage-class) WORKSPACE_STORAGE_CLASS="$2"; shift 2 ;;
    --workspace-access-mode) WORKSPACE_ACCESS_MODE="$2"; shift 2 ;;
    --retries) RETRIES="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown argument: $1" ;;
  esac
done

if ((BACKENDS_ONLY)); then
  [ "$MODE" = execute ] || die "--backends-only requires --execute"
else
  [ -n "$CORPUS" ] || die "--corpus is required"
  [ -n "$OUT" ] || die "--out is required"
  [ -f "$CORPUS" ] || die "corpus not found: $CORPUS"
  [[ "$MODE" == plan || "$MODE" == execute ]] || die "choose exactly one of --plan or --execute"
fi
[[ "$AGENTS" =~ ^([1-9]|[1-9][0-9]|100)$ ]] || die "--agents must be 1..100"
[[ "$WARM_POOL_SIZE" =~ ^[0-9]+$ ]] || die "--warm-pool-size must be zero or positive"
(( WARM_POOL_SIZE <= AGENTS )) || die "--warm-pool-size cannot exceed --agents"
[[ "$TRACES_PER_WORKLOAD" =~ ^[1-9][0-9]*$ ]] || die "--traces-per-workload must be positive"
if [[ -n "$MAX_TRACES" ]]; then
  [[ "$MAX_TRACES" =~ ^[1-9][0-9]*$ ]] || die "--max-traces must be positive"
fi
[[ "$TRACE_OFFSET" =~ ^[0-9]+$ ]] || die "--trace-offset must be zero or positive"
[[ "$AGENT_OFFSET" =~ ^[0-9]+$ ]] || die "--agent-offset must be zero or positive"
if [[ -n "$AGENT_INDICES" && ! "$AGENT_INDICES" =~ ^[0-9]+(,[0-9]+)*$ ]]; then
  die "--agent-indices must be comma-separated positive integers"
fi
if [[ -n "$REQUIRE_TRACES" ]]; then
  [[ "$REQUIRE_TRACES" =~ ^[1-9][0-9]*$ ]] || die "--require-traces must be positive"
fi
[[ "$JOB_DISPATCH_INTERVAL" =~ ^[0-9]+([.][0-9]+)?$ ]] || die "--job-dispatch-interval must be zero or a non-negative number"
[[ "$CLUSTER_RETRY_TIMEOUT" =~ ^[0-9]+([.][0-9]+)?$ ]] || die "--cluster-retry-timeout must be zero or a non-negative number"
[[ "$CLUSTER_RETRY_INTERVAL" =~ ^[0-9]+([.][0-9]+)?$ ]] || die "--cluster-retry-interval must be a positive number"

export KUBECONFIG="$KUBECONFIG_PATH"
export PYTHONPATH="$ROOT/src${PYTHONPATH:+:$PYTHONPATH}"

timestamp() { date '+%Y-%m-%d %H:%M:%S%z'; }
phase() { echo "[$(timestamp)] $*" | tee -a "$LOG_FILE"; }

if [ -z "$OUT" ]; then
  OUT="results/backend-gate-$(date -u +%Y%m%dT%H%M%SZ)"
fi
mkdir -p "$OUT"
LOG_FILE="$OUT/experiment-terminal.log"

phase "OpenClaw/OpenShell experiment"
phase "corpus=$CORPUS"
phase "output=$OUT"
phase "agents=$AGENTS traces_per_workload=$TRACES_PER_WORKLOAD node=$NODE nodes=${NODES:-$NODE} backends_only=$BACKENDS_ONLY"
phase "allow_cross_node=$ALLOW_CROSS_NODE"
phase "namespace=$NAMESPACE system_namespace=$SYSTEM_NAMESPACE openshell_namespace=$OPENSHELL_NAMESPACE"

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  die "python executable not found: $PYTHON_BIN"
fi

if [[ "$MODE" == plan ]]; then
  phase "PLAN: validating corpus and workload allocation"
  PLAN_ARGS=(
    --corpus "$CORPUS" --out "$OUT" --agents "$AGENTS"
    --warm-pool-size "$WARM_POOL_SIZE"
    --traces-per-workload "$TRACES_PER_WORKLOAD" --trace-offset "$TRACE_OFFSET" --namespace "$NAMESPACE"
    --system-namespace "$SYSTEM_NAMESPACE" --openshell-namespace "$OPENSHELL_NAMESPACE"
    --node "$NODE" --kubeconfig "$KUBECONFIG_PATH" --retries "$RETRIES" --job-dispatch-interval "$JOB_DISPATCH_INTERVAL" --cluster-retry-timeout "$CLUSTER_RETRY_TIMEOUT" --cluster-retry-interval "$CLUSTER_RETRY_INTERVAL" --openshell-gateway-endpoint "$OPENSHELL_GATEWAY_ENDPOINT" --openshell-client-secret "$OPENSHELL_CLIENT_SECRET" --plan
  )
  if [[ -n "$NODES" ]]; then PLAN_ARGS+=(--nodes "$NODES"); fi
  if [[ -n "$WORKSPACE_PVC" ]]; then PLAN_ARGS+=(--workspace-pvc "$WORKSPACE_PVC"); fi
  if [[ -n "$WORKSPACE_PVCS" ]]; then PLAN_ARGS+=(--workspace-pvcs "$WORKSPACE_PVCS"); fi
  PLAN_ARGS+=(--workspace-storage-class "$WORKSPACE_STORAGE_CLASS" --workspace-access-mode "$WORKSPACE_ACCESS_MODE")
  if [[ -n "$MAX_TRACES" ]]; then
    PLAN_ARGS+=(--max-traces "$MAX_TRACES")
  fi
  if [[ -n "$REQUIRE_TRACES" ]]; then
    PLAN_ARGS+=(--require-traces "$REQUIRE_TRACES")
  fi
  if [[ -n "$SESSION_ID" ]]; then
    PLAN_ARGS+=(--session-id "$SESSION_ID")
  fi
  if [[ -n "$SESSION_IDS" ]]; then
    PLAN_ARGS+=(--session-ids "$SESSION_IDS")
  fi
  PLAN_ARGS+=(--agent-offset "$AGENT_OFFSET")
  if [[ -n "$AGENT_INDICES" ]]; then
    PLAN_ARGS+=(--agent-indices "$AGENT_INDICES")
  fi
  if ((APPWORLD)); then
    PLAN_ARGS+=(--appworld)
    if [[ -n "${APPWORLD_ENDPOINT:-}" ]]; then PLAN_ARGS+=(--appworld-endpoint "$APPWORLD_ENDPOINT"); fi
  fi
  if [[ -n "$APPWORLD_TASK_MAP" ]]; then PLAN_ARGS+=(--appworld-task-map "$APPWORLD_TASK_MAP"); fi
  if [[ -n "$WORKLOAD_PLAN" ]]; then PLAN_ARGS+=(--workload-plan "$WORKLOAD_PLAN"); fi
  if [[ -n "$BACKEND_MANIFEST" ]]; then PLAN_ARGS+=(--backend-manifest "$BACKEND_MANIFEST"); fi
  if [[ -n "$SWE_TASK_MANIFEST" ]]; then PLAN_ARGS+=(--swe-task-manifest "$SWE_TASK_MANIFEST"); fi
  if ((ALLOW_CROSS_NODE)); then
    PLAN_ARGS+=(--allow-cross-node)
  fi
  "$PYTHON_BIN" "$ROOT/scripts/run_experiment.py" "${PLAN_ARGS[@]}" 2>&1 | tee -a "$LOG_FILE"
  exit "${PIPESTATUS[0]}"
fi

phase "EXECUTE: Python controller will perform cluster preflight"
ARGS=(
  --agents "$AGENTS"
  --warm-pool-size "$WARM_POOL_SIZE"
  --traces-per-workload "$TRACES_PER_WORKLOAD" --trace-offset "$TRACE_OFFSET"
  --namespace "$NAMESPACE" --system-namespace "$SYSTEM_NAMESPACE"
  --openshell-namespace "$OPENSHELL_NAMESPACE" --openshell-gateway-endpoint "$OPENSHELL_GATEWAY_ENDPOINT" --openshell-client-secret "$OPENSHELL_CLIENT_SECRET" --node "$NODE"
  --kubeconfig "$KUBECONFIG_PATH" --retries "$RETRIES" --job-dispatch-interval "$JOB_DISPATCH_INTERVAL" --cluster-retry-timeout "$CLUSTER_RETRY_TIMEOUT" --cluster-retry-interval "$CLUSTER_RETRY_INTERVAL" --execute
)
if ((BACKENDS_ONLY)); then
  ARGS+=(--backends-only)
else
  ARGS+=(--corpus "$CORPUS" --out "$OUT")
fi
if [[ -n "$NODES" ]]; then ARGS+=(--nodes "$NODES"); fi
if [[ -n "$WORKSPACE_PVC" ]]; then ARGS+=(--workspace-pvc "$WORKSPACE_PVC"); fi
if [[ -n "$WORKSPACE_PVCS" ]]; then ARGS+=(--workspace-pvcs "$WORKSPACE_PVCS"); fi
ARGS+=(--workspace-storage-class "$WORKSPACE_STORAGE_CLASS" --workspace-access-mode "$WORKSPACE_ACCESS_MODE")
if [[ -n "$EXECUTION_WORKERS" ]]; then ARGS+=(--execution-workers "$EXECUTION_WORKERS"); fi
if [[ -n "$CLUSTER_API_WORKERS" ]]; then ARGS+=(--cluster-api-workers "$CLUSTER_API_WORKERS"); fi
if [[ -n "$MAX_TRACES" ]]; then
  ARGS+=(--max-traces "$MAX_TRACES")
fi
if [[ -n "$REQUIRE_TRACES" ]]; then
  ARGS+=(--require-traces "$REQUIRE_TRACES")
fi
  if [[ -n "$SESSION_ID" ]]; then
  ARGS+=(--session-id "$SESSION_ID")
fi
if [[ -n "$SESSION_IDS" ]]; then
  ARGS+=(--session-ids "$SESSION_IDS")
fi
ARGS+=(--agent-offset "$AGENT_OFFSET")
if [[ -n "$AGENT_INDICES" ]]; then
  ARGS+=(--agent-indices "$AGENT_INDICES")
fi
if ((REUSE_AGENTS)); then
  ARGS+=(--reuse-agents)
fi
if [[ -n "$REUSE_EXISTING_AGENT_PREFIX" ]]; then
  ARGS+=(--reuse-existing-agent-prefix "$REUSE_EXISTING_AGENT_PREFIX")
fi
if [[ -n "$REUSE_EXISTING_WORKSPACE_PREFIX" ]]; then
  ARGS+=(--reuse-existing-workspace-prefix "$REUSE_EXISTING_WORKSPACE_PREFIX")
fi
if ((RESUME_EXISTING_RUN)); then
  ARGS+=(--resume-existing-run)
fi
if ((DEPLOYMENT_ONLY)); then
  ARGS+=(--deployment-only)
fi
if [[ -n "$RESUME_PREFERRED_NODE" ]]; then
  ARGS+=(--resume-preferred-node "$RESUME_PREFERRED_NODE")
fi
if ((ALLOW_CROSS_NODE)); then
  ARGS+=(--allow-cross-node)
fi
if ((APPWORLD)); then
  ARGS+=(--appworld)
  if [[ -n "${APPWORLD_ENDPOINT:-}" ]]; then ARGS+=(--appworld-endpoint "$APPWORLD_ENDPOINT"); fi
fi
if [[ -n "$APPWORLD_TASK_MAP" ]]; then ARGS+=(--appworld-task-map "$APPWORLD_TASK_MAP"); fi
if [[ -n "$WORKLOAD_PLAN" ]]; then ARGS+=(--workload-plan "$WORKLOAD_PLAN"); fi
if [[ -n "$BACKEND_MANIFEST" ]]; then ARGS+=(--backend-manifest "$BACKEND_MANIFEST"); fi
if [[ -n "$SWE_TASK_MANIFEST" ]]; then ARGS+=(--swe-task-manifest "$SWE_TASK_MANIFEST"); fi

set +e
"$PYTHON_BIN" "$ROOT/scripts/run_experiment.py" "${ARGS[@]}" 2>&1 | tee -a "$LOG_FILE"
status="${PIPESTATUS[0]}"
set -e
exit "$status"
