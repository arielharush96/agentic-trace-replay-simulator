#!/usr/bin/env bash
set -euo pipefail

# Main operator-facing entrypoint. The Python controller is deliberately kept
# behind this shell interface because deployment, kubeconfig, logging and
# lifecycle control belong in the shell; Python is used for JSON/OTel analysis.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
CORPUS=""
OUT=""
AGENTS=9
TRACES_PER_WORKLOAD=25
MAX_TRACES=""
REQUIRE_TRACES=""
SESSION_ID=""
NAMESPACE="trace-replay"
SYSTEM_NAMESPACE="trace-replay"
OPENSHELL_NAMESPACE="openshell-tracesim"
NODE="${TARGET_NODE:-}"
KUBECONFIG_PATH="${KUBECONFIG:-$HOME/.kube/config}"
MODE=""
REUSE_AGENTS=0
RETRIES=1

usage() {
  sed -n '1,45p' "$0"
}

die() { echo "ERROR: $*" >&2; exit 2; }

while (($#)); do
  case "$1" in
    --corpus) CORPUS="$2"; shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    --agents) AGENTS="$2"; shift 2 ;;
    --traces-per-workload) TRACES_PER_WORKLOAD="$2"; shift 2 ;;
    --max-traces) MAX_TRACES="$2"; shift 2 ;;
    --require-traces) REQUIRE_TRACES="$2"; shift 2 ;;
    --session-id) SESSION_ID="$2"; shift 2 ;;
    --namespace) NAMESPACE="$2"; shift 2 ;;
    --system-namespace) SYSTEM_NAMESPACE="$2"; shift 2 ;;
    --openshell-namespace) OPENSHELL_NAMESPACE="$2"; shift 2 ;;
    --node) NODE="$2"; shift 2 ;;
    --kubeconfig) KUBECONFIG_PATH="$2"; shift 2 ;;
    --plan) MODE="plan"; shift ;;
    --execute) MODE="execute"; shift ;;
    --reuse-agents) REUSE_AGENTS=1; shift ;;
    --retries) RETRIES="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown argument: $1" ;;
  esac
done

[ -n "$CORPUS" ] || die "--corpus is required"
[ -n "$OUT" ] || die "--out is required"
[ -f "$CORPUS" ] || die "corpus not found: $CORPUS"
[ -n "$NODE" ] || die "--node or TARGET_NODE is required"
[[ "$MODE" == plan || "$MODE" == execute ]] || die "choose exactly one of --plan or --execute"
[[ "$AGENTS" =~ ^[1-9]$ ]] || die "--agents must be 1..9"
[[ "$TRACES_PER_WORKLOAD" =~ ^[1-9][0-9]*$ ]] || die "--traces-per-workload must be positive"
if [[ -n "$MAX_TRACES" ]]; then
  [[ "$MAX_TRACES" =~ ^[1-9][0-9]*$ ]] || die "--max-traces must be positive"
fi
if [[ -n "$REQUIRE_TRACES" ]]; then
  [[ "$REQUIRE_TRACES" =~ ^[1-9][0-9]*$ ]] || die "--require-traces must be positive"
fi

export KUBECONFIG="$KUBECONFIG_PATH"
export PYTHONPATH="$ROOT/src${PYTHONPATH:+:$PYTHONPATH}"

timestamp() { date '+%Y-%m-%d %H:%M:%S%z'; }
phase() { echo "[$(timestamp)] $*" | tee -a "$LOG_FILE"; }

mkdir -p "$OUT"
LOG_FILE="$OUT/experiment-terminal.log"

phase "OpenClaw/OpenShell experiment"
phase "corpus=$CORPUS"
phase "output=$OUT"
phase "agents=$AGENTS traces_per_workload=$TRACES_PER_WORKLOAD node=$NODE"
phase "namespace=$NAMESPACE system_namespace=$SYSTEM_NAMESPACE openshell_namespace=$OPENSHELL_NAMESPACE"

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  die "python executable not found: $PYTHON_BIN"
fi

if [[ "$MODE" == plan ]]; then
  phase "PLAN: validating corpus and workload allocation"
  PLAN_ARGS=(
    --corpus "$CORPUS" --out "$OUT" --agents "$AGENTS"
    --traces-per-workload "$TRACES_PER_WORKLOAD" --namespace "$NAMESPACE"
    --system-namespace "$SYSTEM_NAMESPACE" --openshell-namespace "$OPENSHELL_NAMESPACE"
    --node "$NODE" --kubeconfig "$KUBECONFIG_PATH" --retries "$RETRIES" --plan
  )
  if [[ -n "$MAX_TRACES" ]]; then
    PLAN_ARGS+=(--max-traces "$MAX_TRACES")
  fi
  if [[ -n "$REQUIRE_TRACES" ]]; then
    PLAN_ARGS+=(--require-traces "$REQUIRE_TRACES")
  fi
  "$PYTHON_BIN" "$ROOT/scripts/run_experiment.py" "${PLAN_ARGS[@]}" 2>&1 | tee -a "$LOG_FILE"
  exit "${PIPESTATUS[0]}"
fi

phase "EXECUTE: Python controller will perform cluster preflight"
ARGS=(
  --corpus "$CORPUS" --out "$OUT" --agents "$AGENTS"
  --traces-per-workload "$TRACES_PER_WORKLOAD"
  --namespace "$NAMESPACE" --system-namespace "$SYSTEM_NAMESPACE"
  --openshell-namespace "$OPENSHELL_NAMESPACE" --node "$NODE"
  --kubeconfig "$KUBECONFIG_PATH" --retries "$RETRIES" --execute
)
if [[ -n "$MAX_TRACES" ]]; then
  ARGS+=(--max-traces "$MAX_TRACES")
fi
if [[ -n "$REQUIRE_TRACES" ]]; then
  ARGS+=(--require-traces "$REQUIRE_TRACES")
fi
if [[ -n "$SESSION_ID" ]]; then
  ARGS+=(--session-id "$SESSION_ID")
fi
if ((REUSE_AGENTS)); then
  ARGS+=(--reuse-agents)
fi

set +e
"$PYTHON_BIN" "$ROOT/scripts/run_experiment.py" "${ARGS[@]}" 2>&1 | tee -a "$LOG_FILE"
status="${PIPESTATUS[0]}"
set -e
exit "$status"
