#!/usr/bin/env bash
set -euo pipefail

# Controlled end-to-end smoke gate for the multi-agent orchestrator.
# It always schedules exactly two traces across exactly two OpenClaw agents.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CORPUS=""
OUT=""
MODE=""
MODE_COUNT=0
KUBECONFIG_PATH="${KUBECONFIG:-$HOME/.kube/config}"
NODE="${TARGET_NODE:-}"
REUSE_AGENTS=0

die() { echo "ERROR: $*" >&2; exit 2; }

while (($#)); do
  case "$1" in
    --corpus) CORPUS="$2"; shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    --kubeconfig) KUBECONFIG_PATH="$2"; shift 2 ;;
    --node) NODE="$2"; shift 2 ;;
    --plan) MODE="plan"; MODE_COUNT=$((MODE_COUNT + 1)); shift ;;
    --execute) MODE="execute"; MODE_COUNT=$((MODE_COUNT + 1)); shift ;;
    --reuse-agents) REUSE_AGENTS=1; shift ;;
    -h|--help)
      echo "Usage: $0 --corpus FILE [--out DIR] [--plan|--execute] [--reuse-agents]"
      exit 0
      ;;
    *) die "unknown argument: $1" ;;
  esac
done

[[ -n "$CORPUS" ]] || die "--corpus is required"
[[ -f "$CORPUS" ]] || die "corpus not found: $CORPUS"
[[ "$MODE_COUNT" -eq 1 ]] || die "choose exactly one of --plan or --execute"
[[ -n "$NODE" ]] || die "--node or TARGET_NODE is required"

if [[ -z "$OUT" ]]; then
  RUN_STAMP="$(date -u '+%Y%m%d-%H%M%S')"
  OUT="$ROOT/results/results-multi-agent/${RUN_STAMP}-smoke-2agents-2traces"
fi

ARGS=(
  --corpus "$CORPUS"
  --out "$OUT"
  --agents 2
  --traces-per-workload 2
  --max-traces 2
  --require-traces 2
  --node "$NODE"
  --kubeconfig "$KUBECONFIG_PATH"
  --retries 0
  "--$MODE"
)
if ((REUSE_AGENTS)); then
  ARGS+=(--reuse-agents)
fi

echo "Two-agent smoke gate: mode=$MODE traces=2 agents=2 output=$OUT"
exec "$ROOT/scripts/run_experiment.sh" "${ARGS[@]}"
