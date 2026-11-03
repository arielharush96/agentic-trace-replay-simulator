#!/bin/bash
#  Trace-Replay Benchmark Demo
#  One command. Four workloads. Real measurements.
#
#  Usage:
#    ./scripts/demo.sh                          # 4 agents, 1 per workload
#    ./scripts/demo.sh --agents 8               # 8 agents, 2 per workload
#    ./scripts/demo.sh --quick                  # skip re-verification (fastest)
#
set -euo pipefail

AGENTS=4
QUICK=false
OUT_DIR="results/demo-$(date -u +%Y%m%dT%H%M%S)"
CORPUS="results/client-boundary-100agents-20261025/corpus-100-normalized-v2.json"
DEMO_SESSIONS="e3bfdef4aca3_9271604a,2a21e94e0687_1b28ad67,ebba346cd20a_23093e5d,d9ccf53fe748_1817d2a6"

while [[ $# -gt 0 ]]; do
    case $1 in
        --agents) AGENTS="$2"; shift 2 ;;
        --quick) QUICK=true; shift ;;
        --out) OUT_DIR="$2"; shift 2 ;;
        --help|-h)
            echo "Usage: $0 [--agents N] [--quick] [--out DIR]"
            echo ""
            echo "  --agents N   Concurrent agents (default: 4, one per workload)"
            echo "  --quick      Skip deployment re-verification (fastest restart)"
            echo "  --out DIR    Output directory (default: results/demo-TIMESTAMP)"
            exit 0 ;;
        *) echo "Unknown: $1"; exit 1 ;;
    esac
done

PER_WORKLOAD=$((AGENTS / 4))
WORKLOAD_PLAN="personal_assistant=${PER_WORKLOAD},research_deep_search=${PER_WORKLOAD},software_engineering_qa=${PER_WORKLOAD},customer_support_ops=${PER_WORKLOAD}"
export KUBECONFIG=${KUBECONFIG:-$HOME/.kube/config}
NODES="${NODES:?set NODES to a comma-separated list of worker nodes}"
PVCS="${PVCS:-}"
STORAGE_CLASS="${STORAGE_CLASS:-}"
OPENSHELL_NAMESPACE="${OPENSHELL_NAMESPACE:-openshell}"
GATEWAY_ENDPOINT="${GATEWAY_ENDPOINT:-}"

echo ""
echo "Trace Replay Benchmark - live demo"
echo "  agents:       $AGENTS"
echo "  workloads:    4 (SWE, PA, Research, CS)"
echo "  per workload: $PER_WORKLOAD"
echo "  mode:         client-boundary replay"
echo "  model:        mock LLM sidecar (deterministic)"
echo "  sandboxes:    warm pool (pre-filled, zero cold-start)"
echo "  output:       $OUT_DIR"
echo ""

EXTRA_ARGS=""
if $QUICK; then
    EXTRA_ARGS="--skip-resume-agent-reconcile"
    echo "  [quick mode] skipping deployment re-verification"
fi

echo "  Starting at $(date '+%H:%M:%S')"
echo ""

mkdir -p "$OUT_DIR"

python3 scripts/run_experiment.py \
    --execute \
    --retries 2 \
    --corpus "$CORPUS" \
    --out "$OUT_DIR" \
    --session-ids "$DEMO_SESSIONS" \
    --require-traces 4 \
    --workload-plan "$WORKLOAD_PLAN" \
    --client-boundaries \
    --backend-manifest config/backend-manifest.example.json \
    --nodes "$NODES" \
    ${STORAGE_CLASS:+--workspace-access-mode ReadWriteOnce --workspace-storage-class "$STORAGE_CLASS"} \
    ${PVCS:+--workspace-pvcs "$PVCS"} \
    --openshell-namespace "$OPENSHELL_NAMESPACE" \
    ${GATEWAY_ENDPOINT:+--openshell-gateway-endpoint "$GATEWAY_ENDPOINT"} \
    --agents "$AGENTS" \
    --warm-pool-size "$AGENTS" \
    --execution-workers "$AGENTS" \
    --job-dispatch-interval 2 \
    $EXTRA_ARGS \
    2>&1 | tee "$OUT_DIR/controller.log" | grep -E '^\[|complete:|failed|sandbox pool:|trace [0-9]|deployment slot|quality'

echo ""
echo "Demo complete"

if [ -f "$OUT_DIR/analysis/quality_gate.json" ]; then
    python3 -c "
import json
q = json.load(open('$OUT_DIR/analysis/quality_gate.json'))
print(f\"  Quality Gate:  {q.get('status', '?')}\")
print(f\"  Failures:      {q.get('failure_count', '?')}\")
wp = q.get('workload_population', {})
for w, p in sorted(wp.items()):
    print(f\"  {w:28s}  {p['total']} sessions ({p['tool_rich']} tool-rich, {p['conversational']} conversational)\")
"
fi

echo ""
echo "  Dashboard:  $OUT_DIR/analysis/executive_multi_agent_dashboard.png"
echo "  Results:    $OUT_DIR"
echo ""
echo "  Finished at $(date '+%H:%M:%S')"
echo ""
