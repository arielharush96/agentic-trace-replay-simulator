#!/usr/bin/env bash
# Run controlled payload-size variants of one replay corpus.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
CORPUS="${1:?Usage: bash scripts/run_payload_sweep.sh path/to/corpus.json}"
TARGET_NODE="${TARGET_NODE:?TARGET_NODE is required}"
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"
KUBECONFIG="${KUBECONFIG:?KUBECONFIG is required}"
export KUBECONFIG TARGET_NODE
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_ROOT="${OUT_ROOT:-$ROOT/results/payload-sweep-$STAMP}"
SWEEP_ROOT="$OUT_ROOT/corpora"

mkdir -p "$SWEEP_ROOT"
PYTHONPATH=src python3 scripts/build_payload_sweep.py --corpus "$CORPUS" --out "$SWEEP_ROOT"

for variant in "$SWEEP_ROOT"/padding-*.json; do
  name="$(basename "$variant" .json)"
  echo "[payload sweep] Running $name"
  TARGET_NODE="$TARGET_NODE" \
  CORPUS="$variant" \
  MOCK_CORPUS="$variant" \
  MOCK_PREFILL_SCALING=0 \
  RESET_MOCK=1 \
  OUT_DIR="$OUT_ROOT/$name" \
  bash scripts/run_profiler_v2.sh
done

echo "Payload sweep complete: $OUT_ROOT"
