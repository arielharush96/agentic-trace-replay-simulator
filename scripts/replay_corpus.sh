#!/usr/bin/env bash
# Run one already-built replay corpus through OpenClaw + OpenShell.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"

CORPUS="${1:?Usage: bash scripts/replay_corpus.sh path/to/corpus.json}"
TARGET_NODE="${TARGET_NODE:?set TARGET_NODE to a schedulable worker node}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
NAME="$(basename "$(dirname "$CORPUS")")"
OUT_DIR="${OUT_DIR:-$ROOT/results/demo-$STAMP/$NAME}"

[ -f "$CORPUS" ] || { echo "Corpus not found: $CORPUS" >&2; exit 1; }

echo "Replaying corpus: $NAME"
echo "Target node: $TARGET_NODE"
echo "Output: $OUT_DIR"

TARGET_NODE="$TARGET_NODE" \
RESTART_SHELL=0 \
RESET_MOCK=1 \
CORPUS="$CORPUS" \
OUT_DIR="$OUT_DIR" \
bash "$ROOT/scripts/run_profiler_v2.sh"

echo "Demo complete: $OUT_DIR"
