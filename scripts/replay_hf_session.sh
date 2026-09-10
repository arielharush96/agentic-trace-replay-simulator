#!/usr/bin/env bash
# Reproduce one Hugging Face trace through OpenClaw + OpenShell.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

SESSION_ID="${SESSION_ID:?SESSION_ID is required}"
TARGET_NODE="${TARGET_NODE:?TARGET_NODE is required}"
KUBECONFIG="${KUBECONFIG:?KUBECONFIG is required}"
DATASET="${DATASET:-Exgentic/agent-llm-traces}"
SOURCE_LIMIT="${SOURCE_LIMIT:-100}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_ROOT="${OUT_ROOT:-$ROOT/results/demo-$STAMP-$SESSION_ID}"
SOURCE_CORPUS="$OUT_ROOT/source/replay.json"
CORPUS="$OUT_ROOT/corpus.json"
TASK_DIR="$OUT_ROOT/$SESSION_ID"

mkdir -p "$OUT_ROOT/source"

echo "[1/5] Fetching filtered trace corpus from $DATASET"
PYTHONPATH=src python3 -m trace_replay_sim.cli ingest \
  --dataset "$DATASET" \
  --harness claude_code \
  --benchmark appworld \
  --limit "$SOURCE_LIMIT" \
  --out "$SOURCE_CORPUS"

echo "[2/5] Selecting session $SESSION_ID"
PYTHONPATH=src python3 -m trace_replay_sim.cli corpus \
  --in "$SOURCE_CORPUS" \
  --session-id "$SESSION_ID" \
  --out "$CORPUS"

echo "[3/5] Running the trace through OpenClaw + OpenShell"
KUBECONFIG="$KUBECONFIG" TARGET_NODE="$TARGET_NODE" \
  CORPUS="$CORPUS" MOCK_CORPUS="$CORPUS" RESET_MOCK=1 OUT_DIR="$TASK_DIR" \
  bash "$ROOT/scripts/run_profiler_v2.sh"

echo "[4/5] Organizing reproducible artifacts"
PYTHONPATH=src python3 "$ROOT/scripts/organize_corpus_output.py" \
  --root "$ROOT" \
  --task "$TASK_DIR" \
  --corpus "$CORPUS" \
  --target-node "$TARGET_NODE" \
  --command "SESSION_ID=$SESSION_ID TARGET_NODE=$TARGET_NODE KUBECONFIG=$KUBECONFIG bash scripts/replay_hf_session.sh"

echo "[5/5] Complete: $TASK_DIR"
