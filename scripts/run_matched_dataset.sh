#!/usr/bin/env bash
# Add the matched plain-OpenClaw phase to an existing shell experiment.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
TARGET_NODE="${TARGET_NODE:?TARGET_NODE is required}"
CORPUS_ROOT="${CORPUS_ROOT:?CORPUS_ROOT is required}"
OUT_ROOT="${OUT_ROOT:?OUT_ROOT is required}"
KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"
export KUBECONFIG TARGET_NODE

oc get node "$TARGET_NODE" >/dev/null
index=0
total=0
for _corpus in "$CORPUS_ROOT"/*__*-corpus-turns/corpus.json; do total=$((total + 1)); done
for corpus in "$CORPUS_ROOT"/*__*-corpus-turns/corpus.json; do
  index=$((index + 1))
  task="${corpus%/corpus.json}"
  task="${task##*/}"
  echo "[matched 1/2] Plain OpenClaw $index/$total: $task"
  CORPUS="$corpus" OUT_DIR="$OUT_ROOT/$task/openclaw" RESET_MOCK=0 TARGET_NODE="$TARGET_NODE" \
    bash "$ROOT/scripts/run_matched_openclaw.sh"
done

echo "[matched 2/2] Validating equal internal step counts and generating the matched overhead plot"
PYTHONPATH=src python3 -m trace_replay_sim.cli study-v3 \
  --root "$OUT_ROOT" \
  --out "$OUT_ROOT/studies-v3"
matched_plot="$OUT_ROOT/studies-v3/diagrams/study_v3_matched_openshell_overhead.png"
if [ -f "$matched_plot" ]; then
  for corpus in "$CORPUS_ROOT"/*__*-corpus-turns/corpus.json; do
    task="${corpus%/corpus.json}"
    task="${task##*/}"
    mkdir -p "$OUT_ROOT/$task/plots-analysis"
    cp "$matched_plot" "$OUT_ROOT/$task/plots-analysis/study_v3_matched_openshell_overhead.png"
  done
fi
for corpus in "$CORPUS_ROOT"/*__*-corpus-turns/corpus.json; do
  task="${corpus%/corpus.json}"
  task="${task##*/}"
  PYTHONPATH=src python3 "$ROOT/scripts/organize_corpus_output.py" \
    --root "$ROOT" \
    --task "$OUT_ROOT/$task" \
    --corpus "$corpus" \
    --target-node "$TARGET_NODE" \
    --command "TARGET_NODE=$TARGET_NODE CORPUS_ROOT=$CORPUS_ROOT OUT_ROOT=$OUT_ROOT bash scripts/run_dataset_100ms.sh"
done
echo "Matched plain-OpenClaw phase complete: $OUT_ROOT/studies-v3"
