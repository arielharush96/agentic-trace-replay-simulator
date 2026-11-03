#!/usr/bin/env bash
# Phase 5 — per-turn 5-bucket deliverables for the rebuilt trace-replay pipeline.
#
# One 20-turn agentic session (data/replay-mvp-session2.json) is replayed N reps
# across three layers. Each rep = one driver request = one W3C trace = one cold
# turn-0 + 19 warm turns (mock turn_seq is keyed by trace_id, so reps and layers
# stay independent even against the single shared mock pod).
#
# Per layer we capture three independent sources and fuse them:
#   - mock edges      (per-turn skew-free t_recv/t_first_emit/t_end_emit + G[i])
#   - Jaeger traces   (OpenClaw model.call / tool.execution spans -> A/B/D)
#   - K8s lifecycle   (sandbox pod creationTimestamp -> Ready, cold-create cross-check; shell only)
# then run `derive` (5 buckets + agreement) and `diagram` (session_waterfall.png).
#
# This script performs cluster mutations (oc/helm). Run it yourself from a logged-in
# terminal; it is NOT auto-run by the assistant.
#
# Usage:
#   ./scripts/run_phase5.sh                 # 5 reps, all 3 layers
#   REPS=1 ./scripts/run_phase5.sh          # smoke: 1 rep
#   LAYERS="shell" REPS=5 ./scripts/run_phase5.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

NS="${NS:-trace-replay}"
NS_OS="${NS_OS:-openshell-tracesim}"
REPS="${REPS:-5}"
LAYERS="${LAYERS:-mock-direct openclaw shell}"
CORPUS="${CORPUS:-$ROOT/data/replay-mvp-session2.json}"
STAMP="$(date +%Y%m%d-%H%M%S)"
OUT_DIR="${OUT_DIR:-$ROOT/results/phase5/${STAMP}}"
JOB_TIMEOUT="${JOB_TIMEOUT:-1200s}"
MOCK_EDGES_FILE="/edges/mock_edges.jsonl"

mkdir -p "$OUT_DIR"
echo "=============================================="
echo "  Phase 5 — per-turn waterfall   $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "  reps=${REPS}  layers=[${LAYERS}]"
echo "  corpus=$(basename "$CORPUS")"
echo "  out=$OUT_DIR"
echo "=============================================="

# --------------------------------------------------------------------------- #
# Source + corpus ConfigMaps (regenerated from the rebuilt modules)           #
# --------------------------------------------------------------------------- #
echo "--- ConfigMaps (mock_llm.py, driver.py, corpus) ---"
oc -n "$NS" create configmap mock-llm-src \
  --from-file=mock_llm.py="$ROOT/src/trace_replay_sim/mock_llm.py" \
  --dry-run=client -o yaml | oc apply -f -
oc -n "$NS" create configmap driver-src \
  --from-file=driver.py="$ROOT/src/trace_replay_sim/driver.py" \
  --dry-run=client -o yaml | oc apply -f -
oc -n "$NS" create configmap replay-corpus \
  --from-file=replay.json="$CORPUS" \
  --dry-run=client -o yaml | oc apply -f -
oc -n "$NS" create configmap response-processing-profile \
  --from-file=response_processing_profile.json="$ROOT/data/response_processing_profile.json" \
  --dry-run=client -o yaml | oc apply -f -

# Assumes 10-mock-llm.yaml / 20-jaeger.yaml / 30-openclaw.yaml / 35-openclaw-shell.yaml
# are already deployed and healthy. We only refresh what changed.
oc -n "$NS" apply -f "$ROOT/k8s/10-mock-llm.yaml"

# --------------------------------------------------------------------------- #
# helpers                                                                      #
# --------------------------------------------------------------------------- #
url_for() {
  case "$1" in
    mock-direct) echo "http://mock-llm.${NS}.svc.cluster.local:8080" ;;
    openclaw)    echo "http://openclaw.${NS}.svc.cluster.local:18789" ;;
    shell)       echo "http://openclaw-shell.${NS}.svc.cluster.local:18790" ;;
  esac
}
service_for() {   # Jaeger service that carries the full OpenClaw span tree
  case "$1" in
    openclaw) echo "openclaw-gateway" ;;
    shell)    echo "openclaw-gateway-shell" ;;
    *)        echo "trace-replay-driver" ;;
  esac
}
job_for() { echo "trace-replay-${1}-c1"; }      # job metadata.name
jobfile_for() { echo "k8s/job-${1}-c1.yaml"; }  # manifest path on disk

reset_mock() {   # fresh /edges file + fresh logs so each layer's edges are isolated
  echo "  resetting mock-llm (fresh edges) ..."
  oc -n "$NS" rollout restart deployment/mock-llm
  oc -n "$NS" rollout status deployment/mock-llm --timeout=120s
}

# --------------------------------------------------------------------------- #
# per-layer run                                                                #
# --------------------------------------------------------------------------- #
run_layer() {
  local layer="$1"
  local job; job="$(job_for "$layer")"
  local jobfile; jobfile="$(jobfile_for "$layer")"
  local ldir="$OUT_DIR/$layer"
  mkdir -p "$ldir"

  echo ""
  echo "=========================================="
  echo "  LAYER: $layer   ($REPS reps)"
  echo "=========================================="
  reset_mock
  local win_start; win_start="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

  for rep in $(seq 1 "$REPS"); do
    echo "  rep $rep/$REPS ..."
    oc -n "$NS" delete job "$job" --ignore-not-found >/dev/null 2>&1 || true
    oc -n "$NS" apply -f "$jobfile" >/dev/null
    oc -n "$NS" wait --for=condition=complete "job/${job}" --timeout="$JOB_TIMEOUT"
    local pod; pod="$(oc -n "$NS" get pod -l "job-name=${job}" -o jsonpath='{.items[0].metadata.name}')"
    oc -n "$NS" logs "$pod" > "$ldir/driver_rep${rep}.log" 2>/dev/null || true
    # last top-level JSON object on stdout == driver summary
    python3 - "$ldir/driver_rep${rep}.log" "$ldir/summary_rep${rep}.json" "$ldir/driver_requests.jsonl" <<'PY' || true
import json, sys
raw = open(sys.argv[1]).read()
s, e = raw.rfind('{'), raw.rfind('}') + 1
if s >= 0 and e > s:
    open(sys.argv[2], 'w').write(raw[s:e])
in_requests = False
with open(sys.argv[3], 'a') as out:
    for line in raw.splitlines():
        if line.strip() == '--- PER_REQUEST_START ---':
            in_requests = True
            continue
        if line.strip() == '--- PER_REQUEST_END ---':
            in_requests = False
            continue
        if in_requests:
            try:
                json.loads(line)
            except json.JSONDecodeError:
                continue
            out.write(line + '\n')
PY
  done
  # keep one summary where the diagram's layer-comparison step looks for it
  cp -f "$ldir/summary_rep${REPS}.json" "$ldir/summary.json" 2>/dev/null || true
  local win_end; win_end="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

  # --- capture mock edges (authoritative file, log fallback) ----------------
  echo "  capturing mock edges ..."
  if ! oc -n "$NS" exec deployment/mock-llm -- cat "$MOCK_EDGES_FILE" > "$ldir/mock_edges.jsonl" 2>/dev/null; then
    oc -n "$NS" logs deployment/mock-llm 2>/dev/null | grep '^MOCK_EDGE ' > "$ldir/mock_edges.jsonl" || true
  fi
  echo "    edges: $(wc -l < "$ldir/mock_edges.jsonl" | tr -d ' ') lines"

  # --- capture Jaeger traces (skip mock-direct: no OpenClaw spans) -----------
  if [ "$layer" != "mock-direct" ]; then
    echo "  exporting Jaeger traces (service=$(service_for "$layer")) ..."
    oc -n "$NS" port-forward svc/jaeger 16686:16686 >/dev/null 2>&1 &
    local pf=$!
    for _ in $(seq 1 15); do curl -sf http://localhost:16686/api/services >/dev/null 2>&1 && break; sleep 1; done
    PYTHONPATH=src python3 -m trace_replay_sim.jaeger_export \
      --out "$ldir/traces" --start "$win_start" --end "$win_end" \
      --service "$(service_for "$layer")" --jaeger http://localhost:16686 \
      >/dev/null 2>&1 || echo "    WARNING: jaeger export failed"
    kill "$pf" 2>/dev/null || true
  fi

  # --- capture K8s sandbox lifecycle (shell only, cold-create Ready-delta) ---
  if [ "$layer" = "shell" ]; then
    echo "  capturing sandbox pod lifecycle ..."
    oc -n "$NS_OS" get pods -o json > "$ldir/k8s_pods_raw.json" 2>/dev/null || echo "{}" > "$ldir/k8s_pods_raw.json"
    python3 - "$ldir/k8s_pods_raw.json" "$ldir/k8s_lifecycle.json" <<'PY' || true
import json, sys
from datetime import datetime
def ts(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").timestamp() if s else None
raw = json.load(open(sys.argv[1]))
out = []
for p in raw.get("items", []):
    md, st = p.get("metadata", {}), p.get("status", {})
    created = ts(md.get("creationTimestamp"))
    ready = None
    for c in st.get("conditions", []):
        if c.get("type") == "Ready" and c.get("status") == "True":
            ready = ts(c.get("lastTransitionTime"))
    if created and ready and ready >= created:
        out.append({"pod": md.get("name"), "ready_delta_ms": round((ready - created) * 1000, 1)})
json.dump(out, open(sys.argv[2], "w"), indent=2)
print(f"    sandbox pods with Ready-delta: {len(out)}")
PY
  fi

  # --- derive 5 buckets + render waterfall ----------------------------------
  echo "  deriving per-turn buckets ..."
    local k8s_arg=(); [ -f "$ldir/k8s_lifecycle.json" ] && k8s_arg=(--k8s "$ldir/k8s_lifecycle.json")
    local traces_arg=(); [ -f "$ldir/traces/raw_traces.json" ] && traces_arg=(--traces "$ldir/traces/raw_traces.json")
    local driver_arg=(); [ -f "$ldir/driver_requests.jsonl" ] && driver_arg=(--driver-requests "$ldir/driver_requests.jsonl")
  PYTHONPATH=src python3 -m trace_replay_sim.cli derive \
    --out "$ldir/derive" \
    --mock-edges "$ldir/mock_edges.jsonl" \
    "${traces_arg[@]}" "${k8s_arg[@]}" "${driver_arg[@]}" || echo "    WARNING: derive failed"

  if [ -f "$ldir/derive/per_turn.json" ]; then
    # Decode (E) is excluded by default: it dwarfs the agentic buckets and would
    # visually wipe A/B/C/D. Override with EXCLUDE_BUCKETS="" to include it.
    if [ -n "${EXCLUDE_BUCKETS-E}" ]; then
      PYTHONPATH=src python3 -m trace_replay_sim.cli diagram \
        --per-turn "$ldir/derive/per_turn.json" \
        --results "$OUT_DIR" \
        --out "$ldir/diagrams" \
        --exclude "${EXCLUDE_BUCKETS}" || echo "    WARNING: diagram failed"
    else
      PYTHONPATH=src python3 -m trace_replay_sim.cli diagram \
        --per-turn "$ldir/derive/per_turn.json" \
        --results "$OUT_DIR" \
        --out "$ldir/diagrams" || echo "    WARNING: diagram failed"
    fi
    if [ -f "$ldir/driver_requests.jsonl" ]; then
      PYTHONPATH=src python3 -m trace_replay_sim.cli profile \
        --per-turn "$ldir/derive/per_turn.json" \
        --out "$ldir/profile" \
        --driver-requests "$ldir/driver_requests.jsonl" || echo "    WARNING: profile report failed"
      PYTHONPATH=src python3 -m trace_replay_sim.cli profile \
        --per-turn "$ldir/derive/per_turn.json" \
        --out "$ldir/profile-warm-only" \
        --driver-requests "$ldir/driver_requests.jsonl" \
        --warm-only || echo "    WARNING: warm profile report failed"
    else
      PYTHONPATH=src python3 -m trace_replay_sim.cli profile \
        --per-turn "$ldir/derive/per_turn.json" \
        --out "$ldir/profile" || echo "    WARNING: profile report failed"
      PYTHONPATH=src python3 -m trace_replay_sim.cli profile \
        --per-turn "$ldir/derive/per_turn.json" \
        --out "$ldir/profile-warm-only" \
        --warm-only || echo "    WARNING: warm profile report failed"
    fi
  fi
}

for layer in $LAYERS; do
  run_layer "$layer"
done

echo ""
echo "=============================================="
echo "  DONE — $OUT_DIR"
echo ""
echo "  Headline (real sandbox): $OUT_DIR/shell/diagrams/session_waterfall.png"
echo "  Per-turn CSV:            $OUT_DIR/shell/derive/per_turn.csv"
echo "  Cross-check summary:     $OUT_DIR/shell/derive/derive_summary.json"
echo ""
echo "  Validate vs baseline anchors:"
echo "    - cold turn-0 B_sandbox ~127ms (shell), warm << cold"
echo "    - A_premodel non-negligible (hundreds of ms range)"
echo "    - G_delta_ms ~ 0  (skew-free gap == A+B+D decomposition)"
echo "=============================================="
