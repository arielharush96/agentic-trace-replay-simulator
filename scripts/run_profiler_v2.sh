#!/usr/bin/env bash
# Run the clean evidence-first OpenClaw/OpenShell profiler.
#
# Prerequisites: OpenClaw shell, OpenShell, Jaeger, mock-LLM, and the v2
# Prometheus scrape configuration are already installed and healthy.
# The shell deployment uses agent-scoped sandbox reuse. The first measured turn
# establishes the sandbox; subsequent turns reuse the same persistent sandbox.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
DEPLOY_DIR="$ROOT/deploy/openshift/base"
NS="${NS:-trace-replay}"
JOB="${JOB:-trace-replay-profiler-v2-shell}"
CORPUS="${CORPUS:-$ROOT/data/replay-mvp-session2.json}"
MOCK_CORPUS="${MOCK_CORPUS:-$CORPUS}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="${OUT_DIR:-$ROOT/results/profiler-v2/$STAMP}"
JOB_TIMEOUT="${JOB_TIMEOUT:-1800s}"
RESTART_SHELL="${RESTART_SHELL:-0}"
RESET_MOCK="${RESET_MOCK:-1}"
TARGET_NODE="${TARGET_NODE:?TARGET_NODE is required}"
CGROUP_SAMPLE_INTERVAL_MS="${CGROUP_SAMPLE_INTERVAL_MS:-100}"

mkdir -p "$OUT_DIR/shell"
mkdir -p "$OUT_DIR/prometheus/cgroup"
echo "Profiler v2 output: $OUT_DIR"

SAMPLE_PIDS=()
PF=""
OPENCLAW_POD=""
SANDBOX_POD=""
cleanup_resources() {
  for pid in "${SAMPLE_PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  if [ -n "$OPENCLAW_POD" ]; then
    oc -n "$NS" exec "$OPENCLAW_POD" -c gateway -- sh -c "pkill -f '[c]group_sampler.mjs' || true; pkill -f '[c]group_sampler.sh' || true" >/dev/null 2>&1 || true
  fi
  if [ -n "$SANDBOX_POD" ]; then
    oc -n openshell-tracesim exec "$SANDBOX_POD" -c agent -- sh -c "pkill -f '[c]group_sampler.mjs' || true; pkill -f '[c]group_sampler.sh' || true" >/dev/null 2>&1 || true
  fi
  [ -z "$PF" ] || kill "$PF" 2>/dev/null || true
}
trap cleanup_resources EXIT

start_cgroup_sampler() {
  local namespace="$1" pod="$2" container="$3" output="$4"
  oc -n "$namespace" cp "$ROOT/scripts/cgroup_sampler.mjs" "$pod:/tmp/cgroup_sampler.mjs" -c "$container" >/dev/null 2>&1 || return 0
  oc -n "$namespace" exec "$pod" -c "$container" -- env CGROUP_SAMPLE_INTERVAL_MS="$CGROUP_SAMPLE_INTERVAL_MS" node /tmp/cgroup_sampler.mjs > "$output" 2>/dev/null &
  SAMPLE_PIDS+=("$!")
}

oc get node "$TARGET_NODE" >/dev/null

echo "[stage 1/6] Preparing monitoring and replay inputs"
if [ "${APPLY_MONITORING:-0}" = "1" ]; then
  oc apply -f "$DEPLOY_DIR/50-monitoring.yaml" >/dev/null
  oc -n openshift-monitoring patch configmap cluster-monitoring-config \
    --type=merge \
    -p '{"data":{"config.yaml":"prometheusK8s:\n  additionalScrapeConfigs:\n    name: trace-replay-additional-scrape\n    key: trace-replay.yaml\n"}}' \
    2>/dev/null || true
else
  echo "Monitoring changes skipped; use APPLY_MONITORING=1 as a cluster-admin."
fi

oc -n "$NS" create configmap mock-llm-src \
  --from-file=mock_llm.py="$ROOT/src/trace_replay_sim/mock_llm.py" \
  --dry-run=client -o yaml | oc apply -f -
oc -n "$NS" create configmap replay-corpus \
  --from-file=replay.json="$MOCK_CORPUS" \
  --dry-run=client -o yaml | oc apply --server-side -f -
oc -n "$NS" create configmap profiler-v2-driver-src \
  --from-file=driver.py="$ROOT/src/trace_replay_sim/driver.py" \
  --dry-run=client -o yaml | oc apply -f -
oc -n "$NS" create configmap profiler-v2-corpus \
  --from-file=replay.json="$CORPUS" \
  --dry-run=client -o yaml | oc apply -f -
oc -n "$NS" apply -f "$DEPLOY_DIR/10-mock-llm.yaml" >/dev/null
if [ "$RESTART_SHELL" = "1" ]; then
  oc -n "$NS" apply -f "$DEPLOY_DIR/35-openclaw-shell.yaml" >/dev/null
  oc -n "$NS" rollout restart deployment/openclaw-shell >/dev/null
  oc -n "$NS" rollout status deployment/openclaw-shell --timeout=600s
fi

# Reset the ephemeral edge recorder so the fresh result directory contains only
# this benchmark's measured requests (the sandbox itself is not restarted).
if [ "$RESET_MOCK" = "1" ]; then
  echo "[stage 2/6] Starting the mock LLM with the merged replay corpus"
  oc -n "$NS" rollout restart deployment/mock-llm
  oc -n "$NS" rollout status deployment/mock-llm --timeout=180s
fi

# Capture high-frequency cgroup samples during the entire agent run. The
# sandbox sampler waits for the agent-scoped OpenShell pod to appear.
OPENCLAW_POD="$(oc -n "$NS" get pod -l app=openclaw-shell -o jsonpath='{.items[0].metadata.name}')"
OPENCLAW_NODE="$(oc -n "$NS" get pod "$OPENCLAW_POD" -o jsonpath='{.spec.nodeName}')"
[ "$OPENCLAW_NODE" = "$TARGET_NODE" ] || { echo "OpenClaw pod is on $OPENCLAW_NODE, expected $TARGET_NODE" >&2; exit 1; }
start_cgroup_sampler "$NS" "$OPENCLAW_POD" gateway "$OUT_DIR/prometheus/cgroup/openclaw_gateway.csv"
(
  while :; do
    SANDBOX_POD=""
    for candidate in $(oc -n openshell-tracesim get pods -o custom-columns=NAME:.metadata.name --no-headers 2>/dev/null || true); do
      case "$candidate" in default--oc-*) SANDBOX_POD="$candidate"; break;; esac
    done
    if [ -n "$SANDBOX_POD" ]; then
      SANDBOX_NODE="$(oc -n openshell-tracesim get pod "$SANDBOX_POD" -o jsonpath='{.spec.nodeName}')"
      [ "$SANDBOX_NODE" = "$TARGET_NODE" ] || { echo "Sandbox pod is on $SANDBOX_NODE, expected $TARGET_NODE" >&2; exit 1; }
      start_cgroup_sampler openshell-tracesim "$SANDBOX_POD" agent "$OUT_DIR/prometheus/cgroup/openshell_sandbox.csv"
      break
    fi
    sleep 1
  done
) &
SAMPLE_PIDS+=("$!")

START_UNIX="$(date +%s)"
echo "[stage 3/6] Running the replay job on the target node"
oc -n "$NS" delete job "$JOB" --ignore-not-found >/dev/null 2>&1 || true
oc -n "$NS" apply -f "$DEPLOY_DIR/job-profiler-v2-shell.yaml" >/dev/null
oc -n "$NS" wait --for=condition=complete "job/$JOB" --timeout="$JOB_TIMEOUT"
END_UNIX="$(date +%s)"
for pid in "${SAMPLE_PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
SAMPLE_PIDS=()
cleanup_resources

POD="$(oc -n "$NS" get pod -l "job-name=$JOB" -o jsonpath='{.items[0].metadata.name}')"
echo "[stage 4/6] Exporting driver logs, traces, and mock timing edges"
oc -n "$NS" logs "$POD" > "$OUT_DIR/shell/driver.log"
oc -n "$NS" logs "$POD" | awk '/^--- PER_REQUEST_START ---/{p=1;next}/^--- PER_REQUEST_END ---/{p=0}p' > "$OUT_DIR/shell/driver_requests.jsonl" || true
oc -n "$NS" cp "$POD:/results/summary.json" "$OUT_DIR/shell/summary.json" 2>/dev/null || true

oc -n "$NS" exec deployment/mock-llm -- cat /edges/mock_edges.jsonl > "$OUT_DIR/shell/mock_edges.jsonl"

oc -n "$NS" port-forward svc/jaeger 16686:16686 >/dev/null 2>&1 &
PF="$!"
for _ in $(seq 1 20); do
  curl -sf http://localhost:16686/api/services >/dev/null 2>&1 && break
  sleep 1
done
echo "[stage 5/6] Exporting Jaeger traces and collecting exact-pod Prometheus/cAdvisor metrics"
PYTHONPATH=src python3 -m trace_replay_sim.jaeger_export \
  --out "$OUT_DIR/shell/traces" \
  --start "$(date -u -r "$START_UNIX" +%Y-%m-%dT%H:%M:%SZ)" \
  --end "$(date -u -r "$((END_UNIX + 30))" +%Y-%m-%dT%H:%M:%SZ)" \
  --service openclaw-gateway-shell \
  --jaeger http://localhost:16686

PYTHONPATH=src python3 -m trace_replay_sim.cli collect \
  --out "$OUT_DIR/prometheus" \
  --start "$START_UNIX" \
  --end "$END_UNIX" \
  --ns-openclaw trace-replay \
  --ns-openshell openshell-tracesim \
  --openclaw-pod "$OPENCLAW_POD"

CGROUP_FILES=("$OUT_DIR/prometheus/cgroup"/*.csv)
if [ -e "${CGROUP_FILES[0]}" ]; then
  PYTHONPATH=src python3 "$ROOT/scripts/resource_analysis_v3.py" \
    --samples "${CGROUP_FILES[@]}" \
    --out "$OUT_DIR/prometheus/cgroup-analysis"
  mkdir -p "$OUT_DIR/resource-study"
  PYTHONPATH=src python3 "$ROOT/scripts/resource_analysis_v3.py" \
    --samples "${CGROUP_FILES[@]}" \
    --edges "$OUT_DIR/shell/mock_edges.jsonl" \
    --out "$OUT_DIR/resource-study"
fi

echo "[stage 6/6] Building per-turn analysis and waterfall/turn plots"
PYTHONPATH=src python3 -m trace_replay_sim.cli profile-v2 \
  --traces "$OUT_DIR/shell/traces/raw_traces.json" \
  --mock-edges "$OUT_DIR/shell/mock_edges.jsonl" \
  --driver-requests "$OUT_DIR/shell/driver.log" \
  --prom "$OUT_DIR/prometheus" \
  --out "$OUT_DIR/shell/profile-v2"

cat > "$OUT_DIR/README.md" <<EOF
# OpenClaw Profiler v2 Run

- Corpus: $CORPUS
- Sandbox mode: agent-scoped warm reuse
- Prometheus: 1-second application and cAdvisor collection
- OTel service: openclaw-gateway-shell
- Output: shell/profile-v2 and prometheus/
EOF

echo "Profiler v2 complete: $OUT_DIR"
