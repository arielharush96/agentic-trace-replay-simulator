#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
DEPLOY_DIR="$ROOT/deploy/openshift/base"
NS="${NS:-trace-replay}"
CORPUS="${CORPUS:?CORPUS is required}"
OUT_DIR="${OUT_DIR:?OUT_DIR is required}"
JOB="trace-replay-profiler-v3-openclaw"
TARGET_NODE="${TARGET_NODE:?TARGET_NODE is required}"
mkdir -p "$OUT_DIR"

oc get node "$TARGET_NODE" >/dev/null

oc -n "$NS" create configmap profiler-v3-driver-src \
  --from-file=driver.py="$ROOT/src/trace_replay_sim/driver.py" \
  --dry-run=client -o yaml | oc apply -f -
oc -n "$NS" create configmap profiler-v3-corpus \
  --from-file=replay.json="$CORPUS" \
  --dry-run=client -o yaml | oc apply -f -

if [ "${RESET_MOCK:-1}" = "1" ]; then
  oc -n "$NS" rollout restart deployment/mock-llm >/dev/null
  oc -n "$NS" rollout status deployment/mock-llm --timeout=180s
fi

START_UNIX="$(date +%s)"
oc -n "$NS" delete job "$JOB" --ignore-not-found >/dev/null 2>&1 || true
oc -n "$NS" apply -f "$DEPLOY_DIR/job-profiler-v3-openclaw.yaml" >/dev/null
oc -n "$NS" wait --for=condition=complete "job/$JOB" --timeout="${JOB_TIMEOUT:-1800s}"
END_UNIX="$(date +%s)"
POD="$(oc -n "$NS" get pod -l "job-name=$JOB" -o jsonpath='{.items[0].metadata.name}')"
POD_NODE="$(oc -n "$NS" get pod "$POD" -o jsonpath='{.spec.nodeName}')"
[ "$POD_NODE" = "$TARGET_NODE" ] || { echo "Plain OpenClaw job is on $POD_NODE, expected $TARGET_NODE" >&2; exit 1; }
oc -n "$NS" logs "$POD" > "$OUT_DIR/driver.log"
oc -n "$NS" logs "$POD" | awk '/^--- PER_REQUEST_START ---/{p=1;next}/^--- PER_REQUEST_END ---/{p=0}p' > "$OUT_DIR/driver_requests.jsonl" || true
oc -n "$NS" exec deployment/mock-llm -- cat /edges/mock_edges.jsonl > "$OUT_DIR/mock_edges.jsonl"

oc -n "$NS" port-forward svc/jaeger 16686:16686 >/dev/null 2>&1 &
PF="$!"
trap 'kill "$PF" 2>/dev/null || true' EXIT
for _ in $(seq 1 20); do curl -sf http://localhost:16686/api/services >/dev/null 2>&1 && break; sleep 1; done
START="$(date -u -r "$START_UNIX" +%Y-%m-%dT%H:%M:%SZ)"
END="$(date -u -r "$((END_UNIX + 30))" +%Y-%m-%dT%H:%M:%SZ)"
PYTHONPATH=src python3 -m trace_replay_sim.jaeger_export \
  --out "$OUT_DIR/traces" --start "$START" --end "$END" \
  --service openclaw-gateway --jaeger http://localhost:16686
START_UNIX="$(jq -s 'map(.t_recv_wall_ns) | min / 1000000000 | floor' "$OUT_DIR/mock_edges.jsonl")"
END_UNIX="$(jq -s 'map(.t_end_emit_wall_ns) | max / 1000000000 | ceil' "$OUT_DIR/mock_edges.jsonl")"
PYTHONPATH=src python3 -m trace_replay_sim.cli collect \
  --out "$OUT_DIR/prometheus" --start "$START_UNIX" --end "$END_UNIX" \
  --ns-openclaw trace-replay --ns-openshell openshell-tracesim \
  --openclaw-pod "$(oc -n "$NS" get pod -l app=openclaw -o jsonpath='{.items[0].metadata.name}')"
PYTHONPATH=src python3 -m trace_replay_sim.cli profile-v2 \
  --traces "$OUT_DIR/traces/raw_traces.json" \
  --mock-edges "$OUT_DIR/mock_edges.jsonl" \
  --driver-requests "$OUT_DIR/driver.log" \
  --prom "$OUT_DIR/prometheus" --out "$OUT_DIR/profile-v2"
