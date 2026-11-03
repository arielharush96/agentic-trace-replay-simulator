#!/usr/bin/env bash
set -euo pipefail

NS="${NS:-trace-replay}"
WORKLOAD_ROOT="${WORKLOAD_ROOT:?WORKLOAD_ROOT is required}"
OUT_DIR="${OUT_DIR:?OUT_DIR is required}"
COUNT="${COUNT:-9}"
NODE="${TARGET_NODE:?set TARGET_NODE to a schedulable worker node}"
MAX_CONFIGMAP_BYTES="${MAX_CONFIGMAP_BYTES:-700000}"
RUN_TAG="${RUN_TAG:-$(date -u '+%Y%m%d%H%M%S')}"
RESULT_HOLD_SECONDS="${RESULT_HOLD_SECONDS:-900}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DRIVER_CONFIG="trace-replay-fleet-experiment-driver"

mkdir -p "$OUT_DIR" "$OUT_DIR/shards" "$OUT_DIR/workers"
: > "$OUT_DIR/jobs.tsv"

for index in $(seq 1 "$COUNT"); do
  oc -n "$NS" get svc "openclaw-fleet-$index" >/dev/null
  oc -n "$NS" get pod -l "app=openclaw-fleet-$index" --field-selector "spec.nodeName=$NODE" -o name | grep -q .
done

oc -n "$NS" create configmap "$DRIVER_CONFIG" \
  --from-file=driver.py="$ROOT/src/trace_replay_sim/driver.py" \
  --dry-run=client -o yaml | oc apply -f - >/dev/null

python3 - "$WORKLOAD_ROOT" "$OUT_DIR/shards" "$MAX_CONFIGMAP_BYTES" <<'PY'
import json, pathlib, sys

root, out, max_bytes = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]), int(sys.argv[3])
out.mkdir(parents=True, exist_ok=True)
manifest = []
for source in sorted(root.glob('*.json')):
    if source.name == 'manifest.json':
        continue
    payload = json.loads(source.read_text())
    workload = payload.get('workload') or source.stem
    current = []
    current_bytes = 0
    shard_no = 0
    for session in payload.get('sessions', []):
        size = len(json.dumps({'dataset': payload.get('dataset'), 'sessions': [session]}).encode())
        if current and current_bytes + size > max_bytes:
            path = out / f'{workload}-{shard_no:03d}.json'
            path.write_text(json.dumps({'dataset': payload.get('dataset'), 'workload': workload, 'sessions': current}))
            manifest.append({'path': str(path), 'workload': workload, 'sessions': len(current)})
            shard_no += 1
            current, current_bytes = [], 0
        current.append(session)
        current_bytes += size
    if current:
        path = out / f'{workload}-{shard_no:03d}.json'
        path.write_text(json.dumps({'dataset': payload.get('dataset'), 'workload': workload, 'sessions': current}))
        manifest.append({'path': str(path), 'workload': workload, 'sessions': len(current)})
(out / 'manifest.json').write_text(json.dumps({'shards': manifest}, indent=2))
print(json.dumps({'shards': len(manifest), 'sessions': sum(x['sessions'] for x in manifest)}))
PY

shard_index=0
for shard in "$OUT_DIR"/shards/*.json; do
  [ "$(basename "$shard")" = manifest.json ] && continue
  workload="$(basename "$shard" | sed 's/-[0-9][0-9][0-9]\.json$//')"
  resource_workload="${workload//_/-}"
  worker=$((shard_index % COUNT + 1))
  shard_name="trace-replay-replay-${RUN_TAG}-${resource_workload}-${shard_index}"
  job_name="trace-replay-workload-${RUN_TAG}-${resource_workload}-${shard_index}"
  if ! oc -n "$NS" get configmap "$shard_name" >/dev/null 2>&1; then
    oc -n "$NS" create configmap "$shard_name" --from-file=replay.json="$shard" >/dev/null
  fi
  cat <<EOF | oc -n "$NS" apply -f - >/dev/null
apiVersion: batch/v1
kind: Job
metadata:
  name: $job_name
  labels:
    app: trace-replay-workload-experiment
    workload: $workload
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 86400
  template:
    metadata:
      labels:
        app: trace-replay-workload-experiment
        workload: $workload
    spec:
      restartPolicy: Never
      nodeSelector:
        kubernetes.io/hostname: $NODE
      containers:
      - name: driver
        image: python:3.12-slim
        command: ["python", "/app/driver.py"]
        args: ["--corpus", "/data/replay.json", "--out", "/results", "--layer", "shell", "--url", "http://openclaw-fleet-$worker.$NS.svc.cluster.local:18790", "--model", "openclaw/perf_agent", "--token", "change-me-gateway-token", "--timeout", "1800", "--stream", "--session-turns", "--prewarm"]
        volumeMounts:
        - {name: source, mountPath: /app}
        - {name: corpus, mountPath: /data}
        - {name: results, mountPath: /results}
      - name: result-hold
        image: python:3.12-slim
        command: ["python", "-c"]
        args: ["import time; time.sleep($RESULT_HOLD_SECONDS)"]
        volumeMounts:
        - {name: results, mountPath: /results}
      volumes:
      - name: source
        configMap: {name: $DRIVER_CONFIG}
      - name: corpus
        configMap: {name: $shard_name}
      - name: results
        emptyDir: {}
EOF
  printf '%s\t%s\t%s\t%s\n' "$job_name" "$workload" "$worker" "$shard" >> "$OUT_DIR/jobs.tsv"
  shard_index=$((shard_index + 1))
done

while IFS=$'\t' read -r job workload worker shard; do
  pod="$(oc -n "$NS" get pod -l "job-name=$job" -o jsonpath='{.items[0].metadata.name}')"
  for attempt in $(seq 1 1800); do
    phase="$(oc -n "$NS" get pod "$pod" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    if oc -n "$NS" exec -c result-hold "$pod" -- test -f /results/summary.json >/dev/null 2>&1; then
      break
    fi
    [[ "$phase" == "Succeeded" || "$phase" == "Failed" ]] && break
    sleep 1
  done
  oc -n "$NS" logs "$pod" > "$OUT_DIR/workers/$job.log"
  mkdir -p "$OUT_DIR/workers/$job-results"
  oc -n "$NS" cp -c result-hold "$pod:/results/." "$OUT_DIR/workers/$job-results/" >/dev/null 2>&1 || true
  oc -n "$NS" wait --for=condition=complete "job/$job" --timeout=7200s
done < "$OUT_DIR/jobs.tsv"

APPWORLD_POD="$(oc -n "$NS" get pod -l app=appworld -o jsonpath='{.items[0].metadata.name}')"
oc -n "$NS" exec "$APPWORLD_POD" -- cat /tmp/appworld-service-raw.log > "$OUT_DIR/appworld-adapter.log" 2>/dev/null || true
EVENTS="$(grep -c '"event": "appworld_tool"' "$OUT_DIR/appworld-adapter.log" 2>/dev/null || true)"
printf '{"sessions":100,"workloads":4,"appworld_events":%s,"target_node":"%s","jobs":%s}\n' "$EVENTS" "$NODE" "$shard_index" > "$OUT_DIR/summary.json"
