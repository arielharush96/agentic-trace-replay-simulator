#!/usr/bin/env bash
set -euo pipefail

NS="${NS:-trace-replay}"
CORPUS="${CORPUS:?CORPUS is required}"
COUNT="${COUNT:-9}"
NODE="${TARGET_NODE:?set TARGET_NODE to a schedulable worker node}"
OUT_DIR="${OUT_DIR:?OUT_DIR is required}"
JOB_PREFIX="${JOB_PREFIX:-trace-replay-fleet-100}"

mkdir -p "$OUT_DIR/workers"
oc -n "$NS" create configmap replay-corpus \
  --from-file=replay.json="$CORPUS" \
  --dry-run=client -o yaml | oc apply --server-side -f -
oc -n "$NS" create configmap fleet-experiment-driver \
  --from-file=driver.py="$(cd "$(dirname "$0")/.." && pwd)/src/trace_replay_sim/driver.py" \
  --dry-run=client -o yaml | oc apply -f -
oc -n "$NS" rollout restart deployment/mock-llm >/dev/null
oc -n "$NS" rollout status deployment/mock-llm --timeout=300s >/dev/null

APPWORLD_POD="$(oc -n "$NS" get pod -l app=appworld -o jsonpath='{.items[0].metadata.name}')"
oc -n "$NS" exec "$APPWORLD_POD" -- sh -c ': > /tmp/appworld-service-raw.log' >/dev/null 2>&1 || true

for index in $(seq 0 $((COUNT - 1))); do
  name="$JOB_PREFIX-$index"
  oc -n "$NS" delete job "$name" --ignore-not-found >/dev/null 2>&1 || true
  cat <<EOF | oc -n "$NS" apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: $name
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 86400
  template:
    spec:
      restartPolicy: Never
      nodeSelector:
        kubernetes.io/hostname: $NODE
      containers:
      - name: driver
        image: python:3.12-slim
        command: ["python", "/app/driver.py"]
        args:
        - --corpus
        - /data/replay.json
        - --out
        - /results
        - --layer
        - shell
        - --url
        - http://openclaw-fleet-$index.$NS.svc.cluster.local:18790
        - --model
        - openclaw/perf_agent
        - --token
        - change-me-gateway-token
        - --timeout
        - "1800"
        - --stream
        - --shard-index
        - "$index"
        - --shard-count
        - "$COUNT"
        volumeMounts:
        - name: source
          mountPath: /app
        - name: corpus
          mountPath: /data
        - name: results
          mountPath: /results
      volumes:
      - name: source
        configMap:
          name: fleet-experiment-driver
      - name: corpus
        configMap:
          name: replay-corpus
      - name: results
        emptyDir: {}
EOF
done

for index in $(seq 0 $((COUNT - 1))); do
  oc -n "$NS" wait --for=condition=complete "job/$JOB_PREFIX-$index" --timeout=7200s
  oc -n "$NS" logs "job/$JOB_PREFIX-$index" > "$OUT_DIR/workers/worker-$index.log"
done

oc -n "$NS" exec "$APPWORLD_POD" -- cat /tmp/appworld-service-raw.log > "$OUT_DIR/appworld-adapter.log" 2>/dev/null || true
EVENTS="$(grep -c '"event": "appworld_tool"' "$OUT_DIR/appworld-adapter.log" 2>/dev/null || true)"
[ "${EVENTS:-0}" -gt 0 ] || { echo "No AppWorld events exported" >&2; exit 1; }
printf '{"workers":%s,"traces":100,"appworld_events":%s,"target_node":"%s"}\n' "$COUNT" "$EVENTS" "$NODE" > "$OUT_DIR/summary.json"
