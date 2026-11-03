#!/usr/bin/env bash
set -euo pipefail

NS="${NS:-trace-replay}"
CORPUS="${CORPUS:?CORPUS is required}"
JOB_PREFIX="${JOB_PREFIX:-trace-replay-fleet-smoke}"
COUNT="${COUNT:-10}"
NODE="${TARGET_NODE:?set TARGET_NODE to a schedulable worker node}"
NODE_ENFORCER_PID=""

enforce_sandbox_nodes() {
  while :; do
    for sandbox in $(oc -n openshell-tracesim get sandbox.agents.x-k8s.io -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null); do
      case "$sandbox" in
        fleet*-*)
          oc -n openshell-tracesim patch sandbox "$sandbox" --type=merge \
            -p "{\"spec\":{\"podTemplate\":{\"spec\":{\"nodeSelector\":{\"kubernetes.io/hostname\":\"$NODE\"}}}}}" >/dev/null 2>&1 || true
          pod_node="$(oc -n openshell-tracesim get pod "$sandbox" -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)"
          ready="$(oc -n openshell-tracesim get sandbox "$sandbox" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)"
          if [ -n "$pod_node" ] && [ "$pod_node" != "$NODE" ] && [ "$ready" != "True" ]; then
            oc -n openshell-tracesim delete pod "$sandbox" --wait=false >/dev/null 2>&1 || true
          fi
          ;;
      esac
    done
    sleep 0.05
  done
}

oc -n "$NS" create configmap fleet-smoke-corpus \
  --from-file=replay.json="$CORPUS" \
  --dry-run=client -o yaml | oc apply --server-side -f -
oc -n "$NS" create configmap replay-corpus \
  --from-file=replay.json="$CORPUS" \
  --dry-run=client -o yaml | oc apply --server-side -f -
oc -n "$NS" rollout restart deployment/mock-llm >/dev/null
oc -n "$NS" rollout status deployment/mock-llm --timeout=180s >/dev/null
enforce_sandbox_nodes &
NODE_ENFORCER_PID=$!
oc -n "$NS" create configmap fleet-smoke-driver \
  --from-file=driver.py="$(cd "$(dirname "$0")/.." && pwd)/src/trace_replay_sim/driver.py" \
  --dry-run=client -o yaml | oc apply -f -

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
  ttlSecondsAfterFinished: 7200
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
        - "900"
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
          name: fleet-smoke-driver
      - name: corpus
        configMap:
          name: fleet-smoke-corpus
      - name: results
        emptyDir: {}
EOF
done

for index in $(seq 0 $((COUNT - 1))); do
  oc -n "$NS" wait --for=condition=complete "job/$JOB_PREFIX-$index" --timeout=1200s
done
kill "$NODE_ENFORCER_PID" 2>/dev/null || true
