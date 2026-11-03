#!/usr/bin/env bash
# MVP: 2 sessions x 3 turns against openclaw-shell. Deploy sources then run job.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
NS=trace-replay

echo "Updating ConfigMaps..."
oc -n "$NS" delete configmap mock-llm-src driver-src replay-corpus --ignore-not-found
oc -n "$NS" create configmap mock-llm-src --from-file=mock_llm.py=src/trace_replay_sim/mock_llm.py
oc -n "$NS" create configmap driver-src --from-file=driver.py=src/trace_replay_sim/driver.py
oc -n "$NS" create configmap replay-corpus --from-file=replay.json=data/replay-mvp.json

oc -n "$NS" apply -f k8s/10-mock-llm.yaml
oc -n "$NS" apply -f k8s/35-openclaw-shell.yaml
oc -n "$NS" set env deployment/mock-llm SINGLE_TURN_MODE-
oc -n "$NS" rollout restart deployment/mock-llm deployment/openclaw-shell
oc -n "$NS" rollout status deployment/mock-llm --timeout=90s
oc -n "$NS" rollout status deployment/openclaw-shell --timeout=120s

oc -n "$NS" delete job trace-replay-shell-c1 --ignore-not-found
oc -n "$NS" apply -f k8s/job-shell-c1.yaml
echo "Job started. Wait until Complete, then: oc -n $NS logs job/trace-replay-shell-c1"
echo "Then port-forward Jaeger and run:"
echo "  python3 -m trace_replay_sim.jaeger_export --out results/mvp --start <ISO> --end <ISO> --service openclaw-gateway-shell"
