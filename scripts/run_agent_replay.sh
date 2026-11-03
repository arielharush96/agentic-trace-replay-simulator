#!/usr/bin/env bash
set -euo pipefail

# Harness-agnostic local/remote replay entrypoint. It assumes the selected
# agent endpoint is already deployed and never creates or mutates cluster
# resources.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
HARNESS="openclaw"
LAYER="openclaw"
CORPUS=""
OUT=""
URL=""
MODEL=""
TOKEN_ENV=""
CONCURRENCY="1"
SESSION_TURNS=0
PREWARM=0
STREAM=0

usage() {
  cat <<'EOF'
Usage: scripts/run_agent_replay.sh --corpus FILE --out DIR --url URL [options]

Options:
  --harness NAME       openclaw or openai-compatible (default: openclaw)
  --layer NAME         mock-direct, openclaw, or shell (default: openclaw)
  --model NAME         harness model id; defaults per harness
  --token-env NAME     read the bearer token from this environment variable
  --concurrency N      independent sessions in parallel (default: 1)
  --session-turns      send one request per recorded turn with session reuse
  --prewarm            issue an uncounted warm-up request before measurement
  --stream             use SSE streaming
EOF
}

die() { echo "ERROR: $*" >&2; exit 2; }

while (($#)); do
  case "$1" in
    --corpus) CORPUS="$2"; shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    --url) URL="$2"; shift 2 ;;
    --harness) HARNESS="$2"; shift 2 ;;
    --layer) LAYER="$2"; shift 2 ;;
    --model) MODEL="$2"; shift 2 ;;
    --token-env) TOKEN_ENV="$2"; shift 2 ;;
    --concurrency) CONCURRENCY="$2"; shift 2 ;;
    --session-turns) SESSION_TURNS=1; shift ;;
    --prewarm) PREWARM=1; shift ;;
    --stream) STREAM=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown argument: $1" ;;
  esac
done

[ -n "$CORPUS" ] || die "--corpus is required"
[ -f "$CORPUS" ] || die "corpus not found: $CORPUS"
[ -n "$OUT" ] || die "--out is required"
[ -n "$URL" ] || die "--url is required"
[[ "$CONCURRENCY" =~ ^[1-9][0-9]*$ ]] || die "--concurrency must be positive"

TOKEN=""
if [ -n "$TOKEN_ENV" ]; then
  # Indirect expansion avoids placing the token in this script or its args.
  TOKEN="${!TOKEN_ENV-}"
fi

ARGS=(
  -m trace_replay_sim drive
  --corpus "$CORPUS"
  --out "$OUT"
  --layer "$LAYER"
  --harness "$HARNESS"
  --url "$URL"
  --concurrency "$CONCURRENCY"
  --token "$TOKEN"
)
[ -n "$MODEL" ] && ARGS+=(--model "$MODEL")
((SESSION_TURNS)) && ARGS+=(--session-turns)
((PREWARM)) && ARGS+=(--prewarm)
((STREAM)) && ARGS+=(--stream)

export PYTHONPATH="$ROOT/src${PYTHONPATH:+:$PYTHONPATH}"
exec "$PYTHON_BIN" "${ARGS[@]}"
