#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════
#  Trace-Replay Benchmark Demo
#  One command. Four workloads. Real measurements.
# ═══════════════════════════════════════════════════════════════
#
#  Usage:
#    ./scripts/demo.sh                        # 4 agents, one per workload
#    ./scripts/demo.sh --agents 8             # 8 agents, two per workload
#    ./scripts/demo.sh --quick                # skip deployment re-verification
#    ./scripts/demo.sh --ttft-simulated 50 --itl-simulated 15
#
#  The configuration box and the live phase stream come from
#  scripts/bench, so the demo and a hand-written bench command
#  produce exactly the same UI.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

case " $* " in
    *" --agents "*|*" -a "*)
        exec "$SCRIPT_DIR/bench" "$@"
        ;;
    *)
        exec "$SCRIPT_DIR/bench" --agents 4 "$@"
        ;;
esac
