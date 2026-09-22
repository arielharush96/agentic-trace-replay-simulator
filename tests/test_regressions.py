"""Regression tests for the multi-agent benchmark fixes."""

from __future__ import annotations

import json
from pathlib import Path

from trace_replay_sim.mock_llm import ReplayIndex
from trace_replay_sim.profiler_v2 import _prom_counter_rate, _trace_rows


def test_explicit_turn_index_keeps_replay_counter_key() -> None:
    session = {
        "session_id": "S1",
        "turns": [{"index": 0, "content": "first", "output_tokens": 1}],
    }
    index = ReplayIndex({"sessions": [session], "by_id": {"S1": session}})
    payload = {
        "messages": [{
            "role": "user",
            "content": "REPLAY_SESSION_ID=S1\nREPLAY_TURN_INDEX=0",
        }],
    }

    result = index.resolve(payload, "turns", trace_id="a" * 32)

    assert result["turn_seq"] == 0
    assert result["prefill_key"] == "a" * 32


def test_prom_counter_rate_reads_valid_payload_and_ignores_missing(tmp_path: Path) -> None:
    missing = tmp_path / "missing.json"
    assert _prom_counter_rate(missing) == []

    path = tmp_path / "counter.json"
    path.write_text(json.dumps({
        "data": {"result": [{"values": [["1", "2"], ["2", "5"]]}]},
    }), encoding="utf-8")

    assert _prom_counter_rate(path) == [(2.0, 3.0)]


def test_profiler_sums_all_tool_wrappers_in_a_turn() -> None:
    def span(span_id: str, operation: str, start_ms: float, duration_ms: float) -> dict:
        return {
            "spanID": span_id,
            "operationName": operation,
            "startTime": int(start_ms * 1000),
            "duration": int(duration_ms * 1000),
            "tags": [],
            "references": [],
        }

    rows = _trace_rows({
        "traceID": "T",
        "spans": [
            span("run", "openclaw.harness.run", 0, 1000),
            span("model0", "openclaw.model.call", 100, 100),
            span("tool0", "openclaw.tool.execution", 220, 30),
            span("tool1", "openclaw.tool.execution", 260, 40),
            span("model1", "openclaw.model.call", 400, 50),
        ],
    }, {})

    assert rows[0]["tool_count"] == 2
    assert rows[0]["tool_dispatch_ms"] == 70.0
