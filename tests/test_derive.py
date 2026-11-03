"""Tests for the per-turn 5-bucket derivation (derive.py)."""

from __future__ import annotations

from pathlib import Path

from trace_replay_sim.derive import derive_per_turn, load_mock_edges, summarize


def _edge(seq, parent, *, t_recv_ms, t_end_ms, ttft_set=51.0, ttft_meas=53.0,
          decode_set=0.0, decode_meas=0.0, out=10, n_tools=1, phantom=False, exhausted=False):
    return {
        "schema": "mock-edge/v1", "trace_id": "T", "parent_span_id": parent,
        "session_id": "S", "turn_seq": seq,
        "t_recv_ns": int(t_recv_ms * 1e6), "t_first_emit_ns": int((t_recv_ms + ttft_set) * 1e6),
        "t_end_emit_ns": int(t_end_ms * 1e6),
        "ttft_set_ms": ttft_set, "ttft_measured_ms": ttft_meas,
        "decode_set_ms": decode_set, "decode_measured_ms": decode_meas,
        "output_tokens": out, "input_tokens": 100, "assembled_tokens": 12,
        "n_tool_calls": n_tools, "phantom": phantom, "exhausted": exhausted, "miss": False,
    }


def _span(span_id, op, start_ms, dur_ms, ttfb=None):
    tags = []
    if ttfb is not None:
        tags.append({"key": "openclaw.model_call.time_to_first_byte_ms", "value": ttfb})
    return {
        "spanID": span_id, "operationName": op,
        "startTime": int(start_ms * 1000), "duration": int(dur_ms * 1000),
        "tags": tags, "references": [],
    }


def _synthetic_trace():
    return [{
        "traceID": "T",
        "spans": [
            _span("RUN", "openclaw.run", 0, 6000),
            _span("MC0", "openclaw.model.call", 10, 4267.8, ttfb=51),
            _span("T0", "openclaw.tool.execution", 4300, 127),   # cold sandbox
            _span("MC1", "openclaw.model.call", 4450, 940, ttfb=51),
            _span("T1", "openclaw.tool.execution", 5400, 50),    # warm sandbox
        ],
    }]


def test_decomposition_and_agreement():
    edges = [
        _edge(0, "MC0", t_recv_ms=10, t_end_ms=4277.8, decode_set=4216.8, decode_meas=4230.0, out=252),
        _edge(1, "MC1", t_recv_ms=4450, t_end_ms=5390, decode_set=890.4, decode_meas=900.0, out=54),
        _edge(2, "MC2", t_recv_ms=5500, t_end_ms=5500, out=1, n_tools=0, exhausted=True),
    ]
    rows = derive_per_turn(edges, _synthetic_trace())
    assert len(rows) == 2  # phantom/exhausted terminal excluded

    r0, r1 = rows
    # Turn 0: A=10, B(cold)=127, C=51, D=22.2, E=4216.8
    assert r0["A_premodel_ms"] == 10.0
    assert r0["B_sandbox_ms"] == 127.0
    assert r0["C_ttft_ms"] == 51.0
    assert r0["C_ttft_openclaw_ms"] == 51.0
    assert abs(r0["D_response_ms"] - 22.2) < 0.5
    assert r0["E_decode_ms"] == 4216.8
    assert r0["is_cold_sandbox"] is True
    # Skew-free constraint: G[0] == D[0] + B[0] + A[1], so delta ~ 0.
    assert abs(r0["G_delta_ms"]) < 0.5

    # Turn 1: warm sandbox, A=23, B=50, D=10
    assert r1["A_premodel_ms"] == 23.0
    assert r1["B_sandbox_ms"] == 50.0
    assert abs(r1["D_response_ms"] - 10.0) < 0.5
    assert r1["is_cold_sandbox"] is False

    summary = summarize(rows)
    assert summary["turns"] == 2
    assert summary["sandbox_cold_vs_warm"]["cold_mean_ms"] == 127.0
    assert summary["sandbox_cold_vs_warm"]["warm_mean_ms"] == 50.0
    assert summary["buckets"]["C_ttft_ms"]["p50"] == 51.0


def test_mock_only_no_traces():
    """With no Jaeger traces, C/E/G survive; A/B/D are absent (not fabricated)."""
    edges = [
        _edge(0, "", t_recv_ms=0, t_end_ms=100, decode_set=49.0, out=4),
        _edge(1, "", t_recv_ms=150, t_end_ms=250, decode_set=49.0, out=4),
        _edge(2, "", t_recv_ms=300, t_end_ms=300, out=1, n_tools=0, exhausted=True),
    ]
    rows = derive_per_turn(edges, [])
    assert len(rows) == 2
    assert all(r["A_premodel_ms"] is None for r in rows)
    assert all(r["B_sandbox_ms"] is None for r in rows)
    assert all(r["C_ttft_ms"] == 51.0 for r in rows)
    # G[0] = t_recv[1] - t_end_emit[0] = 150 - 100 = 50ms
    assert abs(rows[0]["G_gap_ms"] - 50.0) < 0.001


def test_multi_rep_grouped_by_run_not_session():
    """N reps of one session (same session_id, distinct trace_id) stay separate.

    Each rep must yield its own turn 0 (cold) and its own G[i]; grouping by
    session_id alone would collide the two reps' turn_seqs.
    """
    def rep(tid, base_recv):
        return [
            {"schema": "mock-edge/v1", "trace_id": tid, "parent_span_id": "",
             "session_id": "S", "turn_seq": 0,
             "t_recv_ns": int(base_recv * 1e6), "t_end_emit_ns": int((base_recv + 50) * 1e6),
             "ttft_set_ms": 51.0, "ttft_measured_ms": 51.0,
             "decode_set_ms": 49.0, "decode_measured_ms": 49.0,
             "output_tokens": 4, "phantom": False, "exhausted": False, "miss": False},
            {"schema": "mock-edge/v1", "trace_id": tid, "parent_span_id": "",
             "session_id": "S", "turn_seq": 1,
             "t_recv_ns": int((base_recv + 120) * 1e6), "t_end_emit_ns": int((base_recv + 170) * 1e6),
             "ttft_set_ms": 51.0, "ttft_measured_ms": 51.0,
             "decode_set_ms": 49.0, "decode_measured_ms": 49.0,
             "output_tokens": 4, "phantom": False, "exhausted": False, "miss": False},
        ]

    edges = rep("AAAA", 0) + rep("BBBB", 10_000)
    rows = derive_per_turn(edges, [])
    assert len(rows) == 4
    cold = [r for r in rows if r["is_cold_sandbox"]]
    assert len(cold) == 2  # one cold turn-0 per rep, not one across both
    assert {r["trace_id"] for r in cold} == {"AAAA", "BBBB"}
    # G[0] within a rep = 120 - 50 = 70ms; the next rep must NOT bleed in.
    for r in rows:
        if r["turn_seq"] == 0:
            assert abs(r["G_gap_ms"] - 70.0) < 0.001


def test_load_mock_edges_from_oc_log_capture(tmp_path: Path):
    """Loader must tolerate 'MOCK_EDGE ' log-prefixed lines and junk."""
    p = tmp_path / "pod.log"
    p.write_text(
        "some startup noise\n"
        'MOCK_EDGE {"schema": "mock-edge/v1", "session_id": "S", "turn_seq": 0}\n'
        "unrelated log line\n"
        '{"schema": "mock-edge/v1", "session_id": "S", "turn_seq": 1}\n',
        encoding="utf-8",
    )
    edges = load_mock_edges(p)
    assert len(edges) == 2
    assert [e["turn_seq"] for e in edges] == [0, 1]
