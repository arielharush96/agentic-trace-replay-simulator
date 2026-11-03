from trace_replay_sim.profiler_v2 import _collapse_logical_rows


def test_session_turn_rows_collapse_continuation_and_keep_step_zero_pre_llm_only():
    rows = [
        {
            "trace_id": "trace-a",
            "turn": 0,
            "model_start_ms": 100.0,
            "exec_ms": 200.0,
            "response_processing_ms": 7.0,
            "response_processing_full_ms": 700.0,
        },
        {
            "trace_id": "trace-a",
            "turn": 1,
            "model_start_ms": 150.0,
            "exec_ms": None,
            "response_processing_ms": 8.0,
        },
        {
            "trace_id": "trace-b",
            "turn": 0,
            "model_start_ms": 300.0,
            "exec_ms": 180.0,
            "response_processing_ms": 6.0,
            "response_processing_full_ms": 650.0,
        },
    ]
    edges = [
        {"trace_id": "trace-a", "turn_seq": 0, "t_recv_wall_ns": 1_000_000_000, "t_end_emit_wall_ns": 1_100_000_000},
        {"trace_id": "trace-a", "turn_seq": 0, "t_recv_wall_ns": 1_150_000_000, "t_end_emit_wall_ns": 1_200_000_000},
        {"trace_id": "trace-b", "turn_seq": 1, "t_recv_wall_ns": 1_250_000_000, "t_end_emit_wall_ns": 1_300_000_000},
    ]
    requests = [
        {"t_send_wall_ns": 900_000_000},
        {"t_send_wall_ns": 1_220_000_000},
    ]

    result = _collapse_logical_rows(rows, edges, requests)

    assert len(result) == 2
    assert [row["turn"] for row in result] == [0, 1]
    assert result[0]["sandbox_execution_bucket_ms"] == 0.0
    assert result[0]["response_processing_bucket_ms"] == 0.0
    assert result[1]["sandbox_execution_bucket_ms"] == 200.0
    assert result[1]["response_processing_bucket_ms"] == 7.0
    assert result[1]["context_assembly_bucket_ms"] == 50.0
