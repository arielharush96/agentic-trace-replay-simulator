import json

from trace_replay_sim.live_recorder import EventRecorder, load_events, redact


def test_redact_recursively_removes_credentials():
    value = {"headers": {"Authorization": "Bearer secret"}, "nested": [{"api_key": "x"}], "text": "keep"}
    assert redact(value) == {
        "headers": {"Authorization": "[REDACTED]"},
        "nested": [{"api_key": "[REDACTED]"}],
        "text": "keep",
    }


def test_event_recorder_writes_normalized_jsonl(tmp_path):
    path = tmp_path / "events.jsonl"
    with EventRecorder(path) as recorder:
        first = recorder.record(
            "model.request",
            session_id="s-1",
            trace_id="t-1",
            turn_index=0,
            payload={"prompt_tokens": 12, "headers": {"authorization": "secret"}},
        )
        recorder.record("tool.result", session_id="s-1", turn_index=0, payload={"ok": True})

    rows = load_events(path)
    assert recorder.count == 2
    assert first["schema"] == "agent-event/v1"
    assert rows[0]["payload"]["headers"]["authorization"] == "[REDACTED]"
    assert rows[0]["t_monotonic_ns"] > 0
    assert rows[0]["t_wall_ns"] > 0
    assert all(json.dumps(row) for row in rows)
