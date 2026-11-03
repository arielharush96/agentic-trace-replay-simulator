from pathlib import Path

from trace_replay_sim.extract import extract_session, extract_sessions
from trace_replay_sim.ingest import ingest
from trace_replay_sim.toolmap import map_tool
import json


FIXTURE = Path(__file__).parent / "fixtures" / "sample_exgentic_row.json"


def test_map_bash_and_unknown_tools():
    assert map_tool("mcp__environment__bash", {"command": "ls src"}) == ["bash", "-lc", "ls src"]
    assert map_tool("Read", {"path": "/tmp/x"})[0] == "bash"
    assert map_tool("some_unknown_tool", {}) == ["true"]


def test_extract_fixture_session():
    row = json.loads(FIXTURE.read_text())
    session = extract_session(row)
    assert session is not None
    assert session.session_id == "fixture-swe-001"
    assert session.harness == "claude_code"
    assert session.benchmark == "swe_bench"
    assert "Fix the failing unit test" in session.user_prompt
    assert session.llm_calls == 2
    assert session.tool_call_count == 1
    assert session.turns[0].input_tokens == 2400
    assert session.turns[0].tool_calls[0].name == "mcp__environment__bash"
    assert session.turns[0].tool_calls[0].mapped_command[:2] == ["bash", "-lc"]
    assert "Patched src/app.py" in session.gateway_text()
    assert session.recorded_duration_ms > 3000


def test_ingest_local_fixture(tmp_path: Path):
    out = tmp_path / "replay.json"
    result = ingest(out_path=out, local_path=FIXTURE, limit=10)
    assert result["session_count"] == 1
    payload = json.loads(out.read_text())
    assert payload["session_count"] == 1
    assert payload["sessions"][0]["session_id"] == "fixture-swe-001"
    assert payload["sessions"][0]["gateway_text"]
    assert extract_sessions([json.loads(FIXTURE.read_text())])


def test_extract_client_events_single_boundary_from_fixture():
    session = extract_session(json.loads(FIXTURE.read_text()))
    assert session is not None
    assert session.client_events == [
        {"turn_index": 0, "content": "Fix the failing unit test in src/app.py"},
    ]


def _span(index: int, inputs: list[dict], outputs: list[dict]) -> dict:
    return {
        "span_id": f"span-{index}",
        "name": "chat test-model",
        "start_time": f"2026-05-07T09:04:{index:02d}.000000Z",
        "end_time": f"2026-05-07T09:04:{index:02d}.500000Z",
        "attributes": {
            "gen_ai.operation.name": "chat",
            "gen_ai.request.model": "test-model",
            "gen_ai.usage.input_tokens": 10,
            "gen_ai.usage.output_tokens": 2,
            "gen_ai.input.messages": json.dumps(inputs),
            "gen_ai.output.messages": json.dumps(outputs),
        },
    }


def test_extract_client_events_mark_each_new_user_message():
    opening = {"role": "user", "parts": [{"type": "text", "content": "Book a flight"}]}
    tool_call = {
        "role": "assistant",
        "finish_reason": "tool_calls",
        "parts": [{"type": "tool_call", "id": "call-1", "name": "search", "arguments": {}}],
    }
    tool_result = {"role": "tool", "tool_call_id": "call-1", "content": "three options"}
    reply = {"role": "assistant", "parts": [{"type": "text", "content": "Which one?"}]}
    follow_up = {"role": "user", "parts": [{"type": "text", "content": "The cheapest one"}]}
    row = {
        "session_id": "multi-boundary",
        "benchmark": "swe_bench",
        "spans": [
            _span(0, [opening], [tool_call]),
            _span(1, [opening, tool_call, tool_result], [reply]),
            _span(2, [opening, tool_call, tool_result, reply, follow_up], [
                {"role": "assistant", "finish_reason": "stop", "parts": [{"type": "text", "content": "Booked"}]},
            ]),
        ],
    }

    session = extract_session(row)

    assert session is not None
    assert session.client_events == [
        {"turn_index": 0, "content": "Book a flight"},
        {"turn_index": 2, "content": "The cheapest one"},
    ]


def test_extract_client_events_ignore_tool_result_user_parts():
    opening = {"role": "user", "parts": [{"type": "text", "content": "Run the tests"}]}
    tool_result = {
        "role": "user",
        "parts": [{"type": "tool_result", "tool_call_id": "call-1", "content": "all passed"}],
    }
    row = {
        "session_id": "tool-result-user",
        "benchmark": "swe_bench",
        "spans": [
            _span(0, [opening], [
                {"role": "assistant", "finish_reason": "tool_calls", "parts": [
                    {"type": "tool_call", "id": "call-1", "name": "bash", "arguments": {"command": "pytest"}},
                ]},
            ]),
            _span(1, [opening, tool_result], [
                {"role": "assistant", "finish_reason": "stop", "parts": [{"type": "text", "content": "done"}]},
            ]),
        ],
    }

    session = extract_session(row)

    assert session is not None
    assert session.client_events == [{"turn_index": 0, "content": "Run the tests"}]


def test_extract_client_events_keep_boundaries_when_spans_lost_the_user_channel():
    opening = {"role": "user", "parts": [{"type": "text", "content": "Check my balance"}]}
    tool_result = {"role": "tool", "tool_call_id": "call-1", "content": "42"}
    row = {
        "session_id": "lost-user-channel",
        "benchmark": "swe_bench",
        "spans": [
            _span(0, [opening], [
                {"role": "assistant", "finish_reason": "stop", "parts": [{"type": "text", "content": "Which account?"}]},
            ]),
            _span(1, [opening], [
                {"role": "assistant", "finish_reason": "tool_calls", "parts": [
                    {"type": "tool_call", "id": "call-1", "name": "balance", "arguments": {}},
                ]},
            ]),
            _span(2, [opening, tool_result], [
                {"role": "assistant", "finish_reason": "stop", "parts": [{"type": "text", "content": "42"}]},
            ]),
        ],
    }

    session = extract_session(row)

    assert session is not None
    assert [event["turn_index"] for event in session.client_events] == [0, 1]
    assert session.client_events[0]["content"] == "Check my balance"
    assert session.client_events[1]["content"] == "Continue the recorded conversation."


def test_client_events_survive_corpus_round_trip():
    session = extract_session(json.loads(FIXTURE.read_text()))
    assert session is not None
    payload = session.to_dict()
    restored = type(session).from_dict(payload)
    assert restored.client_events == session.client_events
