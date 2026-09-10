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
