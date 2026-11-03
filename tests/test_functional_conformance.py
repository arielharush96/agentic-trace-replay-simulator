import csv
import json
from pathlib import Path

from trace_replay_sim.functional_conformance import arguments_sha256, verify_trace, write_report


def _make_trace(tmp_path: Path, *, wrong_hash: bool = False, wrong_tool: bool = False) -> Path:
    trace = tmp_path / "trace"
    (trace / "data/shell/profile-v2/diagrams").mkdir(parents=True)
    (trace / "data/shell").mkdir(exist_ok=True)
    (trace / "data/appworld").mkdir(parents=True)
    session = {
        "session_id": "session-1",
        "turns": [
            {"index": 0, "tool_calls": [{"name": "TodoWrite", "arguments": {}}]},
            {"index": 1, "tool_calls": [{"name": "mcp__environment__finish", "arguments": {"access_token": "secret"}}]},
        ],
    }
    (trace / "input_corpus.json").write_text(json.dumps({"sessions": [session]}))
    (trace / "data/shell/mock_edges.jsonl").write_text(
        "\n".join(json.dumps({"session_id": "session-1", "turn_seq": i, "n_tool_calls": 1, "miss": False, "exhausted": False}) for i in range(2)) + "\n"
    )
    fields = ["turn", "prompt_boundary_ms", "sandbox_init_ms", "response_processing_ms", "tool_dispatch_overhead_ms", "openshell_exec_ms", "tool_failure_ms"]
    with (trace / "data/shell/profile-v2/diagrams/per_turn_efficiency.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerow({"turn": 0, "prompt_boundary_ms": 10, "sandbox_init_ms": 20, "response_processing_ms": 0, "tool_dispatch_overhead_ms": 0, "openshell_exec_ms": 0, "tool_failure_ms": 0})
        writer.writerow({"turn": 1, "prompt_boundary_ms": 5, "sandbox_init_ms": 0, "response_processing_ms": 2, "tool_dispatch_overhead_ms": 3, "openshell_exec_ms": 4, "tool_failure_ms": 0})
    event = {
        "event": "appworld_tool",
        "session_id": "session-1",
        "task_id": "task-1",
        "tool": "mcp__environment__finish" if not wrong_tool else "mcp__environment__other",
        "turn": 1,
        "call": 0,
        "arguments_sha256": arguments_sha256({"access_token": "secret"}) if not wrong_hash else "bad",
    }
    (trace / "data/appworld/events.jsonl").write_text(json.dumps(event) + "\n")
    return trace


def test_functional_conformance_passes_without_persisting_secrets(tmp_path):
    trace = _make_trace(tmp_path)
    report = verify_trace(trace)
    assert report["status"] == "PASS"
    assert all("secret" not in json.dumps(check) for check in report["checks"])
    assert write_report(trace, report).exists()


def test_functional_conformance_rejects_wrong_backend_tool(tmp_path):
    report = verify_trace(_make_trace(tmp_path, wrong_tool=True))
    assert report["status"] == "FAIL"
    assert "stateful_backend_tool_sequence" in [check["name"] for check in report["checks"] if check["status"] == "FAIL"]


def test_functional_conformance_rejects_wrong_arguments(tmp_path):
    report = verify_trace(_make_trace(tmp_path, wrong_hash=True))
    assert report["status"] == "FAIL"
    assert "stateful_backend_arguments" in [check["name"] for check in report["checks"] if check["status"] == "FAIL"]
