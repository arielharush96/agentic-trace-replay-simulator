import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "scripts"))

from domain_backend import DomainBackend


def test_domain_backends_keep_session_state(tmp_path):
    swe = DomainBackend("software_engineering_qa", tmp_path / "swe")
    first = swe.execute({
        "session_id": "s1",
        "benchmark": "swebench",
        "tool": "mcp__environment__bash",
        "arguments": {"command": "pwd"},
        "execution_mode": "sandbox",
        "sandbox_result": {"output": "/testbed", "returncode": 0, "repo_root": "/testbed", "base_commit": "abc"},
        "turn": 0,
        "call": 0,
    })
    second = swe.execute({
        "session_id": "s1",
        "benchmark": "swebench",
        "tool": "mcp__environment__bash",
        "arguments": {"command": "pytest -q"},
        "execution_mode": "sandbox",
        "sandbox_result": {"output": "2 passed", "returncode": 0, "repo_root": "/testbed", "base_commit": "abc"},
        "turn": 1,
        "call": 0,
    })
    assert first["ok"] and second["ok"]
    assert swe.events[-1]["state_version"] == 2
    assert swe.sessions["s1"]["tests_run"] == 1
    assert swe.events[-1]["execution_mode"] == "sandbox"
    assert swe.events[-1]["sandbox_base_commit"] == "abc"

    research = DomainBackend("research_deep_search", tmp_path / "research")
    result = research.execute({"session_id": "r1", "benchmark": "browsecompplus", "tool": "mcp__environment__search", "arguments": {"query": "agent systems"}, "turn": 0, "call": 0})
    assert json.loads(result["result"])[0]["source"] == "trace-replay-browsecompplus-document-index"
    assert research.sessions["r1"]["searches"] == ["agent systems"]

    support = DomainBackend("customer_support_ops", tmp_path / "support")
    result = support.execute({"session_id": "c1", "benchmark": "tau2_airline", "tool": "book_flight", "arguments": {"route": "A-B"}, "turn": 0, "call": 0})
    assert json.loads(result["result"])["status"] == "created"
    assert support.sessions["c1"]["bookings"]
