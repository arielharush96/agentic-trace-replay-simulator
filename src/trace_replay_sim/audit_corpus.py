"""Classify normalized replay sessions before controlled execution."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any


NATIVE_HARNESSES = {"tool_calling", "tool_calling_with_shortlisting", "openai_solo"}
OPAQUE_HARNESSES = {"claude_code", "smolagents_code"}
NESTED_TOOL_NAMES = {"sessions_spawn", "subagent", "delegate", "task"}


def classify_session(session: dict[str, Any]) -> dict[str, Any]:
    turns = session.get("turns") or []
    tools = [tool for turn in turns for tool in (turn.get("tool_calls") or [])]
    missing = sum(not str(tool.get("recorded_result") or "") for tool in tools)
    nested = sum(
        str(tool.get("name") or "").lower() in NESTED_TOOL_NAMES
        or any(key in (tool.get("arguments") or {}) for key in ("subagent_type", "task_id", "parent_task_id"))
        for tool in tools
    )
    harness = str(session.get("harness") or "")
    if not turns:
        status = "exclude_no_turns"
    elif harness in OPAQUE_HARNESSES:
        status = "opaque_harness_only"
    elif nested:
        status = "exclude_nested_or_subagent"
    elif missing:
        status = "partial_tool_results"
    elif harness in NATIVE_HARNESSES:
        status = "controlled_replay_candidate"
    else:
        status = "review_harness"
    return {
        "session_id": session.get("session_id"),
        "harness": harness,
        "benchmark": session.get("benchmark"),
        "turns": len(turns),
        "tool_calls": len(tools),
        "missing_tool_results": missing,
        "nested_or_subagent_tools": nested,
        "status": status,
    }


def audit_corpus(corpus: Path, out: Path) -> dict[str, Any]:
    payload = json.loads(corpus.read_text(encoding="utf-8"))
    rows = [classify_session(session) for session in payload.get("sessions", [])]
    summary = {
        "schema": "openclaw-replay-corpus-audit-v1",
        "source": str(corpus),
        "sessions": len(rows),
        "status_counts": dict(Counter(row["status"] for row in rows)),
        "harness_counts": dict(Counter(row["harness"] for row in rows)),
        "benchmark_counts": dict(Counter(row["benchmark"] for row in rows)),
        "tool_calls": sum(row["tool_calls"] for row in rows),
        "missing_tool_results": sum(row["missing_tool_results"] for row in rows),
        "nested_or_subagent_tools": sum(row["nested_or_subagent_tools"] for row in rows),
        "sessions_detail": rows,
    }
    out.mkdir(parents=True, exist_ok=True)
    (out / "audit.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary
