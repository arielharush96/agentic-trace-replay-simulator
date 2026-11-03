#!/usr/bin/env python3
"""Audit normalized replay sessions for controlled agent-loop fidelity."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any


NATIVE_HARNESSES = {"tool_calling", "tool_calling_with_shortlisting", "openai_solo"}
OPAQUE_HARNESSES = {"claude_code", "smolagents_code"}
NESTED_TOOL_NAMES = {"sessions_spawn", "subagent", "delegate", "task"}


def classify(session: dict[str, Any]) -> dict[str, Any]:
    turns = session.get("turns") or []
    tools = [tool for turn in turns for tool in (turn.get("tool_calls") or [])]
    missing_results = sum(1 for tool in tools if not str(tool.get("recorded_result") or ""))
    nested_tools = [
        tool for tool in tools
        if str(tool.get("name") or "").lower() in NESTED_TOOL_NAMES
        or any(key in (tool.get("arguments") or {}) for key in ("subagent_type", "task_id", "parent_task_id"))
    ]
    multi_tool_turns = sum(1 for turn in turns if len(turn.get("tool_calls") or []) > 1)
    harness = str(session.get("harness") or "")
    if not turns:
        status = "exclude_no_turns"
    elif harness in OPAQUE_HARNESSES:
        status = "opaque_harness_only"
    elif nested_tools:
        status = "exclude_nested_or_subagent"
    elif missing_results:
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
        "multi_tool_turns": multi_tool_turns,
        "missing_tool_results": missing_results,
        "nested_or_subagent_tools": len(nested_tools),
        "input_tokens_first": session.get("input_tokens_first", 0),
        "output_tokens_first": session.get("output_tokens_first", 0),
        "recorded_duration_ms": session.get("recorded_duration_ms", 0),
        "status": status,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("corpus", type=Path)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    payload = json.loads(args.corpus.read_text(encoding="utf-8"))
    rows = [classify(session) for session in payload.get("sessions", [])]
    status_counts = Counter(row["status"] for row in rows)
    harness_counts = Counter(row["harness"] for row in rows)
    benchmark_counts = Counter(row["benchmark"] for row in rows)
    audit = {
        "schema": "openclaw-replay-corpus-audit-v1",
        "source": str(args.corpus),
        "sessions": len(rows),
        "status_counts": dict(status_counts),
        "harness_counts": dict(harness_counts),
        "benchmark_counts": dict(benchmark_counts),
        "turns": {
            "min": min((row["turns"] for row in rows), default=0),
            "max": max((row["turns"] for row in rows), default=0),
            "multi_tool_turns": sum(row["multi_tool_turns"] for row in rows),
        },
        "tool_calls": {
            "total": sum(row["tool_calls"] for row in rows),
            "missing_results": sum(row["missing_tool_results"] for row in rows),
            "nested_or_subagent": sum(row["nested_or_subagent_tools"] for row in rows),
        },
        "sessions": rows,
    }
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "audit.json").write_text(json.dumps(audit, indent=2), encoding="utf-8")
    lines = [
        "# Replay Corpus Audit",
        "",
        f"Source: `{args.corpus}`",
        f"Sessions: **{len(rows)}**",
        "",
        "## Classification",
        "",
        "| Status | Sessions | Meaning |",
        "|---|---:|---|",
        f"| `controlled_replay_candidate` | {status_counts['controlled_replay_candidate']} | Native-looking tool loop with recorded results and no nested subagent markers |",
        f"| `partial_tool_results` | {status_counts['partial_tool_results']} | One or more tool results are missing; synthetic result policy required |",
        f"| `opaque_harness_only` | {status_counts['opaque_harness_only']} | CLI/harness turn may hide model calls, retries, tools, and context assembly |",
        f"| `exclude_nested_or_subagent` | {status_counts['exclude_nested_or_subagent']} | Subagent/delegation structure is not faithfully represented |",
        f"| `review_harness` | {status_counts['review_harness']} | Harness is not in the validated native set |",
        f"| `exclude_no_turns` | {status_counts['exclude_no_turns']} | No replayable model turns |",
        "",
        "## Replay Rules",
        "",
        "- `controlled_replay_candidate` may run in controlled OpenClaw/OpenShell replay.",
        "- `partial_tool_results` must be labeled partial and never claim exact tool-result replay.",
        "- `opaque_harness_only` cannot be used to measure OpenClaw’s internal model/tool loop faithfully.",
        "- `exclude_nested_or_subagent` requires a separate subagent-aware replay design.",
        "- Parallel tool execution is not represented safely by the normalized corpus and must be treated as unknown unless raw spans are joined.",
        "- `mock-direct` bypasses OpenClaw and is a network/model-floor control only.",
        "",
        "## Composition",
        "",
        f"Harnesses: `{dict(harness_counts)}`",
        f"Benchmarks: `{dict(benchmark_counts)}`",
        f"Turn range: `{audit['turns']['min']}–{audit['turns']['max']}`",
        f"Tool calls: `{audit['tool_calls']['total']}`",
        f"Missing tool results: `{audit['tool_calls']['missing_results']}`",
        f"Nested/subagent markers: `{audit['tool_calls']['nested_or_subagent']}`",
    ]
    (args.out / "AUDIT.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({k: audit[k] for k in ("sessions", "status_counts", "harness_counts", "benchmark_counts", "turns", "tool_calls")}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
