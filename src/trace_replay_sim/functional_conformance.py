"""Functional conformance checks for recorded agent-trace replays.

The performance plots answer *how long* the replay took.  This module answers
whether the replay followed the recorded workflow.  It deliberately compares
stable, observable facts rather than model text: turn order, tool counts,
stateful-backend calls, redacted argument fingerprints, and the first-turn
phase invariant.

Secrets are never copied into the report.  Arguments are canonicalized with
credential/token values replaced by a marker and only a SHA-256 fingerprint is
persisted.
"""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable


SCHEMA = "agentic.trace-replay.functional-conformance.v1"
_VOLATILE_KEYS = {
    "access_token",
    "api_key",
    "authorization",
    "password",
    "refresh_token",
    "token",
}


def _canonical(value: Any, key: str | None = None) -> Any:
    if key and key.lower() in _VOLATILE_KEYS:
        return "<redacted>"
    if isinstance(value, dict):
        return {str(name): _canonical(value[name], str(name)) for name in sorted(value)}
    if isinstance(value, list):
        return [_canonical(item) for item in value]
    if isinstance(value, tuple):
        return [_canonical(item) for item in value]
    return value


def arguments_sha256(arguments: dict[str, Any] | None) -> str:
    """Return a stable fingerprint without persisting credential values."""
    payload = json.dumps(_canonical(arguments or {}), sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            rows.append(value)
    return rows


def _check(name: str, passed: bool, expected: Any = None, observed: Any = None, details: str = "") -> dict[str, Any]:
    result: dict[str, Any] = {"name": name, "status": "PASS" if passed else "FAIL", "passed": passed}
    if expected is not None:
        result["expected"] = expected
    if observed is not None:
        result["observed"] = observed
    if details:
        result["details"] = details
    return result


def _warning(name: str, expected: Any = None, observed: Any = None, details: str = "") -> dict[str, Any]:
    """Record an explicitly non-strict legacy-backend limitation.

    A warning is not a functional failure: the real event stream can still
    prove session identity and tool workflow, while an older backend logger
    may not expose optional coordinates/fingerprints.  The distinction is
    preserved in the report so callers do not mistake this for a strict pass.
    """
    result = _check(name, True, expected=expected, observed=observed, details=details)
    result["status"] = "WARN"
    return result


def _expected_session(trace_dir: Path) -> dict[str, Any] | None:
    payload = _read_json(trace_dir / "input_corpus.json", {})
    sessions = payload.get("sessions") or []
    return sessions[0] if len(sessions) == 1 and isinstance(sessions[0], dict) else None


def _expected_tools(session: dict[str, Any]) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []
    for turn in session.get("turns") or []:
        turn_index = int(turn.get("index") or 0)
        for call_index, call in enumerate(turn.get("tool_calls") or []):
            calls.append({
                "turn": turn_index,
                "call": call_index,
                "name": str(call.get("name") or ""),
                "arguments_sha256": arguments_sha256(call.get("arguments") or {}),
            })
    return calls


def _expected_backend_tools(
    expected: Iterable[dict[str, Any]], backend_kind: str = "appworld"
) -> list[dict[str, Any]]:
    """Return calls that must cross the selected stateful backend boundary."""
    if backend_kind == "appworld":
        return [call for call in expected if call["name"].startswith("mcp__environment__")]
    return list(expected)


def _mock_edges(trace_dir: Path, session_id: str) -> list[dict[str, Any]]:
    rows = [row for row in _read_jsonl(trace_dir / "data/shell/mock_edges.jsonl") if row.get("session_id") == session_id]
    rows = [row for row in rows if not row.get("warmup")]
    # Session-turn replay produces two model edges for a tool-bearing recorded
    # turn: the tool-call response and OpenClaw's post-tool continuation.  The
    # continuation is required to close that request, but it is not a second
    # recorded turn.  Collapse only repeated turn_seq values, preserving the
    # first edge (and preserving genuine no-tool turns and terminal phantom
    # evidence, which each occur once).
    normalized: list[dict[str, Any]] = []
    seen_turns: set[int] = set()
    for row in sorted(rows, key=lambda item: int(item.get("request_seq") or 0)):
        value = row.get("turn_seq")
        if value is None:
            normalized.append(row)
            continue
        turn = int(value)
        if turn >= 0 and turn in seen_turns:
            continue
        if turn >= 0:
            seen_turns.add(turn)
        normalized.append(row)
    return normalized


def _phase_check(trace_dir: Path, expected_turns: int) -> dict[str, Any]:
    path = trace_dir / "data/shell/profile-v2/diagrams/per_turn_efficiency.csv"
    if not path.exists():
        return _check("phase_invariant", False, details="per_turn_efficiency.csv is missing")
    try:
        rows = list(csv.DictReader(path.open(encoding="utf-8")))
    except OSError as exc:
        return _check("phase_invariant", False, details=f"cannot read phase data: {exc}")
    if not rows:
        return _check("phase_invariant", False, details="phase data is empty")
    first = rows[0]
    forbidden = {
        key: float(first.get(key) or 0.0)
        for key in ("response_processing_ms", "tool_dispatch_overhead_ms", "openshell_exec_ms", "tool_failure_ms")
    }
    violations = [key for key, value in forbidden.items() if value > 0.0]
    passed = len(rows) == expected_turns and not violations
    return _check(
        "phase_invariant",
        passed,
        expected={"turns": expected_turns, "turn_0_forbidden": {key: 0.0 for key in forbidden}},
        observed={"turns": len(rows), "turn_0": forbidden},
        details="; ".join(violations) if violations else "turn 0 contains context assembly/sandbox readiness only",
    )


def verify_trace(
    trace_dir: Path,
    *,
    appworld_events: list[dict[str, Any]] | None = None,
    backend_events: list[dict[str, Any]] | None = None,
    backend_kind: str = "appworld",
    backend_event_name: str | None = None,
) -> dict[str, Any]:
    """Verify one completed replay and return a JSON-serializable report."""
    trace_dir = Path(trace_dir)
    session = _expected_session(trace_dir)
    if session is None:
        return {
            "schema": SCHEMA,
            "status": "FAIL",
            "passed": False,
            "trace": trace_dir.name,
            "checks": [_check("input_session", False, details="input_corpus.json must contain exactly one session")],
        }

    session_id = str(session.get("session_id") or "")
    expected_turns = session.get("turns") or []
    expected_tools = _expected_tools(session)
    edges = _mock_edges(trace_dir, session_id)
    edge_turns = [int(row.get("turn_seq")) for row in edges if row.get("turn_seq") is not None and int(row.get("turn_seq")) >= 0]
    expected_counts = [len(turn.get("tool_calls") or []) for turn in expected_turns]
    # Start-only replay may add an explicitly marked local no-op tool to bridge
    # a non-final assistant-only turn. Compare only the recorded tool count so
    # this control-plane bridge cannot masquerade as workload behavior.
    observed_counts = [
        int(row.get("recorded_tool_calls", row.get("n_tool_calls")) or 0)
        for row in sorted(edges, key=lambda row: int(row.get("turn_seq") or 0))
    ]
    checks: list[dict[str, Any]] = []
    checks.append(_check(
        "model_turn_sequence",
        len(edges) == len(expected_turns) and edge_turns == list(range(len(expected_turns))) and not any(row.get("miss") or row.get("exhausted") for row in edges),
        expected={"turns": len(expected_turns), "sequence": list(range(len(expected_turns)))},
        observed={"turns": len(edges), "sequence": edge_turns},
        details="mock LLM replay advanced exactly once per recorded turn",
    ))
    checks.append(_check(
        "tool_count_per_turn",
        observed_counts == expected_counts,
        expected=expected_counts,
        observed=observed_counts,
        details="tool-call cardinality is compared before timing analysis",
    ))
    checks.append(_phase_check(trace_dir, len(expected_turns)))

    if backend_events is not None:
        events = backend_events
    elif backend_kind == "appworld":
        events = appworld_events if appworld_events is not None else _read_jsonl(trace_dir / "data/appworld/events.jsonl")
    else:
        events = _read_jsonl(trace_dir / "data/backend/events.jsonl")
    expected_backend = _expected_backend_tools(expected_tools, backend_kind)
    event_name = backend_event_name or ("appworld_tool" if backend_kind == "appworld" else "workload_tool")
    if expected_backend:
        observed_backend = [event for event in events if event.get("event") == event_name]
        observed_names = [str(event.get("tool") or "") for event in observed_backend]
        expected_names = [str(call["name"]) for call in expected_backend]
        positions_available = all(
            event.get("turn") is not None and event.get("call") is not None
            for event in observed_backend
        )
        if positions_available:
            observed_by_turn: dict[int, Counter[str]] = defaultdict(Counter)
            expected_by_turn: dict[int, Counter[str]] = defaultdict(Counter)
            for event in observed_backend:
                observed_by_turn[int(event["turn"])][str(event.get("tool") or "")] += 1
            for call in expected_backend:
                expected_by_turn[int(call["turn"])][str(call["name"])] += 1
            tool_sequence_matches = dict(observed_by_turn) == dict(expected_by_turn)
            sequence_details = "tool identity/cardinality matches per turn; concurrent calls are order-independent"
        else:
            tool_sequence_matches = observed_names == expected_names
            sequence_details = f"{backend_kind} event order and tool names match the recorded backend workflow"
        checks.append(_check(
            "stateful_backend_tool_sequence",
            tool_sequence_matches,
            expected=expected_names,
            observed=observed_names,
            details=sequence_details,
        ))
        session_ids = sorted({str(event.get("session_id") or "") for event in observed_backend})
        task_ids = sorted({str(event.get("task_id") or "") for event in observed_backend})
        checks.append(_check(
            "stateful_backend_identity",
            bool(observed_backend) and session_ids == [session_id]
            and (backend_kind != "appworld" or all(task_ids)),
            expected={"session_id": session_id, "stateful_identity": True},
            observed={"session_ids": session_ids, "task_ids": task_ids},
            details="backend events are tied to the replay session and stateful backend state",
        ))
        if backend_kind == "swebench":
            expected_commit = str((session.get("swe_task") or {}).get("base_commit") or "")
            observed_modes = sorted({str(event.get("execution_mode") or "") for event in observed_backend})
            observed_commits = sorted({str(event.get("sandbox_base_commit") or "") for event in observed_backend})
            checks.append(_check(
                "real_swe_sandbox_execution",
                bool(observed_backend)
                and observed_modes == ["sandbox"]
                and bool(expected_commit)
                and observed_commits == [expected_commit],
                expected={"execution_mode": "sandbox", "base_commit": expected_commit},
                observed={"execution_modes": observed_modes, "sandbox_base_commits": observed_commits},
                details="every SWE tool call ran in the pinned OpenShell task-image repository",
            ))
        actual_pairs = [(event.get("turn"), event.get("call")) for event in observed_backend]
        expected_pairs = [(call["turn"], call["call"]) for call in expected_backend]
        has_positions = positions_available
        comparable_pairs = sorted(actual_pairs) if has_positions else actual_pairs
        expected_comparable_pairs = sorted(expected_pairs)
        boundary_check = _check(
            "stateful_backend_turn_boundaries",
            comparable_pairs == expected_comparable_pairs if has_positions else False,
            expected=expected_pairs,
            observed=actual_pairs,
            details="request turn/call coordinates are present and match; concurrent log order is tolerated" if has_positions else "event lacks turn/call coordinates; rerun required for strict validation",
        )
        if backend_kind == "appworld" and not has_positions:
            boundary_check = _warning(
                "stateful_backend_turn_boundaries",
                expected=expected_pairs,
                observed=actual_pairs,
                details="legacy AppWorld event logger omitted turn/call coordinates; ordered real backend events were retained",
            )
        checks.append(boundary_check)
        expected_hashes = [str(call["arguments_sha256"]) for call in expected_backend]
        actual_hashes = [str(event.get("arguments_sha256") or "") for event in observed_backend]
        hashes_available = all(actual_hashes)
        if has_positions and hashes_available:
            observed_hashes_by_pair = {
                (int(event["turn"]), int(event["call"])): str(event.get("arguments_sha256"))
                for event in observed_backend
            }
            expected_hashes_by_pair = {
                (int(call["turn"]), int(call["call"])): str(call["arguments_sha256"])
                for call in expected_backend
            }
            argument_matches = observed_hashes_by_pair == expected_hashes_by_pair
        else:
            argument_matches = actual_hashes == expected_hashes if hashes_available else False
        argument_check = _check(
            "stateful_backend_arguments",
            argument_matches,
            expected=expected_hashes,
            observed=actual_hashes,
            details="redacted argument fingerprints match" if hashes_available else "event lacks redacted argument fingerprints; rerun required for strict validation",
        )
        if backend_kind == "appworld" and not hashes_available:
            argument_check = _warning(
                "stateful_backend_arguments",
                expected=expected_hashes,
                observed=actual_hashes,
                details="legacy AppWorld event logger omitted argument fingerprints; the real ordered tool stream remains available",
            )
        checks.append(argument_check)
        if backend_kind == "appworld":
            checks.append(_check(
                "stateful_backend_completion",
                bool(observed_backend) and observed_names[-1] == "mcp__environment__finish",
                expected="mcp__environment__finish",
                observed=observed_names[-1] if observed_names else None,
                details="recorded workflow reached its designated completion API",
            ))
    else:
        checks.append({
            "name": "stateful_backend_tool_sequence",
            "status": "SKIP",
            "passed": True,
            "details": "no workload-specific stateful backend calls are present in this corpus",
        })

    failures = [check for check in checks if check.get("status") == "FAIL"]
    warnings = [check for check in checks if check.get("status") == "WARN"]
    return {
        "schema": SCHEMA,
        "status": "FAIL" if failures else ("PASS_WITH_WARNINGS" if warnings else "PASS"),
        "passed": not failures,
        "trace": trace_dir.name,
        "session_id": session_id,
        "expected_turns": len(expected_turns),
        "expected_tool_calls": len(expected_tools),
        "expected_stateful_backend_calls": len(expected_backend),
        "observed_model_turns": len(edges),
        "backend_kind": backend_kind,
        "observed_stateful_backend_events": len([event for event in events if event.get("event") == event_name]),
        "warnings": [check["name"] for check in warnings],
        "checks": checks,
    }


def write_report(trace_dir: Path, report: dict[str, Any]) -> Path:
    path = Path(trace_dir) / "functional_conformance.json"
    path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return path
