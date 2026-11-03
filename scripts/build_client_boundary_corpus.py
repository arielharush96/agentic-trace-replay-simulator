#!/usr/bin/env python3
"""Build the client-boundary normalized corpus for the 100-agent run.

This is the canary-faithful normalization: the exact transform that produced
the accepted four-workload canary corpus
(``results/client-boundary-canary-20261025/corpus-normalized.json``), applied
to the authoritative 100 sessions from the previous completed
``full-experiment-25x4-final`` run. The raw corpus is never modified.

Per session, using the live ``trace_replay_sim`` modules (the same code the
canary ran):

1. Select the session from the audited full corpus by its authoritative ID.
2. Recover Qwen/Kimi serialized tool calls from turn content
   (``_SERIALIZED_TOOL_CALL_RE`` -> ``serialized-<ordinal>`` ids).
3. Drop blank action-less turns; when the surviving session ends on a tool
   request, append the synthetic ``Task complete.`` / ``stop`` terminal;
   reindex turns (``_normalize_replay_turns``).
4. Regenerate every ``mapped_command`` with the new turn/call coordinates
   (``map_tool_with_replay``).
5. Recount ``tool_call_count`` from the normalized turns.
6. Add ``client_events`` (turn-0 user prompt, matching the accepted canary)
   and infer ``workload`` / ``workload_profile`` from the benchmark.

Output shape (matching the accepted canary):
``{dataset, source_dataset, client_boundary_mode: true, sessions, by_id}``.

Validation performed before writing:
- raw per-workload turn/tool-call totals must equal the manifest totals
  (raw-corpus integrity baseline);
- selected session IDs and workload assignments must match the manifest;
- every session must have non-empty client_events and at least one turn;
- when ``--golden`` is given, the same pipeline must reproduce every golden
  session record field-for-field (regression against the accepted canary).

The normalized totals are reported separately: dropping blank terminal
records, appending terminals, reindexing, and parsing serialized research
tool calls legitimately change turn/tool cardinality, so acceptance for the
run uses the normalized totals while the manifest totals remain the raw
integrity baseline.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from trace_replay_sim.extract import (  # noqa: E402
    _SERIALIZED_TOOL_CALL_RE,
    _normalize_replay_turns,
    _parse_json,
)
from trace_replay_sim.models import ReplaySession, ToolCall  # noqa: E402
from trace_replay_sim.toolmap import map_tool  # noqa: E402


def _load_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def _ordered_ids_by_workload(manifest: dict[str, Any], manifest_path: Path) -> dict[str, list[str]]:
    """Session IDs per workload, in the manifest's authoritative order."""
    root = manifest_path.parents[2] if manifest_path.parents[1].name == "workloads" else manifest_path.parent
    ids: dict[str, list[str]] = {}
    for workload, rel_file in manifest.get("files", {}).items():
        path = Path(rel_file)
        if not path.is_absolute():
            for candidate in (path, manifest_path.parent / path.name, root / rel_file):
                if candidate.exists():
                    path = candidate
                    break
        corpus = _load_json(path)
        ids[workload] = [str(s.get("session_id") or "") for s in corpus.get("sessions", [])]
    return ids


def _recover_serialized_tool_calls(session: ReplaySession) -> int:
    """Recover structured tool calls encoded in raw serialized turn content.

    Mirrors the recovery in ``extract._tool_calls_from_message``: the regex is
    applied to the message text and recovered calls are appended with stable
    ``serialized-<ordinal>`` ids. ``recorded_result`` is empty for recovered
    calls (no structured result exists for them in the raw corpus).
    """
    recovered = 0
    for turn in session.turns:
        content = turn.content or ""
        if "<|tool_call_begin|>" not in content:
            continue
        for ordinal, match in enumerate(_SERIALIZED_TOOL_CALL_RE.finditer(content)):
            name = match.group(1)
            arguments = _parse_json(match.group(2))
            if not isinstance(arguments, dict):
                arguments = {"input": arguments}
            turn.tool_calls.append(
                ToolCall(
                    id=f"serialized-{ordinal}",
                    name=name,
                    arguments=arguments,
                    mapped_command=map_tool(name, arguments),
                )
            )
            recovered += 1
    return recovered


def _normalize_session(raw: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Apply the canary-faithful normalization to one raw session record."""
    session = ReplaySession.from_dict(raw)
    recovered = _recover_serialized_tool_calls(session)
    raw_turns = len(session.turns)
    raw_calls = sum(len(t.tool_calls) for t in session.turns)
    session.turns = _normalize_replay_turns(session.turns, session_id=session.session_id)
    session.tool_call_count = sum(len(t.tool_calls) for t in session.turns)
    if not session.client_events and session.user_prompt:
        session.client_events = [{"turn_index": 0, "content": session.user_prompt}]
    terminal_appended = bool(
        session.turns
        and session.turns[-1].content == "Task complete."
        and session.turns[-1].finish_reason == "stop"
        and not session.turns[-1].tool_calls
        and session.turns[-1].span_id == ""
        and session.turns[-1].duration_ms == 0.0
    )
    stats = {
        "raw_turns": raw_turns,
        "normalized_turns": len(session.turns),
        "raw_tool_calls": raw_calls,
        "normalized_tool_calls": session.tool_call_count,
        "recovered_serialized_calls": recovered,
        "blank_turns_dropped": raw_turns - len(session.turns) + (1 if terminal_appended else 0),
        "terminal_appended": terminal_appended,
        "ends_with_stop": bool(session.turns and session.turns[-1].finish_reason == "stop"),
    }
    return session.to_dict(), stats


def _diff_dicts(left: dict[str, Any], right: dict[str, Any], path: str = "") -> list[str]:
    """Field-level diff between two session records (order-insensitive)."""
    failures: list[str] = []
    for key in sorted(set(left) | set(right)):
        key_path = f"{path}.{key}" if path else key
        if key not in left:
            failures.append(f"{key_path}: missing in built")
        elif key not in right:
            failures.append(f"{key_path}: missing in golden")
        elif left[key] != right[key]:
            failures.append(f"{key_path}: differs")
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", required=True, type=Path, help="audited full replay corpus (raw, never modified)")
    parser.add_argument("--manifest", required=True, type=Path, help="authoritative workload manifest from the previous completed run")
    parser.add_argument("--out", required=True, type=Path, help="output normalized corpus path")
    parser.add_argument("--report", type=Path, help="validation report output path (default: <out>.report.json)")
    parser.add_argument("--golden", type=Path, help="accepted canary corpus-normalized.json for regression validation")
    args = parser.parse_args()

    corpus_payload = _load_json(args.corpus)
    raw_by_id = {str(s.get("session_id")): s for s in corpus_payload.get("sessions", [])}
    manifest = _load_json(args.manifest)
    ids_by_workload = _ordered_ids_by_workload(manifest, args.manifest)

    failures: list[str] = []
    limitations: list[str] = []

    # --- raw-corpus integrity baseline -----------------------------------
    raw_totals: dict[str, dict[str, int]] = {}
    for workload, ids in ids_by_workload.items():
        missing = [sid for sid in ids if sid not in raw_by_id]
        if missing:
            failures.append(f"{workload}: sessions missing from raw corpus: {missing}")
        raw_turns = sum(len(raw_by_id[sid].get("turns") or []) for sid in ids if sid in raw_by_id)
        raw_calls = sum(
            len(t.get("tool_calls") or [])
            for sid in ids if sid in raw_by_id
            for t in (raw_by_id[sid].get("turns") or [])
        )
        expected = manifest.get("workloads", {}).get(workload, {})
        manifest_turns = int(expected.get("turns") or 0)
        manifest_calls = int(expected.get("tool_calls") or 0)
        raw_totals[workload] = {
            "sessions": len(ids),
            "raw_turns": raw_turns,
            "manifest_turns": manifest_turns,
            "raw_tool_calls": raw_calls,
            "manifest_tool_calls": manifest_calls,
        }
        if raw_turns != manifest_turns or raw_calls != manifest_calls:
            failures.append(
                f"{workload}: raw totals {raw_turns}/{raw_calls} != manifest {manifest_turns}/{manifest_calls}"
            )

    all_ids = [sid for ids in ids_by_workload.values() for sid in ids]
    if len(set(all_ids)) != len(all_ids):
        failures.append("duplicate session IDs across workloads")

    # --- build the normalized corpus -------------------------------------
    built_sessions: list[dict[str, Any]] = []
    per_session: dict[str, dict[str, Any]] = {}
    for workload, ids in ids_by_workload.items():
        for sid in ids:
            raw = raw_by_id.get(sid)
            if raw is None:
                continue
            normalized, stats = _normalize_session(raw)
            stats["workload"] = workload
            built_sessions.append(normalized)
            per_session[sid] = stats
            if not normalized.get("client_events"):
                failures.append(f"{sid}: no client_events after normalization")
            if not normalized.get("turns"):
                failures.append(f"{sid}: no turns after normalization")
            if normalized.get("workload") != workload:
                failures.append(
                    f"{sid}: workload {normalized.get('workload')!r} != manifest workload {workload!r}"
                )

    normalized_totals: dict[str, dict[str, int]] = {}
    for workload, ids in ids_by_workload.items():
        normalized_totals[workload] = {
            "sessions": len(ids),
            "normalized_turns": sum(per_session[sid]["normalized_turns"] for sid in ids if sid in per_session),
            "normalized_tool_calls": sum(per_session[sid]["normalized_tool_calls"] for sid in ids if sid in per_session),
        }

    # --- golden regression -------------------------------------------------
    golden_results: dict[str, Any] = {}
    if args.golden:
        golden_payload = _load_json(args.golden)
        golden_by_id = {str(s.get("session_id")): s for s in golden_payload.get("sessions", [])}
        regression_failures: dict[str, list[str]] = {}
        for sid, golden_session in golden_by_id.items():
            raw = raw_by_id.get(sid)
            if raw is None:
                regression_failures[sid] = ["session missing from raw corpus"]
                continue
            built, _ = _normalize_session(raw)
            diff = _diff_dicts(built, golden_session)
            if diff:
                regression_failures[sid] = diff
        golden_results = {
            "golden_sessions": len(golden_by_id),
            "reproduced": len(golden_by_id) - len(regression_failures),
            "failures": regression_failures,
        }
        if regression_failures:
            failures.append(
                f"golden regression: {len(regression_failures)} session(s) not reproduced field-for-field"
            )

    report = {
        "schema": "agentic.trace-replay.corpus-build-report.v1",
        "corpus": str(args.corpus),
        "manifest": str(args.manifest),
        "sessions": len(built_sessions),
        "raw_totals": raw_totals,
        "normalized_totals": normalized_totals,
        "recovered_serialized_calls_total": sum(s["recovered_serialized_calls"] for s in per_session.values()),
        "sessions_with_terminal_appended": sum(1 for s in per_session.values() if s["terminal_appended"]),
        "sessions_with_blank_turns_dropped": sum(1 for s in per_session.values() if s["blank_turns_dropped"]),
        "sessions_not_ending_with_stop": [
            sid for sid, s in per_session.items() if not s["ends_with_stop"]
        ],
        "golden_regression": golden_results,
        "limitations": limitations,
        "failures": failures,
        "passed": not failures,
    }

    if failures:
        report_path = args.report or (args.out.parent / (args.out.name + ".report.json"))
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report, indent=2))
        print(f"VALIDATION FAILED: {len(failures)} failure(s); report at {report_path}", file=sys.stderr)
        return 1

    out_payload = {
        "dataset": corpus_payload.get("dataset"),
        "source_dataset": corpus_payload.get("source_dataset"),
        "client_boundary_mode": True,
        "sessions": built_sessions,
        "by_id": {str(s["session_id"]): s for s in built_sessions},
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(out_payload, ensure_ascii=False) + "\n", encoding="utf-8")
    report_path = args.report or (args.out.parent / (args.out.name + ".report.json"))
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {args.out} ({len(built_sessions)} sessions)")
    print(f"report: {report_path}")
    print(json.dumps({"raw_totals": raw_totals, "normalized_totals": normalized_totals}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
