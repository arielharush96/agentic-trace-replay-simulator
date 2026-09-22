"""Workload taxonomy and corpus partitioning for the Exgentic traces."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable


WORKLOADS = (
    "software_engineering_qa",
    "personal_assistant",
    "research_deep_search",
    "customer_support_ops",
)

WORKLOAD_LABELS = {
    "software_engineering_qa": "Software Engineering & QA",
    "personal_assistant": "Personal Assistant",
    "research_deep_search": "Research & Deep Search",
    "customer_support_ops": "Customer Support & Ops",
}

BENCHMARK_TO_WORKLOAD = {
    "swebench": "software_engineering_qa",
    "appworld": "personal_assistant",
    "browsecompplus": "research_deep_search",
    "tau2_airline": "customer_support_ops",
    "tau2_retail": "customer_support_ops",
    "tau2_telecom": "customer_support_ops",
}


def classify_benchmark(benchmark: str | None) -> tuple[str, str]:
    """Return (workload, profile) for a dataset benchmark."""
    normalized = str(benchmark or "").strip().lower()
    workload = BENCHMARK_TO_WORKLOAD.get(normalized, "unknown")
    return workload, normalized or "unknown"


def classify_session(session: dict[str, Any]) -> dict[str, str]:
    workload, profile = classify_benchmark(session.get("benchmark"))
    return {"workload": workload, "profile": profile}


def _with_workload(session: dict[str, Any]) -> dict[str, Any]:
    enriched = dict(session)
    classification = classify_session(enriched)
    enriched.setdefault("workload", classification["workload"])
    enriched.setdefault("workload_profile", classification["profile"])
    return enriched


def workload_stats(sessions: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    rows = [_with_workload(session) for session in sessions]
    stats: dict[str, dict[str, Any]] = {}
    for workload in [*WORKLOADS, "unknown"]:
        group = [row for row in rows if row["workload"] == workload]
        if not group:
            continue
        stats[workload] = {
            "label": WORKLOAD_LABELS.get(workload, "Unknown"),
            "sessions": len(group),
            "turns": sum(len(row.get("turns") or []) for row in group),
            "tool_calls": sum(
                len(turn.get("tool_calls") or [])
                for row in group
                for turn in (row.get("turns") or [])
            ),
            "benchmarks": dict(Counter(str(row.get("benchmark") or "unknown") for row in group)),
        }
    return stats


def split_corpus(
    corpus: Path,
    out_dir: Path,
    *,
    limit_per_workload: int | None = None,
    min_turns: int = 1,
) -> dict[str, Any]:
    """Write one replay corpus per workload without splitting sessions."""
    payload = json.loads(Path(corpus).read_text(encoding="utf-8"))
    source_sessions = [
        _with_workload(session)
        for session in (payload.get("sessions") or [])
        if len(session.get("turns") or []) >= min_turns
    ]
    groups: dict[str, list[dict[str, Any]]] = {workload: [] for workload in WORKLOADS}
    groups["unknown"] = []
    for session in source_sessions:
        groups[session["workload"]].append(session)

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    files: dict[str, str] = {}
    selected_groups: dict[str, list[dict[str, Any]]] = {}
    for workload, sessions in groups.items():
        if limit_per_workload is not None:
            # Round-robin profiles so the aggregate customer-support cohort
            # does not accidentally become an airline-only experiment.
            by_profile: dict[str, list[dict[str, Any]]] = {}
            for session in sessions:
                by_profile.setdefault(str(session.get("workload_profile") or "unknown"), []).append(session)
            balanced: list[dict[str, Any]] = []
            profiles = sorted(by_profile)
            while len(balanced) < limit_per_workload and profiles:
                progressed = False
                for profile in profiles:
                    if by_profile[profile] and len(balanced) < limit_per_workload:
                        balanced.append(by_profile[profile].pop(0))
                        progressed = True
                if not progressed:
                    break
            sessions = balanced
        if not sessions:
            continue
        selected_groups[workload] = sessions
        result = dict(payload)
        result.update({
            "session_count": len(sessions),
            "workload": workload,
            "workload_label": WORKLOAD_LABELS.get(workload, "Unknown"),
            "sessions": sessions,
        })
        path = out_dir / f"{workload}.json"
        path.write_text(json.dumps(result, indent=2), encoding="utf-8")
        files[workload] = str(path)

    manifest = {
        "schema": "openclaw-workload-partition-v1",
        "source": str(corpus),
        "min_turns": min_turns,
        "limit_per_workload": limit_per_workload,
        "workloads": workload_stats(
            session for sessions in selected_groups.values() for session in sessions
        ),
        "files": files,
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest
