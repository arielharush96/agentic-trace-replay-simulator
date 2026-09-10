"""Download Exgentic/agent-llm-traces and write a compact replay corpus."""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from trace_replay_sim.extract import extract_sessions
from trace_replay_sim.models import ReplaySession, replay_corpus_dict

DEFAULT_DATASET = "Exgentic/agent-llm-traces"


def _row_from_hf(item: Any) -> dict[str, Any]:
    if hasattr(item, "keys"):
        return {k: item[k] for k in item.keys()}
    return dict(item)


def iter_hf_rows(dataset: str = DEFAULT_DATASET, split: str = "train", limit: int | None = None) -> Iterable[dict[str, Any]]:
    from datasets import load_dataset

    stream = load_dataset(dataset, split=split, streaming=True)
    for index, item in enumerate(stream):
        if limit is not None and index >= limit:
            break
        yield _row_from_hf(item)


def load_local_rows(path: Path, limit: int | None = None) -> list[dict[str, Any]]:
    path = Path(path)
    rows: list[dict[str, Any]] = []
    if path.suffix == ".jsonl":
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                rows.append(json.loads(line))
                if limit is not None and len(rows) >= limit:
                    break
        return rows
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict) and "rows" in payload:
        rows = list(payload["rows"])
    elif isinstance(payload, dict):
        rows = [payload]
    if limit is not None:
        rows = rows[:limit]
    return rows


def ingest(
    *,
    out_path: Path,
    dataset: str = DEFAULT_DATASET,
    local_path: Path | None = None,
    limit: int | None = 50,
    harness: str | None = None,
    benchmark: str | None = None,
) -> dict[str, Any]:
    if local_path is not None:
        rows = load_local_rows(local_path, limit=None)
    else:
        fetch_limit = None if (harness or benchmark) else limit
        rows = list(iter_hf_rows(dataset=dataset, limit=fetch_limit))

    sessions = extract_sessions(rows)
    if harness:
        sessions = [s for s in sessions if s.harness == harness]
    if benchmark:
        sessions = [s for s in sessions if s.benchmark == benchmark]
    if limit is not None:
        sessions = sessions[:limit]

    extra = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_dataset": dataset if local_path is None else str(local_path),
        "filters": {"harness": harness, "benchmark": benchmark, "limit": limit},
        "stats": _stats(sessions),
    }
    payload = replay_corpus_dict(sessions, dataset=dataset, extra=extra)
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return extra | {"output": str(out_path), "session_count": len(sessions)}


def _stats(sessions: list[ReplaySession]) -> dict[str, Any]:
    if not sessions:
        return {"sessions": 0}
    return {
        "sessions": len(sessions),
        "llm_calls": sum(s.llm_calls for s in sessions),
        "tool_calls": sum(s.tool_call_count for s in sessions),
        "harnesses": dict(Counter(s.harness for s in sessions)),
        "benchmarks": dict(Counter(s.benchmark for s in sessions)),
        "avg_input_tokens_first": round(sum((s.first_turn.input_tokens if s.first_turn else 0) for s in sessions) / len(sessions), 1),
        "avg_output_tokens_first": round(sum((s.first_turn.output_tokens if s.first_turn else 0) for s in sessions) / len(sessions), 1),
        "avg_recorded_llm_ms": round(sum(s.recorded_duration_ms for s in sessions) / len(sessions), 1),
    }
