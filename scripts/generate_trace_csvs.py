#!/usr/bin/env python3
"""Export per-trace JSON/JSONL measurements as analysis-friendly CSV files."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("\n", encoding="utf-8")
        return
    fields = sorted({key for row in rows for key in row})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def load_json(path: Path) -> list[dict]:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else []


def export_variant(trace: Path, variant: str) -> None:
    base = trace / "data" / variant
    traces = base / "traces"
    profile = base / "profile-v2"
    timing = load_json(traces / "timing_segments.json")
    turns = load_json(traces / "per_turn_segments.json")
    tools = load_json(traces / "per_tool_segments.json")
    merged_turns = []
    for index, row in enumerate(timing):
        merged = dict(row)
        if index < len(turns):
            merged.update(turns[index])
        merged_turns.append(merged)
    write_csv(profile / "per_turn.csv", merged_turns)
    write_csv(profile / "per_tool.csv", tools)

    events_path = base / "appworld" / "events.jsonl"
    if events_path.exists():
        events = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines() if line]
        write_csv(base / "appworld" / "events.csv", events)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment", type=Path, required=True)
    args = parser.parse_args()
    for trace in sorted(args.experiment.glob("[0-9][0-9]-*")):
        export_variant(trace, "shell")
        export_variant(trace, "openclaw")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
