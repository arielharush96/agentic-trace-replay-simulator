#!/usr/bin/env python3
"""Render one corpus's matched OpenShell overhead plot."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def load(path: Path) -> list[dict]:
    return json.loads(path.read_text(encoding="utf-8"))


def current_trace(layer: Path, session_id: str) -> str | None:
    edges = [
        json.loads(line)
        for line in (layer / "mock_edges.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    session_edges = [edge for edge in edges if edge.get("session_id") == session_id]
    edge_trace_ids = {
        str(edge.get("trace_id"))
        for edge in session_edges
        if edge.get("trace_id")
    }
    timing = load(layer / "traces/timing_segments.json")
    by_trace: dict[str, list[dict]] = {}
    for row in timing:
        by_trace.setdefault(str(row.get("trace_id") or ""), []).append(row)
    # Trace IDs are the authoritative join key.  Token sequences are only a
    # compatibility fallback because two sessions can have identical inputs.
    matching_ids = sorted(edge_trace_ids & set(by_trace))
    if len(matching_ids) == 1:
        return matching_ids[0]
    if len(matching_ids) > 1:
        return None

    inputs = [int(edge.get("input_tokens") or 0) for edge in session_edges]
    candidates: list[str] = []
    for trace_id, rows in by_trace.items():
        ordered = sorted(rows, key=lambda row: row.get("turn_idx", 0))
        trace_inputs = [int(row.get("input_tokens") or 0) for row in ordered]
        for start in range(0, len(inputs) - len(trace_inputs) + 1):
            window = inputs[start:start + len(trace_inputs)]
            if trace_inputs == window:
                candidates.append(trace_id)
                break
        if trace_inputs == inputs:
            candidates.append(trace_id)
    return candidates[0] if len(set(candidates)) == 1 else None


def overhead(row: dict) -> float:
    return sum(
        float(row.get(key) or 0.0)
        for key in (
            "context_assembly_bucket_ms",
            "response_processing_bucket_ms",
            "sandbox_init_ms",
            "sandbox_execution_bucket_ms",
        )
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", type=Path, required=True)
    args = parser.parse_args()

    session_id = args.task.name.split("__", 1)[0].split("-", 1)[-1]
    shell_layer = args.task / "data/shell"
    plain_layer = args.task / "data/openclaw"
    shell_trace = current_trace(shell_layer, session_id)
    plain_trace = current_trace(plain_layer, session_id)
    shell = load(shell_layer / "profile-v2/per_turn.json")
    plain = load(plain_layer / "profile-v2/per_turn.json")
    if shell_trace:
        shell = [row for row in shell if row.get("trace_id") == shell_trace]
    if plain_trace:
        plain = [row for row in plain if row.get("trace_id") == plain_trace]
    shell = sorted(shell, key=lambda row: row.get("turn", 0))
    plain = sorted(plain, key=lambda row: row.get("turn", 0))
    shell_steps = [row.get("turn") for row in shell]
    plain_steps = [row.get("turn") for row in plain]
    out = args.task / "plots-analysis/openshell_overhead_by_execution.png"
    if shell_steps != plain_steps:
        out.with_suffix(".not_available.txt").write_text(
            "Matched OpenShell overhead is unavailable: shell and plain OpenClaw step sequences differ.\n",
            encoding="utf-8",
        )
        return 1

    import matplotlib.pyplot as plt

    values = [overhead(shell_row) - overhead(plain_row) for shell_row, plain_row in zip(shell, plain)]
    fig, axis = plt.subplots(figsize=(10, 5.5))
    axis.axhline(0, color="#64748B", linewidth=0.9)
    axis.plot(shell_steps, values, marker="o", color="#2563EB", linewidth=1.8)
    axis.set_title("OpenShell Overhead by Execution")
    axis.set_xlabel("Internal model/tool step")
    axis.set_ylabel("Overhead by tool exec in OpenShell [ms]")
    axis.grid(alpha=0.25)
    fig.tight_layout()
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=180)
    plt.close(fig)
    print(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
