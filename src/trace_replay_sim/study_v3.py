"""Research-oriented multi-cycle study built from persisted replay artifacts.

This is intentionally separate from the legacy A-E and profiler-v2 reports.
It preserves evidence provenance and marks unsupported dimensions unavailable.
"""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path
from typing import Any


def _load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _stats(values: list[float]) -> dict[str, Any]:
    if not values:
        return {"n": 0, "mean": None, "p50": None, "p95": None, "min": None, "max": None}
    ordered = sorted(values)
    return {
        "n": len(values),
        "mean": round(statistics.mean(values), 3),
        "p50": round(statistics.median(values), 3),
        "p95": round(ordered[min(len(ordered) - 1, int(0.95 * (len(ordered) - 1)))], 3),
        "min": round(min(values), 3),
        "max": round(max(values), 3),
    }


def _records(run: Path, layer: str) -> list[dict[str, Any]]:
    layer_root = run / layer
    if not layer_root.exists():
        layer_root = run / "data" / layer
    timing_path = layer_root / "traces" / "timing_segments.json"
    profile_path = layer_root / "profile-v2" / "per_turn.json"
    if not timing_path.exists():
        return []
    timing = _load(timing_path)
    edges_path = layer_root / "mock_edges.jsonl"
    expected_session = run.name.split("__", 1)[0].split("-", 1)[-1]
    edge_trace_ids = set()
    edge_input_tokens: list[int] = []
    if edges_path.exists():
        for line in edges_path.read_text(encoding="utf-8").splitlines():
            try:
                edge = json.loads(line)
            except json.JSONDecodeError:
                continue
            if str(edge.get("session_id") or "") == expected_session:
                trace_id = str(edge.get("trace_id") or "")
                if trace_id:
                    edge_trace_ids.add(trace_id)
                edge_input_tokens.append(int(edge.get("input_tokens") or 0))
    requests_path = layer_root / "driver_requests.jsonl"
    request_trace_ids = set()
    if requests_path.exists():
        for line in requests_path.read_text(encoding="utf-8").splitlines():
            try:
                trace_id = str(json.loads(line).get("trace_id") or "")
            except json.JSONDecodeError:
                continue
            if trace_id:
                request_trace_ids.add(trace_id)
    selected_trace_ids = edge_trace_ids or request_trace_ids
    timing_by_trace: dict[str, list[dict[str, Any]]] = {}
    for segment in timing:
        timing_by_trace.setdefault(str(segment.get("trace_id") or ""), []).append(segment)
    if edge_input_tokens:
        for trace_id, segments in timing_by_trace.items():
            ordered = sorted(segments, key=lambda segment: segment.get("turn_idx", 0))
            inputs = [int(segment.get("input_tokens") or 0) for segment in ordered]
            if inputs == edge_input_tokens:
                selected_trace_ids = {trace_id}
                break
    if selected_trace_ids:
        timing = [segment for segment in timing if str(segment.get("trace_id") or "") in selected_trace_ids]
    profile = _load(profile_path) if profile_path.exists() else []
    profile_by_turn = {(row.get("trace_id"), row.get("turn")): row for row in profile}
    records: list[dict[str, Any]] = []
    for segment in timing:
        row = profile_by_turn.get((segment.get("trace_id"), segment.get("turn_idx")), {})
        records.append({
            "task_id": run.name,
            "trace_id": segment.get("trace_id"),
            "layer": layer,
            "step_index": segment.get("turn_idx"),
            "model_call_ms": segment.get("model_call_total_ms"),
            "context_assembly_ms": segment.get("context_assembly_ms"),
            "response_processing_ms": segment.get("response_processing_ms"),
            "sandbox_init_ms": segment.get("sandbox_init_ms"),
            "sandbox_exec_ms": segment.get("sandbox_exec_ms"),
            "prompt_chars": segment.get("prompt_chars"),
            "input_tokens": segment.get("input_tokens"),
            "output_tokens": segment.get("output_tokens"),
            "tool_output_chars": None,
            "parallel_tool_count": row.get("tool_count"),
            "agent_overhead_ms": sum(
                float(segment.get(key) or 0.0)
                for key in ("context_assembly_ms", "response_processing_ms", "sandbox_init_ms", "sandbox_exec_ms")
            ),
            "cumulative_agent_overhead_ms": None,
            "total_step_latency_ms": (segment.get("model_call_total_ms") or 0.0)
            + sum(float(segment.get(key) or 0.0) for key in ("context_assembly_ms", "response_processing_ms", "sandbox_init_ms", "sandbox_exec_ms")),
            "measurement_source": "jaeger_timing_segments+profiler_v2",
            "tool_output_measurement": "unavailable",
        })
    return records


def _plot(records: list[dict[str, Any]], out: Path) -> list[str]:
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        return []
    out.mkdir(parents=True, exist_ok=True)
    files: list[str] = []
    title = "OpenClaw And OpenShell Multi-Turn Profiling Analysis"
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in records:
        grouped.setdefault((row["task_id"], row["layer"]), []).append(row)
    for rows in grouped.values():
        rows.sort(key=lambda row: row["step_index"] or 0)
        cumulative = 0.0
        for row in rows:
            cumulative += row["agent_overhead_ms"]
            row["cumulative_agent_overhead_ms"] = cumulative

    fig, ax = plt.subplots(figsize=(15, 8))
    for (task, layer), rows in grouped.items():
        x = [row["step_index"] for row in rows]
        y = [row["total_step_latency_ms"] for row in rows]
        ax.plot(x, y, marker="o", label=f"{task} [{layer}]")
    ax.set_title(f"{title} - Total Per-Step Latency [ms]")
    ax.set_xlabel("Internal model/tool step")
    ax.set_ylabel("Milliseconds")
    ax.grid(alpha=0.25)
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(out / "study_v3_step_latency.png", dpi=160)
    plt.close(fig)
    files.append("study_v3_step_latency.png")

    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fields = [
        ("context_assembly_ms", "Context assembly [ms]"),
        ("response_processing_ms", "Response processing [ms]"),
        ("prompt_chars", "Prompt characters"),
        ("sandbox_exec_ms", "Sandbox execution [ms]"),
    ]
    for axis, (field, label) in zip(axes.flat, fields):
        for (task, layer), rows in grouped.items():
            rows.sort(key=lambda row: row["step_index"] or 0)
            points = [(row["step_index"], row[field]) for row in rows if row.get(field) is not None]
            if points:
                axis.plot([x for x, _ in points], [y for _, y in points], marker=".", label=f"{task} [{layer}]")
        axis.set_title(label)
        axis.set_xlabel("Internal step")
        axis.grid(alpha=0.25)
    axes[0, 0].legend(fontsize=7)
    fig.suptitle(f"{title} - Context, Processing, History, and OpenShell")
    fig.tight_layout()
    fig.savefig(out / "study_v3_bucket_scaling.png", dpi=160)
    plt.close(fig)
    files.append("study_v3_bucket_scaling.png")

    # Cumulative orchestration cost and matched shell-vs-plain overhead.
    fig, ax = plt.subplots(figsize=(15, 8))
    for (task, layer), rows in grouped.items():
        ax.plot([row["step_index"] for row in rows], [row["cumulative_agent_overhead_ms"] for row in rows], marker=".", label=f"{task} [{layer}]")
    ax.set_title(f"{title} - Cumulative Agent Overhead [ms]")
    ax.set_xlabel("Internal model/tool step")
    ax.set_ylabel("Cumulative orchestration overhead [ms]")
    ax.grid(alpha=0.25)
    ax.legend(fontsize=7)
    fig.tight_layout()
    fig.savefig(out / "study_v3_cumulative_overhead.png", dpi=160)
    plt.close(fig)
    files.append("study_v3_cumulative_overhead.png")

    matched: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for (task, layer), rows in grouped.items():
        matched.setdefault(task, {})[layer] = rows
    per_trace_dir = out / "per-trace"
    per_trace_dir.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(15, 8))
    matched_tasks = 0
    for task, layers in matched.items():
        if "openclaw" not in layers or "shell" not in layers:
            continue
        plain_steps = sorted(row["step_index"] for row in layers["openclaw"])
        shell_steps = sorted(row["step_index"] for row in layers["shell"])
        if plain_steps != shell_steps:
            continue
        matched_tasks += 1
        shell_by_step = {row["step_index"]: row for row in layers["shell"]}
        plain_by_step = {row["step_index"]: row for row in layers["openclaw"]}
        points = [
            (step, shell_by_step[step]["agent_overhead_ms"] - plain_by_step[step]["agent_overhead_ms"])
            for step in sorted(set(shell_by_step) & set(plain_by_step))
        ]
        if points:
            ax.plot([step for step, _ in points], [value for _, value in points], marker=".", label=task)
            trace_fig, trace_ax = plt.subplots(figsize=(10, 5.5))
            trace_ax.axhline(0, color="black", linewidth=0.8)
            trace_ax.plot([step for step, _ in points], [value for _, value in points], marker="o", color="#2563EB")
            trace_ax.set_title(f"{title} - OpenShell Overhead: {task}")
            trace_ax.set_xlabel("Internal model/tool step")
            trace_ax.set_ylabel("Overhead by tool exec in OpenShell [ms]")
            trace_ax.grid(alpha=0.25)
            trace_fig.tight_layout()
            trace_fig.savefig(per_trace_dir / f"{task}_openshell_overhead.png", dpi=160)
            plt.close(trace_fig)
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_title(f"{title} - Matched OpenShell Incremental Overhead [ms]")
    ax.set_xlabel("Internal model/tool step")
    ax.set_ylabel("Shell overhead vs plain OpenClaw [ms]")
    ax.grid(alpha=0.25)
    if matched_tasks:
        ax.legend(fontsize=7)
    fig.tight_layout()
    fig.savefig(out / "study_v3_matched_openshell_overhead.png", dpi=160)
    plt.close(fig)
    files.append("study_v3_matched_openshell_overhead.png")

    # A representative execution timeline, including model generation explicitly.
    representative = next(iter(grouped.values()), [])
    if representative:
        representative = sorted(representative, key=lambda row: row["step_index"] or 0)
        fig, ax = plt.subplots(figsize=(15, 7))
        left = [0.0] * len(representative)
        for key, label, color in (
            ("context_assembly_ms", "Context assembly", "#2563eb"),
            ("model_call_ms", "Model call / generation", "#94a3b8"),
            ("response_processing_ms", "Response processing", "#10b981"),
            ("sandbox_exec_ms", "Sandbox execution", "#8b5cf6"),
        ):
            values = [row.get(key) or 0.0 for row in representative]
            ax.barh(range(len(representative)), values, 0.65, left=left, color=color, label=label)
            left = [a + b for a, b in zip(left, values)]
        ax.set_yticks(range(len(representative)), [f"T{row['step_index']}" for row in representative])
        ax.set_xlabel("Milliseconds")
        ax.set_title(f"{title} - Representative Step Timeline [ms]")
        ax.legend(loc="lower right")
        ax.grid(axis="x", alpha=0.25)
        fig.tight_layout()
        fig.savefig(out / "study_v3_representative_timeline.png", dpi=160)
        plt.close(fig)
        files.append("study_v3_representative_timeline.png")

    return files


def study(root: Path, out: Path) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    for run in sorted(root.iterdir()):
        if not run.is_dir():
            continue
        for layer in ("openclaw", "shell"):
            records.extend(_records(run, layer))
    out.mkdir(parents=True, exist_ok=True)
    (out / "cycle_records.json").write_text(json.dumps(records, indent=2), encoding="utf-8")
    summary = {
        "schema": "openclaw-multicycle-study-v3",
        "root": str(root),
        "records": len(records),
        "tasks": len({row["task_id"] for row in records}),
        "layers": sorted({row["layer"] for row in records}),
        "coverage": {
            field: sum(row.get(field) is not None for row in records)
            for field in ("context_assembly_ms", "response_processing_ms", "sandbox_init_ms", "sandbox_exec_ms", "prompt_chars", "tool_output_chars")
        },
        "matched_tasks": len({
            task for task in {row["task_id"] for row in records}
            if len([row for row in records if row["task_id"] == task and row["layer"] == "openclaw"]) > 0
            and len([row for row in records if row["task_id"] == task and row["layer"] == "shell"]) > 0
            and len([row for row in records if row["task_id"] == task and row["layer"] == "openclaw"])
            == len([row for row in records if row["task_id"] == task and row["layer"] == "shell"])
        }),
        "limitations": [
            "Controlled backend decisions are replayed; they are not regenerated by a real model.",
            "OpenClaw context.assembled is run-level in the deployed release; per-step context uses the trace-derived boundary in timing_segments.",
            "Tool output size is unavailable unless content capture is enabled; no zero is substituted.",
            "Matched plain-OpenClaw comparison is used only when both layers have the same internal step count; mismatched runs are excluded.",
            "Parallel tool critical paths require raw span overlap; summed durations are not used as a critical path.",
        ],
        "plots": _plot(records, out / "diagrams"),
    }
    (out / "study_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    lines = [
        "# OpenClaw And OpenShell Multi-Turn Profiling Analysis",
        "",
        "This is controlled replay analysis. OpenClaw executes the internal model/tool loop; the scripted backend supplies recorded decisions and the mapped tools supply controlled results.",
        "",
        "## Research Questions",
        "",
        "- OpenShell overhead is represented by sandbox initialization and `openclaw.exec`/tool timing per internal step.",
        "- Context and response-processing records are persisted per step with provenance; missing tool-output size and per-turn native context spans remain unavailable rather than being fabricated.",
        "",
        "## Limitations",
        "",
        *[f"- {item}" for item in summary["limitations"]],
        "",
        f"Records: {summary['records']}; tasks: {summary['tasks']}; layers: {summary['layers']}",
    ]
    (out / "ANALYSIS.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(study(args.root, args.out), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
