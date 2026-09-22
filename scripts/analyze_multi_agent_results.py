#!/usr/bin/env python3
"""Build experiment-level cross-agent/workload summaries and plots."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any


def load(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, round((len(ordered) - 1) * fraction))]


def prom_values(path: Path) -> list[float]:
    payload = load(path, {})
    if not isinstance(payload, dict):
        return []
    data = payload.get("data")
    if not isinstance(data, dict):
        return []
    return [
        float(value)
        for series in data.get("result") or []
        if isinstance(series, dict)
        for _, value in series.get("values") or []
        if value not in ("NaN", "Inf", "+Inf", "-Inf")
    ]


def cgroup_stats(path: Path) -> dict[str, float | int | None]:
    """Summarize the run-local cgroup-v2 sampler, not a namespace aggregate."""
    try:
        rows = [
            (float(row["epoch_ns"]) / 1e9, float(row["cpu_usage_usec"]), float(row["memory_bytes"]))
            for row in csv.DictReader(path.open(encoding="utf-8"))
        ]
    except (OSError, KeyError, TypeError, ValueError):
        return {"samples": 0, "cpu_peak_cores": None, "cpu_mean_cores": None, "memory_peak_mib": None, "memory_mean_mib": None}
    if len(rows) < 2:
        return {"samples": len(rows), "cpu_peak_cores": None, "cpu_mean_cores": None, "memory_peak_mib": None, "memory_mean_mib": None}
    cpu = []
    for previous, current in zip(rows, rows[1:]):
        delta_s = current[0] - previous[0]
        delta_cpu = current[1] - previous[1]
        if delta_s > 0 and delta_cpu >= 0:
            cpu.append(delta_cpu / (delta_s * 1_000_000))
    memory = [row[2] / 1024 / 1024 for row in rows]
    return {
        "samples": len(rows),
        "cpu_peak_cores": max(cpu) if cpu else None,
        "cpu_mean_cores": statistics.mean(cpu) if cpu else None,
        "memory_peak_mib": max(memory),
        "memory_mean_mib": statistics.mean(memory),
    }


def _raw_trace_parts(trace: Path, agent: str) -> list[dict[str, Any]]:
    """Convert Jaeger spans into wall-clock stage intervals for parallel plots."""
    raw = load(trace / "data/shell/traces/raw_traces.json", [])
    if isinstance(raw, dict):
        raw = raw.get("data") or raw.get("traces") or []
    rows: list[dict[str, Any]] = []
    for payload in raw:
        spans = payload.get("spans") or []
        models = sorted((span for span in spans if span.get("operationName") == "openclaw.model.call"), key=lambda s: s.get("startTime") or 0)
        # `openclaw.tool.execution` is a wrapper span in this OpenClaw build
        # and can contain the whole response/dispatch wait.  When the nested
        # `openclaw.exec` span exists, it is the actual sandbox execution
        # interval and must be the only purple OpenShell interval plotted.
        exec_spans = [span for span in spans if span.get("operationName") == "openclaw.exec"]
        tools = sorted(exec_spans or [span for span in spans if span.get("operationName") == "openclaw.tool.execution"], key=lambda s: s.get("startTime") or 0)
        harness = next((span for span in spans if span.get("operationName") == "openclaw.harness.run"), None)
        if not models:
            continue
        harness_start = float((harness or {}).get("startTime") or models[0].get("startTime") or 0) / 1000.0
        previous_end = harness_start
        for turn, model in enumerate(models):
            model_start = float(model.get("startTime") or 0) / 1000.0
            model_duration = float(model.get("duration") or 0) / 1000.0
            model_end = model_start + model_duration
            next_model_start = float(models[turn + 1].get("startTime")) / 1000.0 if turn + 1 < len(models) else None
            following = [
                span for span in tools
                if float(span.get("startTime") or 0) / 1000.0 >= model_end
                and (next_model_start is None or float(span.get("startTime") or 0) / 1000.0 < next_model_start)
            ]
            if model_start > previous_end:
                rows.append({"agent": agent, "trace": trace.name, "turn": turn, "stage": "context/turn boundary", "start_ms": previous_end, "duration_ms": model_start - previous_end})
            rows.append({"agent": agent, "trace": trace.name, "turn": turn, "stage": "model call", "start_ms": model_start, "duration_ms": model_duration})
            first_tool_start = float(following[0].get("startTime") or 0) / 1000.0 if following else next_model_start
            if first_tool_start is not None and first_tool_start > model_end:
                rows.append({"agent": agent, "trace": trace.name, "turn": turn, "stage": "response processing", "start_ms": model_end, "duration_ms": first_tool_start - model_end})
            for tool in following:
                tool_start = float(tool.get("startTime") or 0) / 1000.0
                rows.append({"agent": agent, "trace": trace.name, "turn": turn, "stage": "OpenShell execution", "start_ms": tool_start, "duration_ms": float(tool.get("duration") or 0) / 1000.0})
            previous_end = max((float(tool.get("startTime") or 0) / 1000.0 + float(tool.get("duration") or 0) / 1000.0 for tool in following), default=model_end)
    return rows


def plot_parallel_timing(trace_paths: list[Path], out: Path, summary_rows: list[dict[str, Any]]) -> None:
    import matplotlib.pyplot as plt
    from matplotlib.patches import Patch

    intervals: list[dict[str, Any]] = []
    for trace in trace_paths:
        metadata = load(trace / "trace.json", {})
        intervals.extend(_raw_trace_parts(trace, str(metadata.get("agent") or trace.name)))
    if not intervals:
        return
    origin = min(float(row["start_ms"]) for row in intervals)
    for row in intervals:
        row["relative_s"] = (float(row["start_ms"]) - origin) / 1000.0
        row["duration_s"] = float(row["duration_ms"]) / 1000.0
    fields = ["agent", "trace", "turn", "stage", "start_ms", "duration_ms", "relative_s", "duration_s"]
    with (out / "parallel_step_timing.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(intervals)

    lanes = sorted({str(row["agent"]) for row in intervals})
    lane_index = {agent: index for index, agent in enumerate(lanes)}
    colors = {"context/turn boundary": "#2563EB", "model call": "#94A3B8", "response processing": "#059669", "OpenShell execution": "#7C3AED"}
    fig, ax = plt.subplots(figsize=(16, max(4.5, len(lanes) * 1.8)))
    for row in intervals:
        y = lane_index[str(row["agent"])]
        ax.barh(y, row["duration_s"], left=row["relative_s"], height=0.42, color=colors.get(str(row["stage"]), "#64748B"), edgecolor="white", linewidth=0.5)
        if str(row["stage"]) == "model call" and row["duration_s"] > 0.2:
            ax.text(row["relative_s"] + row["duration_s"] / 2, y, f"T{row['turn']}", ha="center", va="center", fontsize=7, color="#0F172A")
    ax.set_yticks(range(len(lanes)), lanes)
    ax.invert_yaxis()
    ax.set_xlabel("Seconds from first measured agent event")
    ax.set_title("Parallel multi-agent execution timeline: per-step model → response → OpenShell")
    ax.grid(axis="x", alpha=0.25)
    ax.legend(handles=[Patch(color=color, label=label) for label, color in colors.items()], loc="upper right")
    fig.tight_layout()
    fig.savefig(out / "parallel_step_timeline.png", dpi=160)
    plt.close(fig)

    # A normalized per-step comparison complements the wall-clock Gantt view.
    fig, axes = plt.subplots(2, 1, figsize=(15, 9), sharex=False)
    for trace in trace_paths:
        metadata = load(trace / "trace.json", {})
        agent = str(metadata.get("agent") or trace.name)
        source = trace / "data/shell/profile-v2/diagrams/per_turn_efficiency.csv"
        if not source.exists():
            continue
        with source.open(encoding="utf-8") as handle:
            data = list(csv.DictReader(handle))
        turns = [int(row["turn"]) for row in data]
        axes[0].plot(turns, [float(row["openclaw_observed_overhead_ms"]) for row in data], marker=".", label=f"{agent} OpenClaw")
        axes[0].plot(turns, [float(row["openshell_exec_ms"]) for row in data], marker=".", linestyle="--", label=f"{agent} OpenShell")
        axes[1].plot(turns, [float(row["openshell_share_pct"]) for row in data], marker=".", label=agent)
    axes[0].set_ylabel("Milliseconds")
    axes[0].set_title("Parallel agents: normalized per-step overhead")
    axes[1].set_xlabel("Turn")
    axes[1].set_ylabel("OpenShell share [%]")
    axes[1].set_title("Parallel agents: OpenShell share of the observed non-model path")
    for axis in axes:
        axis.grid(alpha=0.25)
        axis.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(out / "parallel_step_latency.png", dpi=160)
    plt.close(fig)


def _load_cgroup_series(path: Path) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
    """Return (CPU cores, memory MiB) samples from one run-local cgroup."""
    try:
        rows = [
            (
                float(row["epoch_ns"]) / 1e9,
                float(row["cpu_usage_usec"]),
                float(row["memory_bytes"]) / 1024 / 1024,
            )
            for row in csv.DictReader(path.open(encoding="utf-8"))
        ]
    except (OSError, KeyError, TypeError, ValueError):
        return [], []
    if len(rows) < 2:
        return [], [(timestamp, memory) for timestamp, _, memory in rows]
    cpu: list[tuple[float, float]] = []
    for previous, current in zip(rows, rows[1:]):
        delta_s = current[0] - previous[0]
        delta_cpu = current[1] - previous[1]
        if delta_s > 0 and delta_cpu >= 0:
            cpu.append(((previous[0] + current[0]) / 2, delta_cpu / (delta_s * 1_000_000)))
    memory = [(timestamp, value) for timestamp, _, value in rows]
    return cpu, memory


def plot_parallel_resources(trace_paths: list[Path], out: Path) -> None:
    """Plot run-local OpenClaw and sandbox resources on a shared time axis."""
    import matplotlib.pyplot as plt

    series: list[tuple[str, list[tuple[float, float]], list[tuple[float, float]]]] = []
    for trace in trace_paths:
        metadata = load(trace / "trace.json", {})
        agent = str(metadata.get("agent") or trace.name)
        cgroup = trace / "data/shell/prometheus/cgroup"
        openclaw_cpu, openclaw_memory = _load_cgroup_series(cgroup / "openclaw_gateway.csv")
        sandbox_cpu, sandbox_memory = _load_cgroup_series(cgroup / "openshell_sandbox.csv")
        if openclaw_cpu or openclaw_memory or sandbox_cpu or sandbox_memory:
            series.append((agent, (openclaw_cpu, sandbox_cpu), (openclaw_memory, sandbox_memory)))
    if not series:
        return

    all_times = [
        timestamp
        for _, cpu_series, memory_series in series
        for component in (*cpu_series, *memory_series)
        for timestamp, _ in component
    ]
    origin = min(all_times)
    colors = ["#2563EB", "#EA580C", "#16A34A", "#9333EA"]
    fig, axes = plt.subplots(2, 1, figsize=(16, 8), sharex=True)
    for index, (agent, cpu_series, memory_series) in enumerate(series):
        color = colors[index % len(colors)]
        for component_index, component in enumerate(cpu_series):
            if component:
                axes[0].plot(
                    [(timestamp - origin) for timestamp, _ in component],
                    [value for _, value in component],
                    color=color,
                    linestyle="-" if component_index == 0 else "--",
                    linewidth=1.4,
                    label=f"{agent} {'OpenClaw' if component_index == 0 else 'OpenShell sandbox'}",
                )
        for component_index, component in enumerate(memory_series):
            if component:
                axes[1].plot(
                    [(timestamp - origin) for timestamp, _ in component],
                    [value for _, value in component],
                    color=color,
                    linestyle="-" if component_index == 0 else "--",
                    linewidth=1.4,
                    label=f"{agent} {'OpenClaw' if component_index == 0 else 'OpenShell sandbox'}",
                )
    axes[0].set_ylabel("CPU cores")
    axes[1].set_ylabel("Memory MiB")
    axes[1].set_xlabel("Seconds from first measured agent event")
    axes[0].set_title("Parallel multi-agent resource timeline: CPU")
    axes[1].set_title("Parallel multi-agent resource timeline: memory")
    for axis in axes:
        axis.grid(alpha=0.25)
        axis.legend(fontsize=8, ncol=2, loc="upper right")
    fig.suptitle("Parallel multi-agent resources: OpenClaw and OpenShell sandbox")
    fig.tight_layout()
    fig.savefig(out / "parallel_resource_timeline.png", dpi=160)
    plt.close(fig)


def summarize_trace(trace: Path) -> dict[str, Any]:
    shell = trace / "data/shell"
    timing = load(shell / "traces/timing_segments.json", [])
    tools = load(shell / "traces/per_tool_segments.json", [])
    metadata = load(trace / "trace.json", {})
    events = [json.loads(line) for line in (trace / "data/appworld/events.jsonl").read_text(encoding="utf-8").splitlines()] if (trace / "data/appworld/events.jsonl").exists() else []
    prom = shell / "prometheus"
    cgroup = prom / "cgroup"
    cgroup_openclaw = cgroup_stats(cgroup / "openclaw_gateway.csv")
    cgroup_sandbox = cgroup_stats(cgroup / "openshell_sandbox.csv")
    tool_ms = [float(row.get("duration_ms") or 0) for row in tools]
    app_ms = [float(row.get("elapsed_ms") or 0) for row in events]
    session = str(metadata.get("session_id") or trace.name.split("__", 1)[0].split("-", 1)[-1])
    workload = trace.name.split("__", 1)[1] if "__" in trace.name else "unknown"
    return {
        "trace": trace.name,
        "session_id": session,
        "workload": workload,
        "agent": metadata.get("agent") or "aharush-openclaw-agent-1",
        "turns": len(timing),
        "tool_calls": len(tools),
        "tool_errors": sum(row.get("error_category") not in (None, "") for row in tools),
        "harness_ms": max((float(row.get("harness_total_ms") or 0) for row in timing), default=0),
        "model_total_ms": sum(float(row.get("model_call_total_ms") or 0) for row in timing),
        "context_total_ms": sum(float(row.get("context_assembly_ms") or 0) for row in timing),
        "response_total_ms": sum(float(row.get("response_processing_ms") or 0) for row in timing),
        "sandbox_total_ms": sum(float(row.get("sandbox_exec_ms") or 0) for row in timing),
        "tool_p50_ms": statistics.median(tool_ms) if tool_ms else None,
        "tool_p95_ms": percentile(tool_ms, 0.95),
        "appworld_mean_ms": statistics.mean(app_ms) if app_ms else None,
        "appworld_p95_ms": percentile(app_ms, 0.95),
        "openclaw_cpu_peak_cores": max(prom_values(prom / "cpu_openclaw.json"), default=None),
        "openclaw_memory_peak_mib": (max(prom_values(prom / "memory_openclaw.json"), default=0) / 1024 / 1024) or None,
        "openshell_cpu_peak_cores": max(prom_values(prom / "cpu_openshell.json"), default=None),
        "openshell_memory_peak_mib": (max(prom_values(prom / "memory_openshell.json"), default=0) / 1024 / 1024) or None,
        "sandbox_cpu_peak_cores": max(prom_values(prom / "cpu_sandbox.json"), default=None),
        "sandbox_memory_peak_mib": (max(prom_values(prom / "memory_sandbox.json"), default=0) / 1024 / 1024) or None,
        "cgroup_openclaw_cpu_peak_cores": cgroup_openclaw["cpu_peak_cores"],
        "cgroup_openclaw_memory_peak_mib": cgroup_openclaw["memory_peak_mib"],
        "cgroup_openshell_sandbox_cpu_peak_cores": cgroup_sandbox["cpu_peak_cores"],
        "cgroup_openshell_sandbox_memory_peak_mib": cgroup_sandbox["memory_peak_mib"],
        "cgroup_openclaw_samples": cgroup_openclaw["samples"],
        "cgroup_openshell_sandbox_samples": cgroup_sandbox["samples"],
    }


def mean_present(rows: list[dict[str, Any]], key: str) -> float:
    values = [float(row[key]) for row in rows if row.get(key) is not None]
    return statistics.mean(values) if values else 0.0


def plot_groups(rows: list[dict[str, Any]], out: Path) -> None:
    import matplotlib.pyplot as plt

    by_agent: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_workload: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_agent[str(row["agent"])].append(row)
        by_workload[str(row["workload"])].append(row)

    agents = sorted(by_agent)
    x = list(range(len(agents)))
    fig, ax = plt.subplots(figsize=(max(8, len(agents) * 1.3), 5))
    width = 0.35
    ax.bar([i - width / 2 for i in x], [mean_present(by_agent[a], "context_total_ms") + mean_present(by_agent[a], "response_total_ms") for a in agents], width, label="Agent processing")
    ax.bar([i + width / 2 for i in x], [mean_present(by_agent[a], "sandbox_total_ms") for a in agents], width, label="OpenShell execution")
    ax.set_xticks(x, agents, rotation=20, ha="right")
    ax.set_ylabel("Mean milliseconds per trace")
    ax.set_title("Cross-agent orchestration latency")
    ax.grid(axis="y", alpha=0.25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(out / "cross_agent_latency.png", dpi=160)
    plt.close(fig)

    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    openclaw_cpu = [mean_present(by_agent[a], "cgroup_openclaw_cpu_peak_cores") for a in agents]
    sandbox_cpu = [mean_present(by_agent[a], "cgroup_openshell_sandbox_cpu_peak_cores") for a in agents]
    gateway_cpu = [mean_present(by_agent[a], "openshell_cpu_peak_cores") for a in agents]
    axes[0].bar(x, openclaw_cpu, label="OpenClaw cgroup")
    axes[0].bar(x, gateway_cpu, bottom=openclaw_cpu, label="OpenShell gateway")
    axes[0].bar(x, sandbox_cpu, bottom=[a + b for a, b in zip(openclaw_cpu, gateway_cpu)], label="OpenShell sandbox")
    axes[0].set_ylabel("Peak CPU cores")
    openclaw_memory = [mean_present(by_agent[a], "cgroup_openclaw_memory_peak_mib") for a in agents]
    sandbox_memory = [mean_present(by_agent[a], "cgroup_openshell_sandbox_memory_peak_mib") for a in agents]
    gateway_memory = [mean_present(by_agent[a], "openshell_memory_peak_mib") for a in agents]
    axes[1].bar(x, openclaw_memory, label="OpenClaw cgroup")
    axes[1].bar(x, gateway_memory, bottom=openclaw_memory, label="OpenShell gateway")
    axes[1].bar(x, sandbox_memory, bottom=[a + b for a, b in zip(openclaw_memory, gateway_memory)], label="OpenShell sandbox")
    axes[1].set_ylabel("Peak memory MiB")
    for axis in axes:
        axis.set_xticks(x, agents, rotation=20, ha="right")
        axis.grid(axis="y", alpha=0.25)
        axis.legend()
    fig.suptitle("Cross-agent resource peaks")
    fig.tight_layout()
    fig.savefig(out / "cross_agent_resources.png", dpi=160)
    plt.close(fig)

    workloads = sorted(by_workload)
    fig, ax = plt.subplots(figsize=(max(8, len(workloads) * 1.5), 5))
    ax.bar(workloads, [mean_present(by_workload[w], "harness_ms") for w in workloads])
    ax.set_ylabel("Mean harness latency ms")
    ax.set_title("Multi-turn latency by workload")
    ax.tick_params(axis="x", rotation=20)
    ax.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(out / "workload_latency.png", dpi=160)
    plt.close(fig)

    # Aggregate the per-turn decomposition emitted by profiler-v2.
    efficiency_rows: list[dict[str, Any]] = []
    for trace in sorted({str(row["trace"]) for row in rows}):
        source = out.parent / "traces" / trace / "data/shell/profile-v2/diagrams/per_turn_efficiency.csv"
        if not source.exists():
            continue
        with source.open(encoding="utf-8") as handle:
            for item in csv.DictReader(handle):
                efficiency_rows.append({"trace": trace, **item})
    if efficiency_rows:
        with (out / "per_turn_efficiency.csv").open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(efficiency_rows[0]))
            writer.writeheader()
            writer.writerows(efficiency_rows)
        fig, axes = plt.subplots(2, 1, figsize=(15, 9), sharex=False)
        for trace in sorted({str(row["trace"]) for row in efficiency_rows}):
            series = [row for row in efficiency_rows if row["trace"] == trace]
            turns = [int(row["turn"]) for row in series]
            axes[0].plot(turns, [float(row["openclaw_observed_overhead_ms"]) for row in series], marker=".", label=f"{trace} OpenClaw")
            axes[0].plot(turns, [float(row["openshell_exec_ms"]) for row in series], marker=".", linestyle="--", label=f"{trace} OpenShell")
            axes[1].plot(turns, [float(row["openshell_share_pct"]) for row in series], marker=".", label=trace)
        axes[0].set_ylabel("Milliseconds")
        axes[0].set_title("Per-turn OpenClaw overhead versus OpenShell execution")
        axes[1].set_xlabel("Turn")
        axes[1].set_ylabel("OpenShell share of non-model path [%]")
        axes[1].set_title("OpenShell efficiency share by turn")
        for axis in axes:
            axis.grid(alpha=0.25)
            axis.legend(fontsize=8)
        fig.tight_layout()
        fig.savefig(out / "cross_agent_per_turn_efficiency.png", dpi=160)
        plt.close(fig)

    trace_paths = [out.parent / "traces" / str(row["trace"]) for row in rows]
    plot_parallel_timing(trace_paths, out, rows)
    plot_parallel_resources(trace_paths, out)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment", type=Path, required=True)
    args = parser.parse_args()
    rows = [summarize_trace(path) for path in sorted((args.experiment / "traces").glob("*")) if path.is_dir()]
    out = args.experiment / "analysis"
    out.mkdir(parents=True, exist_ok=True)
    (out / "cross_agent_summary.json").write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")
    if rows:
        with (out / "cross_agent_summary.csv").open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        plot_groups(rows, out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
