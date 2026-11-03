#!/usr/bin/env python3
"""Build experiment-level cross-agent/workload summaries and plots."""

from __future__ import annotations

import argparse
import csv
import json
import os
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any

# The analyzer is also invoked directly on macOS and in worker processes. Use
# a non-interactive backend so plot generation never aborts on GUI state.
os.environ.setdefault("MPLBACKEND", "Agg")


REQUIRED_PER_AGENT_PLOTS = (
    "latency_waterfall.png",
    "openshell_execution_latency_ms.png",
    "resource_memory_mib.png",
    "resource_cpu_cores.png",
    "resource_timeseries.png",
)


def load(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def workload_label(rows: list[dict[str, Any]]) -> str:
    labels = {
        "personal-assistant": "Personal Assistant",
        "software-engineering-qa": "Software Engineering & QA",
        "research-deep-search": "Research & Deep Search",
        "customer-support-ops": "Customer Support & Ops",
    }
    values = sorted({str(row.get("workload") or "unknown") for row in rows})
    return " · ".join(labels.get(value, value.replace("-", " ").title()) for value in values)


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, round((len(ordered) - 1) * fraction))]


def prom_values(path: Path) -> list[float]:
    payload = load(path, {})
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return []
    return [
        float(value)
        for series in data.get("result") or []
        for _, value in series.get("values") or []
        if value not in ("NaN", "Inf", "+Inf", "-Inf")
    ]


def cgroup_stats(path: Path) -> dict[str, float | int | None]:
    """Summarize the run-local cgroup-v2 sampler, not a namespace aggregate."""
    try:
        rows = []
        with path.open(encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                try:
                    epoch = float(row["epoch_ns"])
                    cpu = float(row["cpu_usage_usec"])
                    memory = float(row["memory_bytes"])
                except (KeyError, TypeError, ValueError):
                    # A partially interleaved sampler row must not invalidate
                    # the valid measurements around it.
                    continue
                if epoch > 0 and cpu >= 0 and memory >= 0:
                    rows.append((epoch / 1e9, cpu, memory))
    except (OSError, UnicodeDecodeError, csv.Error):
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
        # `openclaw.tool.execution` is a wrapper in builds that also emit a
        # nested `openclaw.exec`. Merge the two per call: keep the nested exec
        # where it exists, and retain a wrapper only when no nested exec covers
        # that call. Selecting one operation globally dropped most tool calls
        # from mixed-version traces.
        exec_spans = sorted(
            [span for span in spans if span.get("operationName") == "openclaw.exec"],
            key=lambda s: s.get("startTime") or 0,
        )
        wrappers = sorted(
            [span for span in spans if span.get("operationName") == "openclaw.tool.execution"],
            key=lambda s: s.get("startTime") or 0,
        )

        def is_error(span: dict[str, Any]) -> bool:
            tags = {str(item.get("key")): item.get("value") for item in span.get("tags") or []}
            status = str(tags.get("otel.status_code") or "").upper()
            error_tag = tags.get("error")
            return (
                status == "ERROR"
                or str(tags.get("openclaw.errorCategory") or "") not in ("", "None", "null")
                or error_tag is True
                or str(error_tag).lower() == "true"
            )

        def bounds(span: dict[str, Any]) -> tuple[float, float]:
            start = float(span.get("startTime") or 0) / 1000.0
            return start, start + float(span.get("duration") or 0) / 1000.0

        successful_execs = [span for span in exec_spans if not is_error(span)]
        failed_actions = [span for span in exec_spans if is_error(span)]
        tools = list(successful_execs)
        for wrapper in wrappers:
            wrapper_start, wrapper_end = bounds(wrapper)
            nested = any(
                max(wrapper_start, exec_start) <= min(wrapper_end, exec_end) + 1.0
                for exec_start, exec_end in (bounds(item) for item in exec_spans)
            )
            if not nested:
                if is_error(wrapper):
                    failed_actions.append(wrapper)
                else:
                    tools.append(wrapper)
        tools.sort(key=lambda s: s.get("startTime") or 0)
        failed_actions.sort(key=lambda s: s.get("startTime") or 0)

        first_model_start = float(models[0].get("startTime") or 0) / 1000.0 if models else None
        init_spans = [
            span for span in spans
            if first_model_start is not None
            and float(span.get("startTime") or 0) / 1000.0 < first_model_start
            and (
                span.get("operationName") in {
                    "openclaw.sandbox.init",
                    "openclaw.sandbox.initialize",
                    "openshell.sandbox.create",
                    "openshell.sandbox.ready",
                }
                or (
                    "sandbox" in str(span.get("operationName") or "").lower()
                    and any(token in str(span.get("operationName") or "").lower() for token in ("init", "create", "ready", "startup"))
                )
            )
        ]
        lifecycle = load(trace / "data/shell/sandbox_lifecycle.json", {})
        lifecycle_ready = str(lifecycle.get("status") or "") == "ready"
        lifecycle_created = float(lifecycle.get("prewarm_start_unix") or 0.0) * 1000.0
        lifecycle_ready_at = float(lifecycle.get("prewarm_end_unix") or 0.0) * 1000.0
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
            following_successes = [
                span for span in tools
                if float(span.get("startTime") or 0) / 1000.0 >= model_end
                and (next_model_start is None or float(span.get("startTime") or 0) / 1000.0 < next_model_start)
            ]
            following_failures = [
                span for span in failed_actions
                if float(span.get("startTime") or 0) / 1000.0 >= model_end
                and (next_model_start is None or float(span.get("startTime") or 0) / 1000.0 < next_model_start)
            ]
            following = sorted(following_successes + following_failures, key=lambda s: s.get("startTime") or 0)
            if turn == 0:
                for init in init_spans:
                    init_start, init_end = bounds(init)
                    if init_end > init_start:
                        rows.append({"agent": agent, "trace": trace.name, "turn": 0, "stage": "sandbox initialization", "start_ms": init_start, "duration_ms": init_end - init_start})
                if not init_spans and lifecycle_ready and lifecycle_ready_at > lifecycle_created:
                    rows.append({
                        "agent": agent,
                        "trace": trace.name,
                        "turn": 0,
                        "stage": "sandbox initialization",
                        "start_ms": lifecycle_created,
                        "duration_ms": lifecycle_ready_at - lifecycle_created,
                    })
            if model_start > previous_end:
                rows.append({"agent": agent, "trace": trace.name, "turn": turn, "stage": "context/turn boundary", "start_ms": previous_end, "duration_ms": model_start - previous_end})
            rows.append({"agent": agent, "trace": trace.name, "turn": turn, "stage": "model call", "start_ms": model_start, "duration_ms": model_duration})
            first_tool_start = float(following[0].get("startTime") or 0) / 1000.0 if following else next_model_start
            # Post-LLM work belongs to the transition into the next agent
            # step. Keeping it off turn 0 is the key semantic invariant: the
            # first step has only sandbox initialization (when directly
            # measured) and context assembly before the first model call.
            display_turn = turn + 1 if turn + 1 < len(models) else turn
            if first_tool_start is not None and first_tool_start > model_end:
                rows.append({"agent": agent, "trace": trace.name, "turn": display_turn, "stage": "response processing", "start_ms": model_end, "duration_ms": first_tool_start - model_end})
            for tool in following_successes:
                tool_start = float(tool.get("startTime") or 0) / 1000.0
                rows.append({"agent": agent, "trace": trace.name, "turn": display_turn, "stage": "OpenShell execution", "start_ms": tool_start, "duration_ms": float(tool.get("duration") or 0) / 1000.0})
            for failure in following_failures:
                failure_start = float(failure.get("startTime") or 0) / 1000.0
                rows.append({"agent": agent, "trace": trace.name, "turn": display_turn, "stage": "tool failure / rejected execution", "start_ms": failure_start, "duration_ms": float(failure.get("duration") or 0) / 1000.0})
            previous_end = max((float(tool.get("startTime") or 0) / 1000.0 + float(tool.get("duration") or 0) / 1000.0 for tool in following), default=model_end)
    return rows


def _write_phase_integrity(trace_paths: list[Path], out: Path) -> None:
    """Persist a machine-checkable audit of the turn-0 phase invariant."""
    traces: list[dict[str, Any]] = []
    violations: list[str] = []
    for trace in trace_paths:
        source = trace / "data/shell/profile-v2/diagrams/per_turn_efficiency.csv"
        if not source.exists():
            continue
        with source.open(encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        if not rows:
            continue
        first = rows[0]
        forbidden = {
            "response_processing_ms": float(first.get("response_processing_ms") or 0.0),
            "tool_dispatch_overhead_ms": float(first.get("tool_dispatch_overhead_ms") or 0.0),
            "openshell_exec_ms": float(first.get("openshell_exec_ms") or 0.0),
            "tool_failure_ms": float(first.get("tool_failure_ms") or 0.0),
        }
        trace_violations = [name for name, value in forbidden.items() if value > 0.0]
        if trace_violations:
            violations.append(f"{trace.name}: {', '.join(trace_violations)}")
        traces.append({
            "trace": trace.name,
            "turns": len(rows),
            "turn_0": {
                "sandbox_init_ms": float(first.get("sandbox_init_ms") or 0.0),
                "context_assembly_ms": float(first.get("prompt_boundary_ms") or 0.0),
                **forbidden,
            },
            "turn_0_invariant": not trace_violations,
        })
    payload = {
        "schema": "agentic.trace-replay.phase-integrity.v1",
        "turn_0_definition": "initial user request -> sandbox init if directly measured -> context assembly -> first LLM handoff",
        "forbidden_turn_0_phases": ["response_processing_ms", "tool_dispatch_overhead_ms", "openshell_exec_ms", "tool_failure_ms"],
        "passed": not violations,
        "violations": violations,
        "traces": traces,
    }
    (out / "phase_integrity.json").write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _load_cgroup_phase_series(path: Path) -> list[dict[str, float]]:
    """Load cgroup samples and derive instantaneous CPU for phase joins."""
    try:
        raw: list[tuple[float, float, float]] = []
        with path.open(encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                try:
                    epoch_s = float(row["epoch_ns"]) / 1e9
                    cpu_usec = float(row["cpu_usage_usec"])
                    memory_mib = float(row["memory_bytes"]) / 1024.0 / 1024.0
                except (KeyError, TypeError, ValueError):
                    continue
                if epoch_s > 0 and cpu_usec >= 0 and memory_mib >= 0:
                    raw.append((epoch_s, cpu_usec, memory_mib))
    except (OSError, UnicodeDecodeError, csv.Error):
        return []

    series: list[dict[str, float]] = []
    for previous, current in zip(raw, raw[1:]):
        delta_s = current[0] - previous[0]
        delta_cpu = current[1] - previous[1]
        if delta_s <= 0 or delta_cpu < 0:
            continue
        series.append({
            "epoch_ms": current[0] * 1000.0,
            "cpu_cores": delta_cpu / (delta_s * 1_000_000.0),
            "memory_mib": current[2],
        })
    return series


def write_phase_resource_attribution(
    trace_paths: list[Path],
    intervals: list[dict[str, Any]],
    out: Path,
) -> None:
    """Join run-local cgroup samples to the exact measured phase windows.

    Prometheus remains a useful independent cross-check, but this file uses
    the trace's own cgroup-v2 samples and Jaeger wall-clock intervals.  It
    therefore reports component resources for context, model, response,
    sandbox, and failure phases without mixing samples from other agents.
    """
    by_trace: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for interval in intervals:
        by_trace[str(interval.get("trace"))].append(interval)

    rows: list[dict[str, Any]] = []
    for trace in trace_paths:
        trace_intervals = by_trace.get(trace.name, [])
        if not trace_intervals:
            continue
        component_series = {
            "openclaw": _load_cgroup_phase_series(
                trace / "data/shell/prometheus/cgroup/openclaw_gateway.csv"
            ),
            "openshell_sandbox": _load_cgroup_phase_series(
                trace / "data/shell/prometheus/cgroup/openshell_sandbox.csv"
            ),
        }
        for interval in trace_intervals:
            start_ms = float(interval.get("start_ms") or 0.0)
            end_ms = start_ms + float(interval.get("duration_ms") or 0.0)
            for component, series in component_series.items():
                samples = [
                    sample for sample in series
                    if start_ms <= sample["epoch_ms"] <= end_ms
                ]
                cpu = [sample["cpu_cores"] for sample in samples]
                memory = [sample["memory_mib"] for sample in samples]
                rows.append({
                    "agent": interval.get("agent"),
                    "trace": interval.get("trace"),
                    "turn": interval.get("turn"),
                    "stage": interval.get("stage"),
                    "component": component,
                    "start_ms": round(start_ms, 3),
                    "duration_ms": round(float(interval.get("duration_ms") or 0.0), 3),
                    "sample_count": len(samples),
                    "cpu_mean_cores": round(statistics.mean(cpu), 6) if cpu else None,
                    "cpu_peak_cores": round(max(cpu), 6) if cpu else None,
                    "memory_mean_mib": round(statistics.mean(memory), 3) if memory else None,
                    "memory_peak_mib": round(max(memory), 3) if memory else None,
                    "attribution_status": "measured" if samples else "no_overlapping_samples",
                })

    target = out / "phase_resource_attribution.csv"
    fields = [
        "agent", "trace", "turn", "stage", "component", "start_ms", "duration_ms",
        "sample_count", "cpu_mean_cores", "cpu_peak_cores", "memory_mean_mib",
        "memory_peak_mib", "attribution_status",
    ]
    with target.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    measured = [row for row in rows if row["attribution_status"] == "measured"]
    if not measured:
        return
    import matplotlib.pyplot as plt

    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in measured:
        groups[(str(row["stage"]), str(row["component"]))].append(row)
    labels = sorted({stage for stage, _ in groups})
    components = ["openclaw", "openshell_sandbox"]
    fig, axes = plt.subplots(1, 2, figsize=(14, max(4.5, len(labels) * 0.55)), sharey=True)
    colors = {"openclaw": "#2563EB", "openshell_sandbox": "#7C3AED"}
    y = list(range(len(labels)))
    for component in components:
        cpu_values = [
            statistics.median(
                row["cpu_mean_cores"] for row in groups.get((stage, component), [])
                if row["cpu_mean_cores"] is not None
            ) if any(row["cpu_mean_cores"] is not None for row in groups.get((stage, component), [])) else 0.0
            for stage in labels
        ]
        memory_values = [
            statistics.median(
                row["memory_mean_mib"] for row in groups.get((stage, component), [])
                if row["memory_mean_mib"] is not None
            ) if any(row["memory_mean_mib"] is not None for row in groups.get((stage, component), [])) else 0.0
            for stage in labels
        ]
        offset = -0.18 if component == "openclaw" else 0.18
        axes[0].barh([value + offset for value in y], cpu_values, height=0.32, color=colors[component], label=component)
        axes[1].barh([value + offset for value in y], memory_values, height=0.32, color=colors[component], label=component)
    axes[0].set_xlabel("Mean CPU cores")
    axes[1].set_xlabel("Mean memory MiB")
    axes[0].set_yticks(y, labels)
    axes[0].set_title("CPU by measured phase")
    axes[1].set_title("Memory by measured phase")
    for axis in axes:
        axis.grid(axis="x", alpha=0.22)
        axis.legend(frameon=False)
    fig.suptitle("Cgroup resource attribution to measured execution phases", y=0.995)
    fig.tight_layout()
    fig.savefig(out / "phase_resource_summary.png", dpi=180, bbox_inches="tight")
    plt.close(fig)


def _normalized_efficiency(trace: Path) -> list[dict[str, Any]]:
    """Build turn-boundary phase rows from the profiler's raw per-turn rows.

    Older result bundles contain a model-centric CSV where turn 0 includes the
    first post-LLM response/tool cycle. Rebuilding this small derived table is
    deliberately non-destructive to the raw OTEL and mock-edge records and
    makes the phase ownership explicit for every downstream plot.
    """
    raw_rows = load(trace / "data/shell/profile-v2/per_turn.json", [])
    if not isinstance(raw_rows, list):
        return []
    lifecycle = load(trace / "data/shell/sandbox_lifecycle.json", {})
    lifecycle_mode = str(lifecycle.get("provisioning_mode") or "")
    warm_reuse_modes = {"warm_reused", "cold_pool_fill", "warm_reused_without_prewarm"}
    raw_rows = sorted(raw_rows, key=lambda row: int(row.get("turn") or 0))
    normalized: list[dict[str, Any]] = []
    for index, row in enumerate(raw_rows):
        previous = raw_rows[index - 1] if index > 0 else {}
        context = float(row.get("prompt_boundary_ms") or row.get("context_assembly_window_ms") or 0.0)
        response = float(previous.get("response_processing_full_ms") or 0.0) if index > 0 else 0.0
        dispatch = float(previous.get("tool_dispatch_overhead_ms") or 0.0) if index > 0 else 0.0
        sandbox_exec = float(previous.get("exec_ms") or 0.0) if index > 0 else 0.0
        tool_failure = float(previous.get("tool_failure_ms") or 0.0) if index > 0 else 0.0
        raw_sandbox_init = float(
            row.get("pre_model_sandbox_access_ms") or row.get("sandbox_init_ms") or 0.0
        )
        lifecycle_sandbox_init = float(lifecycle.get("sandbox_init_ms") or 0.0)
        if index != 0:
            sandbox_init = 0.0
        elif lifecycle_mode in warm_reuse_modes:
            # The sandbox was ready before the measured trace. Keep the
            # one-time pool-fill request out of turn 0. Any separately
            # observed warm reuse is reported as sandbox_reuse_ms.
            sandbox_init = 0.0
        else:
            sandbox_init = lifecycle_sandbox_init or raw_sandbox_init
        sandbox_reuse = float(row.get("sandbox_reuse_ms") or 0.0) if index == 0 else 0.0
        openclaw = context + response + dispatch
        non_model = sandbox_init + sandbox_reuse + openclaw + sandbox_exec + tool_failure
        normalized.append({
            "turn": int(row.get("turn") or index),
            "model_call_ms": float(row.get("model_call_ms") or 0.0),
            "sandbox_init_ms": sandbox_init,
            "sandbox_reuse_ms": sandbox_reuse,
            "prompt_boundary_ms": context,
            "response_processing_ms": response,
            "response_processing_raw_ms": float(row.get("response_processing_full_ms") or 0.0),
            "tool_dispatch_overhead_ms": dispatch,
            "openclaw_observed_overhead_ms": openclaw,
            "openshell_exec_ms": sandbox_exec,
            "tool_failure_ms": tool_failure,
            "non_model_path_ms": non_model,
            "openshell_share_pct": 100.0 * sandbox_exec / non_model if non_model else 0.0,
        })
    return normalized


def _write_normalized_efficiency(trace: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    target = trace / "data/shell/profile-v2/diagrams/per_turn_efficiency.csv"
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def plot_parallel_timing(trace_paths: list[Path], out: Path, summary_rows: list[dict[str, Any]]) -> None:
    import matplotlib.pyplot as plt
    from matplotlib.patches import Patch

    intervals: list[dict[str, Any]] = []
    summary_agents = {str(row.get("trace")): str(row.get("agent")) for row in summary_rows}
    for trace in trace_paths:
        metadata = load(trace / "trace.json", {})
        agent = str(metadata.get("agent") or summary_agents.get(trace.name) or trace.name)
        intervals.extend(_raw_trace_parts(trace, agent))
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
    _write_phase_integrity(trace_paths, out)
    write_phase_resource_attribution(trace_paths, intervals, out)

    lanes = sorted({str(row["agent"]) for row in intervals})
    lane_index = {agent: index for index, agent in enumerate(lanes)}
    colors = {"sandbox initialization": "#A16207", "context/turn boundary": "#2563EB", "model call": "#94A3B8", "response processing": "#0F766E", "OpenShell execution": "#7C3AED"}
    colors["tool failure / rejected execution"] = "#DC2626"
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

    plot_executive_summary(intervals, summary_rows, out)
    plot_executive_dashboard(intervals, summary_rows, out, trace_paths)
    plot_parallel_wall_clock(intervals, summary_rows, out)


def plot_executive_summary(intervals: list[dict[str, Any]], summary_rows: list[dict[str, Any]], out: Path) -> None:
    """Create one meeting-ready view of scope, phase cost, and concurrency."""
    import matplotlib.pyplot as plt
    from matplotlib.patches import Patch

    if not intervals or not summary_rows:
        return

    colors = {
        "model call": "#CBD5E1",
        "sandbox initialization": "#A16207",
        "OpenShell execution": "#7C3AED",
        "response processing": "#0F766E",
        "context/turn boundary": "#2563EB",
        "tool failure / rejected execution": "#DC2626",
    }
    labels = {
        "model call": "LLM simulator replay",
        "sandbox initialization": "Sandbox initialization (direct span only)",
        "OpenShell execution": "OpenShell + tool path",
        "response processing": "Response processing",
        "context/turn boundary": "Context / turn boundary",
        "tool failure / rejected execution": "Tool failure / rejected execution",
    }
    ink = "#0F172A"
    muted = "#64748B"
    grid = "#E2E8F0"
    background = "#F8FAFC"

    agents = sorted({str(row["agent"]) for row in intervals})
    agent_rows = {str(row["agent"]): row for row in summary_rows}
    grouped: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for row in intervals:
        grouped[(str(row["agent"]), int(row["turn"]))].append(row)

    fig = plt.figure(figsize=(15, 9.2), facecolor=background)
    grid_spec = fig.add_gridspec(100, 1)
    title_ax = fig.add_subplot(grid_spec[:16, 0])
    bars_ax = fig.add_subplot(grid_spec[20:52, 0])
    timeline_ax = fig.add_subplot(grid_spec[60:94, 0])

    for axis in (title_ax, bars_ax, timeline_ax):
        axis.set_facecolor(background)

    title_ax.axis("off")
    title_ax.text(0.0, 0.83, "Multi-turn agent execution, measured end to end", fontsize=22, fontweight="bold", color=ink, va="top")
    total_turns = sum(int(row.get("turns") or 0) for row in summary_rows)
    total_tools = sum(int(row.get("tool_calls") or 0) for row in summary_rows)
    title_ax.text(0.0, 0.53, f"Preliminary {len(agents)}-agent replay across {workload_label(summary_rows)}", fontsize=12.5, color=muted, va="top")
    title_ax.text(0.0, 0.17, "OpenClaw orchestration + OpenShell tool path measured on a shared wall-clock; model responses replayed by a controlled simulator", fontsize=9.5, color=muted, va="top")
    title_ax.text(1.0, 0.78, f"{len(agents)} AGENTS  ·  {len(summary_rows)} TRACES", fontsize=10, fontweight="bold", color="#5B21B6", ha="right", va="top")
    title_ax.text(1.0, 0.48, f"{total_turns} TURNS  ·  {total_tools} TOOLS", fontsize=10, color=muted, ha="right", va="top")

    bars_ax.set_title("Measured non-model path (simulator duration excluded from this scale)", loc="left", fontsize=12, fontweight="bold", color=ink, pad=10)
    phase_totals: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for item in intervals:
        phase_totals[str(item["agent"])][str(item["stage"])] += float(item["duration_s"])
    max_total = max(sum(value for phase, value in phase_totals[agent].items() if phase != "model call") for agent in agents)
    bars_ax.set_xlim(0, max_total * 1.2)
    bars_ax.set_ylim(-0.6, len(agents) - 0.4)
    bars_ax.set_yticks(range(len(agents)), [f"Agent {index + 1}" for index in range(len(agents))], color=ink, fontsize=10)
    bars_ax.invert_yaxis()
    bars_ax.set_xlabel("")
    bars_ax.grid(axis="x", color=grid, linewidth=0.8)
    bars_ax.set_axisbelow(True)
    for spine in bars_ax.spines.values():
        spine.set_visible(False)

    for index, agent in enumerate(agents):
        values = {
            "sandbox initialization": phase_totals[agent]["sandbox initialization"],
            "OpenShell execution": phase_totals[agent]["OpenShell execution"],
            "response processing": phase_totals[agent]["response processing"],
            "context/turn boundary": phase_totals[agent]["context/turn boundary"],
            "tool failure / rejected execution": phase_totals[agent]["tool failure / rejected execution"],
        }
        left = 0.0
        for phase in ("sandbox initialization", "context/turn boundary", "response processing", "OpenShell execution", "tool failure / rejected execution"):
            width = values[phase]
            bars_ax.barh(index, width, left=left, height=0.48, color=colors[phase], edgecolor=background, linewidth=1)
            if width >= 5:
                bars_ax.text(left + width / 2, index, f"{width:.1f}s", ha="center", va="center", fontsize=9, color=ink, fontweight="bold")
            left += width
        bars_ax.text(left + 0.8, index, f"{left:.1f}s total", ha="left", va="center", fontsize=10, color=ink, fontweight="bold")

    bars_ax.legend(
        handles=[Patch(facecolor=colors[key], label=labels[key]) for key in ("sandbox initialization", "context/turn boundary", "response processing", "OpenShell execution", "tool failure / rejected execution")],
        loc="lower right",
        bbox_to_anchor=(1.0, 1.16),
        ncol=4,
        frameon=False,
        fontsize=8.5,
        handlelength=1.2,
        columnspacing=1.2,
    )

    timeline_ax.set_title("Turns remain serial within each agent, while agents progress concurrently", loc="left", fontsize=12, fontweight="bold", color=ink, pad=10)
    max_relative = max(float(item["relative_s"]) + float(item["duration_s"]) for item in intervals)
    timeline_ax.set_xlim(0, max_relative * 1.04)
    timeline_ax.set_ylim(-0.6, len(agents) - 0.4)
    timeline_ax.set_yticks(range(len(agents)), [f"Agent {index + 1}" for index in range(len(agents))], color=ink, fontsize=10)
    timeline_ax.invert_yaxis()
    timeline_ax.set_xlabel("Shared wall-clock time from first measured event (seconds)", color=muted, fontsize=9)
    timeline_ax.grid(axis="x", color=grid, linewidth=0.8)
    timeline_ax.set_axisbelow(True)
    for spine in timeline_ax.spines.values():
        spine.set_visible(False)

    for index, agent in enumerate(agents):
        turn_groups = sorted((turn, rows) for (group_agent, turn), rows in grouped.items() if group_agent == agent)
        for turn, rows in turn_groups:
            start = min(float(item["relative_s"]) for item in rows)
            end = max(float(item["relative_s"]) + float(item["duration_s"]) for item in rows)
            timeline_ax.barh(index, max(end - start, 0.05), left=start, height=0.34, color="#E2E8F0", edgecolor=background, linewidth=0.8)
            shell_rows = [item for item in rows if item["stage"] == "OpenShell execution"]
            for shell in shell_rows:
                timeline_ax.barh(index, float(shell["duration_s"]), left=float(shell["relative_s"]), height=0.34, color=colors["OpenShell execution"], edgecolor=background, linewidth=0.6)
            for failure in [item for item in rows if item["stage"] == "tool failure / rejected execution"]:
                timeline_ax.barh(index, float(failure["duration_s"]), left=float(failure["relative_s"]), height=0.34, color=colors["tool failure / rejected execution"], edgecolor=background, linewidth=0.6)
            if turn in (0, len(turn_groups) - 1) or turn % 5 == 0:
                timeline_ax.text(start + 0.2, index, f"T{turn}", fontsize=7.5, color=ink, va="center")

    timeline_ax.text(max_relative * 0.98, len(agents) - 0.28, "gray = turn window   purple = successful OpenShell execution   red = rejected/failed tool call", ha="right", va="bottom", fontsize=8.5, color=muted)
    fig.text(0.01, 0.012, "Source: profiler-v2 per-turn and Jaeger-derived timing · simulator time is excluded from the primary bars · backend events and sandbox cgroups validated", fontsize=8, color=muted)
    fig.subplots_adjust(left=0.08, right=0.98, top=0.94, bottom=0.08, hspace=0.55)
    fig.savefig(out / "executive_multi_agent_summary.png", dpi=220, facecolor=background, bbox_inches="tight")
    plt.close(fig)


def plot_executive_dashboard(
    intervals: list[dict[str, Any]],
    summary_rows: list[dict[str, Any]],
    out: Path,
    trace_paths: list[Path],
) -> None:
    """Create a compact dashboard view for an executive review."""
    import matplotlib.pyplot as plt
    from matplotlib.patches import FancyBboxPatch, Patch

    if not intervals or not summary_rows:
        return

    background = "#F5F3F4"
    card = "#FFFFFF"
    ink = "#111827"
    muted = "#6B7280"
    grid = "#E5E7EB"
    colors = {
        "context/turn boundary": "#2F6FED",
        "response processing": "#5D6F9E",
        "sandbox reuse": "#F59E0B",
        "OpenShell execution": "#1E8E5A",
        "sandbox initialization": "#86A51E",
        "tool failure / rejected execution": "#DC2626",
    }
    labels = {
        "context/turn boundary": "Context assembly",
        "response processing": "Response processing",
        "sandbox reuse": "Warm sandbox reuse",
        "OpenShell execution": "Tool execution",
        "sandbox initialization": "Sandbox init (cold)",
        "tool failure / rejected execution": "Tool failure / rejected",
    }
    agents = sorted({str(row["agent"]) for row in summary_rows})
    agent_rows = {str(row["agent"]): row for row in summary_rows}
    trace_by_name = {trace.name: trace for trace in trace_paths}
    grouped: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    totals: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    profile_fields = {
        "context/turn boundary": "prompt_boundary_ms",
        "response processing": "response_processing_ms",
        "sandbox reuse": "sandbox_reuse_ms",
        "OpenShell execution": "openshell_exec_ms",
        "sandbox initialization": "sandbox_init_ms",
        "tool failure / rejected execution": "tool_failure_ms",
    }
    # Raw OTEL exports contain overlapping fragments. Use the validated
    # profiler CSV for one-row-per-turn aggregate bars; retain raw intervals
    # only for the wall-clock views.
    for summary in summary_rows:
        agent = str(summary["agent"])
        trace = trace_by_name.get(str(summary["trace"]))
        source = (
            trace / "data/shell/profile-v2/diagrams/per_turn_efficiency.csv"
            if trace is not None
            else None
        )
        if source is None or not source.exists():
            continue
        with source.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                for stage, field in profile_fields.items():
                    totals[agent][stage] += float(row.get(field) or 0.0) / 1000.0

    # Safe fallback for older result directories without profiler CSVs.
    if not totals:
        for row in intervals:
            grouped[(str(row["agent"]), int(row["turn"]))].append(row)
            if row["stage"] != "model call":
                totals[str(row["agent"])][str(row["stage"])] += float(row["duration_s"])

    max_runtime = 0.0
    for agent in agents:
        agent_intervals = [row for row in intervals if str(row["agent"]) == agent]
        if agent_intervals:
            start = min(float(row["relative_s"]) for row in agent_intervals)
            end = max(float(row["relative_s"]) + float(row["duration_s"]) for row in agent_intervals)
            max_runtime = max(max_runtime, end - start)
    total_turns = sum(int(row.get("turns") or 0) for row in summary_rows)
    total_tools = sum(int(row.get("tool_calls") or 0) for row in summary_rows)
    tool_errors = sum(int(row.get("tool_errors") or 0) for row in summary_rows)
    successful_tools = sum(int(row.get("successful_tool_calls") or 0) for row in summary_rows)
    backend_events = sum(int(row.get("backend_event_count") or 0) for row in summary_rows)
    sandbox_samples = sum(int(row.get("cgroup_openshell_sandbox_samples") or 0) for row in summary_rows)
    measured_stages = [stage for stage in totals for value in totals[stage].values() if value > 0]
    quality_ok = bool(summary_rows) and all(
        int(row.get("tool_errors") or 0) == 0
        # Conversational traces have no tool stage by design; their backend
        # and OpenShell aggregates are not applicable and are excluded.
        and (bool(row.get("conversational_trace")) or int(row.get("backend_event_count") or 0) > 0)
        and int(row.get("cgroup_openshell_sandbox_samples") or 0) > 0
        and row.get("per_agent_plot_status") == "complete"
        and not row.get("missing_per_agent_plots")
        and row.get("functional_conformance_status") in {"PASS", "PASS_WITH_WARNINGS"}
        for row in summary_rows
    )
    quality_label = "PASS" if quality_ok else "FAIL"
    quality_color = "#15803D" if quality_ok else "#B91C1C"

    fig, rows_ax = plt.subplots(
        figsize=(15, max(8, len(agents) * 0.46)),
        facecolor=background,
    )
    rows_ax.set_facecolor(card)
    warm_count = sum(
        str(row.get("sandbox_provisioning_mode") or "") == "warm_reused"
        for row in summary_rows
    )
    cold_count = sum(
        str(row.get("sandbox_provisioning_mode") or "") == "cold_fallback"
        for row in summary_rows
    )
    conversational_count = sum(
        bool(row.get("conversational_trace"))
        for row in summary_rows
    )
    fig.suptitle(
        "Measured non-model execution path by agent",
        fontsize=18,
        fontweight="bold",
        color=ink,
        y=0.99,
    )
    warm_prewarm = [
        float(row.get("sandbox_prewarm_ms") or 0.0)
        for row in summary_rows
        if str(row.get("sandbox_provisioning_mode") or "") == "warm_reused"
    ]
    warm_reuse_values = [
        float(row["sandbox_reuse_ms"])
        for row in summary_rows
        if row.get("sandbox_reuse_ms") is not None
        and float(row["sandbox_reuse_ms"]) > 0.0
    ]
    prewarm_mean = sum(warm_prewarm) / len(warm_prewarm) if warm_prewarm else 0.0
    warm_reuse_label = (
        f"warm reuse p50 {sorted(warm_reuse_values)[len(warm_reuse_values) // 2]:.0f}ms"
        if warm_reuse_values
        else "warm reuse not separately observed"
    )
    # Reserve a right-side annotation column so step/reuse labels stay inside the figure.
    rows_ax.set_xlim(0, max(max(sum(totals[a].values()) for a in agents), 1.0) * 1.30)
    rows_ax.set_ylim(-0.8, len(agents) - 0.2)
    row_labels = []
    workload_names = {
        "software engineering qa": "Software Engineering & QA",
        "personal assistant": "Personal Assistant",
        "research deep search": "Research & Deep Search",
        "customer support ops": "Customer Support & Ops",
    }
    for index, agent in enumerate(agents):
        workload = str(agent_rows.get(agent, {}).get("workload") or "unknown").replace("-", " ")
        row_labels.append(f"Agent {index + 1} ({workload_names.get(workload, workload.title())})")
    rows_ax.set_yticks(range(len(agents)), row_labels, color=ink, fontsize=8.5)
    rows_ax.invert_yaxis()
    rows_ax.set_xlabel("Measured non-model latency (seconds; model replay excluded)", color=muted, fontsize=9)
    rows_ax.grid(axis="x", color=grid, linewidth=0.8)
    rows_ax.set_axisbelow(True)
    for spine in rows_ax.spines.values():
        spine.set_visible(False)
    for index, agent in enumerate(agents):
        left = 0.0
        for stage in ("context/turn boundary", "sandbox initialization", "sandbox reuse", "OpenShell execution", "response processing", "tool failure / rejected execution"):
            width = totals[agent][stage]
            rows_ax.barh(index, width, left=left, height=0.40, color=colors[stage], edgecolor=card, linewidth=1.0)
            label_threshold = 0.18 if stage == "sandbox initialization" else 1.0
            if width >= label_threshold:
                value_label = f"{width:.2f}s" if width < 1.0 else f"{width:.1f}s"
                rows_ax.text(left + width / 2, index, value_label, ha="center", va="center", fontsize=7.5, color="white", fontweight="bold")
            left += width
        turns = next((int(row.get("turns") or 0) for row in summary_rows if str(row["agent"]) == agent), 0)
        mode = str(agent_rows.get(agent, {}).get("sandbox_provisioning_mode") or "unknown")
        mode_label = {"warm_reused": "warm reuse", "cold_fallback": "cold fallback"}.get(mode, mode)
        reuse_ms = float(agent_rows.get(agent, {}).get("sandbox_reuse_ms") or 0.0)
        if mode == "warm_reused" and reuse_ms > 0.0:
            mode_label = f"warm reuse {reuse_ms / 1000.0:.2f}s"
        rows_ax.text(left + 0.25, index, f"{turns} steps · {mode_label}", ha="left", va="center", fontsize=8.5, color=ink, fontweight="bold")
    fig.legend(
        handles=[
            Patch(facecolor=colors[stage], label=labels[stage])
            for stage in (
                "context/turn boundary",
                "sandbox initialization",
                "sandbox reuse",
                "OpenShell execution",
                "response processing",
                "tool failure / rejected execution",
            )
        ],
        loc="upper center",
        bbox_to_anchor=(0.5, 0.91),
        ncol=3,
        frameon=False,
        fontsize=8.5,
    )
    fig.subplots_adjust(left=0.30, right=0.98, top=0.84, bottom=0.08, hspace=0.0)
    fig.savefig(out / "executive_multi_agent_dashboard.png", dpi=220, facecolor=background, bbox_inches="tight")
    plt.close(fig)


def plot_workload_p95_bars(
    efficiency_rows: list[dict[str, Any]],
    summary_rows: list[dict[str, Any]],
    out: Path,
) -> None:
    """P95 latency bar charts per workload for the four key phases."""
    import matplotlib.pyplot as plt

    if not efficiency_rows and not summary_rows:
        return

    trace_to_workload = {str(row["trace"]): str(row.get("workload") or "unknown") for row in summary_rows}

    def p95(values: list[float]) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        return ordered[min(int(len(ordered) * 0.95), len(ordered) - 1)]

    # Per-turn metrics from efficiency rows
    wl_response: dict[str, list[float]] = {}
    wl_exec: dict[str, list[float]] = {}
    for row in efficiency_rows:
        wl = trace_to_workload.get(str(row.get("trace", "")), "unknown")
        rp = row.get("response_processing_ms")
        if rp is not None and float(rp) > 0:
            wl_response.setdefault(wl, []).append(float(rp))
        ex = row.get("openshell_exec_ms")
        if ex is not None and float(ex) > 0:
            wl_exec.setdefault(wl, []).append(float(ex))

    # Context assembly: read per-turn data from per_turn.json (the efficiency
    # CSV does not carry context_assembly_ms; it lives in the profiler output)
    wl_context: dict[str, list[float]] = {}
    for row in summary_rows:
        wl = str(row.get("workload") or "unknown")
        trace_dir = out.parent / "traces" / str(row.get("trace", ""))
        per_turn_path = trace_dir / "data/shell/profile-v2/per_turn.json"
        try:
            per_turn = json.loads(per_turn_path.read_text(encoding="utf-8"))
            for turn_row in per_turn:
                ca = turn_row.get("context_assembly_ms")
                if ca is not None and float(ca) > 0:
                    wl_context.setdefault(wl, []).append(float(ca))
        except (OSError, json.JSONDecodeError):
            continue

    # Per-trace warm-pool acquisition from summary rows
    wl_warm_pool: dict[str, list[float]] = {}
    for row in summary_rows:
        wl = str(row.get("workload") or "unknown")
        wp = row.get("sandbox_request_to_first_action_ms")
        if wp is not None and float(wp) > 0:
            wl_warm_pool.setdefault(wl, []).append(float(wp))

    all_workloads = sorted(set(list(wl_response) + list(wl_context) + list(wl_exec) + list(wl_warm_pool)))
    if not all_workloads:
        return

    # Short labels
    label_map = {
        "software_engineering_qa": "SWE",
        "personal_assistant": "Personal\nAssistant",
        "research_deep_search": "Research &\nDeep Search",
        "customer_support_ops": "Customer\nSupport",
    }

    metrics = [
        (wl_response, "Response Processing", "#2563eb"),
        (wl_exec, "OpenShell Execution", "#7c3aed"),
        (wl_context, "Context Assembly", "#16a34a"),
        (wl_warm_pool, "Warm Pool Acquisition\n(request → first action)", "#ea580c"),
    ]

    background = "#F8FAFC"
    ink = "#0F172A"
    muted = "#475569"
    grid = "#E2E8F0"

    fig, axes = plt.subplots(2, 2, figsize=(14, 10), facecolor=background)
    for ax, (data, title, color) in zip(axes.flat, metrics):
        ax.set_facecolor("#FFFFFF")
        values = [p95(data.get(wl, [])) for wl in all_workloads]
        bars = ax.bar(
            range(len(all_workloads)),
            values,
            color=color,
            edgecolor="white",
            linewidth=1.5,
            width=0.6,
        )
        ax.set_xticks(range(len(all_workloads)))
        ax.set_xticklabels([label_map.get(wl, wl.replace("_", "\n")) for wl in all_workloads], fontsize=9, color=ink)
        ax.set_title(title, fontweight="bold", fontsize=12, color=ink, pad=10)
        ax.set_ylabel("P95 [ms]", fontsize=10, color=muted)
        ax.grid(axis="y", color=grid, alpha=0.6, linewidth=0.8)
        ax.set_axisbelow(True)
        for spine in ("top", "right"):
            ax.spines[spine].set_visible(False)
        for spine in ("left", "bottom"):
            ax.spines[spine].set_color(grid)
        for bar, val in zip(bars, values):
            if val > 0:
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    bar.get_height() * 1.01,
                    f"{val:.0f}",
                    ha="center",
                    va="bottom",
                    fontsize=10,
                    fontweight="bold",
                    color=ink,
                )

    fig.suptitle("P95 Latency by Workload", fontsize=16, fontweight="bold", color=ink, y=0.995)
    fig.text(0.5, 0.965, "Per-turn P95 (response, exec, context) · per-trace P95 (warm pool)", fontsize=10, color=muted, ha="center")
    fig.tight_layout(rect=(0, 0, 1, 0.94))
    path = out / "workload_p95_latency.png"
    fig.savefig(path, dpi=160, facecolor=background)
    plt.close(fig)


def plot_executive_per_turn(efficiency_rows: list[dict[str, Any]], summary_rows: list[dict[str, Any]], out: Path) -> None:
    """Create a clean per-turn view of measured non-model and OpenShell latency."""
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D
    from statistics import median

    if not efficiency_rows or not summary_rows:
        return

    trace_to_agent = {str(row["trace"]): str(row["agent"]) for row in summary_rows}
    workload_by_agent = {
        str(row["agent"]): str(row.get("workload") or "unknown")
        for row in summary_rows
    }
    workload_names = {
        "software-engineering-qa": "Software Engineering & QA",
        "personal-assistant": "Personal Assistant",
        "research-deep-search": "Research & Deep Search",
        "customer-support-ops": "Customer Support & Ops",
    }
    workloads = sorted({workload_by_agent.get(agent, "unknown") for agent in trace_to_agent.values()})
    palette_colors = ("#2563EB", "#059669", "#7C3AED", "#D97706")
    palette = {workload: palette_colors[index % len(palette_colors)] for index, workload in enumerate(workloads)}
    background = "#F8FAFC"
    ink = "#0F172A"
    muted = "#64748B"
    grid = "#E2E8F0"

    fig, axes = plt.subplots(2, 1, figsize=(14, 8), sharex=True, facecolor=background, gridspec_kw={"height_ratios": [1.15, 0.85]})
    for axis in axes:
        axis.set_facecolor(background)
        axis.grid(axis="y", color=grid, linewidth=0.8)
        axis.set_axisbelow(True)
        for spine in axis.spines.values():
            spine.set_visible(False)

    for workload in workloads:
        workload_rows = [
            row
            for row in efficiency_rows
            if workload_by_agent.get(trace_to_agent.get(str(row["trace"]), ""), "unknown") == workload
        ]
        by_turn: dict[int, list[dict[str, Any]]] = {}
        for row in workload_rows:
            by_turn.setdefault(int(row["turn"]), []).append(row)

        turns = sorted(by_turn)
        median_non_model = [median(float(row["non_model_path_ms"]) for row in by_turn[turn]) / 1000.0 for turn in turns]
        axes[0].plot(
            turns,
            median_non_model,
            color=palette[workload],
            linewidth=2.2,
            marker="o",
            markersize=3.5,
            label=workload_names.get(workload, workload.title()),
        )

        tool_turns = {
            turn: rows
            for turn, rows in by_turn.items()
            if any(float(row.get("openshell_exec_ms") or 0.0) > 0.0 for row in rows)
        }
        tool_turn_indexes = sorted(tool_turns)
        axes[0].plot(
            tool_turn_indexes,
            [median(float(row["openshell_exec_ms"]) for row in tool_turns[turn]) / 1000.0 for turn in tool_turn_indexes],
            color=palette[workload],
            linewidth=1.6,
            linestyle="--",
            marker="o",
            markersize=2.8,
            alpha=0.9,
        )
        axes[1].plot(
            tool_turn_indexes,
            [median(float(row["openshell_share_pct"]) for row in tool_turns[turn]) for turn in tool_turn_indexes],
            color=palette[workload],
            linewidth=2.2,
            marker="o",
            markersize=3.5,
        )

    axes[0].set_title("Median per-turn measured latency", loc="left", fontsize=13, fontweight="bold", color=ink, pad=12)
    axes[0].set_ylabel("Latency (seconds)", color=muted, fontsize=9)
    axes[0].set_ylim(bottom=0)
    axes[1].set_title("Median OpenShell share of the measured non-model path", loc="left", fontsize=13, fontweight="bold", color=ink, pad=12)
    axes[1].set_xlabel("Turn index", color=muted, fontsize=9)
    axes[1].set_ylabel("OpenShell share (%)", color=muted, fontsize=9)
    axes[1].set_ylim(0, max(35, max(float(row["openshell_share_pct"]) for row in efficiency_rows) * 1.12))
    workload_handles = [
        Line2D([0], [0], color=palette[workload], linewidth=2.2, label=workload_names.get(workload, workload.title()))
        for workload in workloads
    ]
    phase_handles = [
        Line2D([0], [0], color=ink, linewidth=2.2, label="Median non-model path"),
        Line2D([0], [0], color=ink, linewidth=1.6, linestyle="--", label="Median OpenShell execution (tool turns)"),
    ]
    fig.legend(
        handles=workload_handles + phase_handles,
        loc="upper center",
        bbox_to_anchor=(0.55, 0.94),
        ncol=3,
        frameon=False,
        fontsize=8.5,
    )
    fig.suptitle(
        f"Per-turn workload medians · {len(trace_to_agent)} concurrent traces",
        x=0.06,
        ha="left",
        y=0.995,
        fontsize=20,
        fontweight="bold",
        color=ink,
    )
    fig.text(0.06, 0.012, "Source: profiler-v2 per_turn_efficiency.csv · medians across traces within each workload · model replay excluded", fontsize=8.5, color=muted)
    fig.subplots_adjust(left=0.08, right=0.98, top=0.86, bottom=0.08, hspace=0.46)
    fig.savefig(out / "executive_per_turn_measurement.png", dpi=220, facecolor=background, bbox_inches="tight")
    plt.close(fig)


def plot_turn_anatomy(efficiency_rows: list[dict[str, Any]], summary_rows: list[dict[str, Any]], out: Path) -> None:
    """Show the measured phase order and variation for every turn."""
    import matplotlib.pyplot as plt
    from matplotlib.patches import Patch

    if not efficiency_rows or not summary_rows:
        return

    trace_to_agent = {str(row["trace"]): str(row["agent"]) for row in summary_rows}
    agents = sorted({trace_to_agent.get(str(row["trace"]), str(row["trace"])) for row in efficiency_rows})
    colors = {
        "sandbox_init_ms": "#86A51E",
        "prompt_boundary_ms": "#2F6FED",
        "response_processing_ms": "#5D6F9E",
        "openshell_exec_ms": "#1E8E5A",
        "tool_failure_ms": "#DC2626",
    }
    labels = {
        "sandbox_init_ms": "Sandbox init (cold)",
        "prompt_boundary_ms": "Context assembly",
        "response_processing_ms": "Response processing",
        "openshell_exec_ms": "Tool execution",
        "tool_failure_ms": "Tool failure / rejected execution",
    }
    background = "#F8FAFC"
    ink = "#0F172A"
    muted = "#64748B"
    grid = "#E2E8F0"

    # Keep the visual order aligned with the measured agent lifecycle.
    # Dispatch overhead is part of response processing in this view.
    phase_keys = (
        "prompt_boundary_ms",
        "sandbox_init_ms",
        "openshell_exec_ms",
        "response_processing_ms",
        "tool_failure_ms",
    )
    workload_names = {
        "software engineering qa": "Software Engineering & QA",
        "personal assistant": "Personal Assistant",
        "research deep search": "Research & Deep Search",
        "customer support ops": "Customer Support & Ops",
    }
    agent_labels = {}
    for index, row in enumerate(sorted(summary_rows, key=lambda item: str(item["agent"]))):
        agent = str(row["agent"])
        workload = str(row.get("workload") or "unknown").replace("-", " ")
        agent_labels[agent] = f"Agent {index + 1} ({workload_names.get(workload, workload.title())})"

    global_max_total = max(
        (
            (
                sum(
                    float(row.get(key) or 0.0)
                    for key in phase_keys
                    if key != "response_processing_ms"
                )
                + float(row.get("response_processing_ms") or 0.0)
                + float(row.get("tool_dispatch_overhead_ms") or 0.0)
            )
            / 1000.0
            for row in efficiency_rows
        ),
        default=1.0,
    )
    fig, axes = plt.subplots(
        len(agents),
        1,
        figsize=(16, max(14, len(agents) * 0.75)),
        sharex=True,
        sharey=True,
        squeeze=False,
        facecolor=background,
    )
    axes = [axis[0] for axis in axes]
    for index, agent in enumerate(agents):
        trace_names = [trace for trace, mapped_agent in trace_to_agent.items() if mapped_agent == agent]
        series = sorted((row for row in efficiency_rows if str(row["trace"]) in trace_names), key=lambda row: int(row["turn"]))
        turns = [int(row["turn"]) for row in series]
        bottom = [0.0] * len(series)
        totals = [0.0] * len(series)
        axis = axes[index]
        axis.set_facecolor(background)
        for key in phase_keys:
            if key == "response_processing_ms":
                values = [
                    (float(row.get("response_processing_ms") or 0.0)
                     + float(row.get("tool_dispatch_overhead_ms") or 0.0))
                    / 1000.0
                    for row in series
                ]
            else:
                values = [float(row.get(key) or 0.0) / 1000.0 for row in series]
            axis.bar(turns, values, bottom=bottom, width=0.90, color=colors[key], label=labels[key], edgecolor=background, linewidth=0.7)
            bottom = [left + value for left, value in zip(bottom, values)]
            totals = bottom
        axis.set_ylim(0, global_max_total * 1.12)
        axis.set_ylabel(agent_labels.get(agent, agent), color=muted, fontsize=8.5, rotation=0, ha="right", va="center", labelpad=32)
        axis.grid(axis="y", color=grid, linewidth=0.8)
        axis.set_axisbelow(True)
        for spine in axis.spines.values():
            spine.set_visible(False)

    for axis in axes:
        axis.set_ylim(0, global_max_total * 1.12)
        axis.set_xlim(-0.8, max(int(row["turn"]) for row in efficiency_rows) + 0.8)
    axes[-1].set_xlabel("Turn index", color=muted, fontsize=9)
    fig.legend(
        handles=[
            Patch(facecolor="#2F6FED", label="Context assembly"),
            Patch(facecolor="#86A51E", label="Sandbox init (cold)"),
            Patch(facecolor="#F59E0B", label="Warm sandbox reuse"),
            Patch(facecolor="#1E8E5A", label="Tool execution"),
            Patch(facecolor="#5D6F9E", label="Response processing"),
            Patch(facecolor="#DC2626", label="Tool failure / rejected"),
        ],
        loc="upper center",
        bbox_to_anchor=(0.58, 0.915),
        ncol=3,
        frameon=False,
        fontsize=8.5,
    )
    fig.suptitle(
        f"Total measured time per stage and turn · {len(agents)} concurrent traces",
        x=0.18,
        ha="left",
        y=0.985,
        fontsize=20,
        fontweight="bold",
        color=ink,
    )
    fig.text(0.18, 0.012, "Source: profiler-v2 per_turn_efficiency.csv · times shown in seconds", fontsize=8.5, color=muted)
    fig.subplots_adjust(left=0.22, right=0.98, top=0.84, bottom=0.05, hspace=0.24)
    fig.savefig(out / "meeting_figure_1_turn_anatomy.png", dpi=220, facecolor=background, bbox_inches="tight")
    plt.close(fig)


def plot_parallel_wall_clock(intervals: list[dict[str, Any]], summary_rows: list[dict[str, Any]], out: Path) -> None:
    """Show the measured order of phases across concurrent agent lanes."""
    import matplotlib.pyplot as plt
    from matplotlib.patches import Patch

    if not intervals:
        return

    colors = {
        "model call": "#E2E8F0",
        "sandbox initialization": "#A16207",
        "context/turn boundary": "#2563EB",
        "response processing": "#0F766E",
        "OpenShell execution": "#7C3AED",
        "tool failure / rejected execution": "#DC2626",
    }
    labels = {
        "model call": "Controlled model handoff (neutral)",
        "sandbox initialization": "Sandbox initialization (direct span only)",
        "context/turn boundary": "Context / prompt boundary",
        "response processing": "Response processing",
        "OpenShell execution": "OpenShell execution",
        "tool failure / rejected execution": "Tool failure / rejected execution",
    }
    ink = "#0F172A"
    muted = "#64748B"
    grid = "#E2E8F0"
    background = "#F8FAFC"
    agents = sorted({str(row["agent"]) for row in intervals})
    grouped: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for row in intervals:
        grouped[(str(row["agent"]), int(row["turn"]))].append(row)

    fig, axis = plt.subplots(figsize=(15, 6.6), facecolor=background)
    axis.set_facecolor(background)
    for index, agent in enumerate(agents):
        for turn, rows in sorted((turn, rows) for (group_agent, turn), rows in grouped.items() if group_agent == agent):
            start = min(float(row["relative_s"]) for row in rows)
            end = max(float(row["relative_s"]) + float(row["duration_s"]) for row in rows)
            axis.barh(index, max(end - start, 0.04), left=start, height=0.42, color=colors["model call"], edgecolor=background, linewidth=0.9)
            for stage in ("sandbox initialization", "context/turn boundary", "response processing", "OpenShell execution", "tool failure / rejected execution"):
                for row in rows:
                    if row["stage"] == stage:
                        duration = float(row["duration_s"])
                        if duration < 0.08:
                            # Preserve the measured duration in the CSV while
                            # making sub-80ms phases visible at executive
                            # wall-clock scale.
                            axis.plot(
                                [float(row["relative_s"])], [index],
                                marker="|", markersize=15, markeredgewidth=2.2,
                                color=colors[stage], linestyle="None", zorder=5,
                            )
                        else:
                            axis.barh(index, duration, left=float(row["relative_s"]), height=0.42, color=colors[stage], edgecolor=background, linewidth=0.7)
            if turn == 0 or turn % 5 == 0:
                axis.text(start + 0.18, index, f"T{turn}", fontsize=8, color=ink, va="center")

    axis.set_yticks(range(len(agents)), [f"Agent {index + 1}" for index in range(len(agents))], color=ink, fontsize=10)
    axis.invert_yaxis()
    axis.set_xlim(0, max(float(row["relative_s"]) + float(row["duration_s"]) for row in intervals) * 1.02)
    axis.set_xlabel("Shared wall-clock time from first measured event (seconds)", color=muted, fontsize=9)
    axis.set_title(f"{len(agents)} agents executing serial multi-turn sessions on one shared wall-clock", loc="left", fontsize=14, fontweight="bold", color=ink, pad=28)
    axis.text(0.0, 1.02, "Neutral bands preserve turn order; colored bars/markers identify measured phases (markers make sub-80ms events visible without changing the CSV values).", transform=axis.transAxes, color=muted, fontsize=9, va="bottom")
    axis.grid(axis="x", color=grid, linewidth=0.8)
    axis.set_axisbelow(True)
    for spine in axis.spines.values():
        spine.set_visible(False)
    axis.legend(handles=[Patch(facecolor=colors[key], label=labels[key]) for key in ("model call", "sandbox initialization", "context/turn boundary", "response processing", "OpenShell execution", "tool failure / rejected execution")], loc="upper right", ncol=3, frameon=False, fontsize=8.5)
    workload_values = sorted({str(row.get("workload") or "unknown") for row in summary_rows})
    workload = "Personal Assistant / AppWorld" if workload_values == ["personal-assistant"] else ", ".join(workload_values)
    total_turns = sum(int(row.get("turns") or 0) for row in summary_rows)
    total_tools = sum(int(row.get("tool_calls") or 0) for row in summary_rows)
    fig.suptitle(f"Multi-agent trace replay · {workload} · {total_turns} measured turns · {total_tools} tool calls", x=0.01, ha="left", y=0.995, fontsize=20, fontweight="bold", color=ink)
    fig.text(0.01, 0.012, "Source: Jaeger-derived parallel_step_timing.csv · failed tool wrappers are shown in red and excluded from successful OpenShell execution", fontsize=8.5, color=muted)
    fig.subplots_adjust(left=0.08, right=0.98, top=0.83, bottom=0.10)
    fig.savefig(out / "meeting_figure_2_parallel_wall_clock.png", dpi=220, facecolor=background, bbox_inches="tight")
    fig.savefig(out / "executive_parallel_gantt.png", dpi=220, facecolor=background, bbox_inches="tight")
    plt.close(fig)


def summarize_trace(trace: Path) -> dict[str, Any]:
    shell = trace / "data/shell"
    timing = load(shell / "traces/timing_segments.json", [])
    tools = load(shell / "traces/per_tool_segments.json", [])
    raw_traces = load(shell / "traces/raw_traces.json", [])
    if isinstance(raw_traces, dict):
        raw_traces = raw_traces.get("data") or raw_traces.get("traces") or []
    raw_spans = [span for payload in raw_traces for span in payload.get("spans") or []]
    openshell_exec_span_count = sum(span.get("operationName") == "openclaw.exec" for span in raw_spans)
    lifecycle = load(shell / "sandbox_lifecycle.json", {})
    metadata = load(trace / "trace.json", {})
    driver_summary = load(shell / "summary.json", {})
    appworld_events_path = trace / "data/appworld/events.jsonl"
    generic_backend_events_path = trace / "data/backend/events.jsonl"
    if appworld_events_path.exists():
        backend_kind = "appworld"
        events_path = appworld_events_path
        backend_plot = trace / "data/appworld/appworld_api_latency.png"
    elif generic_backend_events_path.exists():
        backend_kind = "workload"
        events_path = generic_backend_events_path
        backend_plot = trace / "data/backend/backend_api_latency.png"
    else:
        backend_kind = "missing"
        events_path = None
        backend_plot = None
    events = []
    if events_path:
        for line in events_path.read_text(encoding="utf-8").splitlines():
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    prom = shell / "prometheus"
    cgroup = prom / "cgroup"
    cgroup_openclaw = cgroup_stats(cgroup / "openclaw_gateway.csv")
    cgroup_sandbox = cgroup_stats(cgroup / "openshell_sandbox.csv")
    cgroup_openshell_gw = cgroup_stats(cgroup / "openshell_gateway.csv")
    diagrams = shell / "profile-v2/diagrams"
    lifecycle_reuse_raw = lifecycle.get("sandbox_reuse_ms")
    lifecycle_reuse_measurement = str(
        lifecycle.get("sandbox_reuse_measurement") or ""
    )
    # Older runs used 0 as a placeholder for "not measured". Do not carry
    # that sentinel into the report as a real warm-reuse measurement.
    lifecycle_reuse_ms = (
        float(lifecycle_reuse_raw)
        if lifecycle_reuse_raw is not None
        and (float(lifecycle_reuse_raw) > 0.0 or lifecycle_reuse_measurement)
        else None
    )
    missing_plots = [
        name
        for name in REQUIRED_PER_AGENT_PLOTS
        if not (diagrams / name).exists() or (diagrams / name).stat().st_size < 1000
    ]
    if backend_plot is None or not backend_plot.exists() or backend_plot.stat().st_size < 1000:
        missing_plots.append("appworld_api_latency.png" if backend_kind == "appworld" else "backend_api_latency.png")
    tool_ms = [float(row.get("duration_ms") or 0) for row in tools]
    app_ms = [float(row.get("elapsed_ms") or 0) for row in events]
    tool_errors = sum(row.get("error_category") not in (None, "") for row in tools)
    target_url = str(driver_summary.get("target_url") or "")
    target_host = target_url.split("//", 1)[-1].split(":", 1)[0]
    agent = str(metadata.get("agent") or target_host or "trace-replay-openclaw-agent-1")
    agent = agent.split(".trace-replay.svc.cluster.local", 1)[0]
    session = str(metadata.get("session_id") or driver_summary.get("session_id") or trace.name.split("__", 1)[0].split("-", 1)[-1])
    workload = trace.name.split("__", 1)[1] if "__" in trace.name else "unknown"
    return {
        "trace": trace.name,
        "session_id": session,
        "workload": workload,
        "agent": agent,
        "turns": len(timing),
        "tool_calls": len(tools),
        "tool_errors": tool_errors,
        "successful_tool_calls": max(0, len(tools) - tool_errors),
        "tool_failure_calls": tool_errors,
        "openshell_exec_span_count": openshell_exec_span_count,
        "backend_kind": backend_kind,
        "backend_event_count": len(events),
        # Keep the legacy key for existing report consumers; the quality gate
        # and new plots use backend_event_count so every workload is treated
        # according to its selected stateful backend.
        "appworld_event_count": len(events) if backend_kind == "appworld" else 0,
        "sandbox_init_status": str(lifecycle.get("status") or "not_instrumented"),
        "sandbox_init_ms": float(lifecycle.get("sandbox_init_ms") or 0.0),
        "sandbox_reuse_ms": lifecycle_reuse_ms,
        "sandbox_reuse_measurement": lifecycle_reuse_measurement or "not separately observed",
        "sandbox_prewarm_ms": float(lifecycle.get("prewarm_request_ms") or 0.0),
        "sandbox_created_to_ready_ms": float(lifecycle.get("created_to_ready_ms") or 0.0),
        "sandbox_provisioning_mode": str(lifecycle.get("provisioning_mode") or "unknown"),
        "conversational_trace": bool(lifecycle.get("conversational_trace")),
        "sandbox_request_to_first_action_ms": lifecycle.get("sandbox_request_to_first_action_ms"),
        "per_agent_plot_status": "complete" if not missing_plots else "incomplete",
        "missing_per_agent_plots": ";".join(missing_plots),
        "harness_ms": max((float(row.get("harness_total_ms") or 0) for row in timing), default=0),
        "model_total_ms": sum(float(row.get("model_call_total_ms") or 0) for row in timing),
        "context_total_ms": sum(float(row.get("context_assembly_ms") or 0) for row in timing),
        "response_total_ms": sum(float(row.get("response_processing_ms") or 0) for row in timing),
        "sandbox_total_ms": sum(float(row.get("sandbox_exec_ms") or 0) for row in timing),
        "tool_p50_ms": statistics.median(tool_ms) if tool_ms else None,
        "tool_p95_ms": percentile(tool_ms, 0.95),
        "backend_mean_ms": statistics.mean(app_ms) if app_ms else None,
        "backend_p95_ms": percentile(app_ms, 0.95),
        "appworld_mean_ms": statistics.mean(app_ms) if backend_kind == "appworld" and app_ms else None,
        "appworld_p95_ms": percentile(app_ms, 0.95) if backend_kind == "appworld" else None,
        "openclaw_cpu_peak_cores": max(prom_values(prom / "cpu_openclaw.json"), default=None),
        "openclaw_cpu_mean_cores": statistics.mean(prom_values(prom / "cpu_openclaw.json")) if prom_values(prom / "cpu_openclaw.json") else None,
        "openclaw_memory_peak_mib": (max(prom_values(prom / "memory_openclaw.json"), default=0) / 1024 / 1024) or None,
        "openshell_cpu_peak_cores": max(prom_values(prom / "cpu_openshell.json"), default=None),
        "openshell_cpu_mean_cores": statistics.mean(prom_values(prom / "cpu_openshell.json")) if prom_values(prom / "cpu_openshell.json") else None,
        "openshell_memory_peak_mib": (max(prom_values(prom / "memory_openshell.json"), default=0) / 1024 / 1024) or None,
        "sandbox_cpu_peak_cores": max(prom_values(prom / "cpu_sandbox.json"), default=None),
        "sandbox_cpu_mean_cores": statistics.mean(prom_values(prom / "cpu_sandbox.json")) if prom_values(prom / "cpu_sandbox.json") else None,
        "sandbox_memory_peak_mib": (max(prom_values(prom / "memory_sandbox.json"), default=0) / 1024 / 1024) or None,
        "cgroup_openclaw_cpu_peak_cores": cgroup_openclaw["cpu_peak_cores"],
        "cgroup_openclaw_cpu_mean_cores": cgroup_openclaw["cpu_mean_cores"],
        "cgroup_openclaw_memory_peak_mib": cgroup_openclaw["memory_peak_mib"],
        "cgroup_openclaw_memory_mean_mib": cgroup_openclaw["memory_mean_mib"],
        "cgroup_openshell_sandbox_cpu_peak_cores": cgroup_sandbox["cpu_peak_cores"],
        "cgroup_openshell_sandbox_cpu_mean_cores": cgroup_sandbox["cpu_mean_cores"],
        "cgroup_openshell_sandbox_memory_peak_mib": cgroup_sandbox["memory_peak_mib"],
        "cgroup_openshell_gateway_cpu_peak_cores": cgroup_openshell_gw["cpu_peak_cores"],
        "cgroup_openshell_gateway_cpu_mean_cores": cgroup_openshell_gw["cpu_mean_cores"],
        "cgroup_openshell_gateway_memory_peak_mib": cgroup_openshell_gw["memory_peak_mib"],
        "cgroup_openshell_sandbox_memory_mean_mib": cgroup_sandbox["memory_mean_mib"],
        "cgroup_openclaw_samples": cgroup_openclaw["samples"],
        "cgroup_openshell_sandbox_samples": cgroup_sandbox["samples"],
        "functional_conformance_status": str(load(trace / "functional_conformance.json", {}).get("status") or "MISSING"),
        "functional_conformance_failures": ";".join(
            check.get("name", "")
            for check in load(trace / "functional_conformance.json", {}).get("checks", [])
            if check.get("status") == "FAIL"
        ),
    }


def write_quality_gate(rows: list[dict[str, Any]], out: Path) -> None:
    """Reject runs that cannot support a real sandbox/backend conclusion."""
    checks = []
    for row in rows:
        reasons = []
        # Conversational traces (zero recorded tool calls) have no tool stage
        # by design: OpenShell execution and sandbox-acquisition metrics are
        # not applicable and are excluded from tool-stage aggregates. Their
        # lifecycle, plots, and functional conformance must still pass.
        conversational = bool(row.get("conversational_trace"))
        if not conversational and int(row.get("successful_tool_calls") or 0) <= 0:
            reasons.append("no successful tool call")
        if not conversational and int(row.get("backend_event_count") or 0) <= 0:
            reasons.append(f"no {row.get('backend_kind') or 'workload'} backend event records")
        if int(row.get("cgroup_openshell_sandbox_samples") or 0) <= 0:
            reasons.append("no OpenShell sandbox cgroup samples")
        if str(row.get("sandbox_init_status") or "") != "ready":
            reasons.append("sandbox create-to-ready lifecycle not captured")
        if str(row.get("per_agent_plot_status") or "") != "complete":
            reasons.append(f"missing per-agent plots: {row.get('missing_per_agent_plots')}")
        if str(row.get("functional_conformance_status") or "") not in {"PASS", "PASS_WITH_WARNINGS"}:
            reasons.append(f"functional conformance: {row.get('functional_conformance_status')}: {row.get('functional_conformance_failures')}")
        checks.append({"trace": row.get("trace"), "passed": not reasons, "reasons": reasons, "conversational_trace": conversational})
    failures = [item for item in checks if not item["passed"]]
    workload_population: dict[str, dict[str, int]] = {}
    for row in rows:
        workload = str(row.get("workload") or "unknown")
        population = workload_population.setdefault(workload, {"total": 0, "tool_rich": 0, "conversational": 0})
        population["total"] += 1
        if bool(row.get("conversational_trace")):
            population["conversational"] += 1
        else:
            population["tool_rich"] += 1
    payload = {
        "schema": "agentic.trace-replay.quality-gate.v1",
        "status": "PASS" if not failures else "FAIL",
        "valid_for_sandbox_overhead_claim": not failures,
        "workload_population": workload_population,
        "requirements": [
            "at least one successful tool call per trace",
            "at least one selected workload-backend event record per trace",
            "OpenShell sandbox cgroup samples present when resource profiling is claimed",
            "measured prewarm readiness for warm-pool slots or Kubernetes create-to-ready evidence for cold fallback",
            "functional conformance report passes turn/tool/backend/state checks",
        ],
        "sandbox_lifecycle": {
            "warm_initialization": "MEASURED_BY_PREWARM_REQUEST",
            "cold_fallback": "MEASURED_BY_KUBERNETES_CREATE_TO_READY",
            "evidence": [
                "driver prewarm request timestamped before the first replay turn",
                "harmless OpenShell exec completes before measured trace execution",
                "pod create-to-ready retained separately to distinguish cold creation from warm reuse",
            ],
            "interpretation": "prewarm_request_ms is the one-time pool-fill/readiness path; sandbox_init_ms is reserved for cold provisioning on the measured trace path, while warm reuse is reported separately and OpenShell tool execution remains independent",
        },
        "checks": checks,
        "failure_count": len(failures),
    }
    (out / "quality_gate.json").write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


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
    workload_labels = {
        "software-engineering-qa": "Software Engineering & QA",
        "personal-assistant": "Personal Assistant",
        "research-deep-search": "Research & Deep Search",
        "customer-support-ops": "Customer Support & Ops",
    }
    agent_labels = [
        f"Agent {index} ({workload_labels.get(str(by_agent[agent][0].get('workload') or 'unknown'), str(by_agent[agent][0].get('workload') or 'unknown').replace('-', ' ').title())})"
        for index, agent in enumerate(agents, start=1)
    ]
    x = list(range(len(agents)))
    processing = [
        mean_present(by_agent[a], "context_total_ms")
        + mean_present(by_agent[a], "response_total_ms")
        for a in agents
    ]
    sandbox = [mean_present(by_agent[a], "sandbox_total_ms") for a in agents]
    fig, ax = plt.subplots(figsize=(13, max(6, len(agents) * 0.45)))
    ax.barh(x, processing, label="Agent processing")
    ax.barh(x, sandbox, left=processing, label="OpenShell execution")
    ax.set_yticks(x, agent_labels)
    ax.invert_yaxis()
    ax.set_xlabel("Mean milliseconds per trace")
    ax.set_title("Cross-agent orchestration latency")
    ax.grid(axis="x", alpha=0.25)
    ax.legend()
    fig.subplots_adjust(left=0.30, right=0.98, top=0.93, bottom=0.08)
    fig.tight_layout()
    fig.savefig(out / "cross_agent_latency.png", dpi=160)
    plt.close(fig)

    palette = {
        "openclaw": "#2563EB",
        "gateway": "#F59E0B",
        "sandbox": "#16805C",
    }
    fig, axes = plt.subplots(
        1,
        2,
        figsize=(15, max(8, len(agents) * 0.55)),
        sharey=True,
        facecolor="#FFFFFF",
    )
    openclaw_cpu = [mean_present(by_agent[a], "cgroup_openclaw_cpu_peak_cores") for a in agents]
    sandbox_cpu = [mean_present(by_agent[a], "cgroup_openshell_sandbox_cpu_peak_cores") for a in agents]
    gateway_cpu = [mean_present(by_agent[a], "cgroup_openshell_gateway_cpu_peak_cores") or mean_present(by_agent[a], "openshell_cpu_peak_cores") for a in agents]
    axes[0].barh(
        x,
        openclaw_cpu,
        label="OpenClaw cgroup",
        color=palette["openclaw"],
        edgecolor="white",
        linewidth=0.5,
    )
    axes[0].barh(
        x,
        gateway_cpu,
        left=openclaw_cpu,
        label="OpenShell gateway",
        color=palette["gateway"],
        edgecolor="white",
        linewidth=0.5,
    )
    axes[0].barh(
        x,
        sandbox_cpu,
        left=[a + b for a, b in zip(openclaw_cpu, gateway_cpu)],
        label="OpenShell sandbox",
        color=palette["sandbox"],
        edgecolor="white",
        linewidth=0.5,
    )
    axes[0].set_yticks(x, agent_labels)
    axes[0].invert_yaxis()
    axes[0].set_xlabel("Peak CPU cores")
    openclaw_memory = [mean_present(by_agent[a], "cgroup_openclaw_memory_peak_mib") for a in agents]
    sandbox_memory = [mean_present(by_agent[a], "cgroup_openshell_sandbox_memory_peak_mib") for a in agents]
    gateway_memory = [mean_present(by_agent[a], "cgroup_openshell_gateway_memory_peak_mib") or mean_present(by_agent[a], "openshell_memory_peak_mib") for a in agents]
    axes[1].barh(
        x,
        openclaw_memory,
        label="OpenClaw cgroup",
        color=palette["openclaw"],
        edgecolor="white",
        linewidth=0.5,
    )
    axes[1].barh(
        x,
        gateway_memory,
        left=openclaw_memory,
        label="OpenShell gateway",
        color=palette["gateway"],
        edgecolor="white",
        linewidth=0.5,
    )
    axes[1].barh(
        x,
        sandbox_memory,
        left=[a + b for a, b in zip(openclaw_memory, gateway_memory)],
        label="OpenShell sandbox",
        color=palette["sandbox"],
        edgecolor="white",
        linewidth=0.5,
    )
    axes[1].set_xlabel("Peak memory MiB")
    for axis in axes:
        axis.set_facecolor("#FFFFFF")
        axis.grid(axis="x", color="#CBD5E1", alpha=0.45, linewidth=0.8)
        axis.set_axisbelow(True)
        axis.tick_params(axis="y", labelsize=8.5, length=0, pad=6)
        axis.tick_params(axis="x", labelsize=9, colors="#475569")
        axis.spines["top"].set_visible(False)
        axis.spines["right"].set_visible(False)
        axis.spines["left"].set_color("#CBD5E1")
        axis.spines["bottom"].set_color("#CBD5E1")
    axes[0].set_title("Peak CPU", loc="left", fontsize=13, fontweight="bold", color="#0F172A", pad=10)
    axes[1].set_title("Peak memory", loc="left", fontsize=13, fontweight="bold", color="#0F172A", pad=10)
    handles, labels = axes[0].get_legend_handles_labels()
    fig.suptitle("Cross-agent resource peaks", fontsize=18, fontweight="bold", color="#0F172A", y=0.995)
    fig.legend(handles, labels, loc="upper center", ncol=3, frameon=False, bbox_to_anchor=(0.5, 0.955), fontsize=9)
    fig.subplots_adjust(left=0.30, right=0.98, top=0.86, bottom=0.06, wspace=0.18)
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
        trace_path = out.parent / "traces" / trace
        normalized = _normalized_efficiency(trace_path)
        if not normalized:
            continue
        _write_normalized_efficiency(trace_path, normalized)
        for item in normalized:
            efficiency_rows.append({"trace": trace, **item})
    if efficiency_rows:
        with (out / "per_turn_efficiency.csv").open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(efficiency_rows[0]))
            writer.writeheader()
            writer.writerows(efficiency_rows)
        fig, axes = plt.subplots(2, 1, figsize=(13, 9), sharex=True)
        workload_by_trace = {
            str(row["trace"]): str(row.get("workload") or "unknown")
            for row in rows
        }
        workload_labels = {
            "software-engineering-qa": "Software Engineering & QA",
            "personal-assistant": "Personal Assistant",
            "research-deep-search": "Research & Deep Search",
            "customer-support-ops": "Customer Support & Ops",
        }
        workload_colors = {
            "software-engineering-qa": "#2563EB",
            "personal-assistant": "#F59E0B",
            "research-deep-search": "#16805C",
            "customer-support-ops": "#7C3AED",
        }
        for workload in sorted(set(workload_by_trace.values())):
            trace_names = [
                trace for trace, value in workload_by_trace.items() if value == workload
            ]
            per_turn: dict[int, dict[str, list[float]]] = defaultdict(
                lambda: {"openclaw": [], "openshell": [], "share": []}
            )
            for item in efficiency_rows:
                if workload_by_trace.get(str(item["trace"])) != workload:
                    continue
                turn = int(item["turn"])
                per_turn[turn]["openclaw"].append(float(item["openclaw_observed_overhead_ms"]))
                per_turn[turn]["openshell"].append(float(item["openshell_exec_ms"]))
                per_turn[turn]["share"].append(float(item["openshell_share_pct"]))
            turns = sorted(per_turn)
            if not turns:
                continue
            color = workload_colors.get(workload, "#64748B")
            label = workload_labels.get(workload, workload.replace("-", " ").title())
            axes[0].plot(
                turns,
                [statistics.median(per_turn[turn]["openclaw"]) for turn in turns],
                marker="o",
                markersize=3,
                linewidth=2,
                color=color,
                label=f"{label} (n={len(trace_names)})",
            )
            axes[0].plot(
                turns,
                [statistics.median(per_turn[turn]["openshell"]) for turn in turns],
                marker=".",
                markersize=3,
                linewidth=1.4,
                linestyle="--",
                color=color,
                alpha=0.65,
            )
            axes[1].plot(
                turns,
                [statistics.median(per_turn[turn]["share"]) for turn in turns],
                marker="o",
                markersize=3,
                linewidth=2,
                color=color,
                label=f"{label} (n={len(trace_names)})",
            )
        axes[0].set_ylabel("Median milliseconds")
        axes[0].set_title("Per-turn latency by workload · solid = OpenClaw, dashed = OpenShell", loc="left", fontweight="bold")
        axes[1].set_xlabel("Turn")
        axes[1].set_ylabel("Median OpenShell share [%]")
        axes[1].set_title("OpenShell efficiency share by workload", loc="left", fontweight="bold")
        for axis in axes:
            axis.grid(axis="y", color="#CBD5E1", alpha=0.45, linewidth=0.8)
            axis.set_axisbelow(True)
            axis.spines["top"].set_visible(False)
            axis.spines["right"].set_visible(False)
            axis.legend(fontsize=8, frameon=False, ncol=2)
        fig.suptitle("Cross-agent per-turn efficiency", fontsize=18, fontweight="bold", color="#0F172A")
        fig.tight_layout(rect=(0, 0, 1, 0.96))
        fig.savefig(out / "cross_agent_per_turn_efficiency.png", dpi=160)
        plt.close(fig)
        plot_executive_per_turn(efficiency_rows, rows, out)
        plot_turn_anatomy(efficiency_rows, rows, out)
        plot_workload_p95_bars(efficiency_rows, rows, out)

    trace_paths = [out.parent / "traces" / str(row["trace"]) for row in rows]
    plot_parallel_timing(trace_paths, out, rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment", type=Path, required=True)
    args = parser.parse_args()
    rows = [summarize_trace(path) for path in sorted((args.experiment / "traces").glob("*")) if path.is_dir()]
    out = args.experiment / "analysis"
    out.mkdir(parents=True, exist_ok=True)
    (out / "cross_agent_summary.json").write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")
    write_quality_gate(rows, out)
    if rows:
        with (out / "cross_agent_summary.csv").open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        plot_groups(rows, out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
