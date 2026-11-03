#!/usr/bin/env python3
"""Create a trace-first experiment directory from one profiler run."""

from __future__ import annotations

import argparse
import csv
import json
import shutil
from collections import defaultdict
from pathlib import Path
from statistics import mean, median


def load_json(path: Path, default):
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else default


def write_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def group_by(items: list[dict], key: str) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for item in items:
        if item.get(key):
            grouped[str(item[key])].append(item)
    return grouped


def raw_trace_session(raw_trace: dict) -> str | None:
    for span in raw_trace.get("spans", []):
        for tag in span.get("tags", []):
            if tag.get("key") == "mock.session_id":
                return str(tag.get("value"))
    return None


def copy_sliced_prometheus(source: Path, destination: Path, start_ms: float, end_ms: float) -> None:
    if not source.exists():
        return
    payload = load_json(source, {})
    for result in payload.get("data", {}).get("result", []):
        result["values"] = [
            value for value in result.get("values", [])
            if start_ms <= float(value[0]) * 1000 <= end_ms
        ]
    write_json(destination, payload)


def copy_sliced_csv(source: Path, destination: Path, start_ms: float, end_ms: float) -> None:
    if not source.exists():
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    with source.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.reader(handle))
    if not rows:
        return
    kept = [rows[0]]
    for row in rows[1:]:
        if row and start_ms <= float(row[0]) / 1e6 <= end_ms:
            kept.append(row)
    with destination.open("w", newline="", encoding="utf-8") as handle:
        csv.writer(handle).writerows(kept)


def write_plot(path: Path, title: str, values: list[float], ylabel: str) -> None:
    if not values:
        return
    import matplotlib.pyplot as plt

    path.parent.mkdir(parents=True, exist_ok=True)
    figure, axis = plt.subplots(figsize=(8, 4.5))
    axis.plot(range(1, len(values) + 1), values, marker=".", linewidth=1.4)
    axis.set_title(title)
    axis.set_xlabel("Sequence")
    axis.set_ylabel(ylabel)
    axis.grid(True, alpha=0.25)
    figure.tight_layout()
    figure.savefig(path, dpi=160)
    plt.close(figure)


def write_trace_plots(trace_root: Path, session_id: str) -> None:
    import matplotlib.pyplot as plt

    analysis_dir = trace_root / "analysis"
    plots_dir = analysis_dir / "plots"
    timing = load_json(trace_root / "shell" / "timing_segments.json", [])
    if timing:
        turns = [int(row.get("turn_idx", index)) for index, row in enumerate(timing)]
        init = [float(row.get("sandbox_init_ms") or 0) for row in timing]
        context = [float(row.get("context_assembly_ms") or 0) for row in timing]
        response = [float(row.get("response_processing_ms") or 0) for row in timing]
        per_turn = load_json(trace_root / "shell" / "per_turn_segments.json", [])
        per_tool = load_json(trace_root / "shell" / "per_tool_segments.json", [])
        sandbox = []
        tool_cursor = 0
        for index, row in enumerate(timing):
            tool_count = int((per_turn[index].get("tool_calls_this_turn") if index < len(per_turn) else 0) or 0)
            if tool_count:
                measured = sum(float(tool.get("duration_ms") or 0) for tool in per_tool[tool_cursor:tool_cursor + tool_count])
                tool_cursor += tool_count
            else:
                measured = float(row.get("sandbox_exec_ms") or 0)
            # Step 0 is the user's initial request. Tool execution can only
            # happen after the first model response, so do not render a
            # purple OpenShell segment before the first model call. Preserve
            # the measured duration by attributing it to initial context
            # assembly in this legacy waterfall.
            if index == 0:
                context[index] += measured
                sandbox.append(0.0)
            else:
                sandbox.append(measured)
        figure, axis = plt.subplots(figsize=(10, 5))
        left = [0.0] * len(turns)
        stages = (
            ("Context assembly", context, "#1685c4"),
            ("Sandbox initialization", init, "#df7a00"),
            ("Sandbox tool execution", sandbox, "#7738e8"),
            ("Response processing", response, "#079669"),
        )
        for label, values, color in stages:
            axis.barh(turns, values, left=left, color=color, edgecolor="white", label=label)
            for index, value in enumerate(values):
                if value >= 25:
                    axis.text(left[index] + value / 2, turns[index], f"{value:.0f}", ha="center", va="center", color="white", fontsize=8)
                left[index] += value
        totals = [context[index] + init[index] + sandbox[index] + response[index] for index in range(len(turns))]
        for index, total in enumerate(totals):
            axis.text(total + max(totals) * 0.012, turns[index], f"{total:.0f} ms", va="center", fontsize=8)
        axis.set_title("OpenClaw & OpenShell Multi-Step Profiling Analysis", loc="left", fontsize=15, pad=22)
        axis.text(0, 1.02, "Cumulative end-to-end latency per step, categorized by execution stage", transform=axis.transAxes, fontsize=9, color="#596579")
        axis.set_xlabel("Per Step Latency [ms]")
        axis.set_ylabel("Step")
        axis.invert_yaxis()
        axis.legend(
            loc="upper left",
            bbox_to_anchor=(1.01, 1.0),
            frameon=True,
            facecolor="white",
            edgecolor="#D1D5DB",
        )
        axis.grid(axis="x", alpha=0.25, linestyle=":")
        figure.tight_layout()
        figure.subplots_adjust(right=0.78)
        plots_dir.mkdir(parents=True, exist_ok=True)
        figure.savefig(plots_dir / "latency_waterfall.png", dpi=160)
        plt.close(figure)

        figure, axis = plt.subplots(figsize=(10, 4.5))
        axis.plot(turns, [float(row.get("model_call_total_ms") or 0) for row in timing], label="Model call")
        axis.plot(turns, [float(row.get("model_ttfb_ms") or 0) for row in timing], label="TTFB")
        axis.plot(turns, response, label="Response processing")
        axis.plot(turns, sandbox, label="OpenShell execution")
        axis.set_title("OpenClaw And OpenShell Multi-Turn Profiling Analysis - OTel Turn Metrics")
        axis.set_xlabel("Step")
        axis.set_ylabel("Milliseconds")
        axis.grid(True, alpha=0.25)
        axis.legend()
        figure.tight_layout()
        figure.savefig(plots_dir / "otel_turn_metrics.png", dpi=160)
        plt.close(figure)

        figure, axis = plt.subplots(figsize=(10, 4.5))
        axis.bar(turns, [float(row.get("harness_total_ms") or row.get("model_call_total_ms") or 0) for row in timing], color="#1685c4")
        axis.set_title("OpenClaw And OpenShell Multi-Turn Profiling Analysis - Prompt Boundary")
        axis.set_xlabel("Step")
        axis.set_ylabel("Milliseconds")
        axis.grid(axis="y", alpha=0.25)
        figure.tight_layout()
        figure.savefig(plots_dir / "prompt_boundary.png", dpi=160)
        plt.close(figure)

    resource_rows: dict[str, list[tuple[float, float, float]]] = {}
    for source in (trace_root / "prometheus" / "cgroup").glob("*.csv"):
        rows = list(csv.DictReader(source.open(encoding="utf-8")))
        if rows:
            resource_rows[source.stem] = [
                (float(row["epoch_ns"]) / 1e9, float(row["cpu_usage_usec"]), float(row["memory_bytes"]))
                for row in rows
            ]
    if resource_rows:
        figure, axes = plt.subplots(2, 1, figsize=(10, 7), sharex=True)
        for name, rows in resource_rows.items():
            times = [row[0] - rows[0][0] for row in rows]
            cpu = [0.0] + [max(rows[i][1] - rows[i - 1][1], 0) / max((rows[i][0] - rows[i - 1][0]) * 1e6, 1) for i in range(1, len(rows))]
            memory = [row[2] / 1024 / 1024 for row in rows]
            axes[0].plot(times, cpu, label=name)
            axes[1].plot(times, memory, label=name)
        axes[0].set_title("OpenClaw And OpenShell Multi-Turn Profiling Analysis - CPU [cores]")
        axes[0].set_ylabel("CPU cores")
        axes[1].set_title("OpenClaw And OpenShell Multi-Turn Profiling Analysis - Memory [MiB]")
        axes[1].set_ylabel("Memory MiB")
        axes[1].set_xlabel("Seconds from trace start")
        for axis in axes:
            axis.grid(True, alpha=0.25)
            axis.legend()
        figure.tight_layout()
        figure.savefig(plots_dir / "resource_timeseries.png", dpi=160)
        plt.close(figure)

        first_rows = next(iter(resource_rows.values()))
        plot_values = (
            ("resource_cpu_cores.png", "CPU cores", [0.0] + [max(first_rows[i][1] - first_rows[i - 1][1], 0) / max((first_rows[i][0] - first_rows[i - 1][0]) * 1e6, 1) for i in range(1, len(first_rows))]),
            ("resource_memory_mib.png", "Memory MiB", [row[2] / 1024 / 1024 for row in first_rows]),
        )
        for name, ylabel, values in plot_values:
            write_plot(plots_dir / name, f"{ylabel}: {session_id}", values, ylabel)


def organize(root: Path, corpus: Path | None, target_node: str, command: str) -> None:
    old_shell = root / "shell"
    old_prometheus = root / "prometheus"
    old_resource = root / "resource-study"
    old_traces = old_shell / "traces"
    summary_rows = load_json(old_traces / "session_summary.json", [])
    raw_traces = load_json(old_traces / "raw_traces.json", [])
    per_turn = load_json(old_traces / "per_turn_segments.json", [])
    per_tool = load_json(old_traces / "per_tool_segments.json", [])
    timing = load_json(old_traces / "timing_segments.json", [])
    appworld_log = root / "appworld" / "adapter.log"
    driver_requests = []
    requests_path = old_shell / "driver_requests.jsonl"
    if requests_path.exists():
        driver_requests = [json.loads(line) for line in requests_path.read_text(encoding="utf-8").splitlines() if line]

    trace_to_session = {
        str(raw.get("traceID")): raw_trace_session(raw)
        for raw in raw_traces
        if raw_trace_session(raw)
    }
    session_to_trace = {session: trace for trace, session in trace_to_session.items()}
    request_by_trace = {
        session_to_trace.get(str(request.get("session_id")), ""): request
        for request in driver_requests
    }
    appworld_events: dict[str, list[dict]] = defaultdict(list)
    if appworld_log.exists():
        for line in appworld_log.read_text(encoding="utf-8", errors="replace").splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if event.get("event") == "appworld_tool" and event.get("session_id"):
                appworld_events[str(event["session_id"])].append(event)
    summary_by_trace = {str(row.get("trace_id")): row for row in summary_rows}
    turns_by_trace = group_by(per_turn, "trace_id")
    tools_by_trace = group_by(per_tool, "trace_id")
    timing_by_trace = group_by(timing, "trace_id")
    raw_by_trace = {str(raw.get("traceID")): raw for raw in raw_traces}
    if corpus and corpus.exists():
        source_order = [str(session.get("session_id")) for session in load_json(corpus, {}).get("sessions", [])]
        ordered_traces = [session_to_trace[session] for session in source_order if session in session_to_trace]
        ordered_traces.extend(trace for trace in summary_by_trace if trace not in ordered_traces)
    else:
        ordered_traces = [str(row.get("trace_id")) for row in summary_rows]
    summary_rows = [summary_by_trace[trace] for trace in ordered_traces if trace in summary_by_trace]

    experiment_metadata = {
        "target_node": target_node,
        "corpus": str(corpus) if corpus else None,
        "trace_count": len(summary_rows),
        "layout": "trace-first",
        "command": command,
        "analyses": ["shell", "prometheus", "appworld", "openclaw-plain", "analysis"],
    }
    write_json(root / "experiment.json", experiment_metadata)
    (root / "README.md").write_text(
        "# Experiment\n\n"
        "This result is trace-first. Each directory under `traces/` is one independent trace.\n\n"
        "- `shell/`: OpenClaw through OpenShell.\n"
        "- `prometheus/`: resource measurements sliced to the trace interval.\n"
        "- `appworld/`: AppWorld evidence when exported by the runner.\n"
        "- `openclaw-plain/`: plain OpenClaw baseline, when run.\n"
        "- `analysis/`: trace-local JSON, CSV, and plots.\n\n"
        "Each `analysis/plots/` directory contains `latency_waterfall.png`, "
        "`resource_timeseries.png`, `resource_cpu_cores.png`, "
        "`resource_memory_mib.png`, and per-turn latency plots.\n",
        encoding="utf-8",
    )

    traces_root = root / "traces"
    traces_root.mkdir(parents=True, exist_ok=True)
    for index, summary in enumerate(summary_rows):
        trace_id = str(summary["trace_id"])
        session_id = trace_to_session.get(trace_id, "unknown-session")
        trace_root = traces_root / f"{index:02d}_{session_id}"
        shell_dir = trace_root / "shell"
        prometheus_dir = trace_root / "prometheus"
        appworld_dir = trace_root / "appworld"
        plain_dir = trace_root / "openclaw-plain"
        analysis_dir = trace_root / "analysis"
        for directory in (shell_dir, prometheus_dir, appworld_dir, plain_dir, analysis_dir):
            directory.mkdir(parents=True, exist_ok=True)

        request = request_by_trace.get(session_id)
        write_json(shell_dir / "driver_request.json", request or {})
        write_json(shell_dir / "raw_trace.json", raw_by_trace.get(trace_id, {}))
        write_json(shell_dir / "session_summary.json", summary)
        write_json(shell_dir / "per_turn_segments.json", turns_by_trace.get(trace_id, []))
        write_json(shell_dir / "per_tool_segments.json", tools_by_trace.get(trace_id, []))
        write_json(shell_dir / "timing_segments.json", timing_by_trace.get(trace_id, []))

        start_ms = float(summary.get("harness_start_ms") or 0)
        end_ms = start_ms + float(summary.get("harness_duration_ms") or 0)
        for source in old_prometheus.rglob("*.json"):
            relative = source.relative_to(old_prometheus)
            copy_sliced_prometheus(source, prometheus_dir / relative, start_ms, end_ms)
        for source in old_prometheus.rglob("*.csv"):
            relative = source.relative_to(old_prometheus)
            copy_sliced_csv(source, prometheus_dir / relative, start_ms, end_ms)

        (appworld_dir / "README.md").write_text(
            "This directory contains events emitted by the stateful AppWorld adapter. "
            "These events confirm that tool calls reached the backend.\n",
            encoding="utf-8",
        )
        event_rows = appworld_events.get(session_id, [])
        (appworld_dir / "events.jsonl").write_text(
            "".join(json.dumps(event) + "\n" for event in event_rows),
            encoding="utf-8",
        )
        write_json(appworld_dir / "summary.json", {
            "session_id": session_id,
            "event_count": len(event_rows),
            "successful_tool_count": len(event_rows),
            "tools": [event.get("tool") for event in event_rows],
        })
        (plain_dir / "README.md").write_text(
            "The plain OpenClaw baseline was not part of this experiment.\n",
            encoding="utf-8",
        )

        trace_turns = turns_by_trace.get(trace_id, [])
        trace_tools = tools_by_trace.get(trace_id, [])
        write_json(analysis_dir / "summary.json", {
            "trace_id": trace_id,
            "session_id": session_id,
            "outcome": summary.get("outcome"),
            "model_calls": summary.get("model_calls", 0),
            "tool_calls": summary.get("tool_calls", 0),
            "turns": len(trace_turns),
            "tool_segments": len(trace_tools),
            "harness_duration_ms": summary.get("harness_duration_ms"),
            "model_latency_ms": {
                "mean": mean([float(row["model_call_duration_ms"]) for row in trace_turns]) if trace_turns else None,
                "median": median([float(row["model_call_duration_ms"]) for row in trace_turns]) if trace_turns else None,
            },
        })
        (analysis_dir / "per_turn.json").write_text(json.dumps(trace_turns, indent=2) + "\n", encoding="utf-8")
        (analysis_dir / "per_tool.json").write_text(json.dumps(trace_tools, indent=2) + "\n", encoding="utf-8")
        write_plot(
            analysis_dir / "plots" / "model_latency_ms.png",
            f"Model latency: {session_id}",
            [float(row["model_call_duration_ms"]) for row in trace_turns if row.get("model_call_duration_ms") is not None],
            "Milliseconds",
        )
        write_trace_plots(trace_root, session_id)
        write_plot(
            analysis_dir / "plots" / "openshell_execution_latency_ms.png",
            f"OpenShell execution latency: {session_id}",
            [float(row["duration_ms"]) for row in trace_tools if row.get("duration_ms") is not None],
            "Milliseconds",
        )

    for directory in (old_shell, old_prometheus, old_resource, root / "appworld"):
        if directory.exists():
            shutil.rmtree(directory)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--corpus", type=Path)
    parser.add_argument("--target-node", required=True)
    parser.add_argument("--command", required=True)
    args = parser.parse_args()
    organize(args.root, args.corpus, args.target_node, args.command)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
