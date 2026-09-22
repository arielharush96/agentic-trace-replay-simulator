#!/usr/bin/env python3
"""Analyze cgroup-v2 and Prometheus resource samples for multi-turn runs."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
from pathlib import Path

csv.field_size_limit(sys.maxsize)


def load_cgroup(path: Path) -> list[dict[str, float]]:
    rows = []
    with path.open(encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            rows.append({key: float(value) for key, value in row.items()})
    return rows


def summarize(rows: list[dict[str, float]]) -> dict:
    cpu = []
    memory = []
    for previous, current in zip(rows, rows[1:]):
        dt = (current["epoch_ns"] - previous["epoch_ns"]) / 1e9
        if dt <= 0 or current["cpu_usage_usec"] < previous["cpu_usage_usec"]:
            continue
        cpu.append((current["cpu_usage_usec"] - previous["cpu_usage_usec"]) / 1e6 / dt)
        memory.append(current["memory_bytes"] / (1024 * 1024))
    def stats(values: list[float]) -> dict:
        if not values:
            return {"n": 0, "mean": None, "p50": None, "p95": None, "max": None}
        ordered = sorted(values)
        return {"n": len(values), "mean": round(statistics.mean(values), 4),
                "p50": round(statistics.median(values), 4),
                "p95": round(ordered[min(len(ordered) - 1, int(.95 * (len(ordered) - 1)))], 4),
                "max": round(max(values), 4)}
    return {"cpu_cores": stats(cpu), "memory_mib": stats(memory)}


def plot(samples: dict[str, list[dict[str, float]]], out: Path, edges: list[dict] | None = None) -> None:
    import matplotlib.pyplot as plt
    out.mkdir(parents=True, exist_ok=True)
    fig, axes = plt.subplots(2, 1, figsize=(15, 9), sharex=True)
    cpu_plot_series: dict[str, list[tuple[float, float]]] = {}
    for label, rows in samples.items():
        if len(rows) < 2:
            continue
        origin = rows[0]["epoch_ns"] / 1e9
        cpu_x, cpu_y, mem_x, mem_y = [], [], [], []
        for previous, current in zip(rows, rows[1:]):
            dt = (current["epoch_ns"] - previous["epoch_ns"]) / 1e9
            if dt <= 0 or current["cpu_usage_usec"] < previous["cpu_usage_usec"]:
                continue
            cpu_x.append(current["epoch_ns"] / 1e9 - origin)
            cpu_y.append((current["cpu_usage_usec"] - previous["cpu_usage_usec"]) / 1e6 / dt)
            mem_x.append(current["epoch_ns"] / 1e9 - origin)
            mem_y.append(current["memory_bytes"] / (1024 * 1024))
        cpu_plot_series[label] = list(zip(cpu_x, cpu_y))
        axes[1].plot(mem_x, mem_y, label=label)
    all_cpu = [value for series in cpu_plot_series.values() for _, value in series]
    if all_cpu:
        ordered = sorted(all_cpu)
        display_limit = ordered[min(len(ordered) - 1, int(0.995 * (len(ordered) - 1)))]
        for label, series in cpu_plot_series.items():
            visible = [(x, y) for x, y in series if y <= display_limit]
            axes[0].plot([x for x, _ in visible], [y for _, y in visible], label=label)
        axes[0].set_ylim(0, max(0.1, display_limit * 1.15))
    if edges and samples:
        origin = min(rows[0]["epoch_ns"] for rows in samples.values() if rows) / 1e9
        for edge in edges:
            timestamp = edge.get("t_recv_wall_ns")
            if timestamp is not None:
                x = float(timestamp) / 1e9 - origin
                axes[0].axvline(x, color="#64748b", alpha=0.18, linewidth=0.7)
                axes[1].axvline(x, color="#64748b", alpha=0.18, linewidth=0.7)
    axes[0].set_title("OpenClaw And OpenShell Multi-Turn Resource Profiling - CPU [cores; P99.5 display]")
    axes[1].set_title("OpenClaw And OpenShell Multi-Turn Resource Profiling - Memory [MiB]")
    axes[1].set_xlabel("Seconds from sampler start")
    for axis in axes:
        axis.grid(alpha=.25)
        axis.legend()
    fig.tight_layout()
    fig.savefig(out / "cgroup_resource_timeseries.png", dpi=160)
    plt.close(fig)


def plot_summary_bars(summary: dict, out: Path) -> None:
    """Render separate CPU and memory mean/P50/P95 bars for this task."""
    import matplotlib.pyplot as plt

    out.mkdir(parents=True, exist_ok=True)
    components = list(summary)
    labels = ["Mean", "P50", "P95"]
    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    for axis, metric, title, ylabel in (
        (axes[0], "cpu_cores", "CPU Usage", "CPU cores"),
        (axes[1], "memory_mib", "Resident Memory", "MiB"),
    ):
        positions = list(range(len(labels)))
        width = 0.36
        for offset, component in enumerate(components):
            stats = summary[component][metric]
            values = [stats.get("mean"), stats.get("p50"), stats.get("p95")]
            axis.bar(
                [position + (offset - 0.5) * width for position in positions],
                values,
                width,
                label=component,
            )
        axis.set_xticks(positions, labels)
        axis.set_title(title)
        axis.set_ylabel(ylabel)
        axis.grid(axis="y", alpha=0.25)
        axis.legend(fontsize=8)
    fig.suptitle("OpenClaw/OpenShell Resource Summary: Mean, P50, P95")
    fig.tight_layout()
    fig.savefig(out / "resource_summary_bars.png", dpi=180)
    plt.close(fig)


def plot_percentile_timeseries(samples: dict[str, list[dict[str, float]]], summary: dict, out: Path) -> None:
    """Render six separate CPU/memory statistic time series for one corpus."""
    import matplotlib.pyplot as plt

    out.mkdir(parents=True, exist_ok=True)
    for metric, title, ylabel, value_key in (
        ("cpu_cores", "CPU", "CPU cores", "cpu"),
        ("memory_mib", "Memory", "MiB", "memory"),
    ):
        series = {}
        for label, rows in samples.items():
            if len(rows) < 2:
                continue
            origin = rows[0]["epoch_ns"] / 1e9
            points = []
            for previous, current in zip(rows, rows[1:]):
                dt = (current["epoch_ns"] - previous["epoch_ns"]) / 1e9
                if dt <= 0 or current["cpu_usage_usec"] < previous["cpu_usage_usec"]:
                    continue
                value = (
                    (current["cpu_usage_usec"] - previous["cpu_usage_usec"]) / 1e6 / dt
                    if value_key == "cpu"
                    else current["memory_bytes"] / (1024 * 1024)
                )
                points.append((current["epoch_ns"] / 1e9 - origin, value))
            series[label] = points

        for statistic in ("mean", "p50", "p95"):
            fig, axis = plt.subplots(figsize=(11, 4.5))
            for label, points in series.items():
                if not points:
                    continue
                color = "#2563EB" if label == "openclaw_gateway" else "#F97316"
                axis.plot([x for x, _ in points], [y for _, y in points], color=color, alpha=0.45, linewidth=0.8, label=label)
                value = summary[label][metric][statistic]
                axis.axhline(value, color=color, linewidth=1.8, linestyle="--", label=f"{label} {statistic.upper()}: {value}")
            axis.set_title(f"OpenClaw/OpenShell {title} {statistic.upper()} Time Series")
            axis.set_xlabel("Seconds from sampler start")
            axis.set_ylabel(ylabel)
            axis.grid(alpha=0.25)
            axis.legend(fontsize=8)
            fig.tight_layout()
            axis_name = "cpu" if metric == "cpu_cores" else "memory"
            fig.savefig(out / f"{axis_name}_{statistic}_timeseries.png", dpi=180)
            plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--samples", nargs="+", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--edges", type=Path)
    parser.add_argument("--recommend", action="store_true", help="emit a sizing recommendation table")
    args = parser.parse_args()
    data = {path.stem: load_cgroup(path) for path in args.samples}
    summary = {name: summarize(rows) for name, rows in data.items()}
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "cgroup_resource_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    plot_summary_bars(summary, args.out / "diagrams")
    plot_percentile_timeseries(data, summary, args.out / "diagrams")
    edges = []
    if args.edges and args.edges.exists():
        for line in args.edges.read_text(encoding="utf-8").splitlines():
            try:
                edges.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    plot(data, args.out / "diagrams", edges)
    lines = ["# Multi-Turn Resource Analysis", "", "| Component | CPU mean | CPU P50 | CPU P95 | Memory mean MiB | Memory P50 MiB | Memory P95 MiB |", "|---|---:|---:|---:|---:|---:|---:|"]
    for name, values in summary.items():
        lines.append(f"| {name} | {values['cpu_cores']['mean']} | {values['cpu_cores']['p50']} | {values['cpu_cores']['p95']} | {values['memory_mib']['mean']} | {values['memory_mib']['p50']} | {values['memory_mib']['p95']} |")
    (args.out / "RESOURCE_ANALYSIS.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    gateway = summary.get("openclaw_gateway", {})
    sandbox = summary.get("openshell_sandbox", {})
    if args.recommend and gateway and sandbox and gateway["cpu_cores"]["p95"] is not None and sandbox["cpu_cores"]["p95"] is not None:
        rec = [
            "# Multi-Turn Resource Recommendations", "",
            "Sizing uses observed P95 cgroup usage with a 25% headroom factor. These are recommendations for this controlled workload, not cluster guarantees.", "",
            "| Concurrent agents | CPU recommendation | Memory recommendation |",
            "|---:|---:|---:|",
        ]
        for agents in (1, 3, 5, 10):
            cpu = (gateway["cpu_cores"]["p95"] + sandbox["cpu_cores"]["p95"]) * agents * 1.25
            memory = (gateway["memory_mib"]["p95"] + sandbox["memory_mib"]["p95"]) * agents * 1.25
            rec.append(f"| {agents} | {cpu:.2f} cores | {memory / 1024:.2f} GiB |")
        (args.out / "RESOURCE_RECOMMENDATIONS.md").write_text("\n".join(rec) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
