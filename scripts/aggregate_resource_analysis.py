#!/usr/bin/env python3
"""Aggregate per-task cgroup summaries without using unstable maxima."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from pathlib import Path


def load_values(path: Path, limit: int | None = None) -> tuple[list[float], list[float]]:
    rows = []
    with path.open(encoding="utf-8") as handle:
        for index, row in enumerate(csv.DictReader(handle)):
            if limit is not None and index >= limit + 1:
                break
            rows.append({key: float(value) for key, value in row.items()})
    cpu, memory = [], []
    for previous, current in zip(rows, rows[1:]):
        dt = (current["epoch_ns"] - previous["epoch_ns"]) / 1e9
        if dt <= 0 or current["cpu_usage_usec"] < previous["cpu_usage_usec"]:
            continue
        cpu.append((current["cpu_usage_usec"] - previous["cpu_usage_usec"]) / 1e6 / dt)
        memory.append(current["memory_bytes"] / (1024 * 1024))
    return cpu, memory


def load_series(path: Path, limit: int) -> tuple[list[float], list[float], list[float]]:
    rows = []
    with path.open(encoding="utf-8") as handle:
        for index, row in enumerate(csv.DictReader(handle)):
            if index >= limit + 1:
                break
            rows.append({key: float(value) for key, value in row.items()})
    if not rows:
        return [], [], []
    origin = rows[0]["epoch_ns"] / 1e9
    cpu_x, cpu_y, memory_y = [], [], []
    for previous, current in zip(rows, rows[1:]):
        dt = (current["epoch_ns"] - previous["epoch_ns"]) / 1e9
        if dt <= 0 or current["cpu_usage_usec"] < previous["cpu_usage_usec"]:
            continue
        cpu_x.append(current["epoch_ns"] / 1e9 - origin)
        cpu_y.append((current["cpu_usage_usec"] - previous["cpu_usage_usec"]) / 1e6 / dt)
        memory_y.append(current["memory_bytes"] / (1024 * 1024))
    return cpu_x, cpu_y, memory_y


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(fraction * (len(ordered) - 1)))]


def plot_percentiles(root: Path, out: Path) -> dict:
    distributions = {component: {"cpu_cores": [], "memory_mib": []} for component in COMPONENTS}
    for task in sorted(root.iterdir()):
        cgroup = task / "prometheus" / "cgroup"
        if not cgroup.is_dir():
            continue
        summary_path = task / "prometheus" / "cgroup-analysis" / "cgroup_resource_summary.json"
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        for component, filename in (("openclaw_gateway", "openclaw_gateway.csv"), ("openshell_sandbox", "openshell_sandbox.csv")):
            limit = summary[component]["cpu_cores"]["n"]
            cpu, memory = load_values(cgroup / filename, limit=limit)
            distributions[component]["cpu_cores"].extend(cpu)
            distributions[component]["memory_mib"].extend(memory)

    stats = {}
    for component, metrics in distributions.items():
        stats[component] = {}
        for metric, values in metrics.items():
            stats[component][metric] = {
                "mean": round(statistics.mean(values), 4),
                "p50": round(statistics.median(values), 4),
                "p95": round(percentile(values, 0.95), 4),
                "n": len(values),
            }

    import matplotlib.pyplot as plt

    labels = ["Mean", "P50", "P95"]
    for component in COMPONENTS:
        for metric, title, ylabel, filename in (
            ("cpu_cores", "CPU Usage", "Cores", "cpu_percentiles.png"),
            ("memory_mib", "Resident Memory", "MiB", "memory_percentiles.png"),
        ):
            values = [stats[component][metric][key.lower()] for key in labels]
            fig, axis = plt.subplots(figsize=(7, 5))
            axis.bar(labels, values, color=("#2563eb", "#0f766e", "#c2410c"))
            axis.set_title(f"{component}: {title}")
            axis.set_ylabel(ylabel)
            axis.grid(axis="y", alpha=0.25)
            fig.tight_layout()
            fig.savefig(out / f"{component}_{filename}", dpi=180)
            plt.close(fig)

    series_config = (
        ("openclaw_gateway", "cpu_cores", "OpenClaw Gateway CPU", "CPU cores"),
        ("openclaw_gateway", "memory_mib", "OpenClaw Gateway Memory", "MiB"),
    )
    for component, metric, title, ylabel in series_config:
        buckets: dict[int, list[float]] = {}
        for task in sorted(root.iterdir()):
            cgroup = task / "prometheus" / "cgroup"
            summary_path = task / "prometheus" / "cgroup-analysis" / "cgroup_resource_summary.json"
            if not cgroup.is_dir() or not summary_path.exists():
                continue
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            limit = summary[component]["cpu_cores"]["n"]
            filename_csv = "openclaw_gateway.csv" if component == "openclaw_gateway" else "openshell_sandbox.csv"
            x, cpu, memory = load_series(cgroup / filename_csv, limit)
            values = cpu if metric == "cpu_cores" else memory
            for timestamp, value in zip(x, values):
                buckets.setdefault(int(timestamp / 0.1), []).append(value)

        percentile_lines = {"mean": [], "p50": [], "p95": []}
        times = []
        for bucket in sorted(buckets):
            values = buckets[bucket]
            times.append(bucket * 0.1)
            percentile_lines["mean"].append(statistics.mean(values))
            percentile_lines["p50"].append(statistics.median(values))
            percentile_lines["p95"].append(percentile(values, 0.95))

        for statistic, color in (("mean", "#2563eb"), ("p50", "#0f766e"), ("p95", "#c2410c")):
            fig, axis = plt.subplots(figsize=(12, 5))
            axis.plot(times, percentile_lines[statistic], color=color, linewidth=1.8)
            axis.set_title(f"{title}: {statistic.upper()} time series across tasks")
            axis.set_xlabel("Seconds from task sampler start")
            axis.set_ylabel(ylabel)
            axis.grid(alpha=0.25)
            fig.tight_layout()
            fig.savefig(out / f"{component}_{metric}_{statistic}_timeseries.png", dpi=180)
            plt.close(fig)
    return stats


COMPONENTS = ("openclaw_gateway", "openshell_sandbox")
METRICS = (("cpu_cores", "CPU", "cores"), ("memory_mib", "Memory", "MiB"))


def load_summaries(root: Path) -> list[tuple[Path, dict]]:
    paths = sorted(root.glob("*/prometheus/cgroup-analysis/cgroup_resource_summary.json"))
    if len(paths) != 10:
        raise SystemExit(f"expected 10 task summaries under {root}, found {len(paths)}")
    return [(path.parent.parent.parent.parent, json.loads(path.read_text(encoding="utf-8"))) for path in paths]


def aggregate(summaries: list[tuple[Path, dict]]) -> dict:
    result = {}
    for component in COMPONENTS:
        result[component] = {}
        for metric, _, _ in METRICS:
            entries = [summary[component][metric] for _, summary in summaries]
            total_n = sum(entry["n"] for entry in entries)
            weighted_mean = sum(entry["mean"] * entry["n"] for entry in entries) / total_n
            result[component][metric] = {
                "n": total_n,
                "mean": round(weighted_mean, 4),
                "p95": round(max(entry["p95"] for entry in entries), 4),
            }
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()

    summaries = load_summaries(args.root)
    aggregate_summary = aggregate(summaries)
    args.out.mkdir(parents=True, exist_ok=True)
    percentile_summary = plot_percentiles(args.root, args.out)
    (args.out / "resource_aggregate.json").write_text(
        json.dumps({"task_aggregate": aggregate_summary, "pooled_percentiles": percentile_summary}, indent=2) + "\n", encoding="utf-8"
    )

    lines = [
        "# Dataset-10 Resource Aggregate",
        "",
        "Ten controlled multi-turn tasks. Values report mean, P50, and P95; maximum samples are intentionally excluded. The sandbox values are for the OpenShell agent sandbox container, not the OpenShell gateway.",
        "",
        "| Component | CPU mean | CPU P50 | CPU P95 | Memory mean MiB | Memory P50 MiB | Memory P95 MiB | Samples |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for component in COMPONENTS:
        cpu = percentile_summary[component]["cpu_cores"]
        memory = percentile_summary[component]["memory_mib"]
        lines.append(
            f"| {component} | {cpu['mean']} | {cpu['p50']} | {cpu['p95']} | "
            f"{memory['mean']} | {memory['p50']} | {memory['p95']} | {cpu['n']:,} |"
        )
    lines.extend(
        [
            "",
            "Six separate percentile time-series plots are generated for OpenClaw: CPU and memory, each with mean, P50, and P95. The JSON also retains the sample-weighted task aggregate used for conservative reporting. Memory is cgroup resident memory for the persistent agent-scoped sandbox and is not a per-turn allocation.",
        ]
    )
    (args.out / "RESOURCE_AGGREGATE.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    gateway_cpu = aggregate_summary["openclaw_gateway"]["cpu_cores"]
    gateway_memory = aggregate_summary["openclaw_gateway"]["memory_mib"]
    sandbox_cpu = aggregate_summary["openshell_sandbox"]["cpu_cores"]
    sandbox_memory = aggregate_summary["openshell_sandbox"]["memory_mib"]
    message = f"""# Feedback Request: Multi-Step Agent Profiling on RHOAI

I have completed a controlled multi-step OpenClaw/OpenShell profiling pass using ten replayed agent tasks. The model backend is scripted, so the measurements isolate gateway/orchestration and sandbox behavior rather than model quality or GPU decode.

The analysis keeps internal model/tool cycles visible and correlates them with OpenTelemetry, cgroup-v2 resource samples, and Prometheus metrics. Resource CPU is sampled at 100 ms and cross-checked against exact-pod Prometheus/cAdvisor data. For resource reporting, I am using mean, P50, and P95 rather than maximum.

The ten-task aggregate is:

- OpenClaw gateway: {gateway_cpu['mean']} CPU cores mean, {gateway_cpu['p95']} CPU cores P95, {gateway_memory['mean']} MiB mean, {gateway_memory['p95']} MiB P95.
- OpenShell agent sandbox container: {sandbox_cpu['mean']} CPU cores mean, {sandbox_cpu['p95']} CPU cores P95, {sandbox_memory['mean']} MiB mean, {sandbox_memory['p95']} MiB P95.

These sandbox figures should not be interpreted as OpenShell gateway overhead. In the cAdvisor data, the OpenShell gateway itself remains near idle while the agent sandbox performs the replayed mapped commands. Memory is persistent cgroup resident memory for the agent-scoped sandbox, not memory allocated by each individual turn.

Representative plots:

- `latency_waterfall.png`: orchestration latency broken down by sandbox initialization, context assembly, response processing, and tool execution.
- `otel_turn_metrics.png`: per-turn OpenTelemetry timing and internal-step behavior.
- `study_v3_matched_openshell_overhead.png`: cumulative OpenShell overhead study; this comparison should be treated as exploratory because equal-step matching was insufficient across the full set.

I would appreciate feedback on this approach for analyzing multi-step agents on RHOAI, especially whether the separation between internal agent cycles, sandbox execution, context assembly, and resource usage is useful for performance work, and what additional measurements would make it actionable.
"""
    (args.out / "FEEDBACK_MESSAGE.md").write_text(message, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
