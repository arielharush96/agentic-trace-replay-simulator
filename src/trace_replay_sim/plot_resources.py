"""Generate resource utilization plots from Prometheus data.

Reads JSON files produced by collect.py and generates time-series plots for
CPU and memory usage per component (OpenClaw, OpenShell, sandbox, mock-LLM).

Usage:
  python3 -m trace_replay_sim.plot_resources \
    --prom results/prometheus \
    --out results/diagrams
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timezone


def _load_prom_series(path: Path) -> list[tuple[float, float]]:
    """Load Prometheus query_range result as [(timestamp, value), ...]."""
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return []
    if not data or data.get("status") == "error":
        return []
    points: list[tuple[float, float]] = []
    for series in (data.get("data") or {}).get("result") or []:
        for ts, val in series.get("values") or []:
            try:
                points.append((float(ts), float(val)))
            except (TypeError, ValueError):
                continue
    points.sort(key=lambda p: p[0])
    return points


def _plot_time_series(
    series_list: list[tuple[str, list[tuple[float, float]]]],
    title: str,
    ylabel: str,
    out_path: Path,
    scale: float = 1.0,
) -> bool:
    """Plot multiple time series on one axis. Returns True if any data was plotted."""
    has_data = any(len(s) > 0 for _, s in series_list)
    if not has_data:
        return False

    fig, ax = plt.subplots(figsize=(12, 5))
    colors = ["#3B82F6", "#10B981", "#F59E0B", "#EF4444", "#8B5CF6", "#EC4899"]

    for i, (label, points) in enumerate(series_list):
        if not points:
            continue
        times = [datetime.fromtimestamp(t, tz=timezone.utc) for t, _ in points]
        values = [v * scale for _, v in points]
        ax.plot(times, values, label=label, color=colors[i % len(colors)], linewidth=1.5)

    ax.set_title(title)
    ax.set_ylabel(ylabel)
    ax.set_xlabel("Time")
    ax.legend(loc="upper right")
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    fig.autofmt_xdate()

    plt.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out_path}")
    return True


def generate_cpu_plot(prom_dir: Path, out_path: Path) -> bool:
    series = [
        ("OpenClaw", _load_prom_series(prom_dir / "cpu_openclaw.json")),
        ("OpenShell", _load_prom_series(prom_dir / "cpu_openshell.json")),
        ("Sandbox", _load_prom_series(prom_dir / "cpu_sandbox.json")),
        ("Mock LLM", _load_prom_series(prom_dir / "cpu_mock_llm.json")),
    ]
    return _plot_time_series(series, "CPU Usage Over Time", "CPU Cores", out_path)


def generate_memory_plot(prom_dir: Path, out_path: Path) -> bool:
    series = [
        ("OpenClaw", _load_prom_series(prom_dir / "memory_openclaw.json")),
        ("OpenShell", _load_prom_series(prom_dir / "memory_openshell.json")),
        ("Sandbox", _load_prom_series(prom_dir / "memory_sandbox.json")),
        ("Mock LLM", _load_prom_series(prom_dir / "memory_mock_llm.json")),
    ]
    return _plot_time_series(series, "Memory Usage Over Time", "MiB", out_path, scale=1/(1024*1024))


def generate_openclaw_app_plots(prom_dir: Path, out_dir: Path) -> int:
    """Generate plots from OpenClaw's internal application metrics."""
    app_dir = prom_dir / "openclaw_app_1s"
    if not app_dir.exists():
        return 0

    plots_generated = 0

    # Model call latency
    series = [
        ("P50", _load_prom_series(app_dir / "oc_model_call_p50.json")),
        ("P95", _load_prom_series(app_dir / "oc_model_call_p95.json")),
    ]
    if _plot_time_series(series, "OpenClaw Model Call Latency", "Seconds", out_dir / "oc_model_call_latency.png"):
        plots_generated += 1

    # Tool execution latency
    series = [
        ("P50", _load_prom_series(app_dir / "oc_tool_exec_p50.json")),
        ("P95", _load_prom_series(app_dir / "oc_tool_exec_p95.json")),
    ]
    if _plot_time_series(series, "OpenClaw Tool Execution Latency", "Seconds", out_dir / "oc_tool_exec_latency.png"):
        plots_generated += 1

    # Queue wait
    series = [
        ("P50", _load_prom_series(app_dir / "oc_queue_wait_p50.json")),
        ("P95", _load_prom_series(app_dir / "oc_queue_wait_p95.json")),
    ]
    if _plot_time_series(series, "OpenClaw Queue Wait Time", "Seconds", out_dir / "oc_queue_wait.png"):
        plots_generated += 1

    # Memory
    series = [
        ("RSS", _load_prom_series(app_dir / "oc_memory_rss.json")),
        ("Heap", _load_prom_series(app_dir / "oc_memory_heap.json")),
    ]
    if _plot_time_series(series, "OpenClaw Process Memory", "MiB", out_dir / "oc_process_memory.png", scale=1/(1024*1024)):
        plots_generated += 1

    # Harness run duration
    series = [
        ("P50", _load_prom_series(app_dir / "oc_harness_p50.json")),
        ("P95", _load_prom_series(app_dir / "oc_harness_p95.json")),
    ]
    if _plot_time_series(series, "OpenClaw Harness Run Duration (Session E2E)", "Seconds", out_dir / "oc_harness_duration.png"):
        plots_generated += 1

    return plots_generated


def generate_all(prom_dir: Path, out_dir: Path) -> dict[str, Any]:
    """Generate all resource plots. Returns summary of what was generated."""
    out_dir.mkdir(parents=True, exist_ok=True)
    summary: dict[str, Any] = {"plots_generated": [], "no_data": []}

    print("Generating resource utilization plots...")

    if generate_cpu_plot(prom_dir, out_dir / "resource_cpu.png"):
        summary["plots_generated"].append("resource_cpu.png")
    else:
        summary["no_data"].append("cpu (cAdvisor)")
        print("  WARNING: No CPU data from Prometheus")

    if generate_memory_plot(prom_dir, out_dir / "resource_memory.png"):
        summary["plots_generated"].append("resource_memory.png")
    else:
        summary["no_data"].append("memory (cAdvisor)")
        print("  WARNING: No memory data from Prometheus")

    app_count = generate_openclaw_app_plots(prom_dir, out_dir)
    if app_count > 0:
        summary["plots_generated"].append(f"{app_count} OpenClaw app metric plots")
    else:
        summary["no_data"].append("OpenClaw app metrics (all empty)")
        print("  WARNING: No OpenClaw app metrics data")

    (out_dir / "plot_summary.json").write_text(json.dumps(summary, indent=2))
    print(f"  Total: {len(summary['plots_generated'])} plots, {len(summary['no_data'])} missing")
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate resource plots from Prometheus data")
    parser.add_argument("--prom", required=True, help="Prometheus data directory")
    parser.add_argument("--out", required=True, help="Output directory for PNGs")
    args = parser.parse_args(argv)
    generate_all(Path(args.prom), Path(args.out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
