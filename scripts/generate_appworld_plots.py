#!/usr/bin/env python3
"""Generate per-variant AppWorld backend latency waterfalls."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def plot_events(events_path: Path, output: Path, title: str) -> None:
    import matplotlib.pyplot as plt

    if not events_path.exists():
        return
    events = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines() if line]
    if not events:
        return
    labels = []
    values = []
    colors = []
    palette = {}
    colorset = ["#1685c4", "#079669", "#7738e8", "#df7a00", "#c2418c", "#5b8c5a"]
    for index, event in enumerate(events):
        tool = str(event.get("tool", "unknown"))
        short = tool.removeprefix("mcp__environment__")
        app = short.split("__", 1)[0]
        palette.setdefault(app, colorset[len(palette) % len(colorset)])
        labels.append(f"Step {index}: {short}")
        values.append(float(event.get("elapsed_ms") or 0))
        colors.append(palette[app])
    figure_height = max(4.5, min(16, 0.34 * len(labels) + 1.8))
    figure, axis = plt.subplots(figsize=(12, figure_height))
    y = list(range(len(labels)))
    axis.barh(y, values, color=colors, edgecolor="white")
    axis.set_yticks(y, labels)
    axis.invert_yaxis()
    axis.set_title(title, loc="left", fontsize=15, pad=28)
    axis.text(0, 1.02, "Actual AppWorld backend API latency per tool call", transform=axis.transAxes, fontsize=9, color="#596579")
    axis.set_xlabel("AppWorld API latency [ms]")
    axis.grid(axis="x", alpha=0.25, linestyle=":")
    for index, value in enumerate(values):
        axis.text(value + max(values) * 0.012, index, f"{value:.1f} ms", va="center", fontsize=8)
    figure.tight_layout()
    output.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(output, dpi=160)
    plt.close(figure)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment", type=Path, required=True)
    args = parser.parse_args()
    for trace in sorted(args.experiment.glob("[0-9][0-9]-*")):
        plots = trace / "plots-analysis"
        session = trace.name.split("-", 1)[1].split("__", 1)[0]
        plot_events(trace / "data/appworld/events.jsonl", trace / "data/appworld/appworld_api_latency.png", f"Shell AppWorld API latency: {session}")
        plot_events(trace / "data/openclaw/appworld/events.jsonl", trace / "data/openclaw/appworld/appworld_api_latency.png", f"Plain OpenClaw AppWorld API latency: {session}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
