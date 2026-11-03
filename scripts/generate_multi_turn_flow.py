#!/usr/bin/env python3
"""Generate a compact lane diagram plus per-turn waterfall for a trace."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def load(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def generate(trace: Path, output: Path) -> None:
    import matplotlib.pyplot as plt
    from matplotlib.patches import FancyArrowPatch, Patch

    timing = load(trace / "data/shell/traces/timing_segments.json")
    per_turn = load(trace / "data/shell/traces/per_turn_segments.json")
    per_tool = load(trace / "data/shell/traces/per_tool_segments.json")
    event_path = trace / "data/appworld/events.jsonl"
    appworld_events = [json.loads(line) for line in event_path.read_text().splitlines() if line] if event_path.exists() else []

    tool_durations = [float(row.get("duration_ms") or 0) for row in per_tool]
    tool_cursor = 0
    turn_rows = []
    for index, row in enumerate(timing):
        count = int((per_turn[index].get("tool_calls_this_turn") if index < len(per_turn) else 0) or 0)
        tools = sum(tool_durations[tool_cursor:tool_cursor + count]) if count else float(row.get("sandbox_exec_ms") or 0)
        tool_cursor += count
        turn_rows.append({
            "context": float(row.get("context_assembly_ms") or 0),
            "response": float(row.get("response_processing_ms") or 0),
            "model": float(row.get("model_call_total_ms") or 0),
            "tools": tools,
            "tool_count": count,
        })

    figure = plt.figure(figsize=(15, 11))
    layout = figure.add_gridspec(2, 1, height_ratios=[1.0, max(2.4, min(5.5, len(turn_rows) * 0.16))], hspace=0.3)
    flow = figure.add_subplot(layout[0])
    lanes = [("Agent / OpenClaw", 3), ("LLM", 2), ("OpenShell", 1), ("AppWorld", 0)]
    for label, y in lanes:
        flow.axhline(y, color="#d8dee9", linewidth=1)
        flow.text(-0.02, y, label, ha="right", va="center", fontsize=10, fontweight="bold", transform=flow.get_yaxis_transform())
    flow.set_xlim(0, 10)
    flow.set_ylim(-0.55, 3.55)
    flow.set_xticks([])
    flow.set_yticks([])
    flow.set_title("Multi-turn agent flow: one cycle, repeated across the trace", loc="left", fontsize=16, pad=18)
    flow.text(0, 1.02, f"Trace contains {len(turn_rows)} turns and {len(appworld_events)} real AppWorld API events", transform=flow.transAxes, fontsize=10, color="#596579")

    arrows = [
        ("state + tools", 3, 2, 0.8, 2.0, "#1685c4"),
        ("tool decision", 2, 3, 2.2, 3.4, "#079669"),
        ("exec command", 3, 1, 4.0, 5.2, "#7738e8"),
        ("HTTP /execute", 1, 0, 5.7, 6.9, "#df7a00"),
        ("API result", 0, 1, 7.0, 8.1, "#c2418c"),
        ("exec result", 1, 3, 8.3, 9.5, "#079669"),
    ]
    for text, y0, y1, x0, x1, color in arrows:
        flow.add_patch(FancyArrowPatch((x0, y0), (x1, y1), arrowstyle="->", mutation_scale=15, linewidth=2, color=color))
        flow.text((x0 + x1) / 2, (y0 + y1) / 2 + 0.12, text, ha="center", va="bottom", fontsize=9, color="#303846")
    flow.text(5.0, -0.35, "The same order repeats: LLM state -> agent decision -> OpenShell exec -> AppWorld -> agent", ha="center", fontsize=10, color="#596579")

    waterfall = figure.add_subplot(layout[1])
    y = list(range(len(turn_rows)))
    context = [row["context"] for row in turn_rows]
    model = [max(row["model"] - row["context"] - row["response"] - row["tools"], 0) for row in turn_rows]
    response = [row["response"] for row in turn_rows]
    tools = [row["tools"] for row in turn_rows]
    left = [0.0] * len(y)
    for label, values, color in (
        ("Context assembly", context, "#1685c4"),
        ("LLM", model, "#df7a00"),
        ("Response processing", response, "#079669"),
        ("OpenShell tool execution", tools, "#7738e8"),
    ):
        waterfall.barh(y, values, left=left, color=color, edgecolor="white", label=label)
        left = [left[index] + value for index, value in enumerate(values)]
    waterfall.set_yticks(y, [f"Turn {index}" for index in y])
    waterfall.invert_yaxis()
    waterfall.set_xlabel("Per-turn latency [ms]")
    waterfall.set_ylabel("Turn")
    waterfall.set_title("Per-turn timing overview", loc="left", fontsize=13)
    waterfall.grid(axis="x", alpha=0.25, linestyle=":")
    waterfall.legend(loc="upper right", ncol=2)
    figure.savefig(output, dpi=160, bbox_inches="tight")
    plt.close(figure)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment", type=Path, required=True)
    args = parser.parse_args()
    for trace in sorted(args.experiment.glob("[0-9][0-9]-*")):
        generate(trace, trace / "plots-analysis" / "shell_multi_turn_flow.png")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
