#!/usr/bin/env python3
"""Merge shell and plain OpenClaw runs into the established per-trace layout."""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

from generate_multi_turn_flow import generate as generate_multi_turn_flow
from generate_trace_csvs import export_variant


def copy_tree(source: Path, destination: Path) -> None:
    if source.exists():
        shutil.copytree(source, destination, dirs_exist_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shell", type=Path, required=True)
    parser.add_argument("--plain", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    if args.out.exists():
        shutil.rmtree(args.out)
    args.out.mkdir(parents=True)
    shell_traces = sorted(args.shell.glob("[0-9][0-9]-*"))
    plain_by_session = {path.name.split("-", 1)[1].split("__", 1)[0]: path for path in args.plain.glob("[0-9][0-9]-*")}
    for shell_trace in shell_traces:
        session = shell_trace.name.split("-", 1)[1].split("__", 1)[0]
        destination = args.out / shell_trace.name
        shutil.copytree(shell_trace, destination)
        plain_trace = plain_by_session.get(session)
        if plain_trace:
            copy_tree(plain_trace / "data/shell", destination / "data/openclaw")
            copy_tree(plain_trace / "data/appworld", destination / "data/openclaw/appworld")
            for plot in (plain_trace / "plots-analysis").glob("*.png"):
                name = plot.name.replace("shell_", "openclaw_", 1).replace("openclaw_openshell_execution", "openclaw_tool_execution")
                shutil.copy2(plot, destination / "plots-analysis" / name)
            copy_tree(plain_trace / "data/prometheus", destination / "data/openclaw-prometheus")
        else:
            (destination / "data/openclaw/README.md").parent.mkdir(parents=True, exist_ok=True)
            (destination / "data/openclaw/README.md").write_text("Plain OpenClaw baseline missing for this trace.\n", encoding="utf-8")
        generate_multi_turn_flow(destination, destination / "plots-analysis" / "shell_multi_turn_flow.png")
        export_variant(destination, "shell")
        export_variant(destination, "openclaw")
    (args.out / "EXPERIMENT.md").write_text(
        "# Paired OpenClaw/OpenShell Experiment\n\n"
        "Each trace contains both `data/shell` and `data/openclaw` execution data. "
        "Shell plots are prefixed `shell_`; plain baseline plots are prefixed `openclaw_`.\n",
        encoding="utf-8",
    )
    (args.out / "README.md").write_text(
        "# Paired OpenClaw/OpenShell Experiment\n\n"
        "Each trace contains two real executions against the same AppWorld task: "
        "`data/shell` is OpenClaw through OpenShell and `data/openclaw` is the plain baseline.\n\n"
        "- `plots-analysis/shell_*.png`: OpenShell execution.\n"
        "- `plots-analysis/openclaw_*.png`: plain OpenClaw execution.\n"
        "- `plots-analysis/openshell_overhead_*.png`: direct per-trace comparison.\n"
        "- `plots-analysis/shell_latency_waterfall.png`: primary end-to-end waterfall; "
        "the purple stage includes the real AppWorld HTTP call executed through OpenShell.\n"
        "- `data/appworld/` and `data/openclaw/appworld/`: real AppWorld adapter events and backend-only latency.\n",
        encoding="utf-8",
    )
    (args.out / "experiment.json").write_text(json.dumps({"variants": ["shell", "openclaw"], "trace_count": len(shell_traces)}, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
