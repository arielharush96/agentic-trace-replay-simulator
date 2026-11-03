#!/usr/bin/env python3
"""Format an experiment exactly like the established dataset result layout."""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path


def copy_tree(source: Path, destination: Path) -> None:
    if source.exists():
        shutil.copytree(source, destination, dirs_exist_ok=True)


def format_experiment(root: Path, corpus: Path | None) -> None:
    traces_root = root / "traces"
    trace_dirs = sorted(path for path in traces_root.iterdir() if path.is_dir())
    root_metadata = json.loads((root / "experiment.json").read_text(encoding="utf-8"))
    root_metadata["layout"] = "legacy-dataset-trace-first"
    root_metadata["trace_count"] = len(trace_dirs)
    (root / "EXPERIMENT.md").write_text(
        "# AppWorld OpenClaw/OpenShell Experiment\n\n"
        "Each top-level directory is one independent trace. The structure matches "
        "the established dataset experiment layout.\n\n"
        "- `plots-analysis/`: named plots for this trace.\n"
        "- `data/`: shell, Prometheus, AppWorld, and analysis artifacts.\n"
        "- `logs/`: execution logs.\n"
        "- `manifests/`: experiment metadata and reproduction information.\n",
        encoding="utf-8",
    )
    (root / "README.md").write_text(
        "# Experiment\n\n"
        "This result uses the established dataset layout. Each top-level directory is one independent trace.\n\n"
        "- `plots-analysis/`: named plots for the trace.\n"
        "- `data/shell/`: OpenClaw through OpenShell data, with `profile-v2/` and `traces/` subdirectories.\n"
        "- `data/prometheus/`: resource measurements sliced to the trace interval.\n"
        "- `data/appworld/`: AppWorld evidence when exported by the runner.\n"
        "- `data/openclaw-plain/`: plain OpenClaw baseline, when run.\n"
        "- `logs/`: trace logs.\n"
        "- `manifests/`: trace metadata and reproduction information.\n",
        encoding="utf-8",
    )
    (root / "run.log").write_text(
        f"Trace-first experiment formatted with {len(trace_dirs)} traces.\n",
        encoding="utf-8",
    )
    (root / "experiment.json").write_text(json.dumps(root_metadata, indent=2) + "\n", encoding="utf-8")

    corpus_sessions = {}
    if corpus and corpus.exists():
        corpus_sessions = {
            str(session.get("session_id")): len(session.get("turns") or [])
            for session in json.loads(corpus.read_text(encoding="utf-8")).get("sessions", [])
        }
        trace_dirs.sort(key=lambda path: next(
            (session_id for session_id in corpus_sessions if session_id in path.name),
            path.name,
        ))
    for index, source in enumerate(trace_dirs):
        summary = json.loads((source / "analysis" / "summary.json").read_text(encoding="utf-8"))
        session_id = summary["session_id"]
        turns = corpus_sessions.get(session_id, summary.get("turns", 0))
        destination = root / f"{index:02d}-{session_id}__{turns}-corpus-turns"
        plots = destination / "plots-analysis"
        data = destination / "data"
        logs = destination / "logs"
        manifests = destination / "manifests"
        for directory in (plots, data, logs, manifests):
            directory.mkdir(parents=True, exist_ok=True)

        shell_source = source / "shell"
        shell_data = data / "shell"
        shell_traces = shell_data / "traces"
        shell_profile = shell_data / "profile-v2"
        shell_traces.mkdir(parents=True, exist_ok=True)
        shell_profile.mkdir(parents=True, exist_ok=True)
        for name in ("driver_request.json",):
            source_file = shell_source / name
            if source_file.exists():
                (shell_data / "driver_requests.jsonl").write_text(source_file.read_text(encoding="utf-8") + "\n", encoding="utf-8")
        for name in ("per_tool_segments.json", "per_turn_segments.json", "timing_segments.json", "session_summary.json"):
            source_file = shell_source / name
            if source_file.exists():
                shutil.copy2(source_file, shell_traces / name)
        raw_trace = shell_source / "raw_trace.json"
        if raw_trace.exists():
            raw_payload = json.loads(raw_trace.read_text(encoding="utf-8"))
            (shell_traces / "raw_traces.json").write_text(json.dumps([raw_payload], indent=2) + "\n", encoding="utf-8")
        for name in ("per_turn.json", "per_tool.json", "summary.json"):
            source_file = source / "analysis" / name
            if source_file.exists():
                shutil.copy2(source_file, shell_profile / name)
        profile_plots = shell_profile / "diagrams"
        profile_plots.mkdir(parents=True, exist_ok=True)
        for plot in (source / "analysis" / "plots").glob("*.png"):
            target_name = plot.name.removeprefix("shell_")
            shutil.copy2(plot, profile_plots / target_name)
        for name in ("appworld", "prometheus", "openclaw-plain"):
            copy_tree(source / name, data / name)

        source_plots = source / "analysis" / "plots"
        for plot in source_plots.glob("*.png"):
            shutil.copy2(plot, plots / f"shell_{plot.name}")

        (source / "README.md").write_text(
            f"# Trace {session_id}\n\n"
            f"This trace contains {turns} model turns and {summary.get('tool_calls', 0)} tool calls.\n"
            "See `plots-analysis/` for plots and `data/` for raw measurements.\n",
            encoding="utf-8",
        )
        shutil.copy2(source / "README.md", destination / "README.md")
        shutil.copy2(source / "analysis" / "summary.json", manifests / "trace_summary.json")
        (manifests / "experiment_parameters.json").write_text(
            json.dumps(root_metadata | {"session_id": session_id, "trace_index": index}, indent=2) + "\n",
            encoding="utf-8",
        )
        (manifests / "reproduce_command.txt").write_text(
            "See the experiment root command and input corpus metadata.\n",
            encoding="utf-8",
        )
        (logs / "README.md").write_text(
            "Trace-specific logs are exported here when the runner captures them.\n",
            encoding="utf-8",
        )

        shutil.rmtree(source)

    traces_root.rmdir()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--corpus", type=Path)
    args = parser.parse_args()
    format_experiment(args.root, args.corpus)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
