#!/usr/bin/env python3
"""Normalize one corpus result into a reproducible four-directory layout."""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path


def copy_tree_without_png(source: Path, destination: Path) -> None:
    if not source.exists():
        return
    for path in source.rglob("*"):
        relative = path.relative_to(source)
        target = destination / relative
        if path.is_dir():
            target.mkdir(parents=True, exist_ok=True)
        elif path.suffix.lower() != ".png":
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)


def copy_plots(source: Path, destination: Path, prefix: str) -> None:
    if not source.exists():
        return
    for path in source.rglob("*.png"):
        destination.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, destination / f"{prefix}_{path.name}")


def copy_logs(source: Path, destination: Path, prefix: str) -> None:
    if not source.exists():
        return
    for path in source.rglob("*.log"):
        destination.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, destination / f"{prefix}_{path.name}")


def snapshot_files(root: Path, destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    for relative in (
        "scripts/run_profiler_v2.sh",
        "scripts/run_dataset_100ms.sh",
        "scripts/run_matched_openclaw.sh",
        "scripts/run_matched_dataset.sh",
        "scripts/cgroup_sampler.mjs",
        "scripts/cgroup_sampler.sh",
        "scripts/resource_analysis_v3.py",
        "scripts/aggregate_resource_analysis.py",
        "src/trace_replay_sim/profiler_v2.py",
        "src/trace_replay_sim/collect.py",
        "src/trace_replay_sim/study_v3.py",
        "pyproject.toml",
    ):
        source = root / relative
        if source.exists():
            target = destination / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
    k8s_source = root / "k8s"
    if k8s_source.exists():
        shutil.copytree(k8s_source, destination / "k8s", dirs_exist_ok=True)


def organize(root: Path, task: Path, corpus: Path | None, target_node: str, command: str) -> None:
    plots = task / "plots-analysis"
    logs = task / "logs"
    data = task / "data"
    manifests = task / "manifests"
    for directory in (plots, logs, data, manifests):
        directory.mkdir(exist_ok=True)

    layers = [path for path in (task / "shell", task / "openclaw") if path.exists()]
    for layer in layers:
        name = layer.name
        copy_plots(layer / "profile-v2" / "diagrams", plots, name)
        copy_plots(layer / "resource-study" / "diagrams", plots, f"{name}_resource")
        copy_plots(layer / "prometheus" / "cgroup-analysis" / "diagrams", plots, f"{name}_cgroup")
        copy_tree_without_png(layer / "traces", data / name / "traces")
        copy_tree_without_png(layer / "profile-v2", data / name / "profile-v2")
        copy_tree_without_png(layer / "prometheus", data / name / "prometheus")
        for filename in ("summary.json", "mock_edges.jsonl", "driver_requests.jsonl"):
            source = layer / filename
            if source.exists():
                (data / name).mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, data / name / filename)
        copy_logs(layer, logs, name)

    for source in (task / "prometheus", task / "resource-study"):
        copy_tree_without_png(source, data / source.name)
        copy_plots(source / "diagrams", plots, source.name)

    if (root / "run.log").exists():
        shutil.copy2(root / "run.log", logs / "dataset_run.log")

    if corpus and corpus.exists():
        shutil.copy2(corpus, data / "input_corpus.json")
    snapshot_files(root, manifests)
    provenance = task / "HF_PROVENANCE.md"
    if provenance.exists():
        shutil.copy2(provenance, manifests / "HF_PROVENANCE.md")
    metadata = {
        "organized_at": datetime.now(timezone.utc).isoformat(),
        "corpus": str(corpus) if corpus else None,
        "task_directory": task.name,
        "target_node": target_node,
        "command": command,
        "layout": {
            "plots-analysis": "PNG plots and analysis visualizations",
            "logs": "driver and execution logs",
            "data": "CSV, JSON, JSONL, traces, and Prometheus data",
            "manifests": "Kubernetes manifests, source snapshots, and reproduction metadata",
        },
    }
    (manifests / "experiment_parameters.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    (manifests / "reproduce_command.txt").write_text(command + "\n", encoding="utf-8")

    for layer in layers:
        shutil.rmtree(layer)
    for directory in (task / "prometheus", task / "resource-study"):
        if directory.exists():
            shutil.rmtree(directory)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--task", type=Path, required=True)
    parser.add_argument("--corpus", type=Path)
    parser.add_argument("--target-node", required=True)
    parser.add_argument("--command", required=True)
    args = parser.parse_args()
    organize(args.root, args.task, args.corpus, args.target_node, args.command)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
