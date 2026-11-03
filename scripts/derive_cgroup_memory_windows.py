#!/usr/bin/env python3
"""Derive per-agent one-second memory windows from cgroup-v2 gauge samples."""

from __future__ import annotations

import argparse
import bisect
import csv
import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any


MIB = 1024.0 * 1024.0


class GaugeSeries:
    """Piecewise-linear memory gauge with trapezoidal window integration."""

    def __init__(self, samples: list[tuple[float, float]], max_gap_s: float) -> None:
        self.samples = samples
        self.times = [sample[0] for sample in samples]
        self.prefix_byte_seconds = [0.0]
        self.prefix_covered_s = [0.0]
        self.valid: list[bool] = []

        for (t0, value0), (t1, value1) in zip(samples, samples[1:]):
            duration = t1 - t0
            valid = 0 < duration <= max_gap_s and value0 >= 0 and value1 >= 0
            self.valid.append(valid)
            area = (value0 + value1) * 0.5 * duration if valid else 0.0
            self.prefix_byte_seconds.append(self.prefix_byte_seconds[-1] + area)
            self.prefix_covered_s.append(
                self.prefix_covered_s[-1] + (duration if valid else 0.0)
            )

    def cumulative_at(self, timestamp: float) -> tuple[float, float] | None:
        if not self.times or timestamp < self.times[0] or timestamp > self.times[-1]:
            return None
        index = bisect.bisect_right(self.times, timestamp) - 1
        if index >= len(self.times) - 1:
            return self.prefix_byte_seconds[-1], self.prefix_covered_s[-1]
        if not self.valid[index]:
            return self.prefix_byte_seconds[index], self.prefix_covered_s[index]

        t0, t1 = self.times[index], self.times[index + 1]
        value0, value1 = self.samples[index][1], self.samples[index + 1][1]
        elapsed = timestamp - t0
        interpolated = value0 + (value1 - value0) * elapsed / (t1 - t0)
        area = (value0 + interpolated) * 0.5 * elapsed
        return (
            self.prefix_byte_seconds[index] + area,
            self.prefix_covered_s[index] + elapsed,
        )

    def window(self, start: float, end: float, minimum_coverage: float) -> dict[str, float] | None:
        left = self.cumulative_at(start)
        right = self.cumulative_at(end)
        if left is None or right is None or end <= start:
            return None

        duration = end - start
        covered = max(0.0, right[1] - left[1])
        coverage = min(1.0, covered / duration)
        if coverage + 1e-9 < minimum_coverage:
            return None
        sample_count = bisect.bisect_right(self.times, end) - bisect.bisect_right(
            self.times, start
        )
        mean_bytes = max(0.0, right[0] - left[0]) / duration
        return {
            "memory_mib": mean_bytes / MIB,
            "coverage_fraction": coverage,
            "raw_samples": float(sample_count),
        }


def quantile(values: list[float], probability: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def summarize(values: list[float]) -> dict[str, float | int | None]:
    return {
        "n_windows": len(values),
        "mean_mib": sum(values) / len(values) if values else None,
        "p50_mib": quantile(values, 0.50),
        "p95_mib": quantile(values, 0.95),
        "p99_mib": quantile(values, 0.99),
        "max_mib": max(values) if values else None,
    }


def read_samples(path: Path) -> list[tuple[float, float]]:
    samples: dict[float, float] = {}
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            try:
                timestamp = float(row["epoch_ns"]) / 1_000_000_000.0
                memory_bytes = float(row["memory_bytes"])
            except (KeyError, TypeError, ValueError):
                continue
            if math.isfinite(timestamp) and math.isfinite(memory_bytes) and memory_bytes >= 0:
                samples[timestamp] = memory_bytes
    return sorted(samples.items())


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--max-gap-ms", type=float, default=250.0)
    parser.add_argument("--minimum-coverage", type=float, default=0.99)
    args = parser.parse_args()

    run_dir = args.run_dir.resolve()
    output_dir = args.output_dir.resolve()
    if not run_dir.is_dir():
        parser.error(f"run directory does not exist: {run_dir}")
    if args.max_gap_ms <= 0:
        parser.error("--max-gap-ms must be positive")
    if not 0 < args.minimum_coverage <= 1:
        parser.error("--minimum-coverage must be in (0, 1]")
    if output_dir.exists() and any(output_dir.iterdir()):
        parser.error(f"output directory is not empty; refusing to overwrite: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)

    traces = sorted(
        path for path in (run_dir / "traces").iterdir()
        if path.is_dir() and not path.name.startswith("_")
    )
    rows: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []
    issues: list[str] = []
    metadata_fallbacks: list[str] = []

    for trace_dir in traces:
        prom_dir = trace_dir / "data/shell/prometheus"
        manifest_path = prom_dir / "collect_manifest.json"
        trace_path = trace_dir / "trace.json"
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
            trace_meta = json.loads(trace_path.read_text(encoding="utf-8")) if trace_path.exists() else {}
        except (OSError, json.JSONDecodeError) as exc:
            issues.append(f"{trace_dir.name}: metadata could not be read ({exc})")
            continue

        try:
            bounds = (float(manifest["start"]), float(manifest["end"]))
            if bounds[1] <= bounds[0]:
                bounds = None
        except (KeyError, TypeError, ValueError):
            bounds = None
        if bounds is None or not trace_meta:
            metadata_fallbacks.append(trace_dir.name)

        identity = {
            "trace_id": trace_dir.name,
            "job": trace_meta.get("job", "unknown"),
            "agent": trace_meta.get("agent", trace_dir.name),
            "workload": trace_dir.name.partition("__")[2] or "unknown",
        }
        cgroup_dir = prom_dir / "cgroup"
        sandbox_files = sorted((cgroup_dir / "sandbox_samples").glob("*.csv"))
        sources = {
            "openclaw_gateway": cgroup_dir / "openclaw_gateway.csv",
            "openshell_sandbox": sandbox_files[0] if len(sandbox_files) == 1 else None,
        }
        if len(sandbox_files) != 1:
            issues.append(f"{trace_dir.name}: expected one sandbox cgroup CSV; found {len(sandbox_files)}")

        for component, source in sources.items():
            if source is None or not source.exists():
                issues.append(f"{trace_dir.name}: missing {component} memory source")
                continue
            samples = read_samples(source)
            if len(samples) < 2:
                issues.append(f"{trace_dir.name}: fewer than two memory readings in {source.name}")
                continue
            active_start, active_end = bounds or (samples[0][0], samples[-1][0])
            series = GaugeSeries(samples, args.max_gap_ms / 1000.0)
            per_agent_values: list[float] = []
            for start in range(math.ceil(active_start), math.floor(active_end)):
                value = series.window(float(start), float(start + 1), args.minimum_coverage)
                if value is None:
                    continue
                row = {
                    **identity,
                    "component": component,
                    "window_start_epoch_s": start,
                    "window_end_epoch_s": start + 1,
                    "window_seconds": 1,
                    **value,
                }
                rows.append(row)
                per_agent_values.append(float(value["memory_mib"]))
            summaries.append({**identity, "component": component, **summarize(per_agent_values)})

    write_csv(
        output_dir / "cgroup_memory_1s.csv",
        ["trace_id", "job", "agent", "workload", "component", "window_start_epoch_s", "window_end_epoch_s", "window_seconds", "memory_mib", "coverage_fraction", "raw_samples"],
        rows,
    )
    write_csv(
        output_dir / "memory_p95_by_agent.csv",
        ["trace_id", "job", "agent", "workload", "component", "n_windows", "mean_mib", "p50_mib", "p95_mib", "p99_mib", "max_mib"],
        summaries,
    )
    summary = {
        "schema": "agentic.cgroup-memory-window-analysis.v1",
        "run_dir": str(run_dir),
        "trace_count": len(traces),
        "one_second_window_count": len(rows),
        "source": "raw cgroup-v2 memory_bytes gauge samples",
        "method": "trapezoidal integration of linearly interpolated memory gauge samples over Unix-second bins",
        "max_gap_ms_accepted": args.max_gap_ms,
        "minimum_window_coverage": args.minimum_coverage,
        "metadata_fallback_traces": metadata_fallbacks,
        "issues": issues,
        "caveats": [
            "Each p95 is computed within one agent's valid one-second mean-memory windows; it is not a peak or a turn-level metric.",
            "OpenShell gateway cgroup samples are not present here; a separate Prometheus series for container=openshell-gateway is available and must be analyzed separately from the per-agent cgroup data.",
        ],
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    readme = f"""# Per-agent cgroup memory windows

Derived from `{run_dir.name}` without modifying the raw cgroup samples. `cgroup_memory_1s.csv` contains one-second time-weighted average memory for each trace's OpenClaw gateway and OpenShell sandbox. `memory_p95_by_agent.csv` summarizes those one-second means per trace and component.

The memory gauge is integrated with trapezoidal interpolation; intervals longer than {args.max_gap_ms:g} ms are ignored and windows require at least {args.minimum_coverage:.1%} coverage. The p95 is therefore a per-agent percentile across observed one-second windows, not instantaneous peak memory.

This directory contains only per-agent cgroup samples. The OpenShell gateway has no cgroup sample here; its separate Prometheus series (`container=openshell-gateway`) is available in the saved run data and is handled separately from these per-agent windows.
"""
    (output_dir / "README.md").write_text(readme, encoding="utf-8")

    print(f"Wrote {len(rows)} one-second memory windows for {len(traces)} traces to {output_dir}")
    print(f"Agent/component summaries: {len(summaries)}; issues: {len(issues)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
