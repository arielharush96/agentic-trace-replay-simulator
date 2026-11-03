#!/usr/bin/env python3
"""Derive aligned 1s and trailing-60s CPU series from cumulative cgroup-v2 data.

The source CSVs are preserved. Output windows are integrated from consecutive
usage_usec counter deltas, weighted by elapsed time, and emitted only when at
least the configured fraction of the window is covered by valid samples.
"""

from __future__ import annotations

import argparse
import bisect
import csv
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any


WINDOWS = (1, 60)
COMPONENTS = ("openclaw_gateway", "openshell_sandbox")
CSV_FIELDS = (
    "trace_id",
    "job",
    "agent",
    "workload",
    "component",
    "window_start_epoch_s",
    "window_end_epoch_s",
    "window_seconds",
    "cpu_cores",
    "coverage_fraction",
    "raw_samples",
)


class CounterSeries:
    """Piecewise-linear cumulative CPU counter with explicit valid coverage."""

    def __init__(self, samples: list[tuple[float, float]], max_gap_s: float) -> None:
        self.samples = samples
        self.times = [sample[0] for sample in samples]
        self.prefix_cpu_usec = [0.0]
        self.prefix_covered_s = [0.0]
        self.interval_valid: list[bool] = []
        self.interval_delta_usec: list[float] = []

        for (t0, u0), (t1, u1) in zip(samples, samples[1:]):
            dt = t1 - t0
            delta = u1 - u0
            valid = 0 < dt <= max_gap_s and delta >= 0
            self.interval_valid.append(valid)
            self.interval_delta_usec.append(delta if valid else 0.0)
            self.prefix_cpu_usec.append(
                self.prefix_cpu_usec[-1] + (delta if valid else 0.0)
            )
            self.prefix_covered_s.append(
                self.prefix_covered_s[-1] + (dt if valid else 0.0)
            )

    def cumulative_at(self, timestamp: float) -> tuple[float, float] | None:
        """Return integrated CPU usec and valid elapsed seconds through timestamp."""
        if not self.times or timestamp < self.times[0] or timestamp > self.times[-1]:
            return None
        index = bisect.bisect_right(self.times, timestamp) - 1
        if index >= len(self.times) - 1:
            return self.prefix_cpu_usec[-1], self.prefix_covered_s[-1]

        t0, t1 = self.times[index], self.times[index + 1]
        if not self.interval_valid[index]:
            return self.prefix_cpu_usec[index], self.prefix_covered_s[index]

        elapsed = timestamp - t0
        fraction = elapsed / (t1 - t0)
        return (
            self.prefix_cpu_usec[index] + self.interval_delta_usec[index] * fraction,
            self.prefix_covered_s[index] + elapsed,
        )

    def window(self, start: float, end: float, minimum_coverage: float) -> dict[str, float] | None:
        left = self.cumulative_at(start)
        right = self.cumulative_at(end)
        if left is None or right is None or end <= start:
            return None

        duration = end - start
        cpu_usec = max(0.0, right[0] - left[0])
        covered_s = max(0.0, right[1] - left[1])
        coverage = min(1.0, covered_s / duration)
        if coverage + 1e-9 < minimum_coverage:
            return None

        raw_samples = bisect.bisect_right(self.times, end) - bisect.bisect_right(
            self.times, start
        )
        return {
            "cpu_cores": cpu_usec / (duration * 1_000_000.0),
            "coverage_fraction": coverage,
            "raw_samples": float(raw_samples),
        }


def load_samples(path: Path) -> list[tuple[float, float]]:
    samples: list[tuple[float, float]] = []
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            try:
                timestamp = float(row["epoch_ns"]) / 1_000_000_000.0
                usage_usec = float(row["cpu_usage_usec"])
            except (KeyError, TypeError, ValueError):
                continue
            if math.isfinite(timestamp) and math.isfinite(usage_usec) and usage_usec >= 0:
                samples.append((timestamp, usage_usec))

    # Keep one reading per timestamp and sort defensively; do not alter the raw CSV.
    by_timestamp: dict[float, float] = {}
    for timestamp, usage_usec in samples:
        by_timestamp[timestamp] = usage_usec
    return sorted(by_timestamp.items())


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
        "mean_cores": statistics.fmean(values) if values else None,
        "p50_cores": quantile(values, 0.50),
        "p95_cores": quantile(values, 0.95),
        "p99_cores": quantile(values, 0.99),
        "max_cores": max(values) if values else None,
    }


def load_prometheus_values(path: Path, container: str, pod: str | None = None) -> tuple[dict[int, float], str | None, str | None]:
    if not path.exists():
        return {}, None, None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}, None, None

    values: dict[int, float] = {}
    jobs: set[str] = set()
    matched_pods: set[str] = set()
    for series in (payload.get("data") or {}).get("result") or []:
        labels = series.get("metric") or {}
        if labels.get("container") != container:
            continue
        if pod is not None and labels.get("pod") != pod:
            continue
        if labels.get("job"):
            jobs.add(str(labels["job"]))
        if labels.get("pod"):
            matched_pods.add(str(labels["pod"]))
        for timestamp, raw_value in series.get("values") or []:
            try:
                value = float(raw_value)
                if math.isfinite(value):
                    values[int(round(float(timestamp)))] = value
            except (TypeError, ValueError):
                continue
    return values, ";".join(sorted(jobs)) or None, ";".join(sorted(matched_pods)) or None


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True, help="100-agent result directory")
    parser.add_argument("--output-dir", type=Path, help="defaults to RUN_DIR/analysis/cgroup-cpu-windows-v1")
    parser.add_argument("--max-gap-ms", type=float, default=250.0, help="ignore counter intervals longer than this")
    parser.add_argument("--minimum-coverage", type=float, default=0.99, help="minimum window coverage, from 0 to 1")
    args = parser.parse_args()

    run_dir = args.run_dir.resolve()
    output_dir = (args.output_dir or run_dir / "analysis" / "cgroup-cpu-windows-v1").resolve()
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
    if not traces:
        parser.error(f"no trace directories found under {run_dir / 'traces'}")

    per_window_rows: dict[int, list[dict[str, Any]]] = {1: [], 60: []}
    per_trace_rows: list[dict[str, Any]] = []
    prometheus_rows: list[dict[str, Any]] = []
    raw_gap_ms: list[float] = []
    trace_errors: list[str] = []
    metadata_fallbacks: list[str] = []

    for trace_dir in traces:
        manifest_path = trace_dir / "data/shell/prometheus/collect_manifest.json"
        trace_json_path = trace_dir / "trace.json"
        cgroup_dir = trace_dir / "data/shell/prometheus/cgroup"
        manifest: dict[str, Any] = {}
        trace_meta: dict[str, Any] = {}
        try:
            if manifest_path.exists():
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            if trace_json_path.exists():
                trace_meta = json.loads(trace_json_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            trace_errors.append(f"{trace_dir.name}: invalid metadata JSON ({exc})")
            continue
        try:
            manifest_bounds = (float(manifest["start"]), float(manifest["end"]))
            if manifest_bounds[1] <= manifest_bounds[0]:
                manifest_bounds = None
        except (KeyError, TypeError, ValueError):
            manifest_bounds = None
        if manifest_bounds is None or not trace_meta:
            metadata_fallbacks.append(trace_dir.name)

        workload = trace_dir.name.partition("__")[2] or "unknown"
        identity = {
            "trace_id": trace_dir.name,
            "job": trace_meta.get("job", "unknown"),
            "agent": trace_meta.get("agent", trace_dir.name),
            "workload": workload,
        }
        sandbox_files = sorted((cgroup_dir / "sandbox_samples").glob("*.csv"))
        source_paths = {
            "openclaw_gateway": cgroup_dir / "openclaw_gateway.csv",
            "openshell_sandbox": sandbox_files[0] if len(sandbox_files) == 1 else None,
        }
        if len(sandbox_files) != 1:
            trace_errors.append(f"{trace_dir.name}: expected exactly one sandbox sample CSV; found {len(sandbox_files)}")

        for component, source in source_paths.items():
            if source is None or not source.exists():
                trace_errors.append(f"{trace_dir.name}: missing {component} cgroup CSV")
                continue
            samples = load_samples(source)
            if len(samples) < 2:
                trace_errors.append(f"{trace_dir.name}: fewer than two samples in {source.name}")
                continue
            # A few merged trace directories retained the raw samples but not
            # their trace/collection manifests. For those only, use the actual
            # sampler capture bounds and label the fallback in summary.json.
            active_start, active_end = manifest_bounds or (samples[0][0], samples[-1][0])
            if active_end <= active_start:
                trace_errors.append(f"{trace_dir.name}: invalid time bounds for {component}")
                continue
            gaps = [b[0] - a[0] for a, b in zip(samples, samples[1:]) if b[0] > a[0]]
            raw_gap_ms.extend(gap * 1000.0 for gap in gaps)
            series = CounterSeries(samples, args.max_gap_ms / 1000.0)

            window_rows: dict[int, list[dict[str, Any]]] = {1: [], 60: []}
            # One-second bins are Unix-second aligned. 60s windows trail each
            # integer endpoint, matching Prometheus query_range evaluation times.
            first_1s_start = math.ceil(active_start)
            last_1s_start = math.floor(active_end) - 1
            for start in range(first_1s_start, last_1s_start + 1):
                result = series.window(float(start), float(start + 1), args.minimum_coverage)
                if result is None:
                    continue
                row = {
                    **identity,
                    "component": component,
                    "window_start_epoch_s": start,
                    "window_end_epoch_s": start + 1,
                    "window_seconds": 1,
                    **result,
                }
                window_rows[1].append(row)
                per_window_rows[1].append(row)

            first_60s_end = math.ceil(active_start + 60.0)
            last_60s_end = math.floor(active_end)
            for end in range(first_60s_end, last_60s_end + 1):
                result = series.window(float(end - 60), float(end), args.minimum_coverage)
                if result is None:
                    continue
                row = {
                    **identity,
                    "component": component,
                    "window_start_epoch_s": end - 60,
                    "window_end_epoch_s": end,
                    "window_seconds": 60,
                    **result,
                }
                window_rows[60].append(row)
                per_window_rows[60].append(row)

            for width in WINDOWS:
                values = [float(row["cpu_cores"]) for row in window_rows[width]]
                per_trace_rows.append({
                    **identity,
                    "component": component,
                    "window_seconds": width,
                    **summarize(values),
                })

            if component == "openclaw_gateway":
                prom_values, prom_job, prom_pod = load_prometheus_values(
                    trace_dir / "data/shell/prometheus/cpu_openclaw.json", "gateway"
                )
            else:
                prom_values, prom_job, prom_pod = load_prometheus_values(
                    trace_dir / "data/shell/prometheus/cpu_openshell.json",
                    "agent",
                    sandbox_files[0].stem,
                ) if len(sandbox_files) == 1 else ({}, None, None)
            for row in window_rows[60]:
                timestamp = int(row["window_end_epoch_s"])
                prom_cpu = prom_values.get(timestamp)
                if prom_cpu is None:
                    continue
                cg_cpu = float(row["cpu_cores"])
                prometheus_rows.append({
                    **identity,
                    "component": component,
                    "window_end_epoch_s": timestamp,
                    "cgroup_60s_cores": cg_cpu,
                    "prometheus_rate_1m_cores": prom_cpu,
                    "difference_cores": cg_cpu - prom_cpu,
                    "prometheus_job": prom_job or "",
                    "prometheus_pod": prom_pod or "",
                })

    for width in WINDOWS:
        write_csv(
            output_dir / f"cgroup_cpu_{width}s.csv",
            list(CSV_FIELDS),
            per_window_rows[width],
        )

    write_csv(
        output_dir / "summary_by_agent.csv",
        ["trace_id", "job", "agent", "workload", "component", "window_seconds", "n_windows", "mean_cores", "p50_cores", "p95_cores", "p99_cores", "max_cores"],
        per_trace_rows,
    )

    workload_summary: list[dict[str, Any]] = []
    pooled_summary: dict[str, dict[str, dict[str, float | int | None]]] = {}
    pooled_rows: list[dict[str, Any]] = []
    for width in WINDOWS:
        pooled_summary[f"{width}s"] = {}
        for component in COMPONENTS:
            values = [
                float(row["cpu_cores"])
                for row in per_window_rows[width]
                if row["component"] == component
            ]
            stats = summarize(values)
            pooled_summary[f"{width}s"][component] = stats
            pooled_rows.append({
                "component": component,
                "window_seconds": width,
                **stats,
            })

        groups: dict[tuple[str, str], list[float]] = defaultdict(list)
        for row in per_window_rows[width]:
            groups[(str(row["workload"]), str(row["component"]))].append(float(row["cpu_cores"]))
        for (workload, component), values in sorted(groups.items()):
            workload_summary.append({
                "workload": workload,
                "component": component,
                "window_seconds": width,
                **summarize(values),
            })
    write_csv(
        output_dir / "overall_summary.csv",
        ["component", "window_seconds", "n_windows", "mean_cores", "p50_cores", "p95_cores", "p99_cores", "max_cores"],
        pooled_rows,
    )
    write_csv(
        output_dir / "summary_by_workload.csv",
        ["workload", "component", "window_seconds", "n_windows", "mean_cores", "p50_cores", "p95_cores", "p99_cores", "max_cores"],
        workload_summary,
    )

    # Aggregate agents at each aligned second. Sums are marked complete only
    # when both component readings exist for every trace in the run.
    fleet_rows: dict[int, list[dict[str, Any]]] = {1: [], 60: []}
    expected_agents = len(traces)
    for width in WINDOWS:
        by_time_component: dict[int, dict[str, dict[str, float]]] = defaultdict(lambda: defaultdict(dict))
        for row in per_window_rows[width]:
            end = int(row["window_end_epoch_s"])
            by_time_component[end][str(row["component"])][str(row["trace_id"])] = float(row["cpu_cores"])
        for end, components in sorted(by_time_component.items()):
            gateway = components.get("openclaw_gateway", {})
            sandbox = components.get("openshell_sandbox", {})
            observed_total = sum(gateway.values()) + sum(sandbox.values())
            complete = len(gateway) == expected_agents and len(sandbox) == expected_agents
            fleet_rows[width].append({
                "window_end_epoch_s": end,
                "window_seconds": width,
                "expected_agents": expected_agents,
                "openclaw_agents_covered": len(gateway),
                "sandbox_agents_covered": len(sandbox),
                "openclaw_gateway_sum_observed_cores": sum(gateway.values()),
                "sandbox_sum_observed_cores": sum(sandbox.values()),
                "fleet_sum_observed_cores": observed_total,
                "fleet_sum_complete_cores": observed_total if complete else "",
                "fleet_complete": complete,
            })
    for width in WINDOWS:
        write_csv(
            output_dir / f"fleet_cpu_{width}s.csv",
            ["window_end_epoch_s", "window_seconds", "expected_agents", "openclaw_agents_covered", "sandbox_agents_covered", "openclaw_gateway_sum_observed_cores", "sandbox_sum_observed_cores", "fleet_sum_observed_cores", "fleet_sum_complete_cores", "fleet_complete"],
            fleet_rows[width],
        )

    write_csv(
        output_dir / "prometheus_vs_cgroup_60s.csv",
        ["trace_id", "job", "agent", "workload", "component", "window_end_epoch_s", "cgroup_60s_cores", "prometheus_rate_1m_cores", "difference_cores", "prometheus_job", "prometheus_pod"],
        prometheus_rows,
    )

    fleet_complete_summaries: dict[str, dict[str, float | int | None]] = {}
    fleet_coverage_max: dict[str, dict[str, int]] = {}
    for width in WINDOWS:
        complete_values = [
            float(row["fleet_sum_complete_cores"])
            for row in fleet_rows[width]
            if row["fleet_sum_complete_cores"] != ""
        ]
        fleet_complete_summaries[f"{width}s"] = summarize(complete_values)
        fleet_coverage_max[f"{width}s"] = {
            "openclaw_agents": max(
                (int(row["openclaw_agents_covered"]) for row in fleet_rows[width]),
                default=0,
            ),
            "sandbox_agents": max(
                (int(row["sandbox_agents_covered"]) for row in fleet_rows[width]),
                default=0,
            ),
        }
    gaps_sorted = sorted(raw_gap_ms)
    summary = {
        "schema": "agentic.cgroup-cpu-window-analysis.v1",
        "run_dir": str(run_dir),
        "expected_agents": expected_agents,
        "agents_with_derived_1s_data": len({str(row["trace_id"]) for row in per_window_rows[1]}),
        "source": "raw cgroup-v2 cpu.stat usage_usec counters",
        "source_cadence_ms": {
            "median": statistics.median(gaps_sorted) if gaps_sorted else None,
            "p95": quantile(gaps_sorted, 0.95),
            "p99": quantile(gaps_sorted, 0.99),
            "max": max(gaps_sorted) if gaps_sorted else None,
        },
        "window_method": "piecewise-linear integration of cumulative usage_usec deltas over actual elapsed time; 1s bins are Unix-second aligned; 60s windows trail each integer endpoint",
        "max_gap_ms_accepted": args.max_gap_ms,
        "minimum_window_coverage": args.minimum_coverage,
        "source_files": {
            "openclaw": "traces/*/data/shell/prometheus/cgroup/openclaw_gateway.csv",
            "sandbox": "traces/*/data/shell/prometheus/cgroup/sandbox_samples/*.csv (one exact sandbox per trace)",
        },
        "derived_window_counts": {
            f"{width}s": len(per_window_rows[width]) for width in WINDOWS
        },
        "pooled_window_summary": pooled_summary,
        "prometheus_comparison_pairs": len(prometheus_rows),
        "prometheus_comparison_pairs_by_component": {
            component: sum(row["component"] == component for row in prometheus_rows)
            for component in COMPONENTS
        },
        "fleet_complete_summaries": fleet_complete_summaries,
        "maximum_agents_with_same_timestamp_data": fleet_coverage_max,
        "caveats": [
            "A 1s value is average CPU cores during that second, not instantaneous demand.",
            "A 60s cgroup window is aligned to the Prometheus rate(...[1m]) evaluation timestamp; Prometheus scrape/evaluation coverage can still be incomplete.",
            "Fleet observed sums are lower bounds whenever fleet_complete is false; do not use incomplete sums as total fleet CPU.",
            "CPU consumption does not equal unconstrained CPU demand when a container is throttled; this sampler does not collect throttling counters.",
            "Percentiles summarize time windows and are not independent-run confidence intervals.",
        ],
        "trace_errors": trace_errors,
        "metadata_fallback_traces": metadata_fallbacks,
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    readme = f"""# Derived cgroup CPU windows

This directory contains derived data for `{run_dir.name}`. The raw cgroup CSVs were not changed. For traces listed in `summary.json` under `metadata_fallback_traces`, trace/collection metadata was missing, so that component's sampling start and end were used as the window bounds.

- `cgroup_cpu_1s.csv`: per-agent, per-component average CPU cores in each complete Unix-second bin.
- `cgroup_cpu_60s.csv`: per-agent, per-component trailing 60-second average, ending at each integer second. These timestamps align with Prometheus `query_range` evaluation timestamps.
- `fleet_cpu_1s.csv` and `fleet_cpu_60s.csv`: timestamp-aligned sums with per-component coverage counts. `fleet_sum_complete_cores` is populated only when both component measurements exist for every one of the {expected_agents} traces; otherwise the observed sum is only a lower bound.
- `prometheus_vs_cgroup_60s.csv`: cgroup trailing-60s values joined to the saved Prometheus `rate(...[1m])` value for the same trace/component and timestamp where both exist.
- `summary_by_agent.csv` and `summary_by_workload.csv`: mean, p50, p95, p99 and max of the derived time windows.
- `overall_summary.csv`: pooled time-window statistics by component and resolution.
- `summary.json`: methodology, sample cadence, coverage, and caveats.

For each window, the script integrates deltas of the cumulative `usage_usec` counter across sample intervals, weighted by actual elapsed time. Intervals longer than {args.max_gap_ms:g} ms are excluded; a window is emitted only with at least {args.minimum_coverage:.1%} valid coverage. The 1s and 60s data answer different questions: short bursts versus sustained consumption. Neither alone measures unconstrained CPU demand; that requires throttling telemetry and controlled limit tests.
"""
    (output_dir / "README.md").write_text(readme, encoding="utf-8")

    print(f"Wrote derived CPU windows to {output_dir}")
    print(f"Traces: {expected_agents}; 1s rows: {len(per_window_rows[1])}; 60s rows: {len(per_window_rows[60])}")
    print(f"Prometheus/cgroup 60s comparison pairs: {len(prometheus_rows)}")
    print(
        "Complete fleet 1s windows: "
        f"{fleet_complete_summaries['1s']['n_windows']}"
    )
    print(f"Trace/source issues recorded: {len(trace_errors)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
