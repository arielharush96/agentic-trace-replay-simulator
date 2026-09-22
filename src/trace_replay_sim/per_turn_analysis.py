"""Per-turn analysis: answers Q1–Q5 from jaeger_export + Prometheus data.

Q1: Does context assembly cost scale linearly or super-linearly with token count?
Q2: Sandbox cold vs warm — is cold start amortized across a session?
Q3: Tool call parallelism — sequential or concurrent within a turn?
Q4: At what concurrency does queue wait become the bottleneck? (needs C=5/C=10 runs)
Q5: Per-session memory footprint at 112k-token contexts?

Usage:
  python3 -m trace_replay_sim.per_turn_analysis \
    --traces  results/traces \
    --prom    results/prometheus \
    --out     results/per_turn_analysis
"""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path
from typing import Any


def _load(path: Path) -> Any:
    if not path.exists():
        return []
    return json.loads(path.read_text(encoding="utf-8"))


def _profile_context_scaling(rows: list[dict]) -> dict[str, Any]:
    """Analyze the per-turn boundary that is available in profiler-v2.

    OpenClaw currently emits its native context span once at agent-run scope.
    The profiler therefore records the driver/mock prompt boundary for each
    turn.  It is the correct repeated-turn observable for scaling, while the
    one run-level native span remains reported separately in profile-v2.
    """
    usable = []
    for row in rows:
        tokens = row.get("input_tokens") or row.get("mock_input_tokens")
        boundary = row.get("context_assembly_window_ms")
        if boundary is None:
            boundary = row.get("prompt_boundary_ms")
        if tokens is not None and boundary is not None:
            usable.append((int(row.get("turn") or 0), float(tokens), float(boundary)))
    if not usable:
        return {"status": "no_data", "source": "profile-v2/per_turn.json"}
    tokens = [item[1] for item in usable]
    times = [item[2] for item in usable]
    token_mean = statistics.mean(tokens)
    time_mean = statistics.mean(times)
    token_sd = statistics.stdev(tokens) if len(tokens) > 1 else 0.0
    time_sd = statistics.stdev(times) if len(times) > 1 else 0.0
    correlation = None
    if token_sd and time_sd:
        correlation = sum((t - token_mean) * (a - time_mean) for t, a in zip(tokens, times)) / (len(tokens) - 1) / token_sd / time_sd
    ordered = sorted(usable, key=lambda item: item[1])
    third = max(1, len(ordered) // 3)
    low = [item[2] for item in ordered[:third]]
    high = [item[2] for item in ordered[-third:]]
    return {
        "status": "ok",
        "source": "profile-v2/per_turn.json:context_assembly_window_ms",
        "total_rows": len(usable),
        "context_boundary_p50_ms": round(_pct(times, 50), 2),
        "context_boundary_p95_ms": round(_pct(times, 95), 2),
        "token_range": [min(tokens), max(tokens)],
        "pearson_r_tokens_vs_boundary": round(correlation, 3) if correlation is not None else None,
        "assembly_ms_low_tokens": round(statistics.mean(low), 2),
        "assembly_ms_high_tokens": round(statistics.mean(high), 2),
        "per_turn_mean_ms": {str(turn): round(boundary, 2) for turn, _, boundary in usable},
        "assessment": "scaling_signal" if correlation is not None and correlation > 0.5 else "no_clear_linear_signal",
        "note": "This is the repeated-turn driver/mock boundary; the native OpenClaw context span is run-level in this OpenClaw build.",
    }


def _pct(vals: list[float], p: float) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    return s[min(len(s) - 1, max(0, int(round((p / 100) * (len(s) - 1)))))]


# ---------------------------------------------------------------------------
# Q1 — Context assembly scaling
# ---------------------------------------------------------------------------

def q1_context_scaling(per_turn: list[dict]) -> dict[str, Any]:
    """Does context assembly time grow linearly or faster with token count?"""
    rows = [r for r in per_turn if r.get("context_assembly_ms") and r.get("context_tokens")]
    if not rows:
        return {"status": "no_data"}

    # Group by turn index to see per-turn progression
    by_turn: dict[int, list[float]] = {}
    for r in rows:
        idx = r["turn_idx"]
        by_turn.setdefault(idx, []).append(r["context_assembly_ms"])

    turn_means = {
        idx: round(statistics.mean(vals), 2)
        for idx, vals in sorted(by_turn.items())
    }

    # Correlation: tokens vs assembly time
    tokens = [r["context_tokens"] for r in rows]
    times  = [r["context_assembly_ms"] for r in rows]
    n = len(tokens)
    if n > 2:
        mean_t = statistics.mean(tokens)
        mean_a = statistics.mean(times)
        cov = sum((t - mean_t) * (a - mean_a) for t, a in zip(tokens, times)) / n
        std_t = statistics.stdev(tokens) or 1
        std_a = statistics.stdev(times) or 1
        pearson_r = cov / (std_t * std_a)
    else:
        pearson_r = None

    # Per-bucket: low/mid/high token ranges
    rows_sorted = sorted(rows, key=lambda r: r["context_tokens"])
    third = max(1, len(rows_sorted) // 3)
    low  = [r["context_assembly_ms"] for r in rows_sorted[:third]]
    high = [r["context_assembly_ms"] for r in rows_sorted[-third:]]

    return {
        "status": "ok",
        "total_rows": len(rows),
        "context_assembly_p50_ms": round(_pct(times, 50), 2),
        "context_assembly_p95_ms": round(_pct(times, 95), 2),
        "token_range": [min(tokens), max(tokens)],
        "pearson_r_tokens_vs_assembly": round(pearson_r, 3) if pearson_r else None,
        "assessment": (
            "super-linear" if (pearson_r or 0) > 0.7 and
            statistics.mean(high) / max(statistics.mean(low), 0.001) > 2.0
            else "linear"
        ),
        "assembly_ms_low_tokens": round(statistics.mean(low), 2) if low else None,
        "assembly_ms_high_tokens": round(statistics.mean(high), 2) if high else None,
        "per_turn_mean_ms": turn_means,
    }


# ---------------------------------------------------------------------------
# Q2 — OpenShell exec latency profile across turns (scope=session)
# ---------------------------------------------------------------------------

def q2_exec_latency_profile(per_tool: list[dict]) -> dict[str, Any]:
    """With scope=session, does per-exec latency stay constant or degrade over turns?

    scope=session: sandbox created once per session (~250ms warm start),
    then each tool call pays ~130-175ms exec overhead (ExecSandbox gRPC round-trip).
    This function checks whether that per-exec cost is stable across all 27 turns
    or degrades as workspace state accumulates (relevant for remote vs mirror mode).

    If exec latency degrades turn-over-turn → workspace sync overhead is growing.
    If exec latency is flat → scope=session is working as expected (session reuse).
    """
    if not per_tool:
        return {"status": "no_data"}

    durations = [r["duration_ms"] for r in per_tool]

    # Split into first-half vs second-half of session turns to detect degradation
    half = max(1, len(durations) // 2)
    early = durations[:half]
    late  = durations[half:]

    early_p50 = _pct(early, 50) if early else None
    late_p50  = _pct(late,  50) if late  else None

    degradation_pct = None
    if early_p50 and late_p50 and early_p50 > 0:
        degradation_pct = round(100 * (late_p50 - early_p50) / early_p50, 1)

    return {
        "status": "ok",
        "total_exec_calls": len(per_tool),
        "exec_p50_ms": round(_pct(durations, 50), 2),
        "exec_p95_ms": round(_pct(durations, 95), 2),
        "exec_mean_ms": round(sum(durations) / len(durations), 2),
        "early_turns_p50_ms": round(early_p50, 2) if early_p50 else None,
        "late_turns_p50_ms":  round(late_p50,  2) if late_p50  else None,
        "degradation_pct": degradation_pct,
        "assessment": (
            "degrading" if (degradation_pct or 0) > 20
            else "stable"
        ),
        "implication": (
            f"Exec latency grows {degradation_pct}% from early to late turns — "
            "workspace state accumulation is adding overhead. "
            "Switch to scope=agent or reduce workspace writes."
            if (degradation_pct or 0) > 20
            else "Exec latency is stable across all turns — scope=session sandbox reuse is working correctly."
        ),
        "expected_baseline_ms": 150,
        "expected_per_session_overhead_ms": round(250 + len(per_tool) * 150, 0),
    }


# ---------------------------------------------------------------------------
# Q3 — Tool call parallelism
# ---------------------------------------------------------------------------

def q3_parallelism(per_tool: list[dict]) -> dict[str, Any]:
    """Are tool calls within a turn executed in parallel or serially?"""
    parallel = [r for r in per_tool if r.get("parallel_siblings", 0) > 0]
    serial   = [r for r in per_tool if r.get("parallel_siblings", 0) == 0]

    total = len(per_tool)
    if total == 0:
        return {"status": "no_data"}

    parallel_pct = round(100 * len(parallel) / total, 1)
    assessment = (
        "parallel" if parallel_pct > 30
        else "mostly_parallel" if parallel_pct > 10
        else "serial"
    )

    return {
        "status": "ok",
        "total_tool_calls": total,
        "parallel_tool_calls": len(parallel),
        "serial_tool_calls": len(serial),
        "parallel_pct": parallel_pct,
        "assessment": assessment,
        "implication": (
            "Tool calls dispatched concurrently — per-turn latency does not scale with call count"
            if assessment in ("parallel", "mostly_parallel")
            else "Tool calls are sequential — per-turn latency = sum of all tool durations. "
                 "Parallelizing tool dispatch would be a direct latency win"
        ),
    }


# ---------------------------------------------------------------------------
# Q4 — Queue saturation (needs Prometheus data)
# ---------------------------------------------------------------------------

def q4_queue_saturation(prom_dir: Path) -> dict[str, Any]:
    """At what concurrency does queue wait dominate latency?"""
    wait_file = prom_dir / "openclaw_app_1s" / "oc_queue_wait_p95.json"
    depth_file = prom_dir / "openclaw_app_1s" / "oc_queue_depth.json"

    wait_data = _load(wait_file)
    depth_data = _load(depth_file)

    result: dict[str, Any] = {"status": "ok"}

    def _values(data: Any) -> list[float]:
        vals: list[float] = []
        if not isinstance(data, dict):
            return vals
        for series in (data.get("data") or {}).get("result") or []:
            for _ts, v in series.get("values") or []:
                try:
                    vals.append(float(v))
                except (ValueError, TypeError):
                    pass
        return vals

    wait_vals = _values(wait_data)
    depth_vals = _values(depth_data)

    if not wait_vals:
        return {"status": "no_data", "note": "queue metrics not yet available"}

    result["queue_wait_p95_ms"] = round(_pct(wait_vals, 95) * 1000, 2)
    result["queue_wait_mean_ms"] = round(statistics.mean(wait_vals) * 1000, 2)
    result["queue_depth_max"] = round(max(depth_vals), 1) if depth_vals else None
    result["assessment"] = (
        "saturated" if result["queue_wait_p95_ms"] > 500
        else "pressure" if result["queue_wait_p95_ms"] > 100
        else "no_queuing"
    )
    return result


# ---------------------------------------------------------------------------
# Q5 — Per-session memory footprint
# ---------------------------------------------------------------------------

def q5_memory_per_session(prom_dir: Path, session_count: int) -> dict[str, Any]:
    """How much memory per concurrent session at 112k-token contexts?"""
    rss_file  = prom_dir / "openclaw_app_1s" / "oc_memory_rss.json"
    heap_file = prom_dir / "openclaw_app_1s" / "oc_memory_heap.json"

    rss_data  = _load(rss_file)
    heap_data = _load(heap_file)

    def _peak_mib(data: Any) -> float | None:
        vals: list[float] = []
        if not isinstance(data, dict):
            return None
        for series in (data.get("data") or {}).get("result") or []:
            for _ts, v in series.get("values") or []:
                try:
                    vals.append(float(v))
                except (ValueError, TypeError):
                    pass
        if not vals:
            return None
        return round(max(vals) / (1024 * 1024), 1)

    rss_peak  = _peak_mib(rss_data)
    heap_peak = _peak_mib(heap_data)

    result: dict[str, Any] = {
        "status": "ok" if (rss_peak or heap_peak) else "no_data",
        "rss_peak_mib": rss_peak,
        "heap_peak_mib": heap_peak,
        "session_count": session_count,
    }
    if rss_peak and session_count > 0:
        result["rss_per_session_mib"] = round(rss_peak / session_count, 1)
        result["heap_per_session_mib"] = round((heap_peak or rss_peak) / session_count, 1)
    return result


def q5_cgroup_memory_cpu(prom_dir: Path, session_count: int) -> dict[str, Any]:
    """Use the authoritative cgroup-v2 samples when Prometheus app metrics are absent."""
    result: dict[str, Any] = {"status": "no_data", "source": "prometheus/cgroup/*.csv", "session_count": session_count}
    components: dict[str, dict[str, float | int | None]] = {}
    for path in sorted((prom_dir / "cgroup").glob("*.csv")):
        try:
            rows = [
                (float(row["epoch_ns"]) / 1e9, float(row["cpu_usage_usec"]), float(row["memory_bytes"]))
                for row in __import__("csv").DictReader(path.open(encoding="utf-8"))
            ]
        except (OSError, KeyError, TypeError, ValueError):
            continue
        if len(rows) < 2:
            continue
        cpu = []
        for previous, current in zip(rows, rows[1:]):
            delta_s = current[0] - previous[0]
            delta_cpu = current[1] - previous[1]
            if delta_s > 0 and delta_cpu >= 0:
                cpu.append(delta_cpu / (delta_s * 1_000_000))
        components[path.stem] = {
            "samples": len(rows),
            "cpu_peak_cores": round(max(cpu), 4) if cpu else None,
            "cpu_mean_cores": round(statistics.mean(cpu), 4) if cpu else None,
            "memory_peak_mib": round(max(row[2] for row in rows) / 1024 / 1024, 2),
            "memory_mean_mib": round(statistics.mean(row[2] for row in rows) / 1024 / 1024, 2),
        }
    if components:
        result["status"] = "ok"
        result["components"] = components
        result["openclaw_gateway"] = components.get("openclaw_gateway")
        result["openshell_sandbox"] = components.get("openshell_sandbox")
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def analyze(traces_dir: Path, prom_dir: Path, out_dir: Path) -> dict[str, Any]:
    per_turn = _load(traces_dir / "per_turn_segments.json")
    per_tool = _load(traces_dir / "per_tool_segments.json")
    sessions = _load(traces_dir / "session_summary.json")
    profile_dir = traces_dir.parent / "profile-v2"
    profile_turns = _load(profile_dir / "per_turn.json")
    profile_tools = _load(profile_dir / "per_tool.json")
    if profile_turns:
        per_turn = profile_turns
    if profile_tools:
        per_tool = profile_tools

    out_dir.mkdir(parents=True, exist_ok=True)

    report = {
        "sessions_analyzed": len(sessions),
        "Q1_context_assembly_scaling": _profile_context_scaling(per_turn) if profile_turns else q1_context_scaling(per_turn),
        "Q2_exec_latency_profile":         q2_exec_latency_profile(per_tool),
        "Q3_tool_parallelism":         q3_parallelism(per_tool),
        "Q4_queue_saturation":         q4_queue_saturation(prom_dir),
        "Q5_memory_per_session":       q5_cgroup_memory_cpu(prom_dir, len(sessions)) if (prom_dir / "cgroup").exists() else q5_memory_per_session(prom_dir, len(sessions)),
    }

    out_file = out_dir / "per_turn_analysis.json"
    out_file.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(json.dumps(report, indent=2))
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--traces", required=True)
    parser.add_argument("--prom",   required=True)
    parser.add_argument("--out",    required=True)
    args = parser.parse_args(argv)
    analyze(Path(args.traces), Path(args.prom), Path(args.out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
