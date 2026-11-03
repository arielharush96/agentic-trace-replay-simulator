"""Fail-closed validation for one completed trace's measurement artifacts.

The benchmark must not report a successful trace merely because PNG files were
created.  This module checks the relationships that make the plots meaningful:
the expected turn/tool cardinality, the first-turn phase invariant, non-negative
timings, raw telemetry presence, cgroup samples, and valid PNG artifacts.
"""

from __future__ import annotations

import csv
import json
import struct
from pathlib import Path
from typing import Any


REQUIRED_PLOTS = (
    "latency_waterfall.png",
    "openshell_execution_latency_ms.png",
    "resource_memory_mib.png",
    "resource_cpu_cores.png",
    "resource_timeseries.png",
)
PHASE_COLUMNS = (
    "turn",
    "sandbox_init_ms",
    "prompt_boundary_ms",
    "response_processing_ms",
    "tool_dispatch_overhead_ms",
    "openshell_exec_ms",
    "tool_failure_ms",
)
NON_NEGATIVE_PHASE_COLUMNS = PHASE_COLUMNS[1:]


class OutputValidationError(ValueError):
    """Raised when a trace result cannot support a trustworthy analysis."""


def _json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def _number(value: Any, *, path: Path, field: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise OutputValidationError(f"{path}: {field} is not numeric: {value!r}") from exc
    if result < 0:
        raise OutputValidationError(f"{path}: {field} is negative: {result}")
    return result


def _png_dimensions(path: Path) -> tuple[int, int]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise OutputValidationError(f"missing plot: {path}") from exc
    if len(raw) < 24 or raw[:8] != b"\x89PNG\r\n\x1a\n":
        raise OutputValidationError(f"plot is not a PNG: {path}")
    width, height = struct.unpack(">II", raw[16:24])
    if width < 100 or height < 100:
        raise OutputValidationError(f"plot is implausibly small: {path} ({width}x{height})")
    return width, height


def _valid_cgroup_rows(path: Path) -> int:
    try:
        rows = list(csv.DictReader(path.open(encoding="utf-8")))
    except OSError:
        return 0
    valid = 0
    for row in rows:
        try:
            epoch = int(row.get("epoch_ns") or 0)
            cpu = int(row.get("cpu_usage_usec") or -1)
            memory = int(row.get("memory_bytes") or -1)
        except (TypeError, ValueError):
            continue
        if epoch > 0 and cpu >= 0 and memory >= 0:
            valid += 1
    return valid


def validate_trace_output(
    trace_dir: Path,
    *,
    expected_turns: int,
    expected_tool_calls: int,
    backend_dir_name: str,
    backend_plot_name: str,
) -> dict[str, Any]:
    """Validate one trace and return machine-readable evidence.

    This intentionally does not validate semantic task correctness; that is
    handled by ``functional_conformance``.  It validates that the performance
    artifacts are complete enough to make the conformance result and plots
    interpretable.
    """
    trace_dir = Path(trace_dir)
    shell = trace_dir / "data/shell"
    profile = shell / "profile-v2"
    diagrams = profile / "diagrams"
    failures: list[str] = []

    per_turn = _json(profile / "per_turn.json", None)
    per_tool = _json(profile / "per_tool.json", None)
    if not isinstance(per_turn, list) or len(per_turn) != expected_turns:
        failures.append(f"per_turn.json expected {expected_turns} rows")
    if not isinstance(per_tool, list) or len(per_tool) != expected_tool_calls:
        failures.append(f"per_tool.json expected {expected_tool_calls} rows")

    phase_rows: list[dict[str, str]] = []
    phase_path = diagrams / "per_turn_efficiency.csv"
    try:
        with phase_path.open(encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            missing_columns = [column for column in PHASE_COLUMNS if column not in (reader.fieldnames or [])]
            if missing_columns:
                failures.append(f"per_turn_efficiency.csv missing columns: {', '.join(missing_columns)}")
            phase_rows = list(reader)
    except OSError:
        failures.append("per_turn_efficiency.csv is missing")
    if len(phase_rows) != expected_turns:
        failures.append(f"per_turn_efficiency.csv expected {expected_turns} rows")
    if phase_rows:
        try:
            observed_turns = [int(float(row.get("turn") or -1)) for row in phase_rows]
        except (TypeError, ValueError):
            observed_turns = []
            failures.append("per_turn_efficiency.csv contains a non-numeric turn index")
        if observed_turns and observed_turns != list(range(expected_turns)):
            failures.append(f"phase rows are not ordered turns 0..{expected_turns - 1}")
        for row_number, row in enumerate(phase_rows):
            for field in NON_NEGATIVE_PHASE_COLUMNS:
                try:
                    _number(row.get(field), path=phase_path, field=f"row {row_number} {field}")
                except OutputValidationError as exc:
                    failures.append(str(exc))
        first = phase_rows[0]
        for field in ("response_processing_ms", "tool_dispatch_overhead_ms", "openshell_exec_ms", "tool_failure_ms"):
            try:
                if _number(first.get(field), path=phase_path, field=f"turn 0 {field}") > 0:
                    failures.append(f"turn 0 must not contain {field}")
            except OutputValidationError:
                pass
        if expected_tool_calls > 0:
            try:
                later_open_shell = any(float(row.get("openshell_exec_ms") or 0) > 0 for row in phase_rows[1:])
                later_response = any(float(row.get("response_processing_ms") or 0) > 0 for row in phase_rows[1:])
            except (TypeError, ValueError):
                later_open_shell = False
                later_response = False
                failures.append("per_turn_efficiency.csv contains a non-numeric phase value")
            if not later_open_shell:
                failures.append("tool-bearing trace has no measured OpenShell execution after turn 0")
            if expected_turns > 1 and not later_response:
                failures.append("multi-turn trace has no measured response processing after turn 0")

    lifecycle = _json(shell / "sandbox_lifecycle.json", None)
    if not isinstance(lifecycle, dict):
        failures.append("sandbox_lifecycle.json is missing")
    else:
        if lifecycle.get("status") != "ready":
            failures.append(f"sandbox lifecycle is not ready: {lifecycle.get('status')!r}")
        mode = str(lifecycle.get("provisioning_mode") or "")
        if mode not in {
            "warm_reused",
            "warm_reused_without_prewarm",
            "cold_pool_fill",
            "cold_fallback",
            "warm_pool_unverified",
        }:
            failures.append(f"sandbox lifecycle has unknown provisioning mode: {mode!r}")
        if mode == "warm_pool_unverified":
            failures.append("warm pool was requested but OpenClaw sandbox-acquisition timing was not observed")
        try:
            _number(lifecycle.get("sandbox_init_ms") or 0, path=shell / "sandbox_lifecycle.json", field="sandbox_init_ms")
        except OutputValidationError as exc:
            failures.append(str(exc))
        if bool(lifecycle.get("prewarm_requested")):
            if lifecycle.get("prewarm_status") != "measured":
                failures.append("prewarm was requested but its timing was not measured")
        if mode == "warm_reused":
            try:
                request_to_action_ms = _number(
                    lifecycle.get("sandbox_request_to_first_action_ms"),
                    path=shell / "sandbox_lifecycle.json",
                    field="sandbox_request_to_first_action_ms",
                )
                if request_to_action_ms <= 0:
                    failures.append("warm reuse must have a positive request-to-first-action duration")
            except OutputValidationError as exc:
                failures.append(str(exc))
        if not str(lifecycle.get("readiness_evidence") or ""):
            failures.append("sandbox lifecycle has no readiness evidence")

    raw_traces = _json(shell / "traces/raw_traces.json", [])
    mock_edges = shell / "mock_edges.jsonl"
    if not isinstance(raw_traces, list) or not raw_traces:
        failures.append("raw_traces.json is missing or empty")
    if not mock_edges.exists() or not mock_edges.read_text(encoding="utf-8").strip():
        failures.append("mock_edges.jsonl is missing or empty")

    cgroup_dir = shell / "prometheus/cgroup"
    cgroup_files = sorted(cgroup_dir.glob("*.csv")) if cgroup_dir.exists() else []
    cgroup_rows = {path.name: _valid_cgroup_rows(path) for path in cgroup_files}
    _required_cgroup = {"openclaw_gateway.csv", "openshell_sandbox.csv"}
    if not _required_cgroup.issubset(set(cgroup_rows)) or any(cgroup_rows.get(name, 0) < 2 for name in _required_cgroup):
        failures.append(f"expected required cgroup series with at least two samples: {cgroup_rows}")

    collect_manifest = _json(shell / "prometheus/collect_manifest.json", None)
    if not isinstance(collect_manifest, dict):
        failures.append("Prometheus collect_manifest.json is missing")

    plot_paths = [diagrams / name for name in REQUIRED_PLOTS]
    plot_paths.append(trace_dir / "data" / backend_dir_name / backend_plot_name)
    plot_evidence: dict[str, list[int]] = {}
    for path in plot_paths:
        try:
            if path.stat().st_size < 1000:
                raise OutputValidationError(f"plot is empty or too small: {path}")
            plot_evidence[str(path.relative_to(trace_dir))] = list(_png_dimensions(path))
        except (OSError, OutputValidationError) as exc:
            failures.append(str(exc))

    evidence = {
        "schema": "agentic.trace-replay.output-validation.v1",
        "expected_turns": expected_turns,
        "expected_tool_calls": expected_tool_calls,
        "observed_turns": len(per_turn) if isinstance(per_turn, list) else 0,
        "observed_tool_calls": len(per_tool) if isinstance(per_tool, list) else 0,
        "cgroup_rows": cgroup_rows,
        "sandbox_lifecycle": {
            "status": (lifecycle or {}).get("status"),
            "provisioning_mode": (lifecycle or {}).get("provisioning_mode"),
            "sandbox_init_ms": (lifecycle or {}).get("sandbox_init_ms"),
            "sandbox_reuse_ms": (lifecycle or {}).get("sandbox_reuse_ms"),
            "prewarm_status": (lifecycle or {}).get("prewarm_status"),
        },
        "plots": plot_evidence,
        "prometheus": {
            "cadvisor_success": (collect_manifest or {}).get("cadvisor_success"),
            "app_success": (collect_manifest or {}).get("app_success"),
            "warnings": (collect_manifest or {}).get("warnings", []),
        },
        "passed": not failures,
        "failures": failures,
    }
    if failures:
        raise OutputValidationError(json.dumps(evidence, sort_keys=True))
    return evidence
