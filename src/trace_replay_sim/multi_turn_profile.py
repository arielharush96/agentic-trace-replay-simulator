"""Produce an external multi-turn OpenClaw/OpenShell profiler report.

The profiler deliberately does not invent latency. It reports boundaries from
the mock clock, Jaeger tool spans, and the driver SSE timeline:

* ``model_stream_ms``: first mock model event -> final model event.
* ``first_event_to_tool_ms``: first mock event -> OpenClaw tool span start.
* ``response_processing_post_stream_ms``: final mock event -> tool span start.
* ``openshell_exec_ms``: OpenClaw tool execution span duration.

The first-event boundary includes model streaming and any concurrent harness
work. The post-stream boundary is the narrow response-to-dispatch interval;
the two must not be confused.
"""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path
from typing import Any


PHASES = [
    "context_routing_parse_ms",
    "response_processing_ms",
    "response_cycle_ms",
    "openclaw_response_only_ms",
    "sandbox_init_ms",
    "sandbox_exec_ms",
    "model_stream_ms",
    "first_event_to_tool_ms",
    "response_processing_post_stream_ms",
    "openclaw_model_inside_ms",
    "openshell_exec_ms",
    "model_decode_ms",
    "turn_total_ms",
]


def _number(row: dict[str, Any], *names: str) -> float | None:
    for name in names:
        value = row.get(name)
        if value is not None and value != "":
            try:
                return float(value)
            except (TypeError, ValueError):
                return None
    return None


def load_rows(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError(f"expected a JSON array in {path}")
    return [row for row in data if isinstance(row, dict)]


def load_driver_requests(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            rows.append(value)
    return rows


def phase_row(row: dict[str, Any]) -> dict[str, Any]:
    sandbox_init_ms = _number(row, "B_sandbox_k8s_cold_ms")
    sandbox_exec_ms = _number(row, "B_sandbox_ms")
    sandbox_exec_exclusive_ms = sandbox_exec_ms
    if sandbox_init_ms is not None and sandbox_exec_ms is not None:
        sandbox_exec_exclusive_ms = max(0.0, sandbox_exec_ms - sandbox_init_ms)
    return {
        "session_id": row.get("session_id"),
        "turn_seq": row.get("turn_seq"),
        "is_cold_sandbox": bool(row.get("is_cold_sandbox")),
        "context_routing_parse_ms": _number(row, "A_premodel_ms"),
        "response_processing_ms": _number(row, "D_response_ms"),
        "context_assembly_ms": _number(row, "context_assembly_direct_ms"),
        "response_processing_measured_ms": _number(row, "F_mock_last_to_tool_ms"),
        "response_cycle_ms": _number(row, "H_response_cycle_ms"),
        "openclaw_response_only_ms": _number(row, "H_openclaw_response_only_ms"),
        "sandbox_init_ms": sandbox_init_ms,
        "sandbox_exec_ms": sandbox_exec_ms,
        "sandbox_exec_exclusive_ms": sandbox_exec_exclusive_ms,
        "model_stream_ms": _number(row, "F_mock_stream_ms"),
        "first_event_to_tool_ms": _number(row, "F_mock_first_to_tool_ms"),
        "response_processing_post_stream_ms": _number(row, "F_mock_last_to_tool_ms"),
        "openshell_exec_ms": _number(row, "B_sandbox_ms"),
        "model_decode_ms": _number(row, "E_decode_ms"),
        "turn_total_ms": _number(row, "turn_total_ms"),
        "model_first_visible_offset_ms": _number(row, "F_mock_first_visible_ms"),
        "model_first_tool_offset_ms": _number(row, "F_mock_first_tool_ms"),
        "openclaw_model_inside_ms": _number(row, "D_response_inside_ms"),
    }


def _stats(values: list[float]) -> dict[str, float | int | None]:
    if not values:
        return {"n": 0, "mean": None, "p50": None, "p95": None, "min": None, "max": None}
    ordered = sorted(values)
    return {
        "n": len(values),
        "mean": round(statistics.mean(values), 3),
        "p50": round(statistics.median(values), 3),
        "p95": round(ordered[min(len(ordered) - 1, int(round(0.95 * (len(ordered) - 1))) )], 3),
        "min": round(min(values), 3),
        "max": round(max(values), 3),
    }


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "turns": len(rows),
        "cold_turns": sum(1 for row in rows if row["is_cold_sandbox"]),
        "phases": {
            phase: _stats([row[phase] for row in rows if row.get(phase) is not None])
            for phase in PHASES
        },
        "definitions": {
            "first_event_to_tool_ms": "mock first model event to OpenClaw tool.execution span start",
            "response_processing_post_stream_ms": "mock final model event to OpenClaw tool.execution span start",
            "response_cycle_ms": "mock first model event to next mock prompt, excluding OpenShell execution",
            "openclaw_response_only_ms": "response cycle minus theoretical model decode time",
            "openclaw_model_inside_ms": "OpenClaw model.call duration after subtracting mock TTFT and decode",
            "openshell_exec_ms": "OpenClaw tool.execution span duration; includes the live OpenShell path",
            "model_stream_ms": "mock first model event to mock final model event",
        },
    }


def summarize_driver_requests(requests: list[dict[str, Any]]) -> dict[str, Any]:
    """Summarize the client-visible SSE timeline for each top-level request."""
    def values(name: str) -> list[float]:
        return [
            float(request[name] - request.get("t_send_ns", request[name])) / 1e6
            for request in requests
            if request.get(name) not in (None, 0, "")
        ]

    event_types: dict[str, int] = {}
    for request in requests:
        for name, count in (request.get("event_types") or {}).items():
            event_types[name] = event_types.get(name, 0) + int(count)
    return {
        "requests": len(requests),
        "first_event_ms_from_process_start": _stats(values("first_event_ns")),
        "first_content_ms_from_process_start": _stats(values("first_content_ns")),
        "first_tool_ms_from_process_start": _stats(values("first_tool_ns")),
        "event_types": event_types,
    }


def _write_report(summary: dict[str, Any], rows: list[dict[str, Any]], out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    (out / "profile_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    (out / "per_turn_phases.json").write_text(json.dumps(rows, indent=2), encoding="utf-8")
    lines = ["# Multi-turn Harness Profile", "", f"Turns: {summary['turns']}", ""]
    lines += ["| Phase | Mean ms | P50 ms | P95 ms |", "|---|---:|---:|---:|"]
    for phase, stats in summary["phases"].items():
        lines.append(
            f"| {phase} | {stats['mean'] or '-'} | {stats['p50'] or '-'} | {stats['p95'] or '-'} |"
        )
    lines += ["", "## Boundary Definitions", ""]
    for key, value in summary["definitions"].items():
        lines.append(f"- `{key}`: {value}")
    driver = summary.get("driver_sse") or {}
    if driver.get("requests"):
        lines += ["", "## Client SSE Boundary", ""]
        lines.append(f"Top-level requests: {driver['requests']}")
        for key in ("first_event_ms_from_process_start", "first_content_ms_from_process_start", "first_tool_ms_from_process_start"):
            stats = driver.get(key) or {}
            lines.append(f"- `{key}`: mean={stats.get('mean')} ms, p50={stats.get('p50')} ms, p95={stats.get('p95')} ms")
    (out / "PROFILE.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def render_plot(rows: list[dict[str, Any]], out: Path) -> None:
    try:
        import matplotlib.pyplot as plt
    except ImportError as exc:  # pragma: no cover - optional analysis dependency
        raise RuntimeError("Install the analyze extra to render profiler plots") from exc

    out.mkdir(parents=True, exist_ok=True)
    visible = [row for row in rows if row.get("turn_seq") is not None]
    labels = [f"T{row['turn_seq']}" for row in visible]
    x = list(range(len(visible)))
    fig, ax = plt.subplots(figsize=(14, max(6, len(visible) * 0.24)))
    left = [0.0] * len(visible)
    colors = {
        "model_stream_ms": "#8fa1b5",
        "response_processing_post_stream_ms": "#20b486",
        "openshell_exec_ms": "#8b5cf6",
    }
    for phase in ("model_stream_ms", "response_processing_post_stream_ms", "openshell_exec_ms"):
        values = [row.get(phase) or 0.0 for row in visible]
        ax.barh(x, values, left=left, color=colors[phase], label=phase)
        left = [a + b for a, b in zip(left, values)]
    ax.set_yticks(x, labels)
    ax.set_xlabel("Milliseconds")
    ax.set_title("Multi-turn Model Stream, OpenClaw Processing, and OpenShell Execution")
    ax.legend(loc="lower right")
    fig.tight_layout()
    fig.savefig(out / "harness_phase_waterfall.png", dpi=160)
    plt.close(fig)

    stage_fig, stage_ax = plt.subplots(figsize=(14, max(6, len(visible) * 0.24)))
    stage_left = [0.0] * len(visible)
    stage_defs = [
        ("context_assembly_ms", "Context assembly (direct span only)", "#3B82F6"),
        ("model_decode_ms", "Model decode", "#94A3B8"),
        ("response_processing_measured_ms", "Response processing (post-decode)", "#10B981"),
        ("sandbox_init_ms", "Sandbox initialization (cold only)", "#F59E0B"),
        ("sandbox_exec_exclusive_ms", "Sandbox tool execution", "#8B5CF6"),
    ]
    for phase, label, color in stage_defs:
        values = [row.get(phase) or 0.0 for row in visible]
        stage_ax.barh(x, values, 0.62, left=stage_left, color=color, label=label)
        stage_left = [a + b for a, b in zip(stage_left, values)]
    stage_ax.set_yticks(x, labels)
    stage_ax.set_xlabel("Milliseconds")
    stage_ax.set_title("Measured OpenClaw/OpenShell Stages (Decode Separated)")
    stage_ax.legend(loc="lower right")
    stage_fig.tight_layout()
    stage_fig.savefig(out / "stage_only_waterfall.png", dpi=160)
    plt.close(stage_fig)


def profile(*, per_turn: Path, out: Path, driver_requests: Path | None = None,
            warm_only: bool = False) -> dict[str, Any]:
    rows = [phase_row(row) for row in load_rows(per_turn)]
    if warm_only:
        rows = [row for row in rows if not row["is_cold_sandbox"]]
    summary = summarize(rows)
    requests = load_driver_requests(driver_requests)
    summary["driver_sse"] = summarize_driver_requests(requests)
    _write_report(summary, rows, out)
    render_plot(rows, out / "diagrams")
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Profile multi-turn OpenClaw/OpenShell phases")
    parser.add_argument("--per-turn", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--driver-requests")
    parser.add_argument("--warm-only", action="store_true")
    args = parser.parse_args(argv)
    summary = profile(
        per_turn=Path(args.per_turn),
        out=Path(args.out),
        driver_requests=Path(args.driver_requests) if args.driver_requests else None,
        warm_only=args.warm_only,
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
