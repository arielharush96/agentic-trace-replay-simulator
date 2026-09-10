"""Evidence-first multi-turn profiler for OpenClaw/OpenShell.

This module intentionally does not reuse the legacy A-E residual buckets.  It
joins official OpenClaw OTEL spans with the mock's monotonic edge clock and
reports direct measurements separately from derived proxies.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from pathlib import Path
from typing import Any


def _tags(span: dict[str, Any]) -> dict[str, Any]:
    return {str(item.get("key")): item.get("value") for item in span.get("tags", [])}


def _span_ms(span: dict[str, Any] | None) -> float | None:
    if not span:
        return None
    value = span.get("duration")
    return float(value) / 1000.0 if value is not None else None


def _start_ms(span: dict[str, Any] | None) -> float | None:
    if not span:
        return None
    value = span.get("startTime")
    return float(value) / 1000.0 if value is not None else None


def _end_ms(span: dict[str, Any] | None) -> float | None:
    start, duration = _start_ms(span), _span_ms(span)
    return start + duration if start is not None and duration is not None else None


def _number(value: Any) -> float | None:
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _first(tags: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if tags.get(key) not in (None, ""):
            return tags[key]
    return None


def _parent(span: dict[str, Any]) -> str | None:
    refs = span.get("references") or []
    return next((ref.get("spanID") for ref in refs if ref.get("refType") == "CHILD_OF"), None)


def _stats(values: list[float]) -> dict[str, float | int | None]:
    if not values:
        return {"n": 0, "mean": None, "p50": None, "p95": None, "min": None, "max": None}
    ordered = sorted(values)
    return {
        "n": len(values),
        "mean": round(statistics.mean(values), 3),
        "p50": round(statistics.median(values), 3),
        "p95": round(ordered[min(len(ordered) - 1, math.floor(0.95 * (len(ordered) - 1)))], 3),
        "min": round(min(values), 3),
        "max": round(max(values), 3),
    }


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_jsonl(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            rows.append(value)
    return rows


def _edge_index(edges: list[dict[str, Any]]) -> dict[tuple[str, int], dict[str, Any]]:
    return {
        (str(edge.get("trace_id") or ""), int(edge.get("turn_seq", -1))): edge
        for edge in edges
        if not edge.get("warmup") and not edge.get("phantom") and not edge.get("miss") and not edge.get("exhausted")
    }


def _load_driver_requests(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and value.get("t_send_wall_ns") is not None:
            rows.append(value)
    return rows


def _context_span(contexts: list[dict[str, Any]], model: dict[str, Any]) -> dict[str, Any] | None:
    model_id = model.get("spanID")
    for span in contexts:
        if model_id in {ref.get("spanID") for ref in span.get("references") or []}:
            return span
    # Older exporters may omit references. Only accept an unambiguous nearby span.
    model_start = _start_ms(model)
    nearby = [span for span in contexts if model_start is not None and _start_ms(span) is not None
              and abs(_start_ms(span) - model_start) < 1000]
    if len(nearby) == 1:
        return nearby[0]
    # OpenClaw 2026.9.x emits one run-level context span before the first
    # provider call, rather than one context span per internal model call.
    before_model = [
        span for span in contexts
        if model_start is not None and _start_ms(span) is not None and _end_ms(span) is not None
        and _start_ms(span) < model_start and _end_ms(span) <= model_start + 100
    ]
    return max(before_model, key=lambda span: _end_ms(span) or 0) if before_model else None


def _trace_rows(trace: dict[str, Any], edges: dict[tuple[str, int], dict[str, Any]]) -> list[dict[str, Any]]:
    trace_id = str(trace.get("traceID") or "")
    spans = trace.get("spans") or []
    models = sorted((span for span in spans if span.get("operationName") == "openclaw.model.call"),
                    key=lambda span: _start_ms(span) or 0)
    contexts = [span for span in spans if span.get("operationName") == "openclaw.context.assembled"]
    tools = sorted((span for span in spans if span.get("operationName") == "openclaw.tool.execution"),
                   key=lambda span: _start_ms(span) or 0)
    execs = sorted((span for span in spans if span.get("operationName") == "openclaw.exec"),
                   key=lambda span: _start_ms(span) or 0)
    harness = next((span for span in spans if span.get("operationName") == "openclaw.harness.run"), None)
    rows: list[dict[str, Any]] = []

    for turn, model in enumerate(models):
        tags = _tags(model)
        start = _start_ms(model)
        end = _end_ms(model)
        next_start = _start_ms(models[turn + 1]) if turn + 1 < len(models) else _end_ms(harness)
        context = _context_span(contexts, model) if turn == 0 else None
        context_tags = _tags(context or {})
        context_span_ms = _span_ms(context)
        following_tools = [tool for tool in tools if end is not None and (_start_ms(tool) or 0) >= end
                           and (next_start is None or (_start_ms(tool) or 0) < next_start)]
        first_tool = following_tools[0] if following_tools else None
        following_execs = [item for item in execs if end is not None and (_start_ms(item) or 0) >= end
                           and (next_start is None or (_start_ms(item) or 0) < next_start)]
        tool_dispatch_ms = _span_ms(first_tool)
        exec_ms = sum(_span_ms(item) or 0 for item in following_execs) if following_execs else None
        previous_hops = [span for span in tools + execs if _end_ms(span) is not None and start is not None and _end_ms(span) <= start]
        previous_end = max((_end_ms(span) or 0.0) for span in previous_hops) if previous_hops else _start_ms(harness)
        context_assembly_window_ms = max(0.0, start - previous_end) if start is not None and previous_end is not None else None
        sandbox_init_ms = None
        if following_execs and following_execs[0].get("spanID") == (execs[0].get("spanID") if execs else None):
            sandbox_init_ms = _span_ms(following_execs[0])
        edge = edges.get((trace_id, turn), {})
        next_edge = edges.get((trace_id, turn + 1), {})
        response_processing_full_ms = None
        if edge.get("t_first_emit_ns") is not None and next_edge.get("t_recv_ns") is not None:
            decode_ms = None
            if edge.get("t_end_emit_ns") is not None:
                decode_ms = (int(edge["t_end_emit_ns"]) - int(edge["t_first_emit_ns"])) / 1e6
            full_gap_ms = (int(next_edge["t_recv_ns"]) - int(edge["t_first_emit_ns"])) / 1e6
            response_processing_full_ms = max(0.0, full_gap_ms - (decode_ms or 0.0) - (exec_ms or 0.0))
        model_ms = _span_ms(model)
        ttft_ms = _number(_first(tags, "openclaw.model_call.time_to_first_byte_ms"))
        response_processing = None
        response_boundary = "unavailable"
        if end is not None:
            boundary = _start_ms(first_tool) if first_tool else next_start
            if boundary is not None:
                response_processing = max(0.0, boundary - end)
                response_boundary = "model_end_to_tool_start" if first_tool else "model_end_to_next_model"
        rows.append({
            "trace_id": trace_id,
            "turn": turn,
            "observation_unit": _first(tags, "openclaw.model_call.observation_unit") or "request",
            "model": _first(tags, "gen_ai.request.model", "openclaw.model"),
            "provider": _first(tags, "gen_ai.system", "gen_ai.provider.name", "openclaw.provider"),
            "api": tags.get("openclaw.api"),
            "transport": tags.get("openclaw.transport"),
            "model_call_ms": model_ms,
            "ttft_ms": ttft_ms,
            "decode_proxy_ms": max(0.0, model_ms - ttft_ms) if model_ms is not None and ttft_ms is not None else None,
            "context_assembly_ms": context_span_ms if context_span_ms and context_span_ms > 0 else None,
            "context_span_present": context is not None,
            "context_span_duration_ms": context_span_ms,
            "context_measurement_status": (
                "measured" if context_span_ms and context_span_ms > 0
                else "span_present_zero_duration" if context is not None
                else "missing"
            ),
            "context_tokens": _number(_first(context_tags, "openclaw.context.tokens")),
            "history_chars": _number(_first(context_tags, "openclaw.history.size", "openclaw.context.history_text_chars")),
            "prompt_chars": _number(_first(tags, "openclaw.model_call.prompt.total_chars", "openclaw.prompt.size")),
            "input_messages": _number(tags.get("openclaw.model_call.prompt.input_messages_count")),
            "input_message_chars": _number(tags.get("openclaw.model_call.prompt.input_messages_chars")),
            "system_prompt_chars": _number(tags.get("openclaw.model_call.prompt.system_prompt_chars")),
            "tool_definition_count": _number(tags.get("openclaw.model_call.prompt.tool_definitions_count")),
            "tool_definition_chars": _number(tags.get("openclaw.model_call.prompt.tool_definitions_chars")),
            "request_bytes": _number(tags.get("openclaw.model_call.request_bytes")),
            "response_bytes": _number(tags.get("openclaw.model_call.response_bytes")),
            "input_tokens": _number(_first(tags, "gen_ai.usage.input_tokens", "openclaw.model_call.usage.input_tokens")),
            "output_tokens": _number(_first(tags, "gen_ai.usage.output_tokens", "openclaw.model_call.usage.output_tokens")),
            "cache_read_tokens": _number(_first(tags, "gen_ai.usage.cache_read_input_tokens", "gen_ai.usage.cache_read")),
            "cache_write_tokens": _number(_first(tags, "gen_ai.usage.cache_creation_input_tokens", "gen_ai.usage.cache_write")),
            "response_processing_ms": response_processing,
            "response_processing_full_ms": response_processing_full_ms,
            "context_assembly_residual_ms": (
                max(0.0, response_processing_full_ms - response_processing)
                if response_processing_full_ms is not None and response_processing is not None
                else None
            ),
            "response_boundary": response_boundary,
            "tool_dispatch_ms": tool_dispatch_ms,
            "tool_dispatch_overhead_ms": max(0.0, tool_dispatch_ms - exec_ms)
            if tool_dispatch_ms is not None and exec_ms is not None else tool_dispatch_ms,
            "tool_count": len(following_tools),
            "exec_ms": exec_ms,
            "context_assembly_window_ms": context_assembly_window_ms,
            "sandbox_init_ms": sandbox_init_ms,
            "exec_count": len(following_execs),
            "harness_ms": _span_ms(harness),
            "mock_ttft_ms": _number(edge.get("ttft_measured_ms")),
            "mock_decode_ms": _number(edge.get("decode_measured_ms")),
            "mock_prefill_ms": _number(edge.get("prefill_ms")),
            "mock_output_tokens": _number(edge.get("output_tokens")),
            "mock_input_tokens": _number(edge.get("input_tokens")),
            "mock_turn_start_ns": edge.get("t_recv_ns"),
            "mock_turn_end_ns": edge.get("t_end_emit_ns"),
        })
    return rows


def build_rows(raw_traces: list[dict[str, Any]], edges: list[dict[str, Any]], driver_requests: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    edge_map = _edge_index(edges)
    rows: list[dict[str, Any]] = []
    for trace in raw_traces:
        rows.extend(_trace_rows(trace, edge_map))
    requests = driver_requests or []
    for trace_id in {row["trace_id"] for row in rows}:
        trace_edges = sorted(
            [edge for edge in edges if str(edge.get("trace_id") or "") == trace_id],
            key=lambda edge: int(edge.get("turn_seq", 0)),
        )
        if not trace_edges:
            continue
        first_recv = int(trace_edges[0].get("t_recv_wall_ns") or 0)
        candidates = [request for request in requests if int(request.get("t_send_wall_ns") or 0) <= first_recv]
        request_start = max(candidates, key=lambda request: int(request.get("t_send_wall_ns") or 0)).get("t_send_wall_ns") if candidates else None
        for index, edge in enumerate(trace_edges):
            previous_end = request_start if index == 0 else trace_edges[index - 1].get("t_end_emit_wall_ns")
            if previous_end is None or edge.get("t_recv_wall_ns") is None:
                continue
            match = next((row for row in rows if row["trace_id"] == trace_id and row["turn"] == int(edge.get("turn_seq", index))), None)
            if match:
                match["prompt_boundary_ms"] = round((int(edge["t_recv_wall_ns"]) - int(previous_end)) / 1e6, 3)
        ordered_rows = sorted(
            [row for row in rows if row["trace_id"] == trace_id],
            key=lambda row: int(row.get("turn", 0)),
        )
        for index, row in enumerate(ordered_rows):
            if index == 0:
                row["context_assembly_bucket_ms"] = row.get("prompt_boundary_ms")
                row["response_processing_bucket_ms"] = 0.0
                row["sandbox_execution_bucket_ms"] = 0.0
            else:
                previous = ordered_rows[index - 1]
                row["context_assembly_bucket_ms"] = row.get("context_assembly_window_ms")
                row["response_processing_bucket_ms"] = previous.get("response_processing_full_ms") or 0.0
                row["sandbox_execution_bucket_ms"] = previous.get("exec_ms") or 0.0
    return rows


def _prom_series(path: Path) -> list[tuple[float, float]]:
    if not path.exists():
        return []
    try:
        payload = _load_json(path)
        result = (payload.get("data") or {}).get("result") or []
        by_timestamp: dict[float, float] = {}
        for series in result:
            for ts, value in series.get("values", []):
                if value in ("NaN", "Inf", "+Inf", "-Inf"):
                    continue
                timestamp = float(ts)
                by_timestamp[timestamp] = by_timestamp.get(timestamp, 0.0) + float(value)
        return sorted(by_timestamp.items())
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return []


def _counter_rate(series: list[tuple[float, float]]) -> list[tuple[float, float]]:
    rates: list[tuple[float, float]] = []
    for (previous_ts, previous_value), (timestamp, value) in zip(series, series[1:]):
        delta_s = timestamp - previous_ts
        if delta_s <= 0:
            continue
        # Counter resets/restarts are represented as a missing rate, not a spike.
        if value < previous_value:
            continue
        rates.append((timestamp, (value - previous_value) / delta_s))
    return rates


def _prom_counter_rate(path: Path) -> list[tuple[float, float]]:
    if not path.exists():
        return []
    try:
        payload = _load_json(path)
        result = (payload.get("data") or {}).get("result") or []
        by_timestamp: dict[float, float] = {}
        for series in result:
            points = sorted((float(ts), float(value)) for ts, value in series.get("values", []))
            for timestamp, rate in _counter_rate(points):
                by_timestamp[timestamp] = by_timestamp.get(timestamp, 0.0) + rate
        return sorted(by_timestamp.items())
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return []


def _plots(rows: list[dict[str, Any]], prom_dir: Path | None, out: Path) -> list[str]:
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        return []
    out.mkdir(parents=True, exist_ok=True)
    labels = [f"T{row['turn']}" for row in rows]
    x = list(range(len(rows)))
    files: list[str] = []

    import numpy as np
    from matplotlib.font_manager import FontProperties

    # Render the stage breakdown as a chronological stacked horizontal chart.
    # Keep model generation separate: these are orchestration-stage buckets.
    chart_rows = []
    for row in sorted(rows, key=lambda item: item.get("turn", 0)):
        context = row.get("context_assembly_bucket_ms") or 0.0
        if not chart_rows:
            context = row.get("context_assembly_window_ms") or context
        stages = {
            "Sandbox initialization": row.get("sandbox_init_ms") or 0.0,
            "Context assembly": context,
            "Response processing": row.get("response_processing_bucket_ms") or 0.0,
            "Sandbox tool execution": row.get("sandbox_execution_bucket_ms") or 0.0,
        }
        stages["Total_Latency"] = sum(stages.values())
        stages["step"] = f"Step {row.get('turn', 0)}"
        chart_rows.append(stages)

    chart_rows.reverse()
    y = np.arange(len(chart_rows))
    height = 0.62
    left = np.zeros(len(chart_rows))
    colors = {
        "Sandbox initialization": "#D97706",
        "Context assembly": "#0284C7",
        "Response processing": "#059669",
        "Sandbox tool execution": "#7C3AED",
    }
    stages = list(colors)
    regular = FontProperties(family="DejaVu Sans", weight="normal")

    fig, ax = plt.subplots(figsize=(12, 7.5), dpi=300)
    for stage in stages:
        values = np.array([item[stage] for item in chart_rows], dtype=float)
        ax.barh(
            y,
            values,
            left=left,
            height=height,
            label=stage,
            color=colors[stage],
            edgecolor="white",
            linewidth=1,
        )
        for index, (value, left_position) in enumerate(zip(values, left)):
            if value >= 45:
                ax.text(
                    left_position + value / 2,
                    y[index],
                    f"{value:.0f}",
                    va="center",
                    ha="center",
                    color="white",
                    fontsize=8.5,
                    fontproperties=regular,
                )
        left += values

    for index, item in enumerate(chart_rows):
        total = item["Total_Latency"]
        ax.text(
            total + 10,
            y[index],
            f"{total:.0f} ms",
            va="center",
            ha="left",
            fontsize=9.5,
            fontproperties=regular,
            color="#1E293B",
        )

    ax.set_yticks(y)
    ax.set_yticklabels([item["step"] for item in chart_rows], fontsize=10.5, color="#1E293B", fontproperties=regular)
    for tick in ax.get_yticklabels():
        tick.set_fontproperties(regular)
    ax.set_xlabel("Per Step Latency [ms]", fontsize=11, labelpad=10, color="#0F172A", fontproperties=regular)
    ax.set_ylabel("Step", fontsize=11, labelpad=10, color="#0F172A", fontproperties=regular)
    plt.suptitle(
        "OpenClaw & OpenShell Multi-Step Profiling Analysis",
        fontsize=15,
        fontproperties=FontProperties(family="DejaVu Sans", weight="normal", size=15),
        color="#0F172A",
        y=0.98,
        x=0.08,
        ha="left",
    )
    plt.title(
        "Cumulative end-to-end latency per step, categorized by execution stage",
        fontsize=10.5,
        color="#475569",
        pad=18,
        loc="left",
    )
    ax.grid(axis="x", linestyle=":", alpha=0.6, color="#94A3B8")
    ax.set_axisbelow(True)
    max_total = max((item["Total_Latency"] for item in chart_rows), default=0.0)
    ax.set_xlim(0, max(880.0, max_total + 90.0))
    ax.legend(
        loc="upper left",
        bbox_to_anchor=(1.01, 1.0),
        ncol=1,
        frameon=True,
        facecolor="#F8FAFC",
        edgecolor="#E2E8F0",
        fontsize=9.5,
    )
    plt.tight_layout()
    plt.subplots_adjust(top=0.86, right=0.78)
    path = out / "latency_waterfall.png"
    fig.savefig(path, dpi=160)
    plt.close(fig)
    files.append(path.name)

    # This is an observable boundary, not an additive waterfall bucket.
    boundary_values = [row.get("prompt_boundary_ms") for row in rows]
    if any(value is not None for value in boundary_values):
        fig, ax = plt.subplots(figsize=(15, max(5, len(rows) * 0.24)))
        ax.barh(x, [value or 0.0 for value in boundary_values], color="#2563eb")
        ax.set_yticks(x, labels)
        ax.set_xlabel("Milliseconds")
        ax.set_title("OpenClaw And OpenShell Multi-Turn Profiling Analysis - Prompt Boundary [ms]")
        ax.text(
            0.99,
            0.02,
            "Driver → first mock prompt; then previous mock turn end → next mock prompt",
            transform=ax.transAxes,
            ha="right",
            va="bottom",
            fontsize=9,
        )
        fig.tight_layout()
        path = out / "prompt_boundary.png"
        fig.savefig(path, dpi=160)
        plt.close(fig)
        files.append(path.name)

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    metrics = [
        ("prompt_chars", "Assembled prompt characters", axes[0, 0]),
        ("input_tokens", "Input tokens", axes[0, 1]),
        ("response_processing_full_ms", "Response processing ms", axes[1, 0]),
        ("exec_ms", "Sandbox execution ms", axes[1, 1]),
    ]
    for key, title, axis in metrics:
        values = [row.get(key) for row in rows]
        axis.plot(x, [value if value is not None else math.nan for value in values], marker="o")
        axis.set_title(title)
        axis.set_xlabel("Turn")
        axis.grid(alpha=0.25)
    fig.suptitle("OpenClaw And OpenShell Multi-Turn Profiling Analysis - Turn Metrics [ms]")
    fig.tight_layout()
    path = out / "otel_turn_metrics.png"
    fig.savefig(path, dpi=160)
    plt.close(fig)
    files.append(path.name)

    if prom_dir:
        fig, axes = plt.subplots(2, 1, figsize=(15, 9), sharex=True)
        cpu_files = ["cpu_openclaw.json", "cpu_openshell.json", "cpu_sandbox.json"]
        mem_files = ["memory_openclaw.json", "memory_openshell.json", "memory_sandbox.json"]
        cpu_series: dict[str, list[tuple[float, float]]] = {}
        memory_series: dict[str, list[tuple[float, float]]] = {}
        for filename in cpu_files:
            series = _prom_series(prom_dir / filename)
            if series:
                cpu_series[filename.removesuffix(".json")] = series
        for filename in mem_files:
            series = _prom_series(prom_dir / filename)
            if series:
                memory_series[filename.removesuffix(".json")] = series
        all_timestamps = [ts for series in [*cpu_series.values(), *memory_series.values()] for ts, _ in series]
        origin = min(all_timestamps) if all_timestamps else 0.0
        for label, series in cpu_series.items():
            axes[0].plot([ts - origin for ts, _ in series], [value for _, value in series], label=label)
        for label, series in memory_series.items():
            axes[1].plot([ts - origin for ts, _ in series], [value / (1024 * 1024) for _, value in series], label=label)
        axes[0].set_title("OpenClaw And OpenShell Multi-Turn Profiling Analysis - CPU [cores]")
        axes[0].set_ylabel("CPU cores")
        axes[1].set_title("OpenClaw And OpenShell Multi-Turn Profiling Analysis - Memory [MiB]")
        axes[1].set_ylabel("MiB")
        axes[1].set_xlabel("Seconds from benchmark window start")
        for axis in axes:
            axis.grid(alpha=0.25)
            if axis.lines:
                axis.legend()
        fig.tight_layout()
        path = out / "resource_timeseries.png"
        fig.savefig(path, dpi=160)
        plt.close(fig)
        files.append(path.name)
    return files


def profile(*, traces: Path, mock_edges: Path | None, out: Path, prom: Path | None = None, driver_requests: Path | None = None) -> dict[str, Any]:
    raw = _load_json(traces)
    if isinstance(raw, dict):
        raw = raw.get("data") or raw.get("traces") or []
    edges = _load_jsonl(mock_edges)
    driver_requests_rows = _load_driver_requests(driver_requests)
    edge_trace_ids = {str(edge.get("trace_id") or "") for edge in edges if edge.get("trace_id")}
    if edge_trace_ids:
        raw = [trace for trace in raw if str(trace.get("traceID") or "") in edge_trace_ids]
    rows = build_rows(raw, edges, driver_requests_rows)
    out.mkdir(parents=True, exist_ok=True)
    (out / "per_turn.json").write_text(json.dumps(rows, indent=2), encoding="utf-8")
    if rows:
        fieldnames = list(dict.fromkeys(key for row in rows for key in row))
        with (out / "per_turn.csv").open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
    direct_keys = ["context_assembly_ms", "model_call_ms", "ttft_ms", "response_processing_ms", "tool_dispatch_ms", "exec_ms"]
    summary = {
        "schema": "openclaw-profiler-v2",
        "turns": len(rows),
        "traces": len({row["trace_id"] for row in rows}),
        "observation_units": sorted({row["observation_unit"] for row in rows}),
        "direct_measurements": {key: _stats([row[key] for row in rows if row.get(key) is not None]) for key in direct_keys},
        "baseline_buckets": {
            "context_assembly_ms": _stats([row["context_assembly_window_ms"] for row in rows if row.get("context_assembly_window_ms") is not None]),
            "context_span_direct_ms": _stats([row["context_assembly_ms"] for row in rows if row.get("context_assembly_ms") is not None]),
            "prompt_boundary_ms": _stats([row["prompt_boundary_ms"] for row in rows if row.get("prompt_boundary_ms") is not None]),
            "response_processing_ms": _stats([row["response_processing_full_ms"] for row in rows if row.get("response_processing_full_ms") is not None]),
            "sandbox_execution_ms": _stats([row["exec_ms"] for row in rows if row.get("exec_ms") is not None]),
            "sandbox_initialization_ms": _stats([row["sandbox_init_ms"] for row in rows if row.get("sandbox_init_ms") is not None]),
        },
        "context_evidence": {
            "input_tokens": _stats([row["input_tokens"] for row in rows if row.get("input_tokens") is not None]),
            "prompt_chars": _stats([row["prompt_chars"] for row in rows if row.get("prompt_chars") is not None]),
            "system_prompt_chars": _stats([row["system_prompt_chars"] for row in rows if row.get("system_prompt_chars") is not None]),
            "tool_definition_chars": _stats([row["tool_definition_chars"] for row in rows if row.get("tool_definition_chars") is not None]),
        },
        "coverage": {key: sum(row.get(key) is not None for row in rows) for key in direct_keys},
        "definitions": {
            "context_assembly_ms": "OpenClaw trace window from the previous tool/run boundary to the next model call.",
            "context_span_direct_ms": "Direct patched openclaw.context.assembled span; current OpenClaw emits it once per agent run and not once per internal model call.",
            "model_call_ms": "Supporting provider span; excluded from the baseline overhead buckets.",
            "response_processing_ms": "openclaw.model.call end to first tool start or next model start.",
            "response_processing_full_ms": "First streamed model event to next prompt minus mock decode and sandbox execution; baseline-style agent processing bucket.",
            "sandbox_execution_ms": "Sum of direct openclaw.exec spans in the model-call window.",
            "sandbox_initialization_ms": "Not present in the agent-scope warm benchmark; startup is amortized before warm turns.",
            "context_per_turn_latency": "Measured through the driver/mock prompt boundary because the native OpenClaw event is run-level only.",
            "prompt_boundary_ms": "Driver send to the first mock prompt, then previous mock turn end to the next mock prompt; includes all OpenClaw work in that observable boundary.",
        },
        "plots": _plots(rows, prom, out / "diagrams"),
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    (out / "PROFILE.md").write_text(
        "# OpenClaw Profiler v2\n\n"
        "This report separates direct OpenTelemetry measurements from derived proxies.\n\n"
        + "\n".join(f"- `{key}`: {value}" for key, value in summary["definitions"].items())
        + "\n",
        encoding="utf-8",
    )
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Evidence-first OpenClaw multi-turn profiler")
    parser.add_argument("--traces", required=True, type=Path)
    parser.add_argument("--mock-edges", type=Path)
    parser.add_argument("--prom", type=Path)
    parser.add_argument("--driver-requests", type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args(argv)
    print(json.dumps(profile(traces=args.traces, mock_edges=args.mock_edges, prom=args.prom, driver_requests=args.driver_requests, out=args.out), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
