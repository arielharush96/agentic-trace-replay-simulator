"""Unified analysis: combine driver, Jaeger, and Prometheus data into a blog-ready report.

Produces:
  - REPORT.md — narrative report with tables and findings
  - analysis.json — structured data for programmatic use
"""

from __future__ import annotations

import json
import statistics
from pathlib import Path
from typing import Any


def _load_json(path: Path) -> Any:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def _prom_values(path: Path) -> list[float]:
    data = _load_json(path)
    if not data or data.get("status") == "error":
        return []
    values: list[float] = []
    for series in (data.get("data") or {}).get("result") or []:
        for _ts, raw in series.get("values") or []:
            try:
                values.append(float(raw))
            except (TypeError, ValueError):
                continue
    return values


def _pct(values: list[float], p: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round((p / 100.0) * (len(ordered) - 1)))))
    return ordered[idx]


def _stat_block(values: list[float], scale: float = 1.0) -> dict[str, Any]:
    if not values:
        return {"n": 0}
    scaled = [v * scale for v in values]
    return {
        "n": len(scaled),
        "mean": round(sum(scaled) / len(scaled), 4),
        "p50": round(_pct(scaled, 50) or 0.0, 4),
        "p95": round(_pct(scaled, 95) or 0.0, 4),
        "max": round(max(scaled), 4),
    }


def analyze_layer(layer_dir: Path, prom_dir: Path) -> dict[str, Any] | None:
    """Analyze a single layer (mock-direct, openclaw, or shell)."""
    summary = _load_json(layer_dir / "summary.json")
    if not summary:
        return None

    result: dict[str, Any] = {
        "layer": summary.get("layer"),
        "concurrency": summary.get("concurrency"),
        "requests": summary.get("requests"),
        "ok": summary.get("ok"),
        "errors": summary.get("errors"),
        "rps": summary.get("rps"),
        "latency": {
            "e2e_p50_ms": summary.get("e2e_p50_ms"),
            "e2e_p95_ms": summary.get("e2e_p95_ms"),
            "e2e_mean_ms": summary.get("e2e_mean_ms"),
            "ttft_p50_ms": summary.get("ttft_p50_ms"),
            "ttft_p95_ms": summary.get("ttft_p95_ms"),
        },
    }

    # Resource metrics from Prometheus
    if prom_dir and prom_dir.exists():
        cpu_oc = _prom_values(prom_dir / "cpu_openclaw.json")
        mem_oc = _prom_values(prom_dir / "memory_openclaw.json")
        cpu_mock = _prom_values(prom_dir / "cpu_mock_llm.json")
        mem_mock = _prom_values(prom_dir / "memory_mock_llm.json")
        result["resources"] = {
            "openclaw_cpu_cores": _stat_block(cpu_oc),
            "openclaw_memory_mib": _stat_block(mem_oc, 1 / (1024 * 1024)),
            "mock_llm_cpu_cores": _stat_block(cpu_mock),
            "mock_llm_memory_mib": _stat_block(mem_mock, 1 / (1024 * 1024)),
        }

    return result


def analyze_timing(traces_dir: Path) -> dict[str, Any] | None:
    """Analyze per-turn timing from Jaeger export."""
    segments = _load_json(traces_dir / "timing_segments.json")
    if not segments:
        return None

    n = len(segments)
    sandbox_vals = [s["sandbox_init_ms"] for s in segments]
    ctx_vals = [s["context_assembly_ms"] for s in segments]
    ttfb_vals = [s["model_ttfb_ms"] for s in segments if s["model_ttfb_ms"] is not None]
    resp_vals = [s["response_processing_ms"] for s in segments if s["response_processing_ms"] is not None]
    total_vals = [s["model_call_total_ms"] for s in segments]

    result = {
        "total_turns": n,
        "sandbox_init_ms": {"mean": round(statistics.mean(sandbox_vals), 1), "p95": round(_pct(sandbox_vals, 95) or 0, 1)},
        "context_assembly_ms": {"mean": round(statistics.mean(ctx_vals), 1), "p95": round(_pct(ctx_vals, 95) or 0, 1)},
        "model_ttfb_ms": {"mean": round(statistics.mean(ttfb_vals), 1) if ttfb_vals else None, "p95": round(_pct(ttfb_vals, 95) or 0, 1) if ttfb_vals else None},
        "response_processing_ms": {"mean": round(statistics.mean(resp_vals), 1) if resp_vals else None, "p95": round(_pct(resp_vals, 95) or 0, 1) if resp_vals else None},
        "total_per_turn_ms": {"mean": round(statistics.mean(total_vals), 1), "p50": round(_pct(total_vals, 50) or 0, 1), "p95": round(_pct(total_vals, 95) or 0, 1)},
    }

    # Per-turn-index breakdown
    by_turn: dict[int, list[float]] = {}
    for s in segments:
        by_turn.setdefault(s["turn_idx"], []).append(s["model_call_total_ms"])
    result["per_turn_index"] = {
        f"turn_{idx}": {"mean_ms": round(statistics.mean(vals), 1), "count": len(vals)}
        for idx, vals in sorted(by_turn.items())
    }

    return result


def write_report(results_dir: Path) -> Path:
    """Generate the unified REPORT.md and analysis.json."""
    # Find layer directories
    layers: list[dict[str, Any]] = []
    prom_dir = results_dir / "prometheus"

    for layer_name in ["mock-direct", "openclaw", "shell"]:
        layer_dir = results_dir / layer_name
        if layer_dir.exists():
            layer_data = analyze_layer(layer_dir, prom_dir)
            if layer_data:
                layers.append(layer_data)

    # Analyze OTEL timing
    traces_dir = results_dir / "traces"
    timing = analyze_timing(traces_dir) if traces_dir.exists() else None

    # Per-turn analysis (Q1-Q5)
    q_analysis = _load_json(results_dir / "analysis" / "per_turn_analysis.json")

    # Build structured output
    analysis = {
        "layers": layers,
        "timing_breakdown": timing,
        "engineering_questions": q_analysis,
    }
    (results_dir / "analysis.json").write_text(json.dumps(analysis, indent=2), encoding="utf-8")

    # Build REPORT.md
    lines = _build_report_md(layers, timing, q_analysis)
    report_path = results_dir / "REPORT.md"
    report_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Report written: {report_path}")
    return report_path


def _build_report_md(layers: list[dict], timing: dict | None, q_analysis: dict | None) -> list[str]:
    lines = [
        "# Agentic AI Stack Performance: Trace-Replay Characterization",
        "",
        "## Executive Summary",
        "",
        "This experiment isolates the **OpenClaw agentic gateway** and **OpenShell sandbox**",
        "from GPU inference by replaying recorded multi-turn LLM traces through the stack.",
        "A mock LLM returns pre-recorded responses with simulated streaming latency (TTFT=100ms, ITL=50ms),",
        "allowing measurement of pure gateway overhead: context assembly, routing, tool dispatch,",
        "response processing, and sandbox lifecycle costs.",
        "",
    ]

    # Key findings
    if layers:
        mock_direct = next((l for l in layers if l["layer"] == "mock-direct"), None)
        openclaw = next((l for l in layers if l["layer"] == "openclaw"), None)
        if mock_direct and openclaw:
            overhead = (openclaw["latency"]["e2e_p50_ms"] or 0) - (mock_direct["latency"]["e2e_p50_ms"] or 0)
            lines += [
                "### Key Findings",
                "",
                f"- **OpenClaw overhead per session**: ~{overhead:.0f}ms (P50 E2E difference vs mock-direct)",
            ]
            if timing:
                lines.append(f"- **Per-turn overhead**: ~{timing['total_per_turn_ms']['mean']:.0f}ms mean across {timing['total_turns']} turns")
                if timing.get("context_assembly_ms", {}).get("mean"):
                    lines.append(f"- **Context assembly**: ~{timing['context_assembly_ms']['mean']:.0f}ms (first turn)")
                if timing.get("model_ttfb_ms", {}).get("mean"):
                    lines.append(f"- **Model TTFB (simulated)**: ~{timing['model_ttfb_ms']['mean']:.0f}ms")
                if timing.get("response_processing_ms", {}).get("mean"):
                    lines.append(f"- **Response processing**: ~{timing['response_processing_ms']['mean']:.0f}ms per turn")
            lines.append("")

    # Layer comparison table
    lines += [
        "## Layer Comparison",
        "",
        "| Layer | C | Requests | OK | Errors | E2E P50 | E2E P95 | TTFT P50 | TTFT P95 | RPS |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for run in layers:
        lat = run["latency"]
        lines.append(
            f"| {run['layer']} | {run['concurrency']} | {run['requests']} | {run['ok']} | {run['errors']} "
            f"| {_fmt_ms(lat.get('e2e_p50_ms'))} | {_fmt_ms(lat.get('e2e_p95_ms'))} "
            f"| {_fmt_ms(lat.get('ttft_p50_ms'))} | {_fmt_ms(lat.get('ttft_p95_ms'))} | {run.get('rps')} |"
        )
    lines.append("")

    # Timing breakdown
    if timing:
        lines += [
            "## Per-Turn Timing Breakdown (from OTEL/Jaeger)",
            "",
            f"Based on {timing['total_turns']} turns extracted from Jaeger traces.",
            "",
            "| Stage | Mean | P95 |",
            "|---|---:|---:|",
            f"| Sandbox Init | {_fmt_ms_val(timing['sandbox_init_ms'].get('mean'))} | {_fmt_ms_val(timing['sandbox_init_ms'].get('p95'))} |",
            f"| Context Assembly | {_fmt_ms_val(timing['context_assembly_ms'].get('mean'))} | {_fmt_ms_val(timing['context_assembly_ms'].get('p95'))} |",
            f"| Model TTFB | {_fmt_ms_val(timing['model_ttfb_ms'].get('mean'))} | {_fmt_ms_val(timing['model_ttfb_ms'].get('p95'))} |",
            f"| Response Processing | {_fmt_ms_val(timing['response_processing_ms'].get('mean'))} | {_fmt_ms_val(timing['response_processing_ms'].get('p95'))} |",
            f"| **Total per turn** | **{_fmt_ms_val(timing['total_per_turn_ms'].get('mean'))}** | **{_fmt_ms_val(timing['total_per_turn_ms'].get('p95'))}** |",
            "",
        ]
        if timing.get("per_turn_index"):
            lines += ["### Per-Turn-Index Progression", "", "| Turn | Mean (ms) | Sessions |", "|---:|---:|---:|"]
            for key, val in timing["per_turn_index"].items():
                lines.append(f"| {key} | {val['mean_ms']} | {val['count']} |")
            lines.append("")

    # Resource utilization
    res_layers = [l for l in layers if l.get("resources")]
    if res_layers:
        lines += [
            "## Resource Utilization (Prometheus/cAdvisor)",
            "",
            "| Layer | OpenClaw CPU P95 | OpenClaw Mem P95 | Mock-LLM CPU P95 |",
            "|---|---:|---:|---:|",
        ]
        for run in res_layers:
            res = run["resources"]
            lines.append(
                f"| {run['layer']} | {_fmt_num(res['openclaw_cpu_cores'].get('p95'), 'cores')} "
                f"| {_fmt_num(res['openclaw_memory_mib'].get('p95'), 'MiB')} "
                f"| {_fmt_num(res['mock_llm_cpu_cores'].get('p95'), 'cores')} |"
            )
        lines.append("")

    # Engineering questions
    if q_analysis:
        lines += ["## Engineering Analysis (Q1-Q5)", ""]
        for key, val in q_analysis.items():
            if key == "sessions_analyzed":
                continue
            if isinstance(val, dict):
                status = val.get("status", "unknown")
                assessment = val.get("assessment", "")
                implication = val.get("implication", "")
                lines.append(f"### {key.replace('_', ' ').title()}")
                lines.append("")
                if status == "no_data":
                    lines.append("*No data available for this question.*")
                else:
                    if assessment:
                        lines.append(f"**Assessment**: {assessment}")
                    if implication:
                        lines.append(f"")
                        lines.append(f"{implication}")
                    # Add key metrics
                    for k, v in val.items():
                        if k in ("status", "assessment", "implication"):
                            continue
                        if isinstance(v, (int, float)):
                            lines.append(f"- {k}: {v}")
                lines.append("")

    # Methodology
    lines += [
        "## Methodology",
        "",
        "- **Dataset**: Exgentic/agent-llm-traces (HuggingFace) — real recorded LLM sessions",
        "- **Mock LLM**: Returns pre-recorded responses with simulated streaming (TTFT=100ms, ITL=50ms per chunk)",
        "- **Driver**: Sends one request per session to `/v1/responses` with streaming enabled",
        "- **OpenClaw**: Full agentic gateway with context assembly, tool schemas (group:fs, group:sessions, group:memory, group:runtime), and diagnostics-otel/prometheus plugins",
        "- **Measurement**: Driver E2E/TTFT + Jaeger OTEL spans for per-turn breakdown + Prometheus for resource usage",
        "",
        "## Notes",
        "",
        "- `mock-direct` = driver to mock-LLM directly (control, no gateway)",
        "- `openclaw` = driver to OpenClaw gateway to mock-LLM (measures gateway overhead)",
        "- `shell` = driver to OpenClaw+OpenShell (adds sandbox provisioning + tool execution)",
        "- Per-turn breakdown from OTEL reflects OpenClaw's internal processing loop across multiple model calls and tool executions within a single user request",
        "",
    ]

    return lines


def _fmt_ms(value: Any) -> str:
    if value is None:
        return "—"
    return f"{float(value):.1f}ms"


def _fmt_ms_val(value: Any) -> str:
    if value is None:
        return "—"
    return f"{float(value):.1f}ms"


def _fmt_num(value: Any, unit: str) -> str:
    if value is None:
        return "—"
    return f"{float(value):.2f} {unit}"
