"""Export OpenClaw traces from Jaeger and extract per-turn timing segments.

Usage (after experiment, with port-forward to Jaeger running):
  oc -n trace-replay port-forward svc/jaeger 16686:16686 &
  python3 -m trace_replay_sim.jaeger_export \
    --out results/traces \
    --start 2026-08-25T10:00:00Z \
    --end   2026-08-25T11:00:00Z

Output files:
  per_turn_segments.json   — one row per LLM turn per session
  per_tool_segments.json   — one row per tool execution per session
  session_summary.json     — session-level E2E and turn count
  timing_segments.json     — per-turn T0-T4 breakdown (baseline-compatible)
"""

from __future__ import annotations

import argparse
import json
import ssl
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_JAEGER = "http://localhost:16686"
MAX_RETRIES = 3


def _get(url: str, *, insecure_skip_verify: bool = False) -> dict[str, Any]:
    ctx = ssl.create_default_context()
    if insecure_skip_verify:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    for attempt in range(MAX_RETRIES):
        try:
            with urllib.request.urlopen(url, context=ctx, timeout=30) as resp:
                return json.loads(resp.read())
        except Exception:
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(2 ** attempt)
    return {}


def fetch_traces(
    base: str,
    service: str,
    start_us: int,
    end_us: int,
    limit: int = 2000,
    tags: dict[str, str] | None = None,
    *,
    insecure_skip_verify: bool = False,
) -> list[dict[str, Any]]:
    query: dict[str, Any] = {
        "service": service,
        "start": start_us,
        "end": end_us,
        "limit": limit,
    }
    if tags:
        query["tags"] = json.dumps(tags, separators=(",", ":"))
    params = urllib.parse.urlencode(query)
    url = f"{base}/api/traces?{params}"
    print(f"  GET {url}", file=sys.stderr)
    data = _get(url, insecure_skip_verify=insecure_skip_verify)
    traces = data.get("data") or []
    if len(traces) >= limit:
        raise RuntimeError(
            f"Jaeger returned the configured limit ({limit}) for {service}; "
            "increase --limit or narrow the time range to avoid an incomplete export"
        )
    return traces


def _ns_to_ms(ns: int) -> float:
    return ns / 1_000_000.0


def _us_to_ms(us: int) -> float:
    return us / 1_000.0


def _span_duration_ms(span: dict) -> float:
    return _us_to_ms(span.get("duration") or 0)


def _span_start_ms(span: dict) -> float:
    return _us_to_ms(span.get("startTime") or 0)


def _tag(span: dict, key: str) -> Any:
    for t in span.get("tags") or []:
        if t.get("key") == key:
            return t.get("value")
    return None


def _parent_id(span: dict) -> str:
    for ref in span.get("references") or []:
        if ref.get("refType") == "CHILD_OF":
            return str(ref.get("spanID") or "")
    return ""


def compute_timing_segments(traces: list[dict], driver_e2e_ms: float | None = None) -> list[dict]:
    """Per-turn OpenClaw hops: assemble → TTFB → mock wait → process to next hop.

    Model TTFB is OpenClaw's ``time_to_first_byte_ms`` (SSE headers / dispatch),
    not mock first-token delay. Mock prefill sleep is ``model_streaming_ms``.
    Response processing is first content → next hop (tool / next model / harness).
    """
    segments: list[dict] = []

    for trace in traces:
        trace_id = trace.get("traceID", "")
        spans_by_op: dict[str, list[dict]] = {}
        mock_by_parent: dict[str, dict] = {}
        first_by_parent: dict[str, dict] = {}
        assembled_by_parent: dict[str, dict] = {}

        for span in trace.get("spans") or []:
            op = span.get("operationName", "")
            spans_by_op.setdefault(op, []).append(span)
            parent = _parent_id(span)
            if op == "mock-llm.stream" and parent:
                mock_by_parent[parent] = span
            if op == "mock-llm.first_content" and parent:
                first_by_parent[parent] = span
            if op == "mock-llm.assembled_prompt" and parent:
                assembled_by_parent[parent] = span

        harness_list = spans_by_op.get("openclaw.harness.run", [])
        run_list = spans_by_op.get("openclaw.run", [])
        model_list = spans_by_op.get("openclaw.model.call", [])
        tool_list = spans_by_op.get("openclaw.tool.execution", [])
        exec_list = spans_by_op.get("openclaw.exec", [])

        if not harness_list or not model_list:
            continue

        harness = harness_list[0]
        harness_start = (harness.get("startTime") or 0) / 1000.0
        harness_dur = (harness.get("duration") or 0) / 1000.0
        harness_end = harness_start + harness_dur

        run_start = harness_start
        if run_list:
            run_start = (run_list[0].get("startTime") or 0) / 1000.0

        model_list.sort(key=lambda s: s.get("startTime") or 0)
        hops = tool_list + exec_list
        session_execs = sorted(exec_list, key=lambda s: s.get("startTime") or 0)
        first_exec_id = session_execs[0].get("spanID") if session_execs else None
        prev_end = run_start

        for turn_idx, mc in enumerate(model_list):
            mc_start = (mc.get("startTime") or 0) / 1000.0
            mc_dur = (mc.get("duration") or 0) / 1000.0
            mc_end = mc_start + mc_dur
            mc_id = mc.get("spanID", "")
            next_mc_start = None
            if turn_idx + 1 < len(model_list):
                next_mc_start = (model_list[turn_idx + 1].get("startTime") or 0) / 1000.0

            context_assembly = max(0, mc_start - prev_end)

            mock_span = mock_by_parent.get(mc_id)
            first_span = first_by_parent.get(mc_id)
            model_streaming_ms = None
            model_prefill_ms = None
            first_content_ms = None
            mock_end = None
            mock_start = None
            ttfb_raw = _tag(mc, "openclaw.model_call.time_to_first_byte_ms")
            model_ttfb = float(ttfb_raw) if ttfb_raw is not None else None
            if mock_span:
                mock_start = (mock_span.get("startTime") or 0) / 1000.0
                mock_dur = (mock_span.get("duration") or 0) / 1000.0
                mock_end = mock_start + mock_dur
                ttft_attr = _tag(mock_span, "mock.ttft_ms")
                first_content_ms = mock_start + (float(ttft_attr) if ttft_attr is not None else 0)
            if first_span:
                first_content_ms = (first_span.get("startTime") or 0) / 1000.0
            # Prefill = mock TTFT sleep (first token). Decode = rest of mock stream (ITL).
            model_prefill_ms = None
            if first_content_ms is not None:
                ttfb_end = mc_start + (model_ttfb or 0)
                model_prefill_ms = max(0, first_content_ms - ttfb_end)
                if mock_end is not None:
                    model_streaming_ms = max(0, mock_end - first_content_ms)
            elif mock_end is not None:
                ttfb_end = mc_start + (model_ttfb or 0)
                model_streaming_ms = max(0, mock_end - ttfb_end)

            window_end = next_mc_start if next_mc_start is not None else harness_end
            turn_hops = []
            for h in hops:
                hs = (h.get("startTime") or 0) / 1000.0
                if hs >= mc_start - 1 and hs < window_end:
                    turn_hops.append(h)
            turn_hops.sort(key=lambda s: s.get("startTime") or 0)

            next_hop_kind = "harness_end"
            next_hop_start = window_end
            if turn_hops:
                next_hop_start = (turn_hops[0].get("startTime") or 0) / 1000.0
                op = turn_hops[0].get("operationName") or ""
                next_hop_kind = "exec" if op.endswith(".exec") else "tool"
            elif next_mc_start is not None:
                next_hop_kind = "model"
                next_hop_start = next_mc_start

            stream_end = mock_end if mock_end is not None else mc_end
            processing = max(0, next_hop_start - stream_end)

            turn_execs = [h for h in turn_hops if (h.get("operationName") or "").endswith(".exec")]
            sandbox_exec = sum((e.get("duration") or 0) / 1000.0 for e in turn_execs)
            sandbox_cold = 0.0
            if turn_execs and first_exec_id and turn_execs[0].get("spanID") == first_exec_id:
                sandbox_cold = (turn_execs[0].get("duration") or 0) / 1000.0

            prompt_chars = int(_tag(mc, "openclaw.model_call.prompt.total_chars") or 0)
            system_prompt_chars = int(_tag(mc, "openclaw.model_call.prompt.system_prompt_chars") or 0)
            assembled = assembled_by_parent.get(mc_id)
            assembled_chars = prompt_chars
            assembled_tokens = 0
            if assembled:
                assembled_chars = int(_tag(assembled, "mock.assembled_chars") or prompt_chars)
                assembled_tokens = int(_tag(assembled, "mock.assembled_tokens") or 0)
            input_tokens = int(_tag(mc, "gen_ai.usage.input_tokens") or
                             _tag(mc, "openclaw.model_call.usage.input_tokens") or 0)
            output_tokens = int(_tag(mc, "gen_ai.usage.output_tokens") or
                              _tag(mc, "openclaw.model_call.usage.output_tokens") or 0)

            segments.append({
                "trace_id": trace_id,
                "turn_idx": turn_idx,
                "sandbox_init_ms": round(sandbox_cold, 2),
                "sandbox_exec_ms": round(sandbox_exec, 2),
                "context_assembly_ms": round(context_assembly, 2),
                "model_ttfb_ms": round(model_ttfb, 2) if model_ttfb is not None else None,
                "model_prefill_ms": round(model_prefill_ms, 2) if model_prefill_ms is not None else None,
                "model_streaming_ms": round(model_streaming_ms, 2) if model_streaming_ms is not None else None,
                "response_processing_ms": round(processing, 2),
                "next_hop": next_hop_kind,
                "model_call_total_ms": round(mc_dur, 2),
                "harness_total_ms": round(harness_dur, 2),
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "prompt_chars": assembled_chars or prompt_chars,
                "prompt_tokens_est": assembled_tokens,
                "system_prompt_chars": system_prompt_chars,
            })
            prev_end = mc_end
            if turn_hops:
                last = turn_hops[-1]
                last_end = ((last.get("startTime") or 0) + (last.get("duration") or 0)) / 1000.0
                prev_end = max(prev_end, last_end)

    return segments


def extract_per_turn(traces: list[dict]) -> tuple[list[dict], list[dict], list[dict]]:
    """Return (per_turn_rows, per_tool_rows, session_rows)."""
    per_turn: list[dict] = []
    per_tool: list[dict] = []
    sessions: list[dict] = []

    for trace in traces:
        trace_id = trace.get("traceID", "")
        span_map: dict[str, dict] = {}
        for span in trace.get("spans") or []:
            span_map[span["spanID"]] = span

        # Find harness.run spans (one per session)
        harness_spans = [
            s for s in span_map.values()
            if s.get("operationName") == "openclaw.harness.run"
        ]
        # Find context.assembled spans
        ctx_spans = [
            s for s in span_map.values()
            if s.get("operationName") == "openclaw.context.assembled"
        ]
        # Find tool.execution spans
        tool_spans = [
            s for s in span_map.values()
            if s.get("operationName") == "openclaw.tool.execution"
        ]
        # Find model.call spans
        model_spans = [
            s for s in span_map.values()
            if s.get("operationName") == "openclaw.model.call"
        ]

        # Session-level summary
        for h in harness_spans:
            sessions.append({
                "trace_id": trace_id,
                "harness_span_id": h["spanID"],
                "harness_duration_ms": _span_duration_ms(h),
                "harness_start_ms": _span_start_ms(h),
                "model_calls": len(model_spans),
                "tool_calls": len(tool_spans),
                "outcome": _tag(h, "openclaw.outcome"),
            })

        # Per-turn rows: pair each model.call with its context.assembled child
        # Sort by start time to assign turn index
        model_spans.sort(key=lambda s: s.get("startTime") or 0)
        for turn_idx, mc in enumerate(model_spans):
            mc_start = _span_start_ms(mc)
            mc_dur = _span_duration_ms(mc)

            # Find context.assembled child within this model.call's time window
            ctx = next(
                (
                    c for c in ctx_spans
                    if abs(_span_start_ms(c) - mc_start) < 2000  # within 2s
                    and c.get("references", [{}])[0].get("spanID") == mc.get("spanID")
                ),
                None,
            )
            ctx_dur_ms = _span_duration_ms(ctx) if ctx else None
            ctx_tokens = int(_tag(ctx, "openclaw.context.tokens") or 0) if ctx else None
            ctx_prompt_size = int(_tag(ctx, "openclaw.prompt.size") or 0) if ctx else None
            ctx_history_size = int(_tag(ctx, "openclaw.history.size") or 0) if ctx else None

            # Find tool.execution spans that are children of this model.call
            turn_tools = [
                t for t in tool_spans
                if abs(_span_start_ms(t) - mc_start) < mc_dur + 5000
                and _span_start_ms(t) >= mc_start
            ]
            first_byte_ms = _tag(mc, "openclaw.model_call.time_to_first_byte_ms")
            request_bytes = _tag(mc, "openclaw.model_call.request_bytes")

            per_turn.append({
                "trace_id": trace_id,
                "turn_idx": turn_idx,
                "model_call_duration_ms": mc_dur,
                "model_call_start_ms": mc_start,
                "context_assembly_ms": ctx_dur_ms,
                "context_tokens": ctx_tokens,
                "prompt_size_chars": ctx_prompt_size,
                "history_size_chars": ctx_history_size,
                "time_to_first_byte_ms": float(first_byte_ms) if first_byte_ms else None,
                "request_bytes": int(request_bytes) if request_bytes else None,
                "tool_calls_this_turn": len(turn_tools),
            })

        # Per-tool rows: tag cold (first in session) vs warm
        tool_spans.sort(key=lambda s: s.get("startTime") or 0)
        seen_session: dict[str, bool] = {}
        for tool in tool_spans:
            tool_name = _tag(tool, "openclaw.toolName") or _tag(tool, "gen_ai.tool.name") or ""
            session_key = trace_id
            is_cold = session_key not in seen_session
            seen_session[session_key] = True

            # Detect parallelism: does this tool span overlap with any sibling?
            t_start = _span_start_ms(tool)
            t_end = t_start + _span_duration_ms(tool)
            parallel_count = sum(
                1 for other in tool_spans
                if other is not tool
                and _span_start_ms(other) < t_end
                and _span_start_ms(other) + _span_duration_ms(other) > t_start
            )

            per_tool.append({
                "trace_id": trace_id,
                "tool_name": tool_name,
                "duration_ms": _span_duration_ms(tool),
                "start_ms": t_start,
                "is_cold_start": is_cold,
                "parallel_siblings": parallel_count,
                "outcome": _tag(tool, "openclaw.outcome"),
                "error_category": _tag(tool, "openclaw.errorCategory"),
            })

    return per_turn, per_tool, sessions


def export(
    out_dir: Path,
    start: datetime,
    end: datetime,
    jaeger_base: str = DEFAULT_JAEGER,
    service: str = "openclaw-gateway",
    services: list[str] | None = None,
    tags: dict[str, str] | None = None,
    limit: int = 10_000,
    insecure_skip_verify: bool = False,
) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    start_us = int(start.timestamp() * 1_000_000)
    end_us = int(end.timestamp() * 1_000_000)

    print(f"Fetching traces from {jaeger_base} service={service}")
    print(f"  Time range: {start.isoformat()} → {end.isoformat()}")
    traces: list[dict[str, Any]] = []
    seen_trace_ids: set[str] = set()
    for candidate in [service, *(services or [])]:
        candidate_traces = fetch_traces(
            jaeger_base, candidate, start_us, end_us, limit=limit, tags=tags,
            insecure_skip_verify=insecure_skip_verify,
        )
        print(f"  service={candidate}: {len(candidate_traces)} traces found")
        for trace in candidate_traces:
            trace_id = str(trace.get("traceID") or "")
            if trace_id and trace_id in seen_trace_ids:
                continue
            if trace_id:
                seen_trace_ids.add(trace_id)
            traces.append(trace)
    print(f"  {len(traces)} unique traces found")

    if not traces:
        print("ERROR: No traces found. Check Jaeger connectivity and time range.", file=sys.stderr)
        summary = {"traces": 0, "error": "no traces found"}
        (out_dir / "export_summary.json").write_text(json.dumps(summary, indent=2))
        return summary

    # Save raw traces for debugging
    (out_dir / "raw_traces.json").write_text(json.dumps(traces, indent=2), encoding="utf-8")

    per_turn, per_tool, sessions = extract_per_turn(traces)
    timing_segments = compute_timing_segments(traces)

    (out_dir / "per_turn_segments.json").write_text(
        json.dumps(per_turn, indent=2), encoding="utf-8"
    )
    (out_dir / "per_tool_segments.json").write_text(
        json.dumps(per_tool, indent=2), encoding="utf-8"
    )
    (out_dir / "session_summary.json").write_text(
        json.dumps(sessions, indent=2), encoding="utf-8"
    )
    (out_dir / "timing_segments.json").write_text(
        json.dumps(timing_segments, indent=2), encoding="utf-8"
    )

    summary = {
        "traces": len(traces),
        "sessions": len(sessions),
        "per_turn_rows": len(per_turn),
        "per_tool_rows": len(per_tool),
        "timing_segment_rows": len(timing_segments),
        "cold_starts": sum(1 for t in per_tool if t["is_cold_start"]),
        "parallel_tool_pairs": sum(1 for t in per_tool if t["parallel_siblings"] > 0),
        "output": str(out_dir),
    }
    (out_dir / "export_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary, indent=2))
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Export Jaeger traces and extract per-turn segments")
    parser.add_argument("--out", required=True, help="Output directory")
    parser.add_argument("--start", required=True, help="ISO 8601 start time")
    parser.add_argument("--end", required=True, help="ISO 8601 end time")
    parser.add_argument("--jaeger", default=DEFAULT_JAEGER)
    parser.add_argument("--service", default="openclaw-gateway")
    parser.add_argument("--limit", type=int, default=10_000)
    parser.add_argument("--insecure-skip-verify", action="store_true")
    args = parser.parse_args(argv)

    def parse_iso(s: str) -> datetime:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).astimezone(timezone.utc)

    export(
        out_dir=Path(args.out),
        start=parse_iso(args.start),
        end=parse_iso(args.end),
        jaeger_base=args.jaeger,
        service=args.service,
        limit=args.limit,
        insecure_skip_verify=args.insecure_skip_verify,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
