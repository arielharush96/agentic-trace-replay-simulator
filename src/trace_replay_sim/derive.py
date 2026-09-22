"""Fuse all measurement sources into per-turn 5-bucket latency records.

The **mock edge JSONL is the spine** (authoritative, single-clock per-turn
ordering via ``turn_seq`` and the skew-free gap ``G[i]``). Jaeger OpenClaw spans,
K8s pod lifecycle, and driver session edges are joined on top to *decompose* and
*cross-check* each bucket. Every source is optional: with only mock edges we still
get C/E and the combined ``G[i]`` overhead; add Jaeger and ``G[i]`` splits into A/B/D.

Five buckets per turn (execution order within a turn):

* **A. pre-model overhead** (HTTP parse + context assembly, combined per plan):
  ``model.call[i].start - prev_hop.end`` (turn 0: ``model.call[0].start - run.start``).
* **B. sandbox init/exec**: ``tool.execution[i].duration`` (turn-0 first call = cold,
  cross-checked by K8s pod ``creationTimestamp -> Ready``).
* **C. model TTFT**: mock ``ttft_set_ms`` (controlled ground truth); cross-checked by
  mock ``ttft_measured_ms`` and OpenClaw ``time_to_first_byte_ms``.
* **D. response processing**: ``tool.execution[i].start - model.call[i].end``.
* **E. model decode**: mock ``decode_set_ms``; cross-checked by ``decode_measured_ms``.

Skew-free constraint (single mock clock): ``G[i] = t_recv[i+1] - t_end_emit[i]``
should equal ``D[i] + B[i] + A[i+1]``. The delta is reported as an agreement check.
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from pathlib import Path
from typing import Any


# --------------------------------------------------------------------------- #
# Source loaders (all tolerant of missing files).                             #
# --------------------------------------------------------------------------- #

def load_mock_edges(path: Path) -> list[dict[str, Any]]:
    """Load per-turn mock edge records from a JSONL file or a pod-log capture.

    Accepts raw JSONL lines OR log lines prefixed with ``MOCK_EDGE ``.
    """
    if not path.exists():
        return []
    edges: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("MOCK_EDGE "):
            line = line[len("MOCK_EDGE "):]
        if not line.startswith("{"):
            continue
        try:
            rec = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            continue
        if rec.get("schema", "").startswith("mock-edge"):
            edges.append(rec)
    return edges


def load_driver_requests(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            try:
                out.append(json.loads(line))
            except (json.JSONDecodeError, ValueError):
                pass
    return out


def load_traces(path: Path) -> list[dict[str, Any]]:
    """Load raw Jaeger traces from a ``raw_traces.json`` file (list of traces)."""
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        return data.get("data") or []
    return data if isinstance(data, list) else []


def load_k8s_lifecycle(path: Path) -> list[dict[str, Any]]:
    """Load sandbox pod lifecycle deltas (cold create Ready-delta), if captured."""
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        return [data]
    return data if isinstance(data, list) else []


# --------------------------------------------------------------------------- #
# Jaeger span indexing.                                                        #
# --------------------------------------------------------------------------- #

def _us_to_ms(us: Any) -> float:
    return (us or 0) / 1000.0


def _span_end_ms(span: dict) -> float:
    return _us_to_ms(span.get("startTime")) + _us_to_ms(span.get("duration"))


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


def index_traces(traces: list[dict]) -> dict[str, dict]:
    """Return {trace_id: {span_id: span, ops: {op: [spans]}}}."""
    idx: dict[str, dict] = {}
    for trace in traces:
        tid = trace.get("traceID", "")
        by_id: dict[str, dict] = {}
        ops: dict[str, list[dict]] = {}
        for span in trace.get("spans") or []:
            by_id[span.get("spanID", "")] = span
            ops.setdefault(span.get("operationName", ""), []).append(span)
        for lst in ops.values():
            lst.sort(key=lambda s: s.get("startTime") or 0)
        idx[tid] = {"by_id": by_id, "ops": ops}
    return idx


# --------------------------------------------------------------------------- #
# Core derivation.                                                             #
# --------------------------------------------------------------------------- #

def _agree(primary: float | None, cross: float | None) -> float | None:
    if primary is None or cross is None:
        return None
    return round(cross - primary, 3)


def derive_per_turn(
    edges: list[dict],
    traces: list[dict],
    *,
    k8s: list[dict] | None = None,
) -> list[dict]:
    """Build per-turn 5-bucket records keyed by (session_id, turn_seq)."""
    k8s = k8s or []
    # Cold Ready-delta from K8s (first successful pod), used to cross-check turn-0 B.
    cold_ready_ms = None
    for entry in k8s:
        val = entry.get("ready_delta_ms") or entry.get("ready_delta_s")
        if val is not None:
            cold_ready_ms = float(val) * (1000.0 if "ready_delta_s" in entry else 1.0)
            break

    tidx = index_traces(traces)

    # Group edges by (session_id, trace_id): one agentic RUN == one trace, and its
    # turns 0..N. A single-session corpus replayed over N reps shares session_id but
    # has a distinct trace_id per rep, so grouping by session alone would collide
    # duplicate turn_seqs (corrupting G[i]) and mis-flag cold turns. Keying by the
    # run makes turn_seq ordering, the skew-free gap, and the per-rep cold turn-0
    # all correct. Edges with no trace_id (mock-direct --no-spans, tests) fall back
    # to grouping under their session_id.
    by_run: dict[tuple[str, str], list[dict]] = {}
    for e in edges:
        key = (str(e.get("session_id")), str(e.get("trace_id") or ""))
        by_run.setdefault(key, []).append(e)
    for lst in by_run.values():
        lst.sort(key=lambda e: e.get("turn_seq", 0))

    rows: list[dict] = []
    for (sid, _run_tid), sedges in by_run.items():
        real = [e for e in sedges if not e.get("phantom") and not e.get("exhausted") and not e.get("miss")]
        # G[i] from the FULL ordered edge list (needs the next call's t_recv, even if terminal).
        recv_by_seq = {e.get("turn_seq"): e.get("t_recv_ns") for e in sedges}

        for pos, e in enumerate(real):
            seq = e.get("turn_seq")
            trace_id = e.get("trace_id") or ""
            parent = e.get("parent_span_id") or ""
            tinfo = tidx.get(trace_id)

            # --- C: model TTFT (mock authoritative) ---------------------------
            c_primary = e.get("ttft_set_ms")
            c_cross_mock = e.get("ttft_measured_ms")

            # --- E: model decode (mock authoritative) -------------------------
            # The mock's decode_set covers ALL streamed tokens INCLUDING the
            # reasoning preamble.  The gateway buffers that reasoning block before
            # the first VISIBLE token (the baseline's "Response Processing" cost),
            # so we split it off decode and attribute it to bucket D below.  E is
            # then the visible-token streaming only.
            reasoning_ms = e.get("reasoning_set_ms") or 0.0
            response_tail_ms = e.get("response_processing_tail_ms") or 0.0
            decode_total = e.get("decode_set_ms")
            e_primary = (round(max(0.0, decode_total - reasoning_ms), 3)
                         if decode_total is not None else None)
            e_cross_mock = e.get("decode_measured_ms")

            # --- G[i]: skew-free gap to next model call (single clock) --------
            g_ms = None
            next_recv = recv_by_seq.get(seq + 1)
            if next_recv is not None and e.get("t_end_emit_ns") is not None:
                g_ms = round((next_recv - e["t_end_emit_ns"]) / 1e6, 3)

            # --- Jaeger decomposition of the gap into A/B/D -------------------
            # d_span_ms  = tool.start - model.call.end (the naive gap; UNDERCOUNTS -
            #              it misses response work OpenClaw does while model.call is
            #              still open, which is why it collapsed to ~7ms).
            # d_inside_ms= (model.call.dur - (TTFT+decode)) : the response handling
            #              absorbed *inside* the model.call span (serialize request,
            #              read/parse the SSE stream, extract tool-call JSON). Folds
            #              in in-cluster network RTT; the mock-direct layer removes it.
            # d_ms       = the primary D, resolved after we know G[i] and A[i+1]:
            #              the skew-free residual G - B - A_next when available, else
            #              the span-inside estimate. See the "primary D" block below.
            a_ms = b_ms = d_ms = None
            d_span_ms = d_inside_ms = None
            c_cross_ocw = None
            mock_stream_ms = None
            mock_first_visible_ms = None
            mock_first_tool_ms = None
            mock_first_to_tool_ms = None
            mock_last_to_tool_ms = None
            context_assembly_direct_ms = None
            if e.get("t_first_emit_ns") is not None and e.get("t_end_emit_ns") is not None:
                mock_stream_ms = round((e["t_end_emit_ns"] - e["t_first_emit_ns"]) / 1e6, 3)
            if e.get("t_first_visible_emit_ns") is not None and e.get("t_first_emit_ns") is not None:
                mock_first_visible_ms = round((e["t_first_visible_emit_ns"] - e["t_first_emit_ns"]) / 1e6, 3)
            if e.get("t_first_tool_emit_ns") is not None and e.get("t_first_emit_ns") is not None:
                mock_first_tool_ms = round((e["t_first_tool_emit_ns"] - e["t_first_emit_ns"]) / 1e6, 3)
            mc = tinfo["by_id"].get(parent) if tinfo else None
            if tinfo and mc is not None:
                ops = tinfo["ops"]
                context_spans = ops.get("openclaw.context.assembled", [])
                tools = ops.get("openclaw.tool.execution", [])
                execs = ops.get("openclaw.exec", [])
                runs = ops.get("openclaw.run", []) or ops.get("openclaw.harness.run", [])
                hops = sorted(tools + execs, key=lambda s: s.get("startTime") or 0)

                mc_start = _us_to_ms(mc.get("startTime"))
                mc_end = _span_end_ms(mc)
                mc_dur = _us_to_ms(mc.get("duration"))
                for context_span in context_spans:
                    references = context_span.get("references") or []
                    if any(ref.get("spanID") == parent for ref in references):
                        context_assembly_direct_ms = round(_us_to_ms(context_span.get("duration")), 3)
                        break

                # A[i]: pre-model gap. prev hop end (or run start for the first).
                prev_end = None
                prev_hops = [h for h in hops if _span_end_ms(h) <= mc_start + 1]
                if prev_hops:
                    prev_end = max(_span_end_ms(h) for h in prev_hops)
                elif runs:
                    prev_end = _us_to_ms(runs[0].get("startTime"))
                if prev_end is not None:
                    a_ms = round(max(0.0, mc_start - prev_end), 3)

                # This turn's tool hop = first hop starting after this model call.
                turn_hops = [h for h in hops if (h.get("startTime") or 0) / 1000.0 >= mc_start]
                turn_hop = turn_hops[0] if turn_hops else None
                if turn_hop is not None:
                    b_ms = round(_us_to_ms(turn_hop.get("duration")), 3)
                    d_span_ms = round(max(0.0, _us_to_ms(turn_hop.get("startTime")) - mc_end), 3)
                    if e.get("t_first_emit_wall_ns") is not None:
                        mock_first_to_tool_ms = round(
                            _us_to_ms(turn_hop.get("startTime")) - e["t_first_emit_wall_ns"] / 1e6, 3
                        )
                    if e.get("t_end_emit_wall_ns") is not None:
                        mock_last_to_tool_ms = round(
                            _us_to_ms(turn_hop.get("startTime")) - e["t_end_emit_wall_ns"] / 1e6, 3
                        )

                # Response work hidden inside the model.call span: observed call
                # duration minus the mock-controlled model time (TTFT + decode).
                if c_primary is not None and e_primary is not None:
                    d_inside_ms = round(max(0.0, mc_dur - (c_primary + e_primary)), 3)

                ttfb = _tag(mc, "openclaw.model_call.time_to_first_byte_ms")
                c_cross_ocw = float(ttfb) if ttfb is not None else None

            # --- agreement: G[i] vs D[i]+B[i]+A[i+1] -------------------------
            g_decomp = None
            g_delta = None
            next_a = None
            if pos + 1 < len(real):
                # A of the next real turn (computed lazily below is complex; recompute here)
                nxt = real[pos + 1]
                n_mc = tidx.get(nxt.get("trace_id") or "", {}).get("by_id", {}).get(nxt.get("parent_span_id") or "") if traces else None
                if n_mc is not None and tinfo is not None:
                    ops = tinfo["ops"]
                    hops = sorted(ops.get("openclaw.tool.execution", []) + ops.get("openclaw.exec", []),
                                  key=lambda s: s.get("startTime") or 0)
                    n_start = _us_to_ms(n_mc.get("startTime"))
                    prev_hops = [h for h in hops if _span_end_ms(h) <= n_start + 1]
                    if prev_hops:
                        next_a = round(max(0.0, n_start - max(_span_end_ms(h) for h in prev_hops)), 3)
            # Diagnostic decomposition uses the NAIVE span gap on purpose: a large
            # positive g_delta means the spans undercount the real overhead (the
            # missing time is mostly response processing hidden inside model.call).
            if None not in (d_span_ms, b_ms) and next_a is not None:
                g_decomp = round(d_span_ms + b_ms + next_a + response_tail_ms, 3)
                if g_ms is not None:
                    g_delta = round(g_ms - g_decomp, 3)

            # Primary D (robust): skew-free residual G[i] - B[i] - A[i+1] on the
            # single mock clock, which contains ALL the overhead the spans miss.
            # Fallbacks (last turn / missing gap): inside-span work + post-span
            # dispatch, then the naive span gap.
            d_skewfree_ms = None
            if g_ms is not None and b_ms is not None and next_a is not None:
                d_skewfree_ms = round(max(0.0, g_ms - b_ms - next_a), 3)
                d_ms = d_skewfree_ms
            elif d_inside_ms is not None:
                d_ms = round(d_inside_ms + (d_span_ms or 0.0), 3)
            else:
                d_ms = d_span_ms

            # Response processing = gateway dispatch (span/skew-free, above) PLUS the
            # reasoning-preamble buffering the gateway does before the first visible
            # token.  The latter is the mechanism that reproduces the baseline's
            # ~895ms "Response Processing" cost (see mock_llm reasoning notes).
            d_gateway_ms = d_ms
            if reasoning_ms:
                d_ms = round((d_ms or 0.0) + reasoning_ms, 3)
            # The calibrated mock response tail occurs after the mock edge's
            # end timestamp. It is already included in G for non-terminal
            # turns; add it explicitly for the final turn, which has no G.
            if g_ms is None and response_tail_ms:
                d_ms = round((d_ms or 0.0) + response_tail_ms, 3)

            # Full harness cycle requested by the benchmark: from the first
            # model event through the next model prompt, excluding OpenShell.
            # G is final-event -> next prompt, so add the stream duration first.
            response_cycle_ms = None
            openclaw_response_only_ms = None
            if mock_stream_ms is not None and g_ms is not None and b_ms is not None:
                response_cycle_ms = round(mock_stream_ms + g_ms - b_ms, 3)
                if e_primary is not None:
                    openclaw_response_only_ms = round(response_cycle_ms - e_primary, 3)

            is_cold = pos == 0
            row = {
                "session_id": sid,
                "turn_seq": seq,
                "trace_id": trace_id,
                "parent_span_id": parent,
                "output_tokens": e.get("output_tokens"),
                "input_tokens": e.get("input_tokens"),
                "assembled_tokens": e.get("assembled_tokens"),
                "n_tool_calls": e.get("n_tool_calls"),
                "is_cold_sandbox": is_cold,
                # Bucket A (pre-model overhead)
                "A_premodel_ms": a_ms,
                "context_assembly_direct_ms": context_assembly_direct_ms,
                # Bucket B (sandbox init/exec)
                "B_sandbox_ms": b_ms,
                "B_sandbox_k8s_cold_ms": round(cold_ready_ms, 3) if (is_cold and cold_ready_ms is not None) else None,
                # Bucket C (model TTFT). Includes prefill of the uncached context:
                # cold turn-0 (whole prompt) vs warm delta -- exposed separately.
                "C_ttft_ms": c_primary,
                "C_ttft_measured_ms": c_cross_mock,
                "C_ttft_openclaw_ms": c_cross_ocw,
                "C_ttft_delta_measured": _agree(c_primary, c_cross_mock),
                "C_ttft_delta_openclaw": _agree(c_primary, c_cross_ocw),
                "C_prefill_ms": e.get("prefill_ms"),
                "C_prefill_new_tokens": e.get("prefill_new_tokens"),
                "F_mock_stream_ms": mock_stream_ms,
                "F_mock_first_visible_ms": mock_first_visible_ms,
                "F_mock_first_tool_ms": mock_first_tool_ms,
                "F_mock_first_to_tool_ms": mock_first_to_tool_ms,
                "F_mock_last_to_tool_ms": mock_last_to_tool_ms,
                "H_response_cycle_ms": response_cycle_ms,
                "H_model_decode_ms": e_primary,
                "H_openclaw_response_only_ms": openclaw_response_only_ms,
                # Bucket D (response processing). Primary = skew-free residual;
                # the others expose how the naive span gap undercounts it.
                "D_response_ms": d_ms,
                "D_response_gateway_ms": d_gateway_ms,
                "D_response_reasoning_ms": round(reasoning_ms, 3) if reasoning_ms else None,
                "D_response_calibrated_tail_ms": round(response_tail_ms, 3) if response_tail_ms else None,
                "D_response_skewfree_ms": d_skewfree_ms,
                "D_response_span_ms": d_span_ms,
                "D_response_inside_ms": d_inside_ms,
                # Bucket E (model decode)
                "E_decode_ms": e_primary,
                "E_decode_measured_ms": e_cross_mock,
                "E_decode_delta_measured": _agree(e_primary, e_cross_mock),
                # Skew-free gap + agreement
                "G_gap_ms": g_ms,
                "G_decomp_ms": g_decomp,
                "G_delta_ms": g_delta,
                # per-turn total (buckets present)
                "turn_total_ms": round(sum(v for v in (a_ms, b_ms, c_primary, d_ms, e_primary) if v is not None), 3),
            }
            rows.append(row)
    return rows


# --------------------------------------------------------------------------- #
# Aggregation + IO.                                                            #
# --------------------------------------------------------------------------- #

CSV_COLUMNS = [
    "session_id", "turn_seq", "is_cold_sandbox", "output_tokens", "input_tokens",
    "assembled_tokens", "n_tool_calls",
    "A_premodel_ms", "B_sandbox_ms", "B_sandbox_k8s_cold_ms",
    "C_ttft_ms", "C_ttft_measured_ms", "C_ttft_openclaw_ms",
    "C_prefill_ms", "C_prefill_new_tokens",
    "F_mock_stream_ms", "F_mock_first_visible_ms", "F_mock_first_tool_ms",
    "F_mock_first_to_tool_ms", "F_mock_last_to_tool_ms",
    "H_response_cycle_ms", "H_model_decode_ms", "H_openclaw_response_only_ms",
    "D_response_ms", "D_response_gateway_ms", "D_response_reasoning_ms",
    "D_response_calibrated_tail_ms",
    "D_response_skewfree_ms", "D_response_span_ms", "D_response_inside_ms",
    "E_decode_ms", "E_decode_measured_ms",
    "G_gap_ms", "G_decomp_ms", "G_delta_ms", "turn_total_ms",
]


def summarize(rows: list[dict]) -> dict[str, Any]:
    def col(name: str) -> list[float]:
        return [r[name] for r in rows if r.get(name) is not None]

    def stats(name: str) -> dict[str, float] | None:
        vals = col(name)
        if not vals:
            return None
        return {
            "mean": round(statistics.mean(vals), 3),
            "p50": round(statistics.median(vals), 3),
            "min": round(min(vals), 3),
            "max": round(max(vals), 3),
            "n": len(vals),
        }

    cold = [r for r in rows if r.get("is_cold_sandbox")]
    warm = [r for r in rows if not r.get("is_cold_sandbox")]
    cold_b = [r["B_sandbox_ms"] for r in cold if r.get("B_sandbox_ms") is not None]
    warm_b = [r["B_sandbox_ms"] for r in warm if r.get("B_sandbox_ms") is not None]

    return {
        "turns": len(rows),
        "buckets": {b: stats(b) for b in
                    ["A_premodel_ms", "B_sandbox_ms", "C_ttft_ms", "D_response_ms", "E_decode_ms"]},
        "sandbox_cold_vs_warm": {
            "cold_mean_ms": round(statistics.mean(cold_b), 3) if cold_b else None,
            "warm_mean_ms": round(statistics.mean(warm_b), 3) if warm_b else None,
        },
        "agreement": {
            "G_delta_ms": stats("G_delta_ms"),
            "C_ttft_delta_measured": stats("C_ttft_delta_measured"),
            "C_ttft_delta_openclaw": stats("C_ttft_delta_openclaw"),
            "E_decode_delta_measured": stats("E_decode_delta_measured"),
        },
    }


def write_outputs(rows: list[dict], out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "per_turn.json").write_text(json.dumps(rows, indent=2), encoding="utf-8")
    with (out_dir / "per_turn.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)
    summary = summarize(rows)
    (out_dir / "derive_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def derive(
    *,
    out_dir: Path,
    mock_edges: Path,
    traces: Path | None = None,
    driver_requests: Path | None = None,
    k8s: Path | None = None,
) -> dict[str, Any]:
    edges = load_mock_edges(mock_edges)
    tr = load_traces(traces) if traces else []
    kl = load_k8s_lifecycle(k8s) if k8s else []
    rows = derive_per_turn(edges, tr, k8s=kl)
    summary = write_outputs(rows, out_dir)
    summary["sources"] = {
        "mock_edges": len(edges),
        "traces": len(tr),
        "k8s_lifecycle": len(kl),
    }
    (out_dir / "derive_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Derive per-turn 5-bucket latency records")
    p.add_argument("--out", required=True)
    p.add_argument("--mock-edges", required=True, help="mock edge JSONL (or oc-logs capture)")
    p.add_argument("--traces", default=None, help="raw_traces.json from Jaeger export")
    p.add_argument("--driver-requests", default=None, help="driver requests.jsonl")
    p.add_argument("--k8s", default=None, help="sandbox pod lifecycle JSON (cold Ready-delta)")
    args = p.parse_args(argv)
    summary = derive(
        out_dir=Path(args.out),
        mock_edges=Path(args.mock_edges),
        traces=Path(args.traces) if args.traces else None,
        driver_requests=Path(args.driver_requests) if args.driver_requests else None,
        k8s=Path(args.k8s) if args.k8s else None,
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
