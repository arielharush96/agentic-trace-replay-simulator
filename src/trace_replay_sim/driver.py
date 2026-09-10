"""Load generator: replay recorded user sessions against mock-LLM or OpenClaw.

Plays the GuideLLM role: the *client-edge* ground truth. In the openclaw/shell
layers the driver sends ONE high-level prompt per session -> ONE ``openclaw.run``
that internally loops N model calls, so the driver only observes **session-level**
edges (t_send, t_first byte of the whole run, t_end). Per-*turn* skew-free timing
is owned by the mock (see ``mock_llm.EdgeRecorder``). In the mock-direct layer each
request IS a single turn, so the driver's session edges are also that turn's edges
and provide the control net-floor.

The driver generates a W3C ``traceparent`` per request and propagates it so
driver -> OpenClaw -> mock spans stitch into one trace, and exports its own
session span via OTLP (best effort).
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


OTLP_ENDPOINT = os.environ.get("OTLP_ENDPOINT", "http://jaeger.trace-replay.svc.cluster.local:4318")


@dataclass
class RequestResult:
    session_id: str
    layer: str
    ok: bool
    status: int | None
    error: str
    # single-clock (perf_counter) client edges, ms relative to t_send
    e2e_ms: float
    ttft_ms: float | None
    # absolute monotonic ns edges (single clock within this process)
    t_send_ns: int = 0
    t_first_ns: int = 0
    t_end_ns: int = 0
    # wall-clock anchor for cross-source (Jaeger/K8s) alignment
    t_send_wall_ns: int = 0
    # trace-context this request injected (join key to Jaeger)
    trace_id: str = ""
    span_id: str = ""
    bytes_out: int = 0
    prompt_chars: int = 0
    n_sse_events: int = 0
    first_event_ns: int = 0
    first_content_ns: int = 0
    first_reasoning_ns: int = 0
    first_tool_ns: int = 0
    last_event_ns: int = 0
    event_types: dict[str, int] = field(default_factory=dict)
    sse_timeline: list[dict[str, Any]] = field(default_factory=list)
    response_id: str = ""


def _pct(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round((p / 100.0) * (len(ordered) - 1)))))
    return ordered[idx]


def load_sessions(path: Path, limit: int | None = None) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    sessions = payload.get("sessions") or []
    if limit is not None:
        sessions = sessions[:limit]
    return sessions


def tagged_prompt(session: dict[str, Any]) -> str:
    sid = session.get("session_id") or "unknown"
    prompt = session.get("user_prompt") or "Say hello."
    return f"REPLAY_SESSION_ID={sid}\n<!--replay-session:{sid}-->\n{prompt}"


def new_trace_context() -> tuple[str, str, str]:
    """Return (trace_id, span_id, traceparent) as a sampled W3C trace context."""
    trace_id = uuid.uuid4().hex  # 32 hex chars
    span_id = uuid.uuid4().hex[:16]  # 16 hex chars
    traceparent = f"00-{trace_id}-{span_id}-01"
    return trace_id, span_id, traceparent


def _export_session_span(res: RequestResult) -> None:
    if not OTLP_ENDPOINT or not res.trace_id or not res.t_send_ns or not res.t_end_ns:
        return
    dur_ns = max(0, res.t_end_ns - res.t_send_ns)
    start_wall = res.t_send_wall_ns
    payload = {
        "resourceSpans": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "trace-replay-driver"}},
            ]},
            "scopeSpans": [{
                "scope": {"name": "driver"},
                "spans": [{
                    "traceId": res.trace_id,
                    "spanId": res.span_id,
                    "name": f"driver.session.{res.layer}",
                    "kind": 3,  # CLIENT
                    "startTimeUnixNano": str(start_wall),
                    "endTimeUnixNano": str(start_wall + dur_ns),
                    "attributes": [
                        {"key": "replay.session_id", "value": {"stringValue": res.session_id}},
                        {"key": "replay.layer", "value": {"stringValue": res.layer}},
                        {"key": "driver.e2e_ms", "value": {"doubleValue": res.e2e_ms}},
                        {"key": "driver.ttft_ms", "value": {"doubleValue": res.ttft_ms or 0.0}},
                        {"key": "driver.ok", "value": {"boolValue": res.ok}},
                    ],
                    "status": {} if res.ok else {"code": 2},
                }],
            }],
        }]
    }
    try:
        req = urllib.request.Request(
            f"{OTLP_ENDPOINT}/v1/traces",
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=2)
    except Exception:
        pass


def _read_sse_edges(resp) -> tuple[float | None, int, int, bytes, dict[str, Any]]:
    """Read SSE stream and retain event-phase timestamps for the client edge.

    TTFT = time to first CONTENT/tool token (not the connection open).
    """
    started = time.perf_counter()
    ttft: float | None = None
    t_first_ns = 0
    n_events = 0
    chunks: list[bytes] = []
    phases: dict[str, Any] = {
        "first_event_ns": 0, "first_content_ns": 0, "first_reasoning_ns": 0,
        "first_tool_ns": 0, "last_event_ns": 0, "event_types": {},
        "events": [],
        "response_id": "",
    }
    while True:
        line = resp.readline()
        if not line:
            break
        chunks.append(line)
        if not line.startswith(b"data:"):
            continue
        payload = line[5:].strip()
        if payload == b"[DONE]":
            continue
        n_events += 1
        try:
            event = json.loads(payload)
        except (json.JSONDecodeError, ValueError):
            continue
        now_ns = time.perf_counter_ns()
        phases["first_event_ns"] = phases["first_event_ns"] or now_ns
        phases["last_event_ns"] = now_ns
        event_type = str(event.get("type") or "chat.completions.chunk")
        if event_type in {"response.created", "response.completed"}:
            response = event.get("response") or event
            if isinstance(response, dict) and isinstance(response.get("id"), str):
                phases["response_id"] = response["id"]
        types = phases["event_types"]
        types[event_type] = types.get(event_type, 0) + 1
        got = False
        got_content = False
        got_reasoning = False
        got_tool = False
        choices = event.get("choices") or []
        if choices:
            delta = choices[0].get("delta") or {}
            if delta.get("content"):
                got = True
                got_content = True
            if delta.get("reasoning_content"):
                got_reasoning = True
            elif delta.get("tool_calls"):
                got = True
                got_tool = True
        etype = event.get("type", "")
        if "output_text.delta" in etype:
            got = True
            got_content = True
        if "reasoning" in etype.lower():
            got_reasoning = True
        if "tool" in etype.lower() or "function_call" in etype.lower():
            got_tool = True
        phases["events"].append({
            "at_ms": round((now_ns - int(started * 1e9)) / 1e6, 3),
            "type": event_type,
            "content": got_content,
            "reasoning": got_reasoning,
            "tool": got_tool,
        })
        phases["first_content_ns"] = phases["first_content_ns"] or (now_ns if got_content else 0)
        phases["first_reasoning_ns"] = phases["first_reasoning_ns"] or (now_ns if got_reasoning else 0)
        phases["first_tool_ns"] = phases["first_tool_ns"] or (now_ns if got_tool else 0)
        if got:
            ttft = (time.perf_counter() - started) * 1000.0
            t_first_ns = now_ns
    return ttft, t_first_ns, n_events, b"".join(chunks), phases


def run_one(session: dict[str, Any], *, layer: str, url: str, model: str, token: str,
            timeout: float, stream: bool, export_spans: bool,
            prompt_override: str | None = None, user_key: str | None = None,
            previous_response_id: str | None = None) -> RequestResult:
    prompt = prompt_override if prompt_override is not None else tagged_prompt(session)
    trace_id, span_id, traceparent = new_trace_context()
    headers = {"Content-Type": "application/json", "traceparent": traceparent}
    if token:
        headers["Authorization"] = f"Bearer {token}"
        headers["x-openclaw-token"] = token

    if layer == "mock-direct":
        path = url.rstrip("/") + "/v1/chat/completions"
        payload: dict[str, Any] = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "stream": stream,
        }
    else:
        path = url.rstrip("/") + "/v1/responses"
        payload = {"model": model, "input": prompt, "stream": stream}
        if user_key:
            payload["user"] = user_key
        if previous_response_id:
            payload["previous_response_id"] = previous_response_id

    sid = str(session.get("session_id"))
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(path, data=data, headers=headers, method="POST")

    t_send_wall_ns = time.time_ns()
    t_send_ns = time.perf_counter_ns()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = int(getattr(resp, "status", 200))
            if stream:
                ttft_ms, t_first_ns, n_events, body, phases = _read_sse_edges(resp)
                t_end_ns = time.perf_counter_ns()
            else:
                body = resp.read()
                t_end_ns = time.perf_counter_ns()
                t_first_ns = t_end_ns
                ttft_ms = (t_end_ns - t_send_ns) / 1e6
                n_events = 0
                phases = {}
        e2e_ms = (t_end_ns - t_send_ns) / 1e6
        res = RequestResult(
            session_id=sid, layer=layer, ok=200 <= status < 300, status=status, error="",
            e2e_ms=round(e2e_ms, 3), ttft_ms=round(ttft_ms, 3) if ttft_ms is not None else None,
            t_send_ns=t_send_ns, t_first_ns=t_first_ns or t_end_ns, t_end_ns=t_end_ns,
            t_send_wall_ns=t_send_wall_ns, trace_id=trace_id, span_id=span_id,
            bytes_out=len(body), prompt_chars=len(prompt), n_sse_events=n_events,
            first_event_ns=phases.get("first_event_ns", 0),
            first_content_ns=phases.get("first_content_ns", 0),
            first_reasoning_ns=phases.get("first_reasoning_ns", 0),
            first_tool_ns=phases.get("first_tool_ns", 0),
            last_event_ns=phases.get("last_event_ns", 0),
            event_types=phases.get("event_types", {}),
            sse_timeline=phases.get("events", []),
            response_id=phases.get("response_id", ""),
        )
    except urllib.error.HTTPError as exc:
        t_end_ns = time.perf_counter_ns()
        err = exc.read().decode("utf-8", errors="replace")[:500]
        res = RequestResult(
            session_id=sid, layer=layer, ok=False, status=exc.code, error=err,
            e2e_ms=round((t_end_ns - t_send_ns) / 1e6, 3), ttft_ms=None,
            t_send_ns=t_send_ns, t_first_ns=t_end_ns, t_end_ns=t_end_ns,
            t_send_wall_ns=t_send_wall_ns, trace_id=trace_id, span_id=span_id,
            bytes_out=0, prompt_chars=len(prompt), n_sse_events=0,
        )
    except Exception as exc:  # noqa: BLE001
        t_end_ns = time.perf_counter_ns()
        res = RequestResult(
            session_id=sid, layer=layer, ok=False, status=None, error=str(exc)[:500],
            e2e_ms=round((t_end_ns - t_send_ns) / 1e6, 3), ttft_ms=None,
            t_send_ns=t_send_ns, t_first_ns=t_end_ns, t_end_ns=t_end_ns,
            t_send_wall_ns=t_send_wall_ns, trace_id=trace_id, span_id=span_id,
            bytes_out=0, prompt_chars=len(prompt), n_sse_events=0,
        )

    if export_spans:
        _export_session_span(res)
    return res


def summarize(results: list[RequestResult]) -> dict[str, Any]:
    ok = [r for r in results if r.ok]
    e2e = [r.e2e_ms for r in ok]
    ttft = [r.ttft_ms for r in ok if r.ttft_ms is not None]
    duration_s = (max((r.e2e_ms for r in results), default=0.0)) / 1000.0 if len(results) == 1 else None
    return {
        "requests": len(results),
        "ok": len(ok),
        "errors": len(results) - len(ok),
        "e2e_mean_ms": round(statistics.mean(e2e), 3) if e2e else None,
        "e2e_p50_ms": round(_pct(e2e, 50), 3) if e2e else None,
        "e2e_p95_ms": round(_pct(e2e, 95), 3) if e2e else None,
        "ttft_mean_ms": round(statistics.mean(ttft), 3) if ttft else None,
        "ttft_p50_ms": round(_pct(ttft, 50), 3) if ttft else None,
        "ttft_p95_ms": round(_pct(ttft, 95), 3) if ttft else None,
        "error_samples": [r.error for r in results if not r.ok][:5],
        "wall_hint_s": duration_s,
    }


def drive(
    *,
    corpus: Path,
    out_dir: Path,
    layer: str,
    url: str,
    model: str,
    token: str,
    concurrency: int,
    limit: int | None,
    timeout: float,
    stream: bool,
    export_spans: bool = True,
) -> dict[str, Any]:
    sessions = load_sessions(corpus, limit=limit)
    out_dir.mkdir(parents=True, exist_ok=True)
    started = time.time()
    results: list[RequestResult] = []
    run_nonce = uuid.uuid4().hex[:10]
    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
        futs = [
            pool.submit(
                run_one, session, layer=layer, url=url, model=model, token=token,
                timeout=timeout, stream=stream, export_spans=export_spans,
                user_key=f"trace-replay:{session.get('session_id')}:{run_nonce}",
            )
            for session in sessions
        ]
        for fut in as_completed(futs):
            results.append(fut.result())
    ended = time.time()
    wall_s = max(0.001, ended - started)
    summary = summarize(results)
    summary.update(
        {
            "layer": layer,
            "concurrency": concurrency,
            "target_url": url,
            "model": model,
            "stream": stream,
            "start_unix": started,
            "end_unix": ended,
            "wall_s": round(wall_s, 3),
            "rps": round(len(results) / wall_s, 4),
            "ok_rps": round(summary["ok"] / wall_s, 4),
        }
    )
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    with (out_dir / "requests.jsonl").open("w", encoding="utf-8") as handle:
        for item in results:
            handle.write(json.dumps(asdict(item)) + "\n")
    print("--- PER_REQUEST_START ---")
    for item in results:
        request_record = asdict(item)
        print(json.dumps(request_record))
    print("--- PER_REQUEST_END ---")
    metadata = {
        "layer": layer,
        "concurrency": concurrency,
        "sessions": len(sessions),
        "start_unix": started,
        "end_unix": ended,
        "duration_s": wall_s,
        "otlp_endpoint": OTLP_ENDPOINT if export_spans else None,
    }
    (out_dir / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    return summary


def drive_session_turns(
    *,
    corpus: Path,
    out_dir: Path,
    layer: str,
    url: str,
    model: str,
    token: str,
    timeout: float,
    stream: bool,
    export_spans: bool = True,
    prewarm: bool = False,
) -> dict[str, Any]:
    """Send one request per recorded turn while reusing one OpenClaw session."""
    sessions = load_sessions(corpus, limit=1)
    if not sessions:
        raise ValueError("session-turn mode requires a non-empty corpus")
    session = sessions[0]
    session_id = str(session.get("session_id"))
    # Never inherit conversation state from an earlier benchmark. The mock
    # session identity remains stable for replay matching, while OpenClaw gets
    # a unique user/session key for this run.
    user_key = f"trace-replay:{session_id}:{uuid.uuid4().hex[:10]}"
    out_dir.mkdir(parents=True, exist_ok=True)
    started = time.time()
    results: list[RequestResult] = []
    previous_response_id: str | None = None
    if prewarm:
        warmup_prompt = (
            f"REPLAY_SESSION_ID={session_id}\n"
            "REPLAY_WARMUP=1\n"
            f"<!--replay-session:{session_id}-->\n"
            "Prepare the execution environment and reply with warmup."
        )
        warmup = run_one(
            session,
            layer=layer,
            url=url,
            model=model,
            token=token,
            timeout=timeout,
            stream=stream,
            export_spans=export_spans,
            prompt_override=warmup_prompt,
            user_key=user_key,
        )
        if warmup.response_id:
            previous_response_id = warmup.response_id
    for turn in session.get("turns") or []:
        turn_index = int(turn.get("index") or 0)
        turn_prompt = (
            str(session.get("user_prompt") or "Continue the recorded task.")
            if turn_index == 0
            else f"Continue recorded turn {turn_index}."
        )
        prompt = (
            f"REPLAY_SESSION_ID={session_id}\n"
            f"REPLAY_TURN_INDEX={turn_index}\n"
            f"<!--replay-session:{session_id}-->\n"
            f"<!--replay-turn:{turn_index}-->\n"
            f"{turn_prompt}"
        )
        result = run_one(
            session,
            layer=layer,
            url=url,
            model=model,
            token=token,
            timeout=timeout,
            stream=stream,
            export_spans=export_spans,
            prompt_override=prompt,
            user_key=user_key,
            previous_response_id=previous_response_id,
        )
        results.append(result)
        if result.response_id:
            previous_response_id = result.response_id
    ended = time.time()
    wall_s = max(0.001, ended - started)
    summary = summarize(results)
    summary.update({
        "layer": layer,
        "mode": "session-turns",
        "session_id": session_id,
        "turns": len(results),
        "target_url": url,
        "model": model,
        "stream": stream,
        "prewarm": prewarm,
        "start_unix": started,
        "end_unix": ended,
        "wall_s": round(wall_s, 3),
        "rps": round(len(results) / wall_s, 4),
        "ok_rps": round(summary["ok"] / wall_s, 4),
    })
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    with (out_dir / "requests.jsonl").open("w", encoding="utf-8") as handle:
        for item in results:
            handle.write(json.dumps(asdict(item)) + "\n")
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Replay-trace load driver")
    parser.add_argument("--corpus", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--layer", choices=["mock-direct", "openclaw", "shell"], required=True)
    parser.add_argument("--url", required=True)
    parser.add_argument("--model", default="openclaw/perf_agent")
    parser.add_argument("--token", default=os.environ.get("OPENCLAW_TOKEN", ""))
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--stream", action="store_true")
    parser.add_argument("--no-spans", action="store_true", help="disable OTLP span export")
    parser.add_argument("--session-turns", action="store_true",
                        help="send one request per corpus turn while reusing one OpenClaw session")
    parser.add_argument("--prewarm", action="store_true",
                        help="prewarm the session sandbox before measured turns")
    args = parser.parse_args(argv)
    if args.session_turns:
        summary = drive_session_turns(
            corpus=Path(args.corpus), out_dir=Path(args.out), layer=args.layer,
            url=args.url, model=args.model, token=args.token,
            timeout=args.timeout, stream=args.stream,
            export_spans=not args.no_spans,
            prewarm=args.prewarm,
        )
    else:
        summary = drive(
            corpus=Path(args.corpus), out_dir=Path(args.out), layer=args.layer,
            url=args.url, model=args.model, token=args.token,
            concurrency=args.concurrency, limit=args.limit,
            timeout=args.timeout, stream=args.stream,
            export_spans=not args.no_spans,
        )
    print(json.dumps(summary, indent=2))
    return 0 if summary["errors"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
