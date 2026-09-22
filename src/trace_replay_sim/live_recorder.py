"""Generic live agent-event recorder for OpenClaw integrations.

This module is intentionally transport-neutral.  An OpenClaw plugin, gateway
middleware, or sidecar can send normalized events to :class:`EventRecorder`
directly or through the small HTTP collector exposed by ``python -m``.

The recorder captures execution telemetry and replay-relevant payloads without
requiring the benchmark's mock LLM.  Sensitive fields are redacted before they
reach disk.  The JSONL format is append-only and preserves both monotonic and
wall-clock timestamps so it can be correlated with OTEL and cgroup samples.
"""

from __future__ import annotations

import argparse
import json
import re
import threading
import time
import uuid
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterable

SCHEMA = "agent-event/v1"
SENSITIVE_KEY = re.compile(
    r"(?:authorization|api[_-]?key|access[_-]?token|refresh[_-]?token|password|passwd|secret|credential|cookie|set-cookie)",
    re.IGNORECASE,
)


def redact(value: Any) -> Any:
    """Return a recursively redacted copy of JSON-compatible data."""
    if isinstance(value, dict):
        return {
            str(key): "[REDACTED]" if SENSITIVE_KEY.search(str(key)) else redact(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact(item) for item in value]
    if isinstance(value, tuple):
        return [redact(item) for item in value]
    return value


class EventRecorder:
    """Thread-safe append-only recorder for live agent lifecycle events."""

    def __init__(self, path: str | Path, *, redact_payloads: bool = True) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.redact_payloads = redact_payloads
        self._lock = threading.Lock()
        self._handle = self.path.open("a", encoding="utf-8")
        self.count = 0

    def record(
        self,
        event_type: str,
        *,
        session_id: str,
        payload: dict[str, Any] | None = None,
        trace_id: str = "",
        turn_index: int | None = None,
        monotonic_ns: int | None = None,
        wall_ns: int | None = None,
    ) -> dict[str, Any]:
        """Write one normalized event and return the serialized record."""
        body = payload or {}
        if self.redact_payloads:
            body = redact(body)
        record: dict[str, Any] = {
            "schema": SCHEMA,
            "event_id": uuid.uuid4().hex,
            "event_type": str(event_type),
            "session_id": str(session_id),
            "trace_id": str(trace_id or ""),
            "turn_index": turn_index,
            "t_monotonic_ns": int(monotonic_ns or time.perf_counter_ns()),
            "t_wall_ns": int(wall_ns or time.time_ns()),
            "payload": body,
        }
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        with self._lock:
            self._handle.write(line + "\n")
            self._handle.flush()
            self.count += 1
        return record

    def close(self) -> None:
        with self._lock:
            if not self._handle.closed:
                self._handle.close()

    def __enter__(self) -> "EventRecorder":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def load_events(path: str | Path) -> list[dict[str, Any]]:
    """Load valid JSONL event records, ignoring blank lines."""
    rows: list[dict[str, Any]] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if line.strip():
            value = json.loads(line)
            if value.get("schema") == SCHEMA:
                rows.append(value)
    return rows


class _CollectorHandler(BaseHTTPRequestHandler):
    recorder: EventRecorder

    def log_message(self, *_args: object) -> None:
        return

    def do_GET(self) -> None:  # noqa: N802
        if self.path != "/healthz":
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        body = json.dumps({"ok": True, "schema": SCHEMA}).encode()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/v1/events":
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(length))
            if not isinstance(body, dict):
                raise ValueError("event body must be a JSON object")
            event_type = body.pop("event_type", body.pop("type", "agent.event"))
            session_id = body.pop("session_id", "unknown")
            trace_id = body.pop("trace_id", self.headers.get("trace-id", ""))
            turn_index = body.pop("turn_index", None)
            record = self.recorder.record(
                str(event_type), session_id=str(session_id), trace_id=str(trace_id),
                turn_index=int(turn_index) if turn_index is not None else None,
                payload=body.get("payload", body),
            )
            response = json.dumps({"ok": True, "event_id": record["event_id"]}).encode()
            self.send_response(HTTPStatus.CREATED)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            self.send_error(HTTPStatus.BAD_REQUEST, str(exc))


def serve(path: str | Path, *, host: str = "127.0.0.1", port: int = 8787) -> None:
    recorder = EventRecorder(path)
    handler = type("CollectorHandler", (_CollectorHandler,), {"recorder": recorder})
    server = ThreadingHTTPServer((host, port), handler)
    try:
        print(f"Live recorder listening on http://{host}:{port}/v1/events", flush=True)
        server.serve_forever()
    finally:
        server.server_close()
        recorder.close()


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Collect normalized live agent events as JSONL")
    parser.add_argument("--out", required=True, help="append-only JSONL output path")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    args = parser.parse_args(list(argv) if argv is not None else None)
    serve(args.out, host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
