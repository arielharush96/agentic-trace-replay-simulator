#!/usr/bin/env python3
"""Small stateful HTTP backend used by non-AppWorld workload replays.

The benchmark must exercise a real internal tool boundary for every workload;
returning a recorded string directly from the mock model would bypass the
interesting path.  This service therefore accepts tool requests over HTTP,
updates a per-session workload state, emits an auditable event, and returns
the recorded result as the deterministic fixture payload.

It is intentionally a benchmark fixture, not a production implementation of
SWE, search, or support.  The state transitions make the boundary stateful
and workload-specific while keeping model behavior and result content fixed.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


MAX_REQUEST_BYTES = 1_048_576
_VOLATILE_KEYS = {"access_token", "api_key", "authorization", "password", "refresh_token", "token"}


def _canonical(value: Any, key: str | None = None) -> Any:
    if key and key.lower() in _VOLATILE_KEYS:
        return "<redacted>"
    if isinstance(value, dict):
        return {str(k): _canonical(value[k], str(k)) for k in sorted(value)}
    if isinstance(value, list):
        return [_canonical(item) for item in value]
    return value


def arguments_sha256(arguments: dict[str, Any] | None) -> str:
    payload = json.dumps(_canonical(arguments or {}), sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class WorkloadBackend:
    def __init__(self, auth_token: str = "") -> None:
        self.auth_token = auth_token
        self.sessions: dict[str, dict[str, Any]] = {}
        self.events: list[dict[str, Any]] = []
        self.lock = threading.Lock()

    @staticmethod
    def _clock_ns() -> int:
        return time.clock_gettime_ns(getattr(time, "CLOCK_MONOTONIC_RAW", time.CLOCK_MONOTONIC))

    def _state(self, session_id: str, workload: str, benchmark: str) -> dict[str, Any]:
        state = self.sessions.get(session_id)
        if state is None:
            state = {
                "session_id": session_id,
                "workload": workload,
                "benchmark": benchmark,
                "version": 0,
                "authenticated": False,
                "documents": [],
                "files_touched": [],
                "tests_run": 0,
                "support_updates": 0,
            }
            self.sessions[session_id] = state
        return state

    @staticmethod
    def _transition(state: dict[str, Any], tool: str, arguments: dict[str, Any]) -> None:
        lowered = tool.lower()
        state["version"] += 1
        if "login" in lowered or "auth" in lowered:
            state["authenticated"] = True
        if state["workload"] == "research_deep_search":
            if any(word in lowered for word in ("search", "document", "read", "open")):
                state["documents"].append(str(arguments.get("query") or arguments.get("url") or tool))
        elif state["workload"] == "software_engineering_qa":
            if any(word in lowered for word in ("bash", "shell", "file", "patch", "git", "code")):
                state["files_touched"].append(str(arguments.get("path") or arguments.get("command") or tool))
            if any(word in lowered for word in ("test", "pytest", "check")):
                state["tests_run"] += 1
        elif state["workload"] == "customer_support_ops":
            state["support_updates"] += 1

    def execute(self, request: dict[str, Any]) -> dict[str, Any]:
        session_id = str(request.get("session_id") or "")
        workload = str(request.get("workload") or "unknown")
        benchmark = str(request.get("benchmark") or "unknown")
        tool = str(request.get("tool") or "unknown")
        arguments = dict(request.get("arguments") or {})
        if not session_id:
            return {"ok": False, "error": "session_id is required"}
        started = self._clock_ns()
        with self.lock:
            state = self._state(session_id, workload, benchmark)
            self._transition(state, tool, arguments)
            result = request.get("recorded_result")
            if result is None or result == "":
                result = json.dumps({"ok": True, "workload": workload, "state_version": state["version"]})
            else:
                result = str(result)
            elapsed_ms = (self._clock_ns() - started) / 1e6
            event = {
                "event": "workload_tool",
                "backend": "trace-replay-stateful-workload-backend",
                "session_id": session_id,
                "workload": workload,
                "benchmark": benchmark,
                "turn": request.get("turn"),
                "call": request.get("call"),
                "tool": tool,
                "arguments_sha256": arguments_sha256(arguments),
                "elapsed_ms": round(elapsed_ms, 3),
                "result_bytes": len(result.encode("utf-8")),
                "state_version": state["version"],
                "state_summary": {
                    "authenticated": state["authenticated"],
                    "documents": len(state["documents"]),
                    "files_touched": len(state["files_touched"]),
                    "tests_run": state["tests_run"],
                    "support_updates": state["support_updates"],
                },
            }
            self.events.append(event)
        print(json.dumps(event, separators=(",", ":")), flush=True)
        return {"ok": True, "result": result, "state_version": event["state_version"]}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8091)
    parser.add_argument("--auth-token", default="")
    args = parser.parse_args()
    backend = WorkloadBackend(args.auth_token)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *values: Any) -> None:
            return

        def _send(self, status: int, payload: Any) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/health":
                self._send(200, {"status": "ok", "events": len(backend.events)})
                return
            if self.path == "/events":
                self._send(200, {"events": backend.events})
                return
            self._send(404, {"error": "not found"})

        def do_POST(self) -> None:  # noqa: N802
            if self.path != "/execute":
                self._send(404, {"error": "not found"})
                return
            if args.auth_token and not hmac.compare_digest(
                self.headers.get("Authorization", ""), f"Bearer {args.auth_token}"
            ):
                self._send(401, {"error": "authorization required"})
                return
            try:
                length = int(self.headers.get("Content-Length") or 0)
                if length <= 0 or length > MAX_REQUEST_BYTES:
                    raise ValueError("invalid request size")
                request = json.loads(self.rfile.read(length).decode("utf-8"))
                response = backend.execute(request)
                self._send(200 if response.get("ok") else 400, response)
            except Exception as exc:  # noqa: BLE001
                self._send(400, {"ok": False, "error": str(exc)})

    print(f"stateful workload backend listening on {args.host}:{args.port}", flush=True)
    ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
