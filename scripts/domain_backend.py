#!/usr/bin/env python3
"""Small, workload-specific stateful APIs for trace-replay benchmarking.

This service is intentionally separate from the LLM simulator.  It receives
tool calls over an internal HTTP boundary, owns per-session state, performs
domain operations, and emits auditable events.  For SWE, it never owns or
pretends to own a repository: OpenShell executes the command in the pinned
task image and this service records the returned evidence.  The implementations
are benchmark environments for the replay corpus; they are not claims of
official SWE-bench, BrowseComp, or τ-bench evaluation correctness.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


MAX_REQUEST_BYTES = 1_048_576
VOLATILE_KEYS = {"access_token", "api_key", "authorization", "password", "refresh_token", "token"}
EVENT_NAMES = {
    "software_engineering_qa": "swebench_tool",
    "research_deep_search": "browsecompplus_tool",
    "customer_support_ops": "tau2_tool",
}


def _canonical(value: Any, key: str | None = None) -> Any:
    if key and key.lower() in VOLATILE_KEYS:
        return "<redacted>"
    if isinstance(value, dict):
        return {str(k): _canonical(value[k], str(k)) for k in sorted(value)}
    if isinstance(value, list):
        return [_canonical(item) for item in value]
    return value


def arguments_sha256(arguments: dict[str, Any]) -> str:
    payload = json.dumps(_canonical(arguments), sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def now_ns() -> int:
    return time.clock_gettime_ns(getattr(time, "CLOCK_MONOTONIC_RAW", time.CLOCK_MONOTONIC))


class DomainBackend:
    def __init__(self, workload: str, root: Path):
        if workload not in EVENT_NAMES:
            raise ValueError(f"unsupported domain workload: {workload}")
        self.workload = workload
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.sessions: dict[str, dict[str, Any]] = {}
        self.events: list[dict[str, Any]] = []
        self.lock = threading.Lock()

    def _state(self, session_id: str, benchmark: str) -> dict[str, Any]:
        state = self.sessions.get(session_id)
        if state is not None:
            return state
        state = {
            "session_id": session_id,
            "benchmark": benchmark,
            "version": 0,
            "authenticated": False,
            "documents_read": [],
            "searches": [],
            "files_touched": [],
            "tests_run": 0,
            "support_updates": 0,
            "bookings": {},
        }
        self.sessions[session_id] = state
        return state

    def _swe_result(
        self,
        state: dict[str, Any],
        tool: str,
        arguments: dict[str, Any],
        *,
        execution_mode: str = "backend",
        sandbox_result: dict[str, Any] | None = None,
    ) -> str:
        command = str(arguments.get("command") or arguments.get("cmd") or "")
        session_root = self.root / state["session_id"]
        state["files_touched"].append(str(arguments.get("path") or command or tool))
        lowered = command.lower()
        if execution_mode == "sandbox":
            result = sandbox_result
            if not isinstance(result, dict) or "returncode" not in result:
                return json.dumps({"output": "missing OpenShell sandbox result", "returncode": 125})
            if any(token in lowered for token in ("pytest", "unittest", "tox", "test")):
                state["tests_run"] += 1
            return json.dumps({
                "output": str(result.get("output") or ""),
                "returncode": int(result.get("returncode") or 0),
                "execution": "openshell-sandbox",
                "repo_root": str(result.get("repo_root") or ""),
                "base_commit": str(result.get("base_commit") or ""),
            })
        return json.dumps({
            "output": "SWE execution must come from the OpenShell task-image sandbox",
            "returncode": 125,
            "execution": "backend-fallback-rejected",
        })

    def _research_result(self, state: dict[str, Any], tool: str, arguments: dict[str, Any]) -> str:
        query = str(arguments.get("query") or arguments.get("q") or arguments.get("url") or tool)
        lowered = tool.lower()
        if any(word in lowered for word in ("search", "query", "find")):
            state["searches"].append(query)
            digest = hashlib.sha256(query.encode()).hexdigest()[:8]
            return json.dumps([{
                "docid": digest,
                "score": 1.0,
                "snippet": f"Indexed research result for: {query}",
                "source": "trace-replay-browsecompplus-document-index",
            }])
        docid = str(arguments.get("docid") or arguments.get("id") or "unknown")
        state["documents_read"].append(docid)
        return json.dumps({
            "docid": docid,
            "title": f"Research document {docid}",
            "content": f"Document content retrieved from the session-scoped index for {docid}.",
        })

    def _support_result(self, state: dict[str, Any], tool: str, arguments: dict[str, Any]) -> str:
        lowered = tool.lower()
        if "login" in lowered or "auth" in lowered:
            state["authenticated"] = True
        state["support_updates"] += 1
        if any(word in lowered for word in ("book", "reserve", "create")):
            booking_id = f"BK-{state['session_id'][:8]}-{state['version'] + 1:03d}"
            state["bookings"][booking_id] = {"status": "created", "arguments": _canonical(arguments)}
            return json.dumps({"ok": True, "booking_id": booking_id, "status": "created"})
        return json.dumps({
            "ok": True,
            "policy_state": "authenticated" if state["authenticated"] else "anonymous",
            "updates": state["support_updates"],
            "result": f"Support operation {tool} completed against the session policy store.",
        })

    def execute(self, request: dict[str, Any]) -> dict[str, Any]:
        session_id = str(request.get("session_id") or "")
        if not session_id:
            return {"ok": False, "error": "session_id is required"}
        tool = str(request.get("tool") or "unknown")
        arguments = dict(request.get("arguments") or {})
        started = now_ns()
        with self.lock:
            state = self._state(session_id, str(request.get("benchmark") or self.workload))
            state["version"] += 1
            if self.workload == "software_engineering_qa":
                result = self._swe_result(
                    state,
                    tool,
                    arguments,
                    execution_mode=str(request.get("execution_mode") or "backend"),
                    sandbox_result=request.get("sandbox_result"),
                )
            elif self.workload == "research_deep_search":
                result = self._research_result(state, tool, arguments)
            else:
                result = self._support_result(state, tool, arguments)
            event = {
                "event": EVENT_NAMES[self.workload],
                "backend": f"trace-replay-{self.workload}-backend",
                "session_id": session_id,
                "workload": self.workload,
                "benchmark": request.get("benchmark"),
                "turn": request.get("turn"),
                "call": request.get("call"),
                "tool": tool,
                "arguments_sha256": arguments_sha256(arguments),
                "elapsed_ms": round((now_ns() - started) / 1e6, 3),
                "result_bytes": len(result.encode("utf-8")),
                "state_version": state["version"],
                "execution_mode": str(request.get("execution_mode") or "backend"),
            }
            if request.get("execution_mode") == "sandbox":
                sandbox_result = request.get("sandbox_result")
                if isinstance(sandbox_result, dict):
                    event["sandbox_repo_root"] = str(sandbox_result.get("repo_root") or "")
                    event["sandbox_base_commit"] = str(sandbox_result.get("base_commit") or "")
                    event["sandbox_returncode"] = int(sandbox_result.get("returncode") or 0)
            self.events.append(event)
        print(json.dumps(event, separators=(",", ":")), flush=True)
        return {"ok": True, "result": result, "state_version": event["state_version"]}


def serve(workload: str, host: str, port: int, root: Path) -> None:
    backend = DomainBackend(workload, root)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *values: Any) -> None:
            return

        def send_json(self, status: int, payload: Any) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/health":
                self.send_json(200, {"status": "ok", "workload": workload, "events": len(backend.events)})
            elif self.path == "/events":
                self.send_json(200, {"events": backend.events})
            else:
                self.send_json(404, {"error": "not found"})

        def do_POST(self) -> None:  # noqa: N802
            if self.path != "/execute":
                self.send_json(404, {"error": "not found"})
                return
            try:
                length = int(self.headers.get("Content-Length") or 0)
                if length <= 0 or length > MAX_REQUEST_BYTES:
                    raise ValueError("invalid request size")
                response = backend.execute(json.loads(self.rfile.read(length).decode("utf-8")))
                self.send_json(200 if response.get("ok") else 400, response)
            except Exception as exc:  # noqa: BLE001
                self.send_json(400, {"ok": False, "error": str(exc)})

    print(f"domain backend listening workload={workload} port={port}", flush=True)
    ThreadingHTTPServer((host, port), Handler).serve_forever()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workload", required=True, choices=sorted(EVENT_NAMES))
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--root", default="/data/sessions")
    args = parser.parse_args()
    serve(args.workload, args.host, args.port, Path(args.root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
