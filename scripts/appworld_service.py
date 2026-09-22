#!/usr/bin/env python3
"""Minimal HTTP adapter from replayed tool calls to AppWorld APIs."""

from __future__ import annotations

import argparse
import hmac
import json
import os
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path


_SAFE_TOOL_COMPONENT = re.compile(r"^[a-z][a-z0-9_]*$")
MAX_REQUEST_BYTES = 1_048_576


class AppWorldAdapter:
    def __init__(self, root: str, mapping_path: str):
        os.environ["APPWORLD_ROOT"] = root
        from appworld import AppWorld

        self.AppWorld = AppWorld
        self.mapping = json.loads(Path(mapping_path).read_text(encoding="utf-8"))
        self.worlds = {}
        self.tokens = {}
        self.lock = threading.Lock()

    @staticmethod
    def _clock_ns() -> int:
        return time.clock_gettime_ns(getattr(time, "CLOCK_MONOTONIC_RAW", time.CLOCK_MONOTONIC))

    def _world(self, session_id: str, task_id: str):
        world = self.worlds.get(session_id)
        if world is None:
            world = self.AppWorld(task_id=task_id, experiment_name="trace-replay")
            self.worlds[session_id] = world
        return world

    def _account_password(self, world, app_name: str) -> str | None:
        output = world.execute("response = apis.supervisor.show_account_passwords()\nprint(json.dumps(response))")
        for line in reversed((output or "").splitlines()):
            try:
                accounts = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(accounts, list):
                for account in accounts:
                    if account.get("account_name") == app_name:
                        return str(account.get("password") or "")
        return None

    def _translate_arguments(self, session_id: str, world, app_name: str, api_name: str, arguments: dict) -> dict:
        translated = json.loads(json.dumps(arguments))
        if api_name == "login":
            translated["username"] = world.task.supervisor.get("email")
            password = self._account_password(world, app_name)
            if password:
                translated["password"] = password
        token = self.tokens.get(session_id, {}).get(app_name)
        if token:
            for key in ("access_token", "token"):
                if key in translated:
                    translated[key] = token
        return translated

    def execute(self, request: dict) -> dict:
        session_id = str(request.get("session_id") or "")
        task_id = self.mapping.get(session_id)
        if not task_id:
            return {"ok": False, "error": f"no AppWorld task mapping for session {session_id}"}
        name = str(request.get("tool") or "")
        prefix = "mcp__environment__"
        if not name.startswith(prefix):
            return {"ok": False, "error": f"unsupported tool name: {name}"}
        if name == "mcp__environment__finish":
            app_name, api_name = "supervisor", "complete_task"
            expression = "response = apis.supervisor.complete_task(**arguments)"
        else:
            parts = name[len(prefix):].split("__", 1)
            if len(parts) != 2:
                return {"ok": False, "error": f"cannot map AppWorld tool: {name}"}
            app_name, api_name = parts
            if not all(_SAFE_TOOL_COMPONENT.fullmatch(part) for part in (app_name, api_name)):
                return {"ok": False, "error": f"invalid AppWorld tool component: {name}"}
            if app_name == "supervisor" and api_name == "complete_task":
                expression = "response = apis.supervisor.complete_task(**arguments)"
            else:
                expression = f"response = apis.{app_name}.{api_name}(**arguments)"
        # AppWorld freezes Python clocks; use the kernel raw monotonic clock.
        started_ns = self._clock_ns()
        with self.lock:
            world = self._world(session_id, task_id)
            arguments = self._translate_arguments(session_id, world, app_name, api_name, dict(request.get("arguments") or {}))
            code = "import json\n" + expression + "\nprint(json.dumps(response, default=str))"
            output = world.execute(code.replace("**arguments", f"**{json.dumps(arguments)}"))
            if api_name == "login":
                for line in reversed((output or "").splitlines()):
                    try:
                        result = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(result, dict) and result.get("access_token"):
                        self.tokens.setdefault(session_id, {})[app_name] = result["access_token"]
                        break
        elapsed_ms = (self._clock_ns() - started_ns) / 1e6
        print(json.dumps({
            "event": "appworld_tool",
            "session_id": session_id,
            "task_id": task_id,
            "tool": name,
            "elapsed_ms": round(elapsed_ms, 3),
            "result_bytes": len((output or "").encode("utf-8")),
        }), flush=True)
        return {"ok": True, "session_id": session_id, "task_id": task_id, "tool": name, "result": output}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8090)
    parser.add_argument("--root", default=os.environ.get("APPWORLD_ROOT", ""))
    parser.add_argument("--task-map", default=os.environ.get("APPWORLD_TASK_MAP", ""))
    parser.add_argument("--auth-token", default=os.environ.get("APPWORLD_AUTH_TOKEN", ""))
    args = parser.parse_args()
    if not args.root or not args.task_map or not args.auth_token:
        raise SystemExit("APPWORLD_ROOT, APPWORLD_TASK_MAP, and APPWORLD_AUTH_TOKEN are required")
    adapter = AppWorldAdapter(args.root, args.task_map)

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            if self.path != "/execute":
                self.send_error(404)
                return
            expected = f"Bearer {args.auth_token}"
            if not hmac.compare_digest(self.headers.get("Authorization", ""), expected):
                self.send_error(401, "authorization required")
                return
            try:
                length = int(self.headers.get("Content-Length", 0))
            except ValueError:
                self.send_error(400, "invalid Content-Length")
                return
            if length <= 0 or length > MAX_REQUEST_BYTES:
                self.send_error(413, "request is too large")
                return
            try:
                response = adapter.execute(json.loads(self.rfile.read(length)))
                body = json.dumps(response).encode()
                self.send_response(200 if response.get("ok") else 400)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception:
                print("AppWorld request failed", flush=True)
                self.send_error(500, "internal server error")

        def log_message(self, format, *args):
            print(format % args, flush=True)

    print(f"AppWorld adapter listening on {args.host}:{args.port}", flush=True)
    # AppWorld installs process-level signal handlers during execution, so its
    # stateful world must run on the main thread. Serialization also preserves
    # deterministic state transitions within a session.
    HTTPServer((args.host, args.port), Handler).serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
