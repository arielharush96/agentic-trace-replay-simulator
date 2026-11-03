#!/usr/bin/env python3
"""HTTP adapter from replayed environment tool calls to stateful AppWorld APIs."""

from __future__ import annotations

import argparse
import hashlib
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
_VOLATILE_KEYS = {"access_token", "api_key", "authorization", "password", "refresh_token", "token"}


def _canonical_arguments(value, key=None):
    if key and str(key).lower() in _VOLATILE_KEYS:
        return "<redacted>"
    if isinstance(value, dict):
        return {str(name): _canonical_arguments(value[name], name) for name in sorted(value)}
    if isinstance(value, list):
        return [_canonical_arguments(item) for item in value]
    return value


def _arguments_sha256(arguments):
    payload = json.dumps(_canonical_arguments(arguments or {}), sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class AppWorldAdapter:
    def __init__(self, root: str, mapping_path: str):
        os.environ["APPWORLD_ROOT"] = root
        from appworld import AppWorld

        self.AppWorld = AppWorld
        self.mapping = json.loads(Path(mapping_path).read_text(encoding="utf-8"))
        self.worlds: dict[str, object] = {}
        self.tokens: dict[str, dict[str, str]] = {}
        # Keep a bounded-in-practice, read-only audit stream for the
        # experiment controller.  The controller filters this by session ID
        # after each trace; no benchmark state is changed by the read path.
        self.events: list[dict[str, object]] = []
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

    @staticmethod
    def _account_password(world, app_name: str) -> str | None:
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
        def reject(error: str) -> dict:
            print(json.dumps({
                "event": "appworld_reject",
                "session_id": session_id,
                "tool": str(request.get("tool") or ""),
                "error": error,
            }), flush=True)
            return {"ok": False, "error": error}
        if not task_id:
            return reject(f"no AppWorld task mapping for session {session_id}")
        name = str(request.get("tool") or "")
        original_arguments = dict(request.get("arguments") or {})
        prefix = "mcp__environment__"
        if not name.startswith(prefix):
            return reject(f"unsupported tool name: {name}")
        if name == "mcp__environment__finish":
            app_name, api_name = "supervisor", "complete_task"
            expression = "response = apis.supervisor.complete_task(**arguments)"
        else:
            parts = name[len(prefix):].split("__", 1)
            if len(parts) != 2:
                return reject(f"cannot map AppWorld tool: {name}")
            app_name, api_name = parts
            if not all(_SAFE_TOOL_COMPONENT.fullmatch(part) for part in (app_name, api_name)):
                return reject(f"invalid AppWorld tool component: {name}")
            expression = f"response = apis.{app_name}.{api_name}(**arguments)"

        started_ns = self._clock_ns()
        with self.lock:
            world = self._world(session_id, task_id)
            arguments = self._translate_arguments(
                session_id, world, app_name, api_name, dict(request.get("arguments") or {})
            )
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
        event = {
            "event": "appworld_tool",
            "session_id": session_id,
            "task_id": task_id,
            "tool": name,
            "turn": request.get("turn"),
            "call": request.get("call"),
            "arguments_sha256": _arguments_sha256(original_arguments),
            "elapsed_ms": round(elapsed_ms, 3),
            "result_bytes": len((output or "").encode("utf-8")),
        }
        with self.lock:
            self.events.append(event)
        print(json.dumps(event), flush=True)
        return {"ok": True, "session_id": session_id, "task_id": task_id, "tool": name, "result": output}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8090)
    parser.add_argument("--root", default=os.environ.get("APPWORLD_ROOT", ""))
    parser.add_argument("--task-map", default=os.environ.get("APPWORLD_TASK_MAP", ""))
    parser.add_argument("--auth-token", default=os.environ.get("APPWORLD_AUTH_TOKEN", ""))
    args = parser.parse_args()
    if not args.root or not args.task_map:
        raise SystemExit("APPWORLD_ROOT and APPWORLD_TASK_MAP are required")
    adapter = AppWorldAdapter(args.root, args.task_map)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            if self.path == "/events":
                with adapter.lock:
                    body = json.dumps({"events": list(adapter.events)}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if self.path != "/health":
                self.send_error(404)
                return
            body = json.dumps({"ok": True, "service": "appworld"}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):  # noqa: N802
            if self.path != "/execute":
                self.send_error(404)
                return
            if args.auth_token:
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
            except Exception as exc:  # noqa: BLE001
                print(json.dumps({"event": "appworld_error", "error": str(exc)}), flush=True)
                self.send_error(500, "internal server error")

        def log_message(self, format, *args):
            print(format % args, flush=True)

    print(f"AppWorld adapter listening on {args.host}:{args.port}", flush=True)
    HTTPServer((args.host, args.port), Handler).serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
