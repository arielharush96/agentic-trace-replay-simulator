#!/usr/bin/env python3
"""Minimal HTTP adapter from replayed tool calls to AppWorld APIs."""

from __future__ import annotations

import argparse
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


class AppWorldAdapter:
    def __init__(self, root: str, mapping_path: str):
        os.environ["APPWORLD_ROOT"] = root
        from appworld import AppWorld

        self.AppWorld = AppWorld
        self.mapping = json.loads(Path(mapping_path).read_text(encoding="utf-8"))
        self.worlds = {}
        self.lock = threading.Lock()

    def execute(self, request: dict) -> dict:
        session_id = str(request.get("session_id") or "")
        task_id = self.mapping.get(session_id)
        if not task_id:
            return {"ok": False, "error": f"no AppWorld task mapping for session {session_id}"}
        name = str(request.get("tool") or "")
        prefix = "mcp__environment__"
        if not name.startswith(prefix):
            return {"ok": False, "error": f"unsupported tool name: {name}"}
        parts = name[len(prefix):].split("__", 1)
        if len(parts) != 2:
            return {"ok": False, "error": f"cannot map AppWorld tool: {name}"}
        app_name, api_name = parts
        if app_name == "supervisor" and api_name == "complete_task":
            expression = "response = apis.supervisor.complete_task(**arguments)"
        else:
            expression = f"response = apis.{app_name}.{api_name}(**arguments)"
        with self.lock:
            world = self.worlds.get(session_id)
            if world is None:
                world = self.AppWorld(task_id=task_id, experiment_name="trace-replay")
                self.worlds[session_id] = world
        code = "import json\n" + expression + "\nprint(json.dumps(response, default=str))"
        output = world.execute(code.replace("**arguments", f"**{json.dumps(dict(request.get('arguments') or {}))}"))
        return {"ok": True, "session_id": session_id, "task_id": task_id, "tool": name, "result": output}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8090)
    parser.add_argument("--root", default=os.environ.get("APPWORLD_ROOT", ""))
    parser.add_argument("--task-map", default=os.environ.get("APPWORLD_TASK_MAP", ""))
    args = parser.parse_args()
    if not args.root or not args.task_map:
        raise SystemExit("APPWORLD_ROOT and APPWORLD_TASK_MAP are required")
    adapter = AppWorldAdapter(args.root, args.task_map)

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            if self.path != "/execute":
                self.send_error(404)
                return
            length = int(self.headers.get("Content-Length", 0))
            try:
                response = adapter.execute(json.loads(self.rfile.read(length)))
                body = json.dumps(response).encode()
                self.send_response(200 if response.get("ok") else 400)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception as exc:
                self.send_error(500, str(exc))

        def log_message(self, format, *args):
            print(format % args, flush=True)

    print(f"AppWorld adapter listening on {args.host}:{args.port}", flush=True)
    ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
