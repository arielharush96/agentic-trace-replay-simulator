#!/usr/bin/env python3
"""Convert a normalized corpus to stateful AppWorld tool calls."""

from __future__ import annotations

import argparse
import base64
import copy
import json
from pathlib import Path


def appworld_command(session_id: str, turn: int, call: int, name: str, arguments: dict, endpoint: str) -> list[str]:
    payload = base64.b64encode(json.dumps({
        "session_id": session_id,
        "turn": turn,
        "call": call,
        "tool": name,
        "arguments": arguments,
    }, separators=(",", ":")).encode()).decode()
    code = (
        "import base64,json,os,urllib.request;"
        f"p=json.loads(base64.b64decode('{payload}'));"
        f"r=urllib.request.Request(os.environ.get('APPWORLD_API_URL','{endpoint}'),"
        "data=json.dumps(p).encode(),headers={'Content-Type':'application/json'});"
        "print(urllib.request.urlopen(r,timeout=180).read().decode(),end='')"
    )
    return ["python3", "-c", code]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in", dest="input_path", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--endpoint", default="http://appworld:8090/execute")
    args = parser.parse_args()
    payload = copy.deepcopy(json.loads(args.input_path.read_text(encoding="utf-8")))
    session = (payload.get("sessions") or [None])[0]
    if session is None:
        raise SystemExit("corpus has no session")
    session_id = str(session.get("session_id"))
    for turn in session.get("turns") or []:
        for call_index, tool in enumerate(turn.get("tool_calls") or []):
            name = str(tool.get("name") or "")
            if name.startswith("mcp__environment__"):
                tool["mapped_command"] = appworld_command(
                    session_id, int(turn.get("index", 0)), call_index, name,
                    dict(tool.get("arguments") or {}), args.endpoint,
                )
    payload["tool_backend"] = "appworld"
    payload["appworld_endpoint"] = args.endpoint
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote AppWorld-backed corpus: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
