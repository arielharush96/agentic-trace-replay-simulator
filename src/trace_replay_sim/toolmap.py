"""Map recorded agent tools onto OpenShell exec commands."""

from __future__ import annotations

from typing import Any


_EXEC_HINTS = (
    "bash",
    "shell",
    "exec",
    "terminal",
    "command",
    "cmd",
    "python",
    "code_execution",
    "computer",
)

_READ_HINTS = ("read", "cat", "view", "open_file", "get_file")
_WRITE_HINTS = ("write", "edit", "str_replace", "create_file", "patch")

_MOCK_LLM_URL_DEFAULT = "http://mock-llm.trace-replay.svc.cluster.local:8080"


def _arg(arguments: dict[str, Any], *keys: str, default: str = "") -> str:
    for key in keys:
        value = arguments.get(key)
        if value is None:
            continue
        if isinstance(value, list):
            return " ".join(str(x) for x in value)
        text = str(value).strip()
        if text:
            return text
    return default


def map_tool(name: str, arguments: dict[str, Any] | None = None) -> list[str]:
    """Return an argv list that OpenShell can exec (no session context).

    Unknown tools still hit the sandbox (`true`) so OpenShell is exercised
    even when the original tool has no local equivalent.
    """
    args = arguments or {}
    lowered = (name or "").lower()

    if any(hint in lowered for hint in _EXEC_HINTS):
        command = _arg(args, "command", "cmd", "code", "script", "input")
        if not command:
            command = "echo ok"
        return ["bash", "-lc", command]

    if any(hint in lowered for hint in _READ_HINTS):
        path = _arg(args, "path", "file_path", "filename", "target", default="/etc/hostname")
        return ["bash", "-lc", f"cat {path!s} 2>/dev/null || echo missing"]

    if any(hint in lowered for hint in _WRITE_HINTS):
        return ["bash", "-lc", "echo replay-write >/tmp/trace-replay.txt"]

    return ["true"]


def map_tool_with_replay(
    name: str,
    arguments: dict[str, Any] | None = None,
    *,
    session_id: str = "",
    turn_idx: int = 0,
    call_idx: int = 0,
) -> list[str]:
    """Return an argv list with real recorded result for non-exec tools.

    Exec/bash tools run the actual recorded command.
    All other tools (API calls, etc.) curl the mock LLM tool-result endpoint
    which returns the verbatim response the real AppWorld server gave.
    Falls back to empty string if the mock is unreachable.
    """
    args = arguments or {}
    lowered = (name or "").lower()

    if any(hint in lowered for hint in _EXEC_HINTS):
        command = _arg(args, "command", "cmd", "code", "script", "input")
        if not command:
            command = "echo ok"
        return ["bash", "-lc", command]

    if any(hint in lowered for hint in _READ_HINTS):
        path = _arg(args, "path", "file_path", "filename", "target", default="/etc/hostname")
        return ["bash", "-lc", f"cat {path!s} 2>/dev/null || echo missing"]

    if any(hint in lowered for hint in _WRITE_HINTS):
        return ["bash", "-lc", "echo replay-write >/tmp/trace-replay.txt"]

    # For all other tools: fetch the recorded AppWorld response from mock LLM.
    # MOCK_LLM_URL env var overrides the in-cluster default.
    # Uses python3 stdlib so no curl/wget dependency in the sandbox image.
    url = (
        f"${{MOCK_LLM_URL:-{_MOCK_LLM_URL_DEFAULT}}}"
        f"/v1/tool-result"
        f"?session_id={session_id}&turn={turn_idx}&call={call_idx}"
    )
    py = (
        "import sys,urllib.request;"
        f"u='{url}';"
        "print(urllib.request.urlopen(u,timeout=10).read().decode(),end='')"
    )
    return ["sh", "-c", f'python3 -c "{py}" 2>/dev/null || echo ""']
