#!/usr/bin/env python3
"""Build a trace-session -> AppWorld-task mapping from replay evidence."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


def instruction(prompt: str) -> str:
    match = re.search(r"Task from supervisor:\n(.*?)(?:\n\n|$)", prompt, re.S)
    if not match:
        raise ValueError("session prompt has no task instruction")
    return match.group(1).strip()


def login_values(session: dict) -> set[str]:
    values: set[str] = set()
    for turn in session.get("turns") or []:
        for call in turn.get("tool_calls") or []:
            arguments = call.get("arguments") or {}
            for key in ("username", "email", "phone_number"):
                value = arguments.get(key)
                if value:
                    values.add(str(value))
    return values


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--replay", type=Path, required=True)
    parser.add_argument("--tasks", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    task_specs = []
    for path in args.tasks.glob("*/specs.json"):
        task_specs.append((path.parent.name, json.loads(path.read_text(encoding="utf-8"))))
    replay = json.loads(args.replay.read_text(encoding="utf-8"))
    mapping: dict[str, str] = {}
    unresolved: list[str] = []
    for session in replay.get("sessions") or []:
        session_id = str(session["session_id"])
        target_instruction = instruction(str(session.get("user_prompt") or ""))
        values = login_values(session)
        candidates = []
        for task_id, spec in task_specs:
            supervisor = spec.get("supervisor") or {}
            supervisor_values = {str(supervisor.get(key)) for key in ("email", "phone_number") if supervisor.get(key)}
            if spec.get("instruction", "").strip() == target_instruction and (not values or values & supervisor_values):
                candidates.append(task_id)
        if len(candidates) != 1:
            unresolved.append(f"{session_id}: {candidates}")
        else:
            mapping[session_id] = candidates[0]
    if unresolved:
        raise SystemExit("Unresolved mappings:\n" + "\n".join(unresolved))
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(mapping, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {len(mapping)} AppWorld mappings: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
