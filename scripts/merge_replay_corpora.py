#!/usr/bin/env python3
"""Merge single-session replay corpora into one mock-LLM corpus."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()

    files = sorted(args.root.glob("*__*-corpus-turns/corpus.json"))
    if not files:
        raise SystemExit(f"no corpus files found under {args.root}")

    sessions = []
    dataset = None
    for path in files:
        payload = json.loads(path.read_text(encoding="utf-8"))
        dataset = dataset or payload.get("dataset")
        sessions.extend(payload.get("sessions") or [])

    merged = {
        "schema": "replay-corpus-v1",
        "dataset": dataset or "merged",
        "sessions": sessions,
        "by_id": {str(session.get("session_id")): session for session in sessions},
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(merged, ensure_ascii=False), encoding="utf-8")
    print(f"Merged {len(files)} corpora and {len(sessions)} sessions into {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
