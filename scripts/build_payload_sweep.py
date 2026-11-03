#!/usr/bin/env python3
"""Create controlled replay variants with synthetic prompt padding."""

from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--padding-bytes", nargs="+", type=int, default=[0, 16384, 32768, 65536])
    args = parser.parse_args()

    source = json.loads(args.corpus.read_text(encoding="utf-8"))
    sessions = source.get("sessions") or []
    if len(sessions) != 1:
        raise SystemExit("payload sweep requires a corpus containing exactly one session")
    args.out.mkdir(parents=True, exist_ok=True)
    manifest = []
    for padding in args.padding_bytes:
        payload = copy.deepcopy(source)
        session = payload["sessions"][0]
        session["user_prompt"] = str(session.get("user_prompt") or "") + ("\n" + ("P" * padding) if padding else "")
        payload["synthetic_payload_sweep"] = {
            "padding_bytes": padding,
            "method": "append deterministic bytes to the initial user prompt",
        }
        path = args.out / f"padding-{padding:06d}.json"
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        manifest.append({"padding_bytes": padding, "corpus": str(path)})
    (args.out / "sweep_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
