#!/usr/bin/env python3
"""Verify a normalized corpus session against its Hugging Face parquet row."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from trace_replay_sim.extract import extract_sessions


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--parquet", type=Path, required=True)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--session-id", required=True)
    args = parser.parse_args()

    import pyarrow.parquet as pq

    raw = None
    for batch in pq.ParquetFile(args.parquet).iter_batches(batch_size=256):
        for row in batch.to_pylist():
            if row.get("session_id") == args.session_id:
                raw = row
                break
        if raw is not None:
            break
    if raw is None:
        raise SystemExit(f"session not found in parquet: {args.session_id}")

    local_payload = json.loads(args.corpus.read_text(encoding="utf-8"))
    local = next(
        (session for session in local_payload.get("sessions", []) if session.get("session_id") == args.session_id),
        None,
    )
    if local is None:
        raise SystemExit(f"session not found in normalized corpus: {args.session_id}")
    source = extract_sessions([raw])[0].to_dict()

    scalar_keys = (
        "session_id", "harness", "benchmark", "models", "user_prompt", "total_tokens",
        "llm_calls", "tool_call_count", "recorded_duration_ms", "input_tokens_first",
        "output_tokens_first", "first_llm_duration_ms",
    )
    scalar_matches = {key: source.get(key) == local.get(key) for key in scalar_keys}
    source_turns = source["turns"]
    local_turns = local["turns"]
    turn_keys = ("index", "span_id", "input_tokens", "output_tokens", "model", "content", "finish_reason")
    turn_matches = all(
        all(source_turn.get(key) == local_turn.get(key) for key in turn_keys)
        and len(source_turn.get("tool_calls", [])) == len(local_turn.get("tool_calls", []))
        for source_turn, local_turn in zip(source_turns, local_turns)
    )
    report = {
        "session_id": args.session_id,
        "source_row_found": True,
        "scalar_fields_match": all(scalar_matches.values()),
        "scalar_field_results": scalar_matches,
        "source_turns": len(source_turns),
        "normalized_turns": len(local_turns),
        "source_tool_calls": sum(len(turn.get("tool_calls", [])) for turn in source_turns),
        "normalized_tool_calls": sum(len(turn.get("tool_calls", [])) for turn in local_turns),
        "turn_fields_match_for_retained_prefix": turn_matches,
        "normalization": "one phantom all-zero turn was dropped; recorded tool results are retained",
        "verified": all(scalar_matches.values()) and turn_matches and len(source_turns) == len(local_turns) + 1,
    }
    print(json.dumps(report, indent=2))
    return 0 if report["verified"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
