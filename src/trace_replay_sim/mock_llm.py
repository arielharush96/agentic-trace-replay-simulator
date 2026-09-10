"""Stdlib OpenAI-compatible mock LLM that deterministically replays recorded turns.

Design goals (rewritten from scratch):

* **Controlled model time, not fitted.** Per-turn model time is a deterministic
  function of a *named profile* and the turn's recorded ``output_tokens``:
  ``model_ms = TTFT + max(0, output_tokens - 1) * ITL`` with BOTH TTFT and ITL
  taken from the profile. The recorded ``duration_ms`` is E2E wall-clock (tool +
  network + think time), NOT decode, so it is kept only as a sanity reference and
  never used to derive timing.

* **Skew-free per-turn edges.** Each request records ``t_recv`` / ``t_first_emit``
  / ``t_end_emit`` on a single monotonic clock. Downstream, the inter-turn gap
  ``G[i] = t_recv[i+1] - t_end_emit[i]`` (same clock, same process) is the
  authoritative skew-free bound on OpenClaw+OpenShell overhead between model calls.

* **Redundant, self-validating correlation.** Every edge record carries four
  independent keys so the analyzer can join on the strongest present and
  cross-check the rest: ``(trace_id, parent_span_id)`` from the incoming
  ``traceparent`` (authoritative Jaeger join to OpenClaw's ``model.call`` span),
  ``session_id`` (grouping), a mock per-session monotonic ``turn_seq`` (single-clock
  ordering that holds even with zero spans), and a global ``request_seq`` +
  monotonic ``t_recv_ns`` (ultimate ordering / retry-dedup fallback).
"""

from __future__ import annotations

import json
import hashlib
import os
import re
import shlex
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse
import urllib.request


# --------------------------------------------------------------------------- #
# Delay profiles.  (TTFT ms, ITL ms).  Model time is derived ONLY from these.  #
# --------------------------------------------------------------------------- #

DELAY_PROFILES: dict[str, tuple[float, float]] = {
    # RHAIIS 1k/1k inference-sim reference (default): fast TTFT, small ITL.
    "rhaiis": (130.0, 7.0),
    # Coarser interactive profiles for sensitivity sweeps.
    "moderate": (400.0, 30.0),
    "realistic": (3700.0, 85.0),
    # This repo's ground-truth anchor: 8xA100 Qwen3-8B HumanEval GuideLLM,
    # real vLLM at concurrency=1, P50 (TTFT, ITL).
    "humaneval-vllm-c1": (51.0, 16.8),
}

DEFAULT_PROFILE = "rhaiis"

# --------------------------------------------------------------------------- #
# Prefill scaling (prefix-cache aware).  A real LLM prefills the assembled       #
# prompt before it emits the first token, so TTFT grows with context size.       #
# But with prefix/KV caching, only the NEW tokens the gateway appended this turn  #
# are actually prefilled -- the shared prefix from earlier turns is a cache hit.  #
# So within a session: turn 0 prefills the WHOLE context cold (heavy TTFT), and   #
# later turns prefill only the per-turn DELTA (cheap).  This reconciles the       #
# baseline: the 51ms->804ms TTFT jump (110->7862 prompt tokens) is the cold       #
# turn-0 prefill; warm turns collapse back toward the profile TTFT.               #
# Calibrated to the 8xA100 Qwen3-8B HumanEval baseline (GuideLLM, C=1, P50):     #
#   vLLM     117 tok -> 51.3 ms TTFT                                             #
#   OpenClaw 7862 tok -> 804.5 ms TTFT                                           #
#   => prefill ~0.0972 ms/token, base ~40 ms.  The profile TTFT is the value at #
#   PREFILL_REF_TOKENS, so we add prefill only for the uncached (new) context.   #
# NOTE: the /no_think baseline's TTFT is prefill + shell gateway overhead, NOT   #
# reasoning (its <think> blocks are empty -- all layers ran "/no_think").         #
PREFILL_PER_TOKEN_MS = float(os.environ.get("MOCK_PREFILL_PER_TOKEN_MS", "0.0972"))
PREFILL_REF_TOKENS = int(os.environ.get("MOCK_PREFILL_REF_TOKENS", "117"))

# Per-session prefix-cache state: the largest assembled-prompt size already
# prefilled for a session.  New tokens this turn = assembled - cached_prefix.
_SESSION_PREFIX_TOKENS: dict[str, int] = {}


def _prefill_enabled() -> bool:
    # Default ON: a realistic model must charge cold turn-0 prefill of the large
    # assembled context. Set MOCK_PREFILL_SCALING=0 to force the flat profile TTFT.
    return os.environ.get("MOCK_PREFILL_SCALING", "1") != "0"


def prefill_new_tokens(session_id: str | None, assembled_tokens: int | None) -> int:
    """Uncached tokens to prefill this turn under prefix caching (mutates state).

    Turn 0 of a session has no cached prefix beyond ``PREFILL_REF_TOKENS`` -> the
    whole assembled context is prefilled cold.  Later turns reuse the KV cache and
    prefill only the token DELTA the gateway appended.  Returns 0 when there is no
    context info.  Call exactly once per served turn.
    """
    if assembled_tokens is None:
        return 0
    key = session_id or "__default__"
    cached = _SESSION_PREFIX_TOKENS.get(key, PREFILL_REF_TOKENS)
    new = max(0, int(assembled_tokens) - cached)
    _SESSION_PREFIX_TOKENS[key] = max(cached, int(assembled_tokens))
    return new


def prefill_ttft_ms(base_ttft_ms: float, new_tokens: int | None) -> float:
    """TTFT with prefill for the uncached (new) context tokens this turn.

    ``base_ttft_ms`` is the profile TTFT measured at ``PREFILL_REF_TOKENS``; we
    add ``PREFILL_PER_TOKEN_MS`` for each uncached token (see
    :func:`prefill_new_tokens`).  With scaling disabled or no new tokens, the base
    TTFT is returned unchanged.
    """
    if new_tokens is None or not _prefill_enabled():
        return base_ttft_ms
    return base_ttft_ms + PREFILL_PER_TOKEN_MS * max(0, int(new_tokens))


# --------------------------------------------------------------------------- #
# Reasoning preamble (REALISTIC "thinking ON" mode).  A reasoning model (Qwen3) #
# emits a <think>...</think> block BEFORE any visible content.  OpenClaw's       #
# tool-call detection pipeline classifies every streamed chunk                   #
# (Text/ToolCall/ThinkingContent) and BUFFERS the reasoning block before         #
# releasing the first CLIENT-visible token -- verified live: enabling reasoning   #
# on the real openclaw-shell gateway delayed first-visible token by +1782ms and  #
# swallowed the reasoning content.  The mock reproduces this by streaming N       #
# reasoning tokens FIRST (paced at the profile ITL) so OpenClaw does the real     #
# buffering work; nothing is hardcoded.                                          #
#                                                                                #
# IMPORTANT: this is NOT what the 8xA100 baseline measured.  That benchmark ran  #
# "/no_think" on ALL layers (vLLM/openclaw/shell) -- its <think> blocks are       #
# empty, so its 895ms "Response Processing" is prefill + shell overhead, not     #
# reasoning.  Reasoning models the PRODUCTION case the /no_think baseline         #
# deliberately excluded; it is an explicit, DOCUMENTED workload parameter, not a  #
# fit to any baseline number.  The count has no GPU-measured ground truth here,   #
# so treat it as a swept assumption.                                             #
#                                                                                #
# The reasoning count is carved OUT of the turn's recorded output_tokens (total  #
# decode is unchanged) -- it only moves where the first VISIBLE token lands.     #
# Resolution per turn: the turn's own "reasoning_tokens" field, else             #
# MOCK_REASONING_TOKENS (fixed count), else MOCK_REASONING_FRACTION *            #
# output_tokens.  Default 0 -> off (honest no-reasoning stream).                 #
REASONING_TOKENS_ENV = os.environ.get("MOCK_REASONING_TOKENS")      # fixed count, optional
REASONING_FRACTION = float(os.environ.get("MOCK_REASONING_FRACTION", "0"))
# "think_tags": <think>..</think> inside content deltas (raw model; OpenClaw parses the tags).
# "reasoning_content": a separate reasoning_content delta field (vLLM reasoning-parser mode).
REASONING_MODE = os.environ.get("MOCK_REASONING_MODE", "think_tags").strip().lower()
RESPONSE_PROCESSING_PROFILE_PATH = os.environ.get("MOCK_RESPONSE_PROCESSING_PROFILE", "").strip()


def load_response_processing_profile() -> list[float]:
    if not RESPONSE_PROCESSING_PROFILE_PATH:
        return []
    try:
        values = json.loads(Path(RESPONSE_PROCESSING_PROFILE_PATH).read_text(encoding="utf-8"))
        return [float(value) for value in values if float(value) >= 0]
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return []


RESPONSE_PROCESSING_PROFILE = load_response_processing_profile()


def response_processing_tail_ms(replay: dict[str, Any], traceparent: str | None = None) -> float:
    """Return a calibrated gateway-processing tail for this replay turn."""
    if not RESPONSE_PROCESSING_PROFILE:
        return 0.0
    seed = f"{traceparent or ''}:{replay.get('session_id', '')}:{replay.get('turn_seq', 0)}"
    index = int(hashlib.sha256(seed.encode()).hexdigest()[:8], 16) % len(RESPONSE_PROCESSING_PROFILE)
    return RESPONSE_PROCESSING_PROFILE[index]


def resolve_reasoning_tokens(output_tokens: int, turn_reasoning: Any = None) -> int:
    """Reasoning tokens to carve from ``output_tokens`` (see module notes).

    Clamped to ``[0, output_tokens - 1]`` so at least one visible token remains
    and the total decode budget is never inflated.
    """
    if turn_reasoning is not None:
        n = int(turn_reasoning)
    elif REASONING_TOKENS_ENV is not None:
        n = int(REASONING_TOKENS_ENV)
    elif REASONING_FRACTION > 0:
        n = int(round(REASONING_FRACTION * max(0, int(output_tokens))))
    else:
        n = 0
    return max(0, min(n, max(0, int(output_tokens) - 1)))


def resolve_profile(name: str | None) -> tuple[str, float, float]:
    """Return (profile_name, ttft_ms, itl_ms).

    ``name`` may be a known profile key or ``env`` to read SSE_TTFT_MS/SSE_ITL_MS.
    Unknown names fall back to the default profile (never to duration fitting).
    """
    key = (name or os.environ.get("MOCK_DELAY_PROFILE") or DEFAULT_PROFILE).strip().lower()
    if key == "env":
        return "env", float(os.environ.get("SSE_TTFT_MS", "130")), float(os.environ.get("SSE_ITL_MS", "7"))
    if key in DELAY_PROFILES:
        ttft, itl = DELAY_PROFILES[key]
        return key, ttft, itl
    ttft, itl = DELAY_PROFILES[DEFAULT_PROFILE]
    return DEFAULT_PROFILE, ttft, itl


def compute_timing(output_tokens: int, profile_name: str,
                   new_tokens: int | None = None) -> tuple[float, float, float, str]:
    """Deterministic model time from the profile (seconds).

    Returns (ttft_s, itl_s, decode_s, resolved_profile_name).  TTFT includes
    prefill of the ``new_tokens`` uncached context (see :func:`prefill_ttft_ms`)
    -- cold turn-0 prefill of the whole assembled prompt, warm delta after, so the
    gateway's first-token overhead matches the baseline's cold/warm split. decode
    is ``max(0, output_tokens - 1) * ITL`` -- the first token is TTFT.
    """
    name, ttft_ms, itl_ms = resolve_profile(profile_name)
    ttft_ms = prefill_ttft_ms(ttft_ms, new_tokens)
    extra = max(0, int(output_tokens) - 1)
    decode_ms = extra * itl_ms
    return ttft_ms / 1000.0, itl_ms / 1000.0, decode_ms / 1000.0, name


# --------------------------------------------------------------------------- #
# W3C trace-context + lightweight OTLP span export (no SDK dependency).        #
# --------------------------------------------------------------------------- #

OTLP_ENDPOINT = os.environ.get("OTLP_ENDPOINT", "http://jaeger.trace-replay.svc.cluster.local:4318")


def parse_traceparent(header: str | None) -> tuple[str, str] | None:
    """Parse a W3C ``traceparent`` header into (trace_id, parent_span_id)."""
    if not header:
        return None
    parts = header.split("-")
    if len(parts) >= 4 and parts[1] and parts[2]:
        return parts[1], parts[2]
    return None


def _export_span(
    trace_id: str,
    parent_span_id: str,
    name: str,
    start_ns: int,
    end_ns: int,
    attributes: dict[str, Any] | None = None,
) -> None:
    """Fire-and-forget OTLP/HTTP span to Jaeger (best effort)."""
    if not OTLP_ENDPOINT:
        return
    attrs = []
    for k, v in (attributes or {}).items():
        if isinstance(v, bool):
            attrs.append({"key": k, "value": {"boolValue": v}})
        elif isinstance(v, int):
            attrs.append({"key": k, "value": {"intValue": str(v)}})
        elif isinstance(v, float):
            attrs.append({"key": k, "value": {"doubleValue": v}})
        else:
            attrs.append({"key": k, "value": {"stringValue": str(v)}})
    payload = {
        "resourceSpans": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "mock-llm"}},
            ]},
            "scopeSpans": [{
                "scope": {"name": "mock-llm"},
                "spans": [{
                    "traceId": trace_id,
                    "spanId": uuid.uuid4().hex[:16],
                    "parentSpanId": parent_span_id,
                    "name": name,
                    "kind": 2,  # SERVER
                    "startTimeUnixNano": str(start_ns),
                    "endTimeUnixNano": str(end_ns),
                    "attributes": attrs,
                    "status": {},
                }],
            }],
        }]
    }
    try:
        req = urllib.request.Request(
            f"{OTLP_ENDPOINT}/v1/traces",
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=2)
    except Exception:
        pass  # fire-and-forget


# --------------------------------------------------------------------------- #
# Per-turn edge recorder: the skew-free measurement backbone.                  #
# --------------------------------------------------------------------------- #

EDGE_SCHEMA = "mock-edge/v1"


class EdgeRecorder:
    """Thread-safe sink for per-turn edge records.

    Writes each record as one JSON line to an optional file AND to stdout with a
    ``MOCK_EDGE `` marker, so records are retrievable from ``oc logs`` in-cluster
    even without a shared volume.
    """

    def __init__(self, path: str | None) -> None:
        self._lock = threading.Lock()
        self._fh = None
        if path:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            self._fh = open(path, "a", encoding="utf-8")  # noqa: SIM115
        self.count = 0

    def emit(self, record: dict[str, Any]) -> None:
        line = json.dumps(record, ensure_ascii=False)
        with self._lock:
            self.count += 1
            print(f"MOCK_EDGE {line}", flush=True)
            if self._fh is not None:
                self._fh.write(line + "\n")
                self._fh.flush()


# --------------------------------------------------------------------------- #
# Corpus loading + session/turn resolution.                                   #
# --------------------------------------------------------------------------- #

SESSION_TAG_RE = re.compile(r"(?:<!--replay-session:([^>]+)-->|REPLAY_SESSION_ID=([^\s]+))")
TURN_TAG_RE = re.compile(r"(?:<!--replay-turn:(\d+)-->|REPLAY_TURN_INDEX=(\d+))")


def load_corpus(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    sessions = payload.get("sessions") or []
    return {
        "payload": payload,
        "sessions": sessions,
        "by_id": {str(item.get("session_id")): item for item in sessions},
    }


def extract_session_id(payload: dict[str, Any]) -> str | None:
    blob = json.dumps(payload, ensure_ascii=False)
    match = SESSION_TAG_RE.search(blob)
    if match:
        return (match.group(1) or match.group(2) or "").strip() or None
    extra = payload.get("extra_body") or {}
    if extra.get("replay_session_id"):
        return str(extra["replay_session_id"])
    return None


def first_user_text(payload: dict[str, Any]) -> str:
    for message in payload.get("messages") or []:
        if not isinstance(message, dict):
            continue
        if str(message.get("role") or "").lower() != "user":
            continue
        content = message.get("content")
        if isinstance(content, str) and content.strip():
            return content
        if isinstance(content, list):
            parts: list[str] = []
            for part in content:
                if isinstance(part, dict) and part.get("text"):
                    parts.append(str(part["text"]))
                elif isinstance(part, str):
                    parts.append(part)
            if parts:
                return "\n".join(parts)
    if isinstance(payload.get("input"), str):
        return str(payload["input"])
    return ""


def last_user_text(payload: dict[str, Any]) -> str:
    """Return only the newest user message, excluding historical warmup tags."""
    for message in reversed(payload.get("messages") or []):
        if not isinstance(message, dict) or str(message.get("role") or "").lower() != "user":
            continue
        content = message.get("content")
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            return "\n".join(
                str(item.get("text") or item) if isinstance(item, dict) else str(item)
                for item in content
            )
    if isinstance(payload.get("input"), str):
        return str(payload["input"])
    return ""


def assembled_prompt_text(payload: dict[str, Any]) -> str:
    """Reconstruct what the caller sent us this turn (the assembled prompt)."""
    parts: list[str] = []
    for message in payload.get("messages") or []:
        if not isinstance(message, dict):
            continue
        content = message.get("content")
        if isinstance(content, str):
            parts.append(content)
        elif isinstance(content, list):
            for item in content:
                if isinstance(item, dict) and item.get("text"):
                    parts.append(str(item["text"]))
                elif isinstance(item, str):
                    parts.append(item)
    tools = payload.get("tools") or payload.get("functions") or []
    if tools:
        parts.append(json.dumps(tools, ensure_ascii=False))
    inp = payload.get("input")
    if isinstance(inp, str):
        parts.append(inp)
    return "\n".join(parts)


def prompt_stats(text: str) -> dict[str, int]:
    """Cheap deterministic size stats over the assembled prompt."""
    units = 0
    for ch in text:
        units += 1 if ord(ch) < 128 else 2
    tokens = max(1, units // 4) if text else 0
    return {"chars": len(text), "tokens": tokens}


def is_phantom_turn(turn: dict[str, Any]) -> bool:
    """A degenerate all-zero recorded turn that must not be timed as real."""
    return (
        int(turn.get("output_tokens") or 0) <= 0
        and int(turn.get("input_tokens") or 0) <= 0
        and not (turn.get("content") or "").strip()
        and not (turn.get("tool_calls") or [])
    )


class ReplayIndex:
    """Resolves an incoming request to a recorded turn and advances the counter."""

    def __init__(self, corpus: dict[str, Any]):
        self.sessions = corpus["sessions"]
        self.by_id = corpus["by_id"]
        self._turn: dict[str, int] = {}
        self.lock = threading.Lock()
        self.stats = {"requests": 0, "hits": 0, "misses": 0, "bytes_in": 0}

    def _match_session(self, payload: dict[str, Any]) -> tuple[str | None, dict[str, Any] | None]:
        session_id = extract_session_id(payload)
        session = self.by_id.get(session_id) if session_id else None
        if session is None:
            blob = json.dumps(payload, ensure_ascii=False)
            for sid, candidate in self.by_id.items():
                if sid and sid in blob:
                    return sid, candidate
        if session is None:
            user_text = SESSION_TAG_RE.sub("", first_user_text(payload)).strip()
            for candidate in self.sessions:
                prompt = str(candidate.get("user_prompt") or "").strip()
                if prompt and (prompt in user_text or user_text in prompt):
                    return str(candidate.get("session_id")), candidate
        if session is None and len(self.sessions) == 1:
            only = self.sessions[0]
            return str(only.get("session_id")), only
        return session_id, session

    def resolve(self, payload: dict[str, Any], mode: str, trace_id: str | None = None) -> dict[str, Any]:
        session_id, session = self._match_session(payload)

        if session is None:
            with self.lock:
                self.stats["misses"] += 1
            return {
                "session_id": None, "turn_seq": -1, "content": "ok", "tool_calls": [],
                "input_tokens": 0, "output_tokens": 8, "duration_ms": 0.0,
                "finish_reason": "stop", "miss": True, "phantom": False, "exhausted": False,
            }

        # The turn counter must reset per AGENTIC RUN, not accumulate across every
        # request the mock ever sees. One OpenClaw run == one W3C trace: all its
        # internal model.calls share a trace_id and iterate turns 0..N; a fresh
        # driver request (next rep, or a different layer) is a new trace_id and
        # must restart at turn 0. Keying by trace_id gives that for free and keeps
        # mock-direct/openclaw/shell independent even against one shared mock pod.
        # When no traceparent is present (mock-direct --no-spans, unit tests), fall
        # back to session_id so single-session sequential replay still advances.
        blob = json.dumps(payload, ensure_ascii=False)
        turn_matches = list(TURN_TAG_RE.finditer(blob))
        turn_match = turn_matches[-1] if turn_matches else None
        if turn_match:
            turn_seq = int(turn_match.group(1) or turn_match.group(2))
        else:
            counter_key = trace_id or session_id
            with self.lock:
                turn_seq = self._turn.get(counter_key, 0)
                self._turn[counter_key] = turn_seq + 1
        with self.lock:
            self.stats["hits"] += 1

        # Prewarm the OpenShell session without consuming a recorded turn.
        if "REPLAY_WARMUP=1" in last_user_text(payload):
            return {
                "session_id": session_id, "turn_seq": -1, "content": "warmup", "tool_calls": [],
                "prefill_key": trace_id or session_id, "input_tokens": 1, "output_tokens": 1,
                "reasoning_tokens": 0, "duration_ms": 0.0, "finish_reason": "stop",
                "miss": False, "phantom": False, "exhausted": False, "warmup": True,
            }

        if mode == "text":
            content = str(session.get("gateway_text") or "ok")
            return {
                "session_id": session_id, "turn_seq": turn_seq, "content": content, "tool_calls": [],
                "input_tokens": int(session.get("input_tokens_first") or 0),
                "output_tokens": int(session.get("output_tokens_first") or max(1, len(content) // 4)),
                "duration_ms": float(session.get("first_llm_duration_ms") or 0.0),
                "finish_reason": "stop", "miss": False, "phantom": False, "exhausted": False,
            }

        turns = session.get("turns") or []
        if not turns or turn_seq >= len(turns):
            # Loop ran past the recorded turns -> emit a terminal, no-op turn.
            return {
                "session_id": session_id, "turn_seq": turn_seq, "content": "done", "tool_calls": [],
                "input_tokens": 0, "output_tokens": 1, "duration_ms": 0.0,
                "finish_reason": "stop", "miss": False, "phantom": False, "exhausted": True,
            }

        turn = turns[turn_seq]
        phantom = is_phantom_turn(turn)
        content = str(turn.get("content") or session.get("gateway_text") or ("ok" if not phantom else ""))
        tool_calls = list(turn.get("tool_calls") or [])
        return {
            "session_id": session_id, "turn_seq": turn_seq, "content": content, "tool_calls": tool_calls,
            # Per-run key (== turn-counter key): a fresh trace/rep resets the prefix
            # cache so each rep's turn 0 is a genuine cold prefill, not a cache hit.
            "prefill_key": counter_key,
            "input_tokens": int(turn.get("input_tokens") or 0),
            "output_tokens": int(turn.get("output_tokens") or (0 if phantom else max(1, len(content) // 4))),
            "reasoning_tokens": turn.get("reasoning_tokens"),
            "duration_ms": float(turn.get("duration_ms") or 0.0),
            "finish_reason": "tool_calls" if tool_calls else "stop",
            "miss": False, "phantom": phantom, "exhausted": False,
        }

    def reset(self) -> None:
        with self.lock:
            self._turn.clear()
            self.stats = {"requests": 0, "hits": 0, "misses": 0, "bytes_in": 0}

    def tool_result(self, session_id: str, turn_idx: int, call_idx: int) -> str:
        session = self.by_id.get(session_id)
        if not session:
            return ""
        turns = session.get("turns") or []
        if turn_idx >= len(turns):
            return ""
        tool_calls = turns[turn_idx].get("tool_calls") or []
        if call_idx >= len(tool_calls):
            return ""
        return str(tool_calls[call_idx].get("recorded_result") or "")


# --------------------------------------------------------------------------- #
# Tool-call encoding.                                                          #
# --------------------------------------------------------------------------- #

def _sandbox_exec_args(mapped_command: list[Any] | None) -> str:
    """Encode the trace's replay command for OpenClaw's owned ``exec`` tool."""
    command = shlex.join([str(part) for part in (mapped_command or ["echo", "ok"])])
    return json.dumps({"command": command})


# Realistic word-like filler so every streamed token carries real bytes (a real
# vLLM token is ~4 chars). Used only to pad a turn's *content* up to output_tokens
# when the recorded payload is shorter, so the gateway pays a realistic per-token
# SSE parse/serialize cost (bucket D) instead of the near-zero cost of empty deltas.
_FILLER_TOKENS = [
    "the ", "and ", "to ", "of ", "a ", "in ", "that ", "is ", "for ", "with ",
    "this ", "code ", "value ", "return ", "function ", "result ", "which ",
    "then ", "will ", "data ", "name ", "type ", "call ", "file ", "here ",
    "should ", "when ", "from ", "into ", "each ",
]


def _even_chunks(text: str, n: int) -> list[str]:
    """Split *text* into ~*n* contiguous pieces, each carrying real bytes.

    The last piece absorbs the remainder so no bytes are dropped. When *n*
    exceeds the length, one char per piece is returned (caller pads the rest).
    """
    text = text or ""
    n = max(1, n)
    if not text:
        return []
    if n >= len(text):
        return list(text)
    step = len(text) // n
    pieces = [text[i * step:(i + 1) * step] for i in range(n - 1)]
    pieces.append(text[(n - 1) * step:])
    return pieces


def realistic_content_stream(text: str, n_tokens: int) -> list[str]:
    """~*n_tokens* content deltas of real bytes, padded with word-like filler.

    Reproduces a real model's per-token payload so the gateway's SSE
    serialization cost (bucket D) is genuinely exercised rather than collapsing
    to near-zero on empty deltas.
    """
    n_tokens = max(1, n_tokens)
    pieces = _even_chunks(text, n_tokens)
    k = 0
    while len(pieces) < n_tokens:
        pieces.append(_FILLER_TOKENS[k % len(_FILLER_TOKENS)])
        k += 1
    return pieces


_REASONING_FILLER = [
    "let ", "me ", "check ", "the ", "input ", "first ", "then ", "consider ",
    "whether ", "we ", "need ", "to ", "handle ", "the ", "edge ", "case ",
    "where ", "the ", "value ", "is ", "empty ", "so ", "I ", "should ",
    "iterate ", "over ", "each ", "element ", "and ", "verify ", "the ",
    "result ", "matches ", "what ", "the ", "test ", "expects ", "before ",
    "returning ",
]


def reasoning_stream(n_tokens: int) -> list[str]:
    """~*n_tokens* thinking-like tokens (real bytes) for the reasoning preamble.

    Deterministic word-like tokens so the gateway pays a realistic per-chunk
    classification cost on the buffered ThinkingContent (see module notes).
    """
    n_tokens = max(0, n_tokens)
    return [_REASONING_FILLER[i % len(_REASONING_FILLER)] for i in range(n_tokens)]


def openai_tool_calls(raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
    encoded = []
    to_sandbox = os.environ.get("MAP_TOOLS_TO_SANDBOX", "") == "1"
    for index, item in enumerate(raw):
        arguments: Any = item.get("arguments") or {}
        name = item.get("name") or "true"
        if to_sandbox:
            name = "exec"
            arguments = _sandbox_exec_args(item.get("mapped_command"))
        elif not isinstance(arguments, str):
            arguments = json.dumps(arguments)
        encoded.append({
            "id": item.get("id") or f"call_{index}",
            "type": "function",
            "function": {"name": name, "arguments": arguments},
        })
    return encoded


def chat_completion(replay: dict[str, Any], model: str) -> dict[str, Any]:
    tool_calls = openai_tool_calls(replay.get("tool_calls") or [])
    message: dict[str, Any] = {"role": "assistant", "content": replay["content"]}
    finish = "stop"
    if tool_calls:
        message["tool_calls"] = tool_calls
        finish = "tool_calls"
    return {
        "id": f"chatcmpl-{uuid.uuid4().hex[:12]}",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model,
        "choices": [{"index": 0, "message": message, "finish_reason": finish}],
        "usage": {
            "prompt_tokens": int(replay.get("input_tokens") or 0),
            "completion_tokens": int(replay.get("output_tokens") or 0),
            "total_tokens": int(replay.get("input_tokens") or 0) + int(replay.get("output_tokens") or 0),
        },
    }


def responses_body(replay: dict[str, Any], model: str) -> dict[str, Any]:
    return {
        "id": f"resp_{uuid.uuid4().hex[:12]}",
        "object": "response",
        "created_at": int(time.time()),
        "model": model,
        "status": "completed",
        "output": [{
            "type": "message",
            "id": f"msg_{uuid.uuid4().hex[:8]}",
            "role": "assistant",
            "content": [{"type": "output_text", "text": replay["content"]}],
        }],
        "usage": {
            "input_tokens": int(replay.get("input_tokens") or 0),
            "output_tokens": int(replay.get("output_tokens") or 0),
        },
    }


# --------------------------------------------------------------------------- #
# HTTP handler.                                                                #
# --------------------------------------------------------------------------- #

def make_handler(index: ReplayIndex, recorder: EdgeRecorder, *, mode: str, timing: str):
    _request_seq = [0]
    _request_seq_lock = threading.Lock()

    def next_request_seq() -> int:
        with _request_seq_lock:
            _request_seq[0] += 1
            return _request_seq[0]

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt: str, *args: Any) -> None:
            return

        # -- io helpers -------------------------------------------------------
        def _read_json(self) -> dict[str, Any]:
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length) if length else b"{}"
            with index.lock:
                index.stats["requests"] += 1
                index.stats["bytes_in"] += len(raw)
            return json.loads(raw.decode("utf-8")) if raw else {}

        def _send(self, code: int, payload: dict[str, Any] | None) -> None:
            body = b"" if payload is None else json.dumps(payload).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if body:
                self.wfile.write(body)

        def _write_sse(self, obj: dict[str, Any]) -> None:
            self.wfile.write(f"data: {json.dumps(obj)}\n\n".encode())
            self.wfile.flush()

        # -- streaming response with edge recording ---------------------------
        def _sse(self, replay: dict[str, Any], model: str, traceparent: str, t_recv_ns: int, t_recv_wall_ns: int, payload: dict[str, Any]) -> None:
            content = replay["content"] or ""
            tool_calls_raw = openai_tool_calls(replay.get("tool_calls") or [])
            if os.environ.get("SINGLE_TURN_MODE", "") == "1":
                tool_calls_raw = []
                if not content:
                    content = "I'll help you with that task."

            output_tokens = int(replay.get("output_tokens") or (0 if replay.get("phantom") else max(1, len(content) // 4)))
            input_tokens = int(replay.get("input_tokens") or 0)
            recorded_ms = float(replay.get("duration_ms") or 0.0)
            response_tail_ms = response_processing_tail_ms(replay, traceparent)

            # Prefill scales TTFT with the UNCACHED context this turn: cold turn-0
            # prefills the whole assembled prompt, warm turns only the delta
            # (prefix-cache aware -- real vLLM reuses the shared prefix KV).
            assembled_tokens = prompt_stats(assembled_prompt_text(payload))["tokens"]

            if timing == "instant" or replay.get("phantom"):
                ttft_s, itl_s, decode_s, profile = 0.0, 0.0, 0.0, ("instant" if timing == "instant" else "phantom")
                new_ctx_tokens = 0
            else:
                new_ctx_tokens = prefill_new_tokens(
                    replay.get("prefill_key") or replay.get("session_id"), assembled_tokens)
                ttft_s, itl_s, decode_s, profile = compute_timing(output_tokens, timing, new_ctx_tokens)
            decode_budget = [decode_s]
            first_visible_emit_ns = 0
            first_tool_emit_ns = 0
            reasoning_phase = False

            def take_decode(want: float) -> None:
                take = min(max(0.0, want), decode_budget[0])
                if take > 0:
                    time.sleep(take)
                    decode_budget[0] -= take

            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "close")
            self.end_headers()
            cid = f"chatcmpl-{uuid.uuid4().hex[:12]}"
            created = int(time.time())

            # --- TTFT gate --------------------------------------------------
            if ttft_s > 0:
                time.sleep(ttft_s)
            t_first_emit_ns = time.perf_counter_ns()

            # Unified realistic streamer: emit ~output_tokens ITL-paced SSE deltas,
            # each carrying REAL payload bytes (a real vLLM token is ~4 chars), so
            # the gateway pays a realistic per-token SSE parse/serialize cost — that
            # cost is bucket D (response processing). The old code streamed only the
            # real args then padded the rest of the token budget with empty ""
            # deltas, so the gateway parsed near-zero bytes per token and D collapsed
            # to ~0. Real content is streamed first (reasoning), then the tool-call
            # header(s), then the arguments as ~4-char fragments; any leftover token
            # budget is filled with word-like filler so no delta is empty.
            first = [True]

            def emit(delta: dict[str, Any]) -> None:
                nonlocal first_visible_emit_ns, first_tool_emit_ns, reasoning_phase
                now_ns = time.perf_counter_ns()
                if first_visible_emit_ns == 0 and not reasoning_phase:
                    first_visible_emit_ns = now_ns
                if first_tool_emit_ns == 0 and delta.get("tool_calls"):
                    first_tool_emit_ns = now_ns
                if not first[0]:
                    take_decode(itl_s)
                delta = {"role": "assistant", **delta} if first[0] else delta
                first[0] = False
                self._write_sse({
                    "id": cid, "object": "chat.completion.chunk", "created": created, "model": model,
                    "choices": [{"index": 0, "delta": delta, "finish_reason": None}],
                })

            def emit_reasoning(n: int) -> int:
                """Stream an n-token reasoning preamble; return deltas emitted.

                OpenClaw buffers/classifies this ThinkingContent before releasing
                the first client-visible token -- that delay reproduces the
                baseline's 895ms. Carved from output_tokens, so decode is unchanged.
                """
                if n <= 0:
                    return 0
                toks = reasoning_stream(n)
                nonlocal reasoning_phase
                reasoning_phase = True
                if REASONING_MODE == "reasoning_content":
                    for t in toks:
                        emit({"reasoning_content": t})
                else:  # think_tags: wrap the preamble in <think>...</think> content
                    toks[0] = "<think>" + toks[0]
                    toks[-1] = toks[-1] + "</think>"
                    for t in toks:
                        emit({"content": t})
                reasoning_phase = False
                return len(toks)

            reasoning_n = resolve_reasoning_tokens(output_tokens, replay.get("reasoning_tokens"))

            if tool_calls_raw:
                headers = len(tool_calls_raw)
                # Real args -> ~4-char fragments per call (valid JSON accumulation).
                arg_frags = []
                for tc in tool_calls_raw:
                    args = tc["function"]["arguments"] or ""
                    n = max(1, -(-len(args) // 4))  # ceil(len/4)
                    arg_frags.append(_even_chunks(args, n) or [args])
                total_arg_frags = sum(len(f) for f in arg_frags)
                # Reasoning preamble first (buffered/classified by OpenClaw), then
                # visible content fills the rest so total deltas track output_tokens.
                budget = max(0, output_tokens - headers - total_arg_frags)
                r_emitted = emit_reasoning(min(reasoning_n, max(0, budget - 1)))
                content_budget = max(0, budget - r_emitted)
                if content_budget:
                    for piece in realistic_content_stream(content, content_budget):
                        emit({"content": piece})
                for idx, tc in enumerate(tool_calls_raw):
                    emit({"tool_calls": [{"index": idx, "id": tc["id"], "type": "function",
                                          "function": {"name": tc["function"]["name"], "arguments": ""}}]})
                    for frag in arg_frags[idx]:
                        emit({"tool_calls": [{"index": idx, "function": {"arguments": frag}}]})
                finish_reason = "tool_calls"
            else:
                r_emitted = emit_reasoning(reasoning_n)
                for piece in realistic_content_stream(content or "ok", max(1, output_tokens - r_emitted)):
                    emit({"content": piece})
                finish_reason = "stop"

            # Drain any remaining decode budget so decode duration matches the profile.
            take_decode(decode_budget[0])

            self._write_sse({
                "id": cid, "object": "chat.completion.chunk", "created": created, "model": model,
                "choices": [],
                "usage": {"prompt_tokens": input_tokens, "completion_tokens": output_tokens,
                          "total_tokens": input_tokens + output_tokens},
            })
            self._write_sse({
                "id": cid, "object": "chat.completion.chunk", "created": created, "model": model,
                "choices": [{"index": 0, "delta": {}, "finish_reason": finish_reason}],
            })
            t_end_emit_ns = time.perf_counter_ns()

            self._record_and_span(
                replay, traceparent, payload, mode="stream",
                t_recv_ns=t_recv_ns, t_recv_wall_ns=t_recv_wall_ns,
                t_first_emit_ns=t_first_emit_ns, t_end_emit_ns=t_end_emit_ns,
                ttft_s=ttft_s, itl_s=itl_s, decode_s=decode_s, profile=profile,
                output_tokens=output_tokens, input_tokens=input_tokens,
                recorded_ms=recorded_ms, finish_reason=finish_reason,
                n_tool_calls=len(tool_calls_raw), reasoning_tokens=reasoning_n,
                prefill_new_tokens=new_ctx_tokens, response_tail_ms=response_tail_ms,
                first_visible_emit_ns=first_visible_emit_ns or t_first_emit_ns,
                first_tool_emit_ns=first_tool_emit_ns or t_end_emit_ns,
            )
            if response_tail_ms > 0:
                time.sleep(response_tail_ms / 1000.0)
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()

        # -- edge record + OTLP spans ----------------------------------------
        def _record_and_span(self, replay, traceparent, payload, *, mode, t_recv_ns, t_recv_wall_ns,
                             t_first_emit_ns, t_end_emit_ns, ttft_s, itl_s, decode_s, profile,
                             output_tokens, input_tokens, recorded_ms, finish_reason, n_tool_calls,
                              reasoning_tokens=0, prefill_new_tokens=0, response_tail_ms=0.0,
                              first_visible_emit_ns=0, first_tool_emit_ns=0) -> None:
            tp = parse_traceparent(traceparent)
            trace_id, parent_span_id = (tp if tp else ("", ""))
            stats = prompt_stats(assembled_prompt_text(payload))
            record = {
                "schema": EDGE_SCHEMA,
                # correlation keys (redundant / self-validating)
                "trace_id": trace_id,
                "parent_span_id": parent_span_id,
                "session_id": replay.get("session_id"),
                "turn_seq": replay.get("turn_seq"),
                "request_seq": next_request_seq(),
                # single-clock edge timestamps (monotonic ns) + wall anchor
                "t_recv_ns": t_recv_ns,
                "t_first_emit_ns": t_first_emit_ns,
                "t_first_visible_emit_ns": first_visible_emit_ns or t_first_emit_ns,
                "t_first_tool_emit_ns": first_tool_emit_ns or t_end_emit_ns,
                "t_end_emit_ns": t_end_emit_ns,
                "t_recv_wall_ns": t_recv_wall_ns,
                "t_first_emit_wall_ns": t_recv_wall_ns + (t_first_emit_ns - t_recv_ns),
                "t_first_visible_emit_wall_ns": t_recv_wall_ns + ((first_visible_emit_ns or t_first_emit_ns) - t_recv_ns),
                "t_first_tool_emit_wall_ns": t_recv_wall_ns + ((first_tool_emit_ns or t_end_emit_ns) - t_recv_ns),
                "t_end_emit_wall_ns": t_recv_wall_ns + (t_end_emit_ns - t_recv_ns),
                # measured durations (derived from the single clock)
                "ttft_measured_ms": round((t_first_emit_ns - t_recv_ns) / 1e6, 3),
                "decode_measured_ms": round((t_end_emit_ns - t_first_emit_ns) / 1e6, 3),
                # controlled inputs
                "ttft_set_ms": round(ttft_s * 1000, 3),
                "itl_set_ms": round(itl_s * 1000, 3),
                "decode_set_ms": round(decode_s * 1000, 3),
                # Reasoning preamble buffered by the gateway BEFORE the first visible
                # token (the baseline's "Response Processing" cost). Carved from the
                # decode budget above; derive attributes it to bucket D, not E.
                "reasoning_tokens": int(reasoning_tokens or 0),
                "reasoning_set_ms": round(max(0, int(reasoning_tokens or 0)) * itl_s * 1000, 3),
                "response_processing_tail_ms": round(response_tail_ms, 3),
                # Prefill: uncached tokens prefilled this turn (cold turn-0 vs warm
                # delta) and the TTFT they added -- the cold/warm split lives in C.
                "prefill_new_tokens": int(prefill_new_tokens or 0),
                "prefill_ms": round(max(0, int(prefill_new_tokens or 0)) * PREFILL_PER_TOKEN_MS, 3)
                if _prefill_enabled() else 0.0,
                "profile": profile,
                # payload facts
                "output_tokens": output_tokens,
                "input_tokens": input_tokens,
                "assembled_chars": stats["chars"],
                "assembled_tokens": stats["tokens"],
                "recorded_duration_ms": recorded_ms,  # E2E wall-clock reference ONLY
                "finish_reason": finish_reason,
                "n_tool_calls": n_tool_calls,
                "mode": mode,
                "phantom": bool(replay.get("phantom")),
                "miss": bool(replay.get("miss")),
                "exhausted": bool(replay.get("exhausted")),
                "warmup": bool(replay.get("warmup")),
            }
            recorder.emit(record)

            if trace_id and parent_span_id:
                _export_span(trace_id, parent_span_id, "mock-llm.turn", t_recv_ns, t_end_emit_ns, {
                    "mock.session_id": str(replay.get("session_id")),
                    "mock.turn_seq": int(replay.get("turn_seq") or 0),
                    "mock.output_tokens": output_tokens,
                    "mock.input_tokens": input_tokens,
                    "mock.ttft_set_ms": record["ttft_set_ms"],
                    "mock.itl_set_ms": record["itl_set_ms"],
                    "mock.decode_set_ms": record["decode_set_ms"],
                    "mock.ttft_measured_ms": record["ttft_measured_ms"],
                    "mock.decode_measured_ms": record["decode_measured_ms"],
                    "mock.profile": profile,
                    "mock.assembled_tokens": stats["tokens"],
                    "mock.has_tool_calls": bool(n_tool_calls),
                    "mock.phantom": bool(replay.get("phantom")),
                })
                _export_span(trace_id, parent_span_id, "mock-llm.first_content",
                             t_first_emit_ns, t_first_emit_ns + 1_000_000,
                             {"mock.ttft_measured_ms": record["ttft_measured_ms"]})

        # -- routing ----------------------------------------------------------
        def do_GET(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            if path in {"/health", "/v1/health"}:
                self._send(200, {"status": "ok", "mode": mode, "timing": timing, "edges": recorder.count})
                return
            if path == "/v1/models":
                self._send(200, {"object": "list", "data": [{"id": "replay", "object": "model"}]})
                return
            if path == "/admin/stats":
                self._send(200, dict(index.stats) | {"sessions": len(index.sessions), "edges": recorder.count})
                return
            if path == "/v1/tool-result":
                qs = parse_qs(urlparse(self.path).query)
                result = index.tool_result(
                    (qs.get("session_id") or [""])[0],
                    int((qs.get("turn") or ["0"])[0]),
                    int((qs.get("call") or ["0"])[0]),
                )
                body = result.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self._send(404, {"error": "not found"})

        def do_POST(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            if path == "/admin/reset":
                index.reset()
                self._send(200, {"ok": True})
                return

            t_recv_ns = time.perf_counter_ns()
            t_recv_wall_ns = time.time_ns()
            traceparent = self.headers.get("traceparent", "")
            payload = self._read_json()
            _tp = parse_traceparent(traceparent)
            replay = index.resolve(payload, mode=mode, trace_id=(_tp[0] if _tp else None))
            model = str(payload.get("model") or "replay")

            if path in {"/v1/chat/completions", "/chat/completions"}:
                if payload.get("stream"):
                    self._sse(replay, model, traceparent, t_recv_ns, t_recv_wall_ns, payload)
                    return
                body = chat_completion(replay, model)
                self._send(200, body)
                t_end_ns = time.perf_counter_ns()
                self._record_and_span(
                    replay, traceparent, payload, mode="unary",
                    t_recv_ns=t_recv_ns, t_recv_wall_ns=t_recv_wall_ns,
                    t_first_emit_ns=t_end_ns, t_end_emit_ns=t_end_ns,
                    ttft_s=0.0, itl_s=0.0, decode_s=0.0, profile="unary",
                    output_tokens=int(replay.get("output_tokens") or 0),
                    input_tokens=int(replay.get("input_tokens") or 0),
                    recorded_ms=float(replay.get("duration_ms") or 0.0),
                    finish_reason=str(replay.get("finish_reason") or "stop"),
                    n_tool_calls=len(replay.get("tool_calls") or []),
                )
                return
            if path in {"/v1/responses", "/responses"}:
                self._send(200, responses_body(replay, model))
                t_end_ns = time.perf_counter_ns()
                self._record_and_span(
                    replay, traceparent, payload, mode="unary",
                    t_recv_ns=t_recv_ns, t_recv_wall_ns=t_recv_wall_ns,
                    t_first_emit_ns=t_end_ns, t_end_emit_ns=t_end_ns,
                    ttft_s=0.0, itl_s=0.0, decode_s=0.0, profile="unary",
                    output_tokens=int(replay.get("output_tokens") or 0),
                    input_tokens=int(replay.get("input_tokens") or 0),
                    recorded_ms=float(replay.get("duration_ms") or 0.0),
                    finish_reason=str(replay.get("finish_reason") or "stop"),
                    n_tool_calls=len(replay.get("tool_calls") or []),
                )
                return
            self._send(404, {"error": f"no handler for {path}"})

    return Handler


def serve(host: str, port: int, corpus_path: str, *, mode: str = "text", timing: str = "instant",
          edges_path: str | None = None) -> None:
    index = ReplayIndex(load_corpus(corpus_path))
    recorder = EdgeRecorder(edges_path or os.environ.get("MOCK_EDGES_PATH") or None)
    handler = make_handler(index, recorder, mode=mode, timing=timing)
    server = ThreadingHTTPServer((host, port), handler)
    print(
        f"mock-llm listening on {host}:{port} sessions={len(index.sessions)} "
        f"mode={mode} timing={timing} edges_path={edges_path or os.environ.get('MOCK_EDGES_PATH') or '(stdout-only)'}",
        flush=True,
    )
    server.serve_forever()


if __name__ == "__main__":
    serve(
        os.environ.get("MOCK_LLM_HOST", "0.0.0.0"),
        int(os.environ.get("MOCK_LLM_PORT", "8080")),
        os.environ.get("REPLAY_CORPUS", "/data/replay.json"),
        mode=os.environ.get("REPLAY_MODE", "text"),
        timing=os.environ.get("REPLAY_TIMING", "instant"),
        edges_path=os.environ.get("MOCK_EDGES_PATH") or None,
    )
