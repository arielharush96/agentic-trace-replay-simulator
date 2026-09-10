import json
import threading
import time
import urllib.request
from pathlib import Path

from trace_replay_sim import mock_llm
from trace_replay_sim.ingest import ingest
from trace_replay_sim.mock_llm import (
    ReplayIndex,
    compute_timing,
    is_phantom_turn,
    parse_traceparent,
    resolve_profile,
    serve,
)
from trace_replay_sim.driver import drive


FIXTURE = Path(__file__).parent / "fixtures" / "sample_exgentic_row.json"


def _wait_ready(port: int) -> None:
    for _ in range(100):
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=0.2)
            return
        except Exception:
            time.sleep(0.05)
    raise AssertionError("mock llm did not start")


def test_mock_llm_and_driver_roundtrip(tmp_path: Path):
    corpus = tmp_path / "replay.json"
    ingest(out_path=corpus, local_path=FIXTURE, limit=1)

    thread = threading.Thread(
        target=serve,
        kwargs={"host": "127.0.0.1", "port": 18080, "corpus_path": str(corpus), "mode": "text", "timing": "instant"},
        daemon=True,
    )
    thread.start()
    for _ in range(50):
        try:
            urllib.request.urlopen("http://127.0.0.1:18080/health", timeout=0.2)
            break
        except Exception:
            pass
    else:
        raise AssertionError("mock llm did not start")

    raw = json.dumps({
        "model": "replay",
        "messages": [{"role": "user", "content": "<!--replay-session:fixture-swe-001-->\nFix the failing unit test in src/app.py"}],
    }).encode()
    req = urllib.request.Request(
        "http://127.0.0.1:18080/v1/chat/completions",
        data=raw,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        body = json.loads(resp.read())
    assert "Patched src/app.py" in body["choices"][0]["message"]["content"]

    out = tmp_path / "run"
    summary = drive(
        corpus=corpus,
        out_dir=out,
        layer="mock-direct",
        url="http://127.0.0.1:18080",
        model="replay",
        token="",
        concurrency=1,
        limit=1,
        timeout=10,
        stream=False,
    )
    assert summary["ok"] == 1
    assert summary["errors"] == 0
    assert summary["e2e_p50_ms"] is not None


# --------------------------------------------------------------------------- #
# Pure timing / correlation unit tests (no server).                           #
# --------------------------------------------------------------------------- #

def test_compute_timing_uses_profile_not_duration():
    # decode = (output_tokens - 1) * ITL, from the profile only.
    ttft_s, itl_s, decode_s, name = compute_timing(5, "humaneval-vllm-c1")
    assert name == "humaneval-vllm-c1"
    assert round(ttft_s * 1000, 1) == 51.0
    assert round(itl_s * 1000, 1) == 16.8
    assert round(decode_s * 1000, 1) == round(4 * 16.8, 1)
    # single-token turn has zero decode; first token accounted by TTFT.
    _, _, decode_one, _ = compute_timing(1, "humaneval-vllm-c1")
    assert decode_one == 0.0


def test_prefill_cold_turn0_warm_after(monkeypatch):
    """Prefix-cache aware prefill: cold turn-0 prefills the whole context, warm
    turns only the per-turn delta -- reproducing the baseline 51ms->804ms jump on
    turn 0 and collapsing back toward the profile TTFT afterwards.
    """
    monkeypatch.delenv("MOCK_PREFILL_SCALING", raising=False)  # default ON
    mock_llm._SESSION_PREFIX_TOKENS.clear()

    # No context info -> base profile TTFT unchanged.
    ttft_base, _, _, _ = compute_timing(5, "humaneval-vllm-c1")
    assert round(ttft_base * 1000, 1) == 51.0

    # Turn 0 of a fresh session: whole ~7862-token context prefilled cold.
    new0 = mock_llm.prefill_new_tokens("sess-A", 7862)
    assert new0 == 7862 - mock_llm.PREFILL_REF_TOKENS
    ttft0, _, _, _ = compute_timing(5, "humaneval-vllm-c1", new0)
    assert abs(ttft0 * 1000 - 804.5) < 6.0  # baseline cold TTFT

    # Turn 1: context grew by ~120 tokens -> only the delta is prefilled (warm).
    new1 = mock_llm.prefill_new_tokens("sess-A", 7982)
    assert new1 == 120
    ttft1, _, _, _ = compute_timing(5, "humaneval-vllm-c1", new1)
    assert round(ttft1 * 1000, 1) == round(51.0 + 120 * mock_llm.PREFILL_PER_TOKEN_MS, 1)
    assert ttft1 < ttft0  # warm << cold

    # A different session starts cold again (independent prefix cache).
    assert mock_llm.prefill_new_tokens("sess-B", 7862) == 7862 - mock_llm.PREFILL_REF_TOKENS

    # Opt-out restores the flat profile TTFT regardless of new tokens.
    monkeypatch.setenv("MOCK_PREFILL_SCALING", "0")
    ttft_off, _, _, _ = compute_timing(5, "humaneval-vllm-c1", 7745)
    assert round(ttft_off * 1000, 1) == 51.0


def test_resolve_reasoning_tokens_carves_from_output(monkeypatch):
    """Reasoning tokens come from the turn field / env, clamped to output-1."""
    monkeypatch.setattr(mock_llm, "REASONING_TOKENS_ENV", None)
    monkeypatch.setattr(mock_llm, "REASONING_FRACTION", 0.0)
    assert mock_llm.resolve_reasoning_tokens(100) == 0            # default OFF
    assert mock_llm.resolve_reasoning_tokens(100, 40) == 40       # per-turn field
    assert mock_llm.resolve_reasoning_tokens(10, 999) == 9        # clamp to output-1
    assert mock_llm.resolve_reasoning_tokens(1, 5) == 0           # keep >=1 visible
    monkeypatch.setattr(mock_llm, "REASONING_FRACTION", 0.2)
    assert mock_llm.resolve_reasoning_tokens(100) == 20           # fraction path
    monkeypatch.setattr(mock_llm, "REASONING_TOKENS_ENV", "30")
    assert mock_llm.resolve_reasoning_tokens(100) == 30           # fixed-count env wins


def _stream_deltas(port: int, prompt: str, trace_id: str = "d" * 32) -> list[dict]:
    raw = json.dumps({"model": "replay", "stream": True,
                      "messages": [{"role": "user", "content": prompt}]}).encode()
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/v1/chat/completions", data=raw,
        headers={"Content-Type": "application/json",
                 "traceparent": f"00-{trace_id}-{'0'*16}-01"}, method="POST")
    deltas = []
    with urllib.request.urlopen(req, timeout=10) as resp:
        for line in resp.read().decode().splitlines():
            if line.startswith("data: ") and line != "data: [DONE]":
                obj = json.loads(line[6:])
                ch = obj.get("choices") or []
                if ch and ch[0].get("delta"):
                    deltas.append(ch[0]["delta"])
    return deltas


def test_reasoning_preamble_precedes_visible_content(tmp_path: Path, monkeypatch):
    """A reasoning turn streams <think>..</think> BEFORE any visible content,
    carved from output_tokens (total decode budget unchanged)."""
    monkeypatch.setattr(mock_llm, "OTLP_ENDPOINT", "")
    monkeypatch.setattr(mock_llm, "REASONING_MODE", "think_tags")
    monkeypatch.setattr(mock_llm, "REASONING_TOKENS_ENV", None)
    monkeypatch.setattr(mock_llm, "REASONING_FRACTION", 0.0)
    sid = "reason-001"
    corpus = tmp_path / "reason.json"
    corpus.write_text(json.dumps({"sessions": [{
        "session_id": sid, "turns": [
            {"index": 0, "output_tokens": 20, "input_tokens": 10,
             "content": "final answer here", "reasoning_tokens": 8, "tool_calls": []},
        ]}]}), encoding="utf-8")
    port = 18085
    threading.Thread(target=serve, kwargs={
        "host": "127.0.0.1", "port": port, "corpus_path": str(corpus),
        "mode": "turns", "timing": "instant"}, daemon=True).start()
    _wait_ready(port)

    deltas = _stream_deltas(port, f"<!--replay-session:{sid}-->\ngo")
    joined = "".join(d.get("content", "") for d in deltas)
    assert "<think>" in joined and "</think>" in joined
    # think block closes before the visible content begins
    assert joined.index("</think>") < joined.index("final") if "final" in joined else True
    assert len(deltas) == 20  # reasoning carved from output_tokens, not added



    name, ttft, itl = resolve_profile("does-not-exist")
    assert name == "rhaiis"  # unknown -> default, never duration-fitting
    assert (ttft, itl) == (130.0, 7.0)


def test_phantom_turn_detection():
    assert is_phantom_turn({"output_tokens": 0, "input_tokens": 0, "content": "", "tool_calls": []})
    assert not is_phantom_turn({"output_tokens": 5, "content": "hi", "tool_calls": []})
    assert not is_phantom_turn({"output_tokens": 0, "content": "", "tool_calls": [{"id": "x"}]})


def test_parse_traceparent():
    tid, pid = parse_traceparent("00-" + "a" * 32 + "-" + "b" * 16 + "-01")
    assert tid == "a" * 32 and pid == "b" * 16
    assert parse_traceparent(None) is None
    assert parse_traceparent("garbage") is None


def _one_session_index():
    session = {
        "session_id": "S1",
        "turns": [
            {"index": i, "output_tokens": 4, "input_tokens": 10,
             "content": f"turn {i}", "tool_calls": []}
            for i in range(3)
        ],
    }
    return ReplayIndex({"sessions": [session], "by_id": {"S1": session}})


def test_turn_seq_resets_per_trace_run():
    """turn_seq must restart at 0 for each agentic run (trace_id), not accumulate.

    Guards the cluster scenario where mock-direct/openclaw/shell and repeated reps
    all hit ONE shared mock pod; without per-trace keying rep 2 would start
    'exhausted' and layers would steal each other's turns.
    """
    idx = _one_session_index()
    payload = {"messages": [{"role": "user", "content": "x"}]}

    run_a = "a" * 32
    seqs_a = [idx.resolve(payload, "turns", trace_id=run_a)["turn_seq"] for _ in range(3)]
    assert seqs_a == [0, 1, 2]

    # A brand-new run (new trace) on the SAME single session restarts at 0.
    run_b = "b" * 32
    r0 = idx.resolve(payload, "turns", trace_id=run_b)
    assert r0["turn_seq"] == 0 and not r0["exhausted"]

    # mock-direct-style: each request its own trace -> always turn 0 (net floor).
    for _ in range(3):
        assert idx.resolve(payload, "turns", trace_id=("c" * 31 + str(_)))["turn_seq"] == 0

    # No traceparent -> fall back to session_id counter (sequential advance).
    idx2 = _one_session_index()
    seqs_none = [idx2.resolve(payload, "turns", trace_id=None)["turn_seq"] for _ in range(2)]
    assert seqs_none == [0, 1]


# --------------------------------------------------------------------------- #
# Per-turn edge recorder + skew-free G[i] + correlation (server integration).  #
# --------------------------------------------------------------------------- #

def _synthetic_corpus(path: Path) -> str:
    sid = "syn-agentic-001"
    corpus = {
        "sessions": [{
            "session_id": sid,
            "user_prompt": "do the task",
            "gateway_text": "done thanks bye",
            "turns": [
                {"index": 0, "output_tokens": 5, "input_tokens": 100,
                 "content": "hello world here now", "duration_ms": 30000.0,
                 "tool_calls": [{"id": "call_0", "name": "bash",
                                 "arguments": {"command": "ls"}, "recorded_result": "ok"}]},
                {"index": 1, "output_tokens": 3, "input_tokens": 120,
                 "content": "done thanks bye", "duration_ms": 5000.0, "tool_calls": []},
                {"index": 2, "output_tokens": 0, "input_tokens": 0,
                 "content": "", "duration_ms": 0.0, "tool_calls": []},  # phantom
            ],
        }]
    }
    path.write_text(json.dumps(corpus), encoding="utf-8")
    return sid


def test_per_turn_edges_timing_and_correlation(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(mock_llm, "OTLP_ENDPOINT", "")  # no external export in tests
    corpus = tmp_path / "syn.json"
    sid = _synthetic_corpus(corpus)
    edges = tmp_path / "edges.jsonl"
    port = 18082

    thread = threading.Thread(
        target=serve,
        kwargs={"host": "127.0.0.1", "port": port, "corpus_path": str(corpus),
                "mode": "turns", "timing": "humaneval-vllm-c1", "edges_path": str(edges)},
        daemon=True,
    )
    thread.start()
    _wait_ready(port)

    trace_id = "c" * 32
    prompt = f"<!--replay-session:{sid}-->\ndo the task"
    # Simulate the agentic loop: 3 sequential model calls, same trace, distinct parent spans.
    for turn in range(3):
        parent = f"{turn:016x}"
        raw = json.dumps({"model": "replay", "stream": True,
                          "messages": [{"role": "user", "content": prompt}]}).encode()
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/v1/chat/completions", data=raw,
            headers={"Content-Type": "application/json",
                     "traceparent": f"00-{trace_id}-{parent}-01"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()

    recs = [json.loads(l) for l in edges.read_text().splitlines() if l.strip()]
    assert len(recs) == 3
    recs.sort(key=lambda r: r["turn_seq"])

    # Correlation: monotonic turn_seq, shared trace_id, per-turn parent span echoed.
    assert [r["turn_seq"] for r in recs] == [0, 1, 2]
    assert all(r["trace_id"] == trace_id for r in recs)
    assert [r["parent_span_id"] for r in recs] == [f"{i:016x}" for i in range(3)]
    assert all(r["session_id"] == sid for r in recs)

    # Turn 0 is a tool call; turn 1 is text; turn 2 is the phantom guard.
    assert recs[0]["n_tool_calls"] >= 1 and recs[0]["finish_reason"] == "tool_calls"
    assert recs[1]["n_tool_calls"] == 0 and recs[1]["finish_reason"] == "stop"
    assert recs[2]["phantom"] is True
    assert recs[2]["decode_set_ms"] == 0.0

    # Timing derives from the profile, not duration_ms (30000ms recorded).
    assert recs[0]["ttft_set_ms"] == 51.0
    assert recs[0]["decode_set_ms"] == round(4 * 16.8, 3)
    assert recs[0]["ttft_measured_ms"] >= 45.0            # sleep only adds time
    assert recs[0]["decode_measured_ms"] >= 60.0          # ~4*16.8
    assert recs[0]["recorded_duration_ms"] == 30000.0     # kept as reference only

    # Skew-free inter-turn gap G[i] is computable and non-negative (single clock).
    g0 = recs[1]["t_recv_ns"] - recs[0]["t_end_emit_ns"]
    g1 = recs[2]["t_recv_ns"] - recs[1]["t_end_emit_ns"]
    assert g0 >= 0 and g1 >= 0
