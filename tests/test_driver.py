import json

from trace_replay_sim import driver
from trace_replay_sim.driver import RequestResult, drive_client_turns


def _result(**overrides):
    base = {
        "session_id": "s1",
        "layer": "shell",
        "ok": True,
        "status": 200,
        "error": "",
        "e2e_ms": 1.0,
        "ttft_ms": 0.5,
        "harness": "openclaw",
        "t_send_ns": 1,
        "t_first_ns": 2,
        "t_end_ns": 3,
        "t_send_wall_ns": 10,
        "trace_id": "trace",
    }
    base.update(overrides)
    return RequestResult(**base)


def test_drive_client_turns_sends_one_request_per_recorded_user_message(tmp_path, monkeypatch):
    corpus = tmp_path / "replay.json"
    corpus.write_text(json.dumps({
        "sessions": [{
            "session_id": "s1",
            "user_prompt": "open the ticket",
            "turns": [{"index": 0}, {"index": 1}, {"index": 2}],
            "client_events": [
                {"turn_index": 0, "content": "open the ticket"},
                {"turn_index": 2, "content": "close it"},
            ],
        }],
    }), encoding="utf-8")
    prompts: list[str] = []

    def fake_run_one(session, **kwargs):
        prompts.append(str(kwargs["prompt_override"]))
        return _result(response_id=f"resp-{len(prompts)}")

    monkeypatch.setattr(driver, "run_one", fake_run_one)

    summary = drive_client_turns(
        corpus=corpus,
        out_dir=tmp_path / "out",
        layer="shell",
        url="http://example.invalid",
        model="openclaw/perf_agent",
        token="",
        timeout=1.0,
        stream=False,
        export_spans=False,
    )

    assert len(prompts) == 2
    assert "<!--replay-client-turn:0-->" in prompts[0]
    assert "open the ticket" in prompts[0]
    assert "<!--replay-client-turn:2-->" in prompts[1]
    assert "close it" in prompts[1]
    assert summary["mode"] == "client-turns"
    assert summary["client_events"] == 2
    assert summary["turns"] == 2
    written = json.loads((tmp_path / "out" / "summary.json").read_text(encoding="utf-8"))
    assert written["client_events"] == 2
    assert len((tmp_path / "out" / "requests.jsonl").read_text(encoding="utf-8").splitlines()) == 2
