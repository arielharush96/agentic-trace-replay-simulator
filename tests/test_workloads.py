import json

from trace_replay_sim.workloads import classify_benchmark, select_tool_rich_sessions, split_corpus


def test_benchmark_mapping():
    assert classify_benchmark("swebench") == ("software_engineering_qa", "swebench")
    assert classify_benchmark("appworld") == ("personal_assistant", "appworld")
    assert classify_benchmark("browsecompplus") == ("research_deep_search", "browsecompplus")
    assert classify_benchmark("tau2_telecom") == ("customer_support_ops", "tau2_telecom")


def test_split_preserves_sessions_and_writes_manifest(tmp_path):
    corpus = tmp_path / "corpus.json"
    corpus.write_text(json.dumps({
        "dataset": "test",
        "sessions": [
            {"session_id": "swe-1", "benchmark": "swebench", "turns": [{"index": 0}, {"index": 1}]},
            {"session_id": "app-1", "benchmark": "appworld", "turns": [{"index": 0}]},
        ],
    }))
    manifest = split_corpus(corpus, tmp_path / "split")
    assert manifest["workloads"]["software_engineering_qa"]["sessions"] == 1
    assert manifest["workloads"]["personal_assistant"]["turns"] == 1
    assert json.loads((tmp_path / "split/software_engineering_qa.json").read_text())["session_count"] == 1


def test_tool_rich_selection_balances_customer_support_profiles():
    sessions = []
    for profile in ("tau2_airline", "tau2_retail", "tau2_telecom"):
        for index in range(10):
            sessions.append({
                "session_id": f"{profile}-{index}",
                "benchmark": profile,
                "workload_profile": profile,
                "turns": [{"tool_calls": [{"name": "tool"}]}],
            })
    selected = select_tool_rich_sessions(sessions, 8)
    counts = {profile: sum(row["benchmark"] == profile for row in selected) for profile in ("tau2_airline", "tau2_retail", "tau2_telecom")}
    assert len(selected) == 8
    assert max(counts.values()) - min(counts.values()) <= 1
