import json
from collections import Counter
from pathlib import Path

import pytest

import sys

sys.path.insert(0, str(Path(__file__).parents[1] / "scripts"))

from run_experiment import (
    _preflight_kueue_admission,
    build_queue,
    ensure_shared_workspace,
    pack_mock_corpus_sessions,
    prepare_workload_corpus,
)
from trace_replay_sim.backend_registry import load_backends, parse_workload_plan
from trace_replay_sim.swe_manifest import SweTask
from trace_replay_sim.workloads import WORKLOADS


def _synthetic_100_session_corpus(path: Path) -> tuple[Path, dict[str, SweTask]]:
    sessions = []
    swe_tasks: dict[str, SweTask] = {}
    benchmarks = (
        ("software_engineering_qa", "swebench"),
        ("personal_assistant", "appworld"),
        ("research_deep_search", "browsecompplus"),
        ("customer_support_ops", "tau2_airline"),
    )
    for workload, benchmark in benchmarks:
        for index in range(25):
            session_id = f"{workload}-{index:02d}"
            tool_name = {
                "software_engineering_qa": "Read",
                "personal_assistant": "mcp__environment__search_notes",
                "research_deep_search": "search",
                "customer_support_ops": "lookup_policy",
            }[workload]
            session = {
                "session_id": session_id,
                "benchmark": benchmark,
                "turns": [
                    {"index": 0, "tool_calls": [{"name": tool_name, "arguments": {}}]},
                    {"index": 1, "tool_calls": [{"name": tool_name, "arguments": {}}]},
                ],
            }
            sessions.append(session)
            if workload == "software_engineering_qa":
                swe_tasks[session_id] = SweTask(
                    session_id=session_id,
                    instance_id=f"owner__repo-{index}",
                    repo="owner/repo",
                    repo_url="https://github.com/owner/repo.git",
                    base_commit="0123456789abcdef0123456789abcdef01234567",
                    environment_setup_commit="fedcba9876543210fedcba9876543210fedcba98",
                    image=f"swebench/task:{index}",
                    version="test",
                )
    path.write_text(json.dumps({"dataset": "readiness-test", "sessions": sessions}), encoding="utf-8")
    return path, swe_tasks


def test_single_agent_single_workload_plan_builds_one_trace(tmp_path):
    corpus, _ = _synthetic_100_session_corpus(tmp_path / "corpus.json")
    allocation = parse_workload_plan("software_engineering_qa=1", 1)
    assert allocation == {"software_engineering_qa": 1}

    queue = build_queue(corpus, tmp_path / "single", allocation)
    assert len(queue) == 1
    assert queue[0]["workload"] == "software_engineering_qa"


def test_100_agent_four_workload_plan_is_complete_without_cluster(tmp_path):
    corpus, swe_tasks = _synthetic_100_session_corpus(tmp_path / "corpus.json")
    allocation = parse_workload_plan(None, 100, default_per_workload=25)
    assert allocation == {workload: 25 for workload in WORKLOADS}

    queue = build_queue(corpus, tmp_path / "initial", allocation)
    assert len(queue) == 100
    assert Counter(item["workload"] for item in queue) == Counter(allocation)

    mapped = prepare_workload_corpus(
        corpus,
        tmp_path / "mapped.json",
        load_backends(None),
        swe_tasks,
        selected_session_ids={item["session"]["session_id"] for item in queue},
    )
    mapped_queue = build_queue(mapped, tmp_path / "mapped-queue", allocation)
    assert len(mapped_queue) == 100
    assert Counter(item["workload"] for item in mapped_queue) == Counter(allocation)
    assert all(item["session"].get("swe_task") for item in mapped_queue if item["workload"] == "software_engineering_qa")
    assert all(
        tool.get("mapped_command")
        for item in mapped_queue
        for turn in item["session"].get("turns", [])
        for tool in turn.get("tool_calls", [])
    )


def test_mock_corpus_packing_preserves_sessions_and_bounds_objects():
    sessions = [
        {"session_id": f"session-{index}", "payload": "x" * 80}
        for index in range(20)
    ]
    parts = pack_mock_corpus_sessions(sessions, max_bytes=500)
    assert len(parts) > 1
    assert [session["session_id"] for part in parts for session in part] == [
        session["session_id"] for session in sessions
    ]
    assert all(
        len(json.dumps({"sessions": part}, separators=(",", ":")).encode("utf-8")) <= 500
        for part in parts
    )


class _FakeCluster:
    def __init__(self, payloads):
        self.payloads = payloads

    def json(self, args):
        if args[args.index("get") + 1] == "service":
            return self.payloads["service"]
        if args[args.index("get") + 1] == "endpoints":
            return self.payloads["endpoints"]
        if args[args.index("get") + 1] == "endpointslice":
            return self.payloads.get("endpointslice", {"items": []})
        if args[args.index("get") + 1] == "pods":
            return self.payloads["pods"]
        raise AssertionError(args)


def test_kueue_admission_preflight_requires_ready_endpoint():
    healthy = _FakeCluster(
        {
            "service": {"spec": {"clusterIP": "172.30.0.10"}},
            "endpoints": {"subsets": [{"addresses": [{"ip": "10.0.0.1"}]}]},
            "pods": {
                "items": [
                    {
                        "metadata": {"name": "kueue-controller"},
                        "status": {"containerStatuses": [{"ready": True}]},
                    }
                ]
            },
        }
    )
    _preflight_kueue_admission(healthy)

    broken = _FakeCluster(
        {
            "service": {"spec": {"clusterIP": "172.30.0.10"}},
            "endpoints": {"subsets": []},
            "pods": {"items": []},
        }
    )
    with pytest.raises(RuntimeError, match="no ready endpoints"):
        _preflight_kueue_admission(broken)


class _FakeWorkspaceCluster:
    node = "node-a"

    def __init__(self):
        self.applied = []

    def run(self, args, *, input_text=None, check=True):
        return ""

    def json(self, args):
        raise AssertionError(args)

    def apply_json_in(self, namespace, value):
        self.applied.append(value)

    def delete_owned(self, namespace, kind, name):
        return None


def _workspace_pvc(storage_class):
    cluster = _FakeWorkspaceCluster()
    ensure_shared_workspace(
        cluster,
        "openshell-tracesim",
        "trace-replay-workspace",
        storage_class,
        ["agent-1"],
        "ReadWriteMany",
        target_node="node-a",
    )
    pvc = next(item for item in cluster.applied if item["kind"] == "PersistentVolumeClaim")
    return pvc["spec"]


def test_workspace_pvc_omits_storage_class_so_the_cluster_default_applies():
    assert "storageClassName" not in _workspace_pvc("")


def test_workspace_pvc_pins_an_explicit_storage_class():
    assert _workspace_pvc("cephfs-fast")["storageClassName"] == "cephfs-fast"
