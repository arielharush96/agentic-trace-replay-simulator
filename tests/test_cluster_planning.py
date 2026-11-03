import pytest

from trace_replay_sim.cluster_planning import (
    MAX_EXPERIMENT_AGENTS,
    balanced_node_assignments,
    node_agent_indices,
    parse_target_nodes,
    validate_agent_count,
    warm_pool_assignments,
)


def test_parse_target_nodes_deduplicates_without_reordering():
    assert parse_target_nodes("node-a", "node-a,node-b,node-a") == ("node-a", "node-b")


def test_parse_target_nodes_rejects_empty_entries():
    with pytest.raises(ValueError, match="empty"):
        parse_target_nodes("node-a", "node-a,")


def test_parse_target_nodes_accepts_any_node_without_an_allow_list(monkeypatch):
    monkeypatch.delenv("TRACE_REPLAY_ALLOWED_NODES", raising=False)
    assert parse_target_nodes("any-worker") == ("any-worker",)


def test_parse_target_nodes_rejects_forbidden_nodes(monkeypatch):
    monkeypatch.setenv("TRACE_REPLAY_ALLOWED_NODES", "node-a,node-b")
    with pytest.raises(ValueError, match="approved experiment set"):
        parse_target_nodes("node-c")


def test_balanced_assignments_for_one_hundred_agents():
    assignments = balanced_node_assignments(100, ("node-a", "node-b"))
    assert len(assignments) == 100
    assert assignments.count("node-a") == 50
    assert assignments.count("node-b") == 50


def test_node_agent_indices_are_one_based():
    grouped = node_agent_indices(5, ("node-a", "node-b"))
    assert grouped == {"node-a": [1, 3, 5], "node-b": [2, 4]}


def test_warm_pool_is_partitioned_by_node():
    grouped = warm_pool_assignments(100, 100, ("node-a", "node-b"))
    assert len(grouped["node-a"]) == 50
    assert len(grouped["node-b"]) == 50


def test_agent_limit_is_one_hundred():
    validate_agent_count(MAX_EXPERIMENT_AGENTS)
    with pytest.raises(ValueError, match="100"):
        validate_agent_count(MAX_EXPERIMENT_AGENTS + 1)
