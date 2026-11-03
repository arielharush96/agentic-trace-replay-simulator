"""Pure planning helpers for multi-node benchmark runs.

These functions intentionally do not talk to Kubernetes.  Keeping node and
warm-pool assignment pure makes the controller testable before any cluster
mutation and prevents accidental dependence on scheduler timing.
"""

from __future__ import annotations

import os
from collections import defaultdict


MAX_EXPERIMENT_AGENTS = 100


def allowed_benchmark_nodes() -> tuple[str, ...]:
    """Return the optional node allow-list from ``TRACE_REPLAY_ALLOWED_NODES``.

    The variable takes a comma-separated list.  When it is unset or empty the
    controller accepts any schedulable worker.
    """

    raw = os.environ.get("TRACE_REPLAY_ALLOWED_NODES", "")
    return tuple(node.strip() for node in raw.split(",") if node.strip())


def is_allowed_node(node: str) -> bool:
    """Return True when ``node`` passes the configured allow-list, if any."""

    allowed = allowed_benchmark_nodes()
    return not allowed or node in allowed


def parse_target_nodes(primary: str | None, nodes: str | None = None) -> tuple[str, ...]:
    """Return an ordered, de-duplicated node list.

    ``--node`` remains the backwards-compatible single-node spelling.  When
    ``--nodes`` is provided it is authoritative, but an empty or malformed
    list is rejected before the controller can mutate the cluster.
    """

    value = nodes if nodes is not None else primary
    if not value:
        raise ValueError("at least one target node is required")
    result: list[str] = []
    for item in value.split(","):
        node = item.strip()
        if not node:
            raise ValueError("target node list contains an empty entry")
        if node not in result:
            result.append(node)
    invalid = [node for node in result if not is_allowed_node(node)]
    if invalid:
        raise ValueError(
            "refusing target node(s) outside the approved experiment set: "
            + ", ".join(invalid)
            + "; approved nodes are "
            + ", ".join(allowed_benchmark_nodes())
        )
    return tuple(result)


def balanced_node_assignments(agent_count: int, nodes: tuple[str, ...]) -> tuple[str, ...]:
    """Assign agents round-robin so node counts differ by at most one."""

    if agent_count < 1:
        raise ValueError("agent_count must be positive")
    if not nodes:
        raise ValueError("at least one node is required")
    return tuple(nodes[index % len(nodes)] for index in range(agent_count))


def node_agent_indices(agent_count: int, nodes: tuple[str, ...]) -> dict[str, list[int]]:
    """Return one-based agent indices grouped by target node."""

    assignments = balanced_node_assignments(agent_count, nodes)
    grouped: dict[str, list[int]] = defaultdict(list)
    for index, node in enumerate(assignments, start=1):
        grouped[node].append(index)
    return {node: grouped.get(node, []) for node in nodes}


def warm_pool_assignments(
    agent_count: int,
    warm_pool_size: int,
    nodes: tuple[str, ...],
) -> dict[str, list[int]]:
    """Assign the first warm-pool slots to agents, grouped by node."""

    if warm_pool_size < 0 or warm_pool_size > agent_count:
        raise ValueError("warm_pool_size must be between zero and agent_count")
    assignments = balanced_node_assignments(agent_count, nodes)
    grouped: dict[str, list[int]] = defaultdict(list)
    for index, node in enumerate(assignments, start=1):
        if index <= warm_pool_size:
            grouped[node].append(index)
    return {node: grouped.get(node, []) for node in nodes}


def validate_agent_count(agent_count: int) -> None:
    if agent_count < 1 or agent_count > MAX_EXPERIMENT_AGENTS:
        raise ValueError(
            f"--agents must be between 1 and {MAX_EXPERIMENT_AGENTS}"
        )
