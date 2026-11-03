from dataclasses import replace
from pathlib import Path

import pytest

from trace_replay_sim.backend_registry import (
    BackendConfigurationError,
    load_backends,
    parse_workload_plan,
    validate_backends,
)


def test_default_allocation_is_five_per_workload():
    allocation = parse_workload_plan(None, 20)
    assert set(allocation) == {
        "personal_assistant",
        "software_engineering_qa",
        "research_deep_search",
        "customer_support_ops",
    }
    assert set(allocation.values()) == {5}


def test_symmetric_one_hundred_agent_allocation():
    allocation = parse_workload_plan(None, 100, default_per_workload=25)
    assert set(allocation.values()) == {25}


def test_zero_count_workloads_select_a_subset():
    allocation = parse_workload_plan(
        "software_engineering_qa=4,personal_assistant=0,research_deep_search=0,customer_support_ops=0",
        4,
    )
    assert allocation == {"software_engineering_qa": 4}


def test_single_agent_plan_is_accepted():
    allocation = parse_workload_plan(
        "personal_assistant=0,research_deep_search=0,software_engineering_qa=1,customer_support_ops=0",
        1,
    )
    assert allocation == {"software_engineering_qa": 1}


def test_negative_workload_count_is_rejected():
    with pytest.raises(BackendConfigurationError, match="negative"):
        parse_workload_plan("software_engineering_qa=-1,personal_assistant=5", 4)


def test_all_zero_allocation_is_rejected():
    with pytest.raises(BackendConfigurationError, match="at least one agent"):
        parse_workload_plan("software_engineering_qa=0", 0)


def test_missing_real_backends_fail_closed():
    backends = load_backends(None)
    backends["software_engineering_qa"] = replace(
        backends["software_engineering_qa"],
        real_backend=False,
        deployment_mode="missing",
        notes="test backend missing",
    )
    with pytest.raises(BackendConfigurationError, match="test backend missing"):
        validate_backends(backends, ["software_engineering_qa"])


def test_manifest_override_must_be_real(tmp_path: Path):
    manifest = tmp_path / "backends.json"
    manifest.write_text(
        '{"backends": {"software_engineering_qa": {"real_backend": false}}}',
        encoding="utf-8",
    )
    with pytest.raises(BackendConfigurationError, match="real_backend=true"):
        load_backends(manifest)
