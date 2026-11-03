"""Declarative workload-backend registry for the cluster experiment.

The trace corpus contains four workload profiles, but a corpus label is not a
backend.  This module makes that distinction explicit: the controller must
resolve a stateful backend, verify its health, and record its identity before
it creates any OpenClaw agents.  Missing backends are a hard preflight error;
the development fixture is never selected implicitly.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .workloads import WORKLOADS, WORKLOAD_LABELS


@dataclass(frozen=True)
class BackendSpec:
    workload: str
    benchmark: str
    name: str
    namespace: str
    service: str
    port: int
    health_path: str
    execute_path: str
    events_path: str
    event_name: str
    real_backend: bool
    deployment_mode: str
    image: str | None = None
    endpoint: str | None = None
    manifest: str | None = None
    notes: str = ""

    @property
    def base_url(self) -> str:
        if self.endpoint:
            return self.endpoint.rstrip("/")
        return f"http://{self.service}.{self.namespace}.svc.cluster.local:{self.port}"

    def public_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["base_url"] = self.base_url
        return result


# AppWorld and the other entries are owned, domain-specific replay environments
# deployed by the controller. They
# implement the internal tool boundary needed for performance measurement; the
# results must not be described as official benchmark-quality scores without
# the corresponding benchmark packages and datasets.
DEFAULT_BACKENDS: dict[str, BackendSpec] = {
    "personal_assistant": BackendSpec(
        workload="personal_assistant",
        benchmark="appworld",
        name="trace-replay-appworld-backend",
        namespace="trace-replay",
        service="trace-replay-appworld-backend",
        port=8090,
        health_path="/health",
        execute_path="/execute",
        events_path="/events",
        event_name="appworld_tool",
        real_backend=True,
        deployment_mode="managed",
        manifest="k8s/appworld-backend.yaml",
        notes="Stateful AppWorld API adapter; task/session mapping is required.",
    ),
    "software_engineering_qa": BackendSpec(
        workload="software_engineering_qa",
        benchmark="swebench",
        name="trace-replay-swebench-backend",
        namespace="trace-replay",
        service="trace-replay-swebench-backend",
        port=8092,
        health_path="/health",
        execute_path="/execute",
        events_path="/events",
        event_name="swebench_tool",
        real_backend=True,
        deployment_mode="managed",
        manifest="k8s/swebench-backend.yaml",
        notes="Owned SWE audit boundary; repository, patch, and tests run in the pinned OpenShell task image.",
    ),
    "research_deep_search": BackendSpec(
        workload="research_deep_search",
        benchmark="browsecompplus",
        name="trace-replay-browsecompplus-backend",
        namespace="trace-replay",
        service="trace-replay-browsecompplus-backend",
        port=8093,
        health_path="/health",
        execute_path="/execute",
        events_path="/events",
        event_name="browsecompplus_tool",
        real_backend=True,
        deployment_mode="managed",
        manifest="k8s/browsecompplus-backend.yaml",
        notes="Owned stateful search/document environment for replay profiling.",
    ),
    "customer_support_ops": BackendSpec(
        workload="customer_support_ops",
        benchmark="tau2",
        name="trace-replay-tau2-backend",
        namespace="trace-replay",
        service="trace-replay-tau2-backend",
        port=8094,
        health_path="/health",
        execute_path="/execute",
        events_path="/events",
        event_name="tau2_tool",
        real_backend=True,
        deployment_mode="managed",
        manifest="k8s/tau2-backend.yaml",
        notes="Owned stateful airline/retail/telecom policy environment for replay profiling.",
    ),
}


class BackendConfigurationError(ValueError):
    """Raised before cluster mutation when a workload backend is unresolved."""


def _validate_override(workload: str, value: dict[str, Any]) -> BackendSpec:
    base = DEFAULT_BACKENDS[workload]
    merged = base.public_dict()
    merged.update(value)
    merged.pop("base_url", None)
    required = {"workload", "benchmark", "name", "namespace", "service", "port", "health_path", "execute_path", "events_path", "event_name", "real_backend", "deployment_mode"}
    missing = sorted(required - set(merged))
    if missing:
        raise BackendConfigurationError(f"backend override for {workload} is missing: {', '.join(missing)}")
    if not bool(merged["real_backend"]):
        raise BackendConfigurationError(
            f"backend override for {workload} is not marked real_backend=true; refusing to use a replay fixture as a real workload backend"
        )
    if merged["deployment_mode"] not in {"existing", "managed"}:
        raise BackendConfigurationError(f"unsupported deployment_mode for {workload}: {merged['deployment_mode']}")
    return BackendSpec(**{key: merged[key] for key in BackendSpec.__dataclass_fields__})


def load_backends(path: Path | None) -> dict[str, BackendSpec]:
    """Load a complete backend manifest, overriding known defaults.

    The file may contain either ``{"backends": {workload: {...}}}`` or the
    mapping directly.  Every workload used by the run must be listed and
    explicitly marked as a real backend.
    """
    result = dict(DEFAULT_BACKENDS)
    if path is None:
        return result
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    values = payload.get("backends", payload)
    if not isinstance(values, dict):
        raise BackendConfigurationError("backend manifest must contain a JSON object")
    for workload, value in values.items():
        if workload not in WORKLOADS:
            raise BackendConfigurationError(f"unknown workload in backend manifest: {workload}")
        if not isinstance(value, dict):
            raise BackendConfigurationError(f"backend manifest entry for {workload} must be an object")
        result[workload] = _validate_override(workload, value)
    return result


def parse_workload_plan(
    value: str | None,
    total_agents: int,
    default_per_workload: int | None = None,
) -> dict[str, int]:
    """Parse ``workload=count,...`` and require an exact allocation.

    When no explicit plan is supplied, ``default_per_workload`` lets the
    operator request a symmetric workload run (for example 25 per category
    for 100 agents). The legacy 20-agent default remains unchanged.

    Workloads assigned zero agents are dropped from the returned plan, so one
    plan can select any subset of the four workloads.
    """
    if not value:
        per_workload = default_per_workload if default_per_workload is not None else 5
        if per_workload < 1:
            raise BackendConfigurationError("default workload count must be positive")
        expected = per_workload * len(WORKLOADS)
        if total_agents != expected:
            raise BackendConfigurationError(
                f"workload allocation requires {expected} agents for {per_workload} per workload, "
                f"but --agents={total_agents}; provide --workload-plan for an asymmetric run"
            )
        return {workload: per_workload for workload in WORKLOADS}
    result: dict[str, int] = {workload: 0 for workload in WORKLOADS}
    for part in value.split(","):
        if "=" not in part:
            raise BackendConfigurationError(f"invalid workload allocation: {part!r}")
        workload, count = (piece.strip() for piece in part.split("=", 1))
        if workload not in result:
            raise BackendConfigurationError(f"unknown workload in allocation: {workload}")
        try:
            result[workload] = int(count)
        except ValueError as exc:
            raise BackendConfigurationError(f"invalid agent count for {workload}: {count!r}") from exc
    if any(count < 0 for count in result.values()):
        raise BackendConfigurationError("workload allocation cannot contain negative agent counts")
    selected = {workload: count for workload, count in result.items() if count > 0}
    if not selected:
        raise BackendConfigurationError("workload allocation requires at least one agent")
    if sum(result.values()) != total_agents:
        raise BackendConfigurationError(f"workload allocation totals {sum(result.values())}, but --agents={total_agents}")
    return selected


def validate_backends(backends: dict[str, BackendSpec], workloads: list[str]) -> None:
    failures: list[str] = []
    for workload in workloads:
        spec = backends.get(workload)
        if spec is None:
            failures.append(f"{workload}: no backend specification")
            continue
        if not spec.real_backend:
            failures.append(f"{workload}: {spec.notes}")
        if spec.deployment_mode == "missing":
            failures.append(f"{workload}: deployment is missing")
    if failures:
        raise BackendConfigurationError(
            "workload backend gate failed before cluster mutation:\n- " + "\n- ".join(failures)
        )


def plan_payload(backends: dict[str, BackendSpec], allocation: dict[str, int]) -> dict[str, Any]:
    return {
        "schema": "agentic.trace-replay.backend-plan.v1",
        "allocation": allocation,
        "backends": {workload: backends[workload].public_dict() for workload in allocation},
        "labels": {workload: WORKLOAD_LABELS[workload] for workload in allocation},
    }
