#!/usr/bin/env python3
"""Single entrypoint for the workload-partitioned OpenClaw benchmark.

The controller deliberately keeps deployment, execution, collection and
analysis in one trace-first lifecycle. It creates only ``trace-replay-*``
resources, never deletes resources, and requires ``--execute`` for cluster
mutation. ``--plan`` is safe and is useful for validating the corpus first.
"""

from __future__ import annotations

import argparse
import base64
import fcntl
import hashlib
import io
import json
import logging
import os
import re
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import tarfile
import threading
import time
import urllib.request
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))
# The controller collects traces concurrently; plotting must never try to
# initialize the macOS GUI backend from a worker thread.
os.environ.setdefault("MPLBACKEND", "Agg")

# Baseline objects are explicitly owned by this benchmark. Run-scoped objects
# use an additional hash suffix; these names are template/telemetry services
# and are never deleted by the controller.
BASE_JAEGER = "trace-replay-jaeger"
BASE_MOCK_LLM = "trace-replay-mock-llm"
BASE_OPENCLAW = "trace-replay-openclaw-shell"
BASE_OPENCLAW_CONFIG = "trace-replay-openclaw-shell-config"
# The plain public image does not carry the OpenSSH client that the published
# OpenShell plugin uses to reach a sandbox.  Use the benchmark's pinned
# OpenShell-enabled image for experiment-owned gateways; callers may override
# it explicitly when validating another approved image.
EXPERIMENT_OPENCLAW_IMAGE = os.environ.get(
    "TRACE_REPLAY_OPENCLAW_IMAGE",
    "quay.io/example/openclaw-openshell:latest",
)

from trace_replay_sim.collect import collect as collect_prometheus
from trace_replay_sim.functional_conformance import verify_trace, write_report
from trace_replay_sim.jaeger_export import compute_timing_segments, extract_per_turn, export as export_jaeger
from trace_replay_sim.per_turn_analysis import analyze as analyze_per_turn
from trace_replay_sim.profiler_v2 import profile as profile_v2
from trace_replay_sim.output_validation import OutputValidationError, validate_trace_output
from trace_replay_sim.backend_registry import (
    BackendConfigurationError,
    load_backends,
    parse_workload_plan,
    plan_payload,
    validate_backends,
)
from trace_replay_sim.cluster_planning import (
    MAX_EXPERIMENT_AGENTS,
    balanced_node_assignments,
    is_allowed_node,
    parse_target_nodes,
    validate_agent_count,
)
from trace_replay_sim.swe_manifest import (
    SweManifestError,
    SweTask,
    load_swe_manifest,
    manifest_sha256,
    validate_swe_sessions,
)
from trace_replay_sim.workloads import (
    WORKLOADS,
    classify_session,
    select_tool_rich_sessions,
    split_corpus,
)
from generate_appworld_plots import plot_events as plot_appworld_events

LOG = logging.getLogger("run-experiment")
TRANSIENT_API_MARKERS = (
    "tls handshake timeout",
    "context deadline exceeded",
    "server was unable to return a response",
    "i/o timeout",
    "connection reset by peer",
    "failed calling webhook",
    "no endpoints available for service",
    "connection refused",
    "unexpected eof",
    "error: eof",
    "internal error occurred",
    "timed out after",
    "too many requests",
    "storage is (re)initializing",
    "admission plugin",
    "caches not synchronized",
    "client connection lost",
    "stream error",
    "received from peer",
    # A broken oc exec stream can reach the target Python process as a
    # syntactically valid command with a truncated/non-gzip tar payload.  The
    # API transport often reports no useful error on the source side, so the
    # target-side tarfile exception is the only transient signal available.
    "not a gzip file",
    "unexpected end of data",
    "not yet ready to handle request",
    "serviceunavailable",
    "service unavailable",
    "server is currently unable to handle the request",
)
READ_ONLY_OC_COMMANDS = {"get", "wait", "rollout", "whoami", "config", "version", "api-resources", "cluster-info"}
DEFAULT_KUEUE_NAMESPACE = "kueue-system"
DEFAULT_KUEUE_WEBHOOK_SERVICE = "kueue-webhook-service"
MOCK_CORPUS_CHUNK_BYTES = 900_000
SANDBOX_POLL_INTERVAL_SECONDS = 2.0
RESULT_POLL_INTERVAL_SECONDS = 3.0


def safe_name(value: str) -> str:
    return "".join(char if char.isalnum() or char == "-" else "-" for char in value.lower()).strip("-")


def is_transient_api_error(message: str) -> bool:
    lowered = message.lower()
    return any(marker in lowered for marker in TRANSIENT_API_MARKERS)


def pack_mock_corpus_sessions(
    sessions: list[dict[str, Any]],
    *,
    max_bytes: int = MOCK_CORPUS_CHUNK_BYTES,
) -> list[list[dict[str, Any]]]:
    """Pack replay sessions into ConfigMap-safe JSON chunks.

    A full multi-workload run can contain tens of MiB of replay data. Keeping
    one ConfigMap per session creates unnecessary API-server and admission
    traffic, while one ConfigMap for the whole corpus exceeds Kubernetes'
    object-size limit. Packing whole sessions into bounded chunks preserves
    directory-based loading and materially reduces the number of write
    operations without splitting any stateful session.
    """
    if max_bytes <= 0:
        raise ValueError("max_bytes must be positive")
    parts: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    for session in sessions:
        candidate = current + [session]
        encoded_size = len(json.dumps({"sessions": candidate}, separators=(",", ":")).encode("utf-8"))
        if current and encoded_size > max_bytes:
            parts.append(current)
            current = []
            encoded_size = len(json.dumps({"sessions": [session]}, separators=(",", ":")).encode("utf-8"))
        if encoded_size > max_bytes:
            raise ValueError(
                f"replay session exceeds the mock corpus chunk limit ({max_bytes} bytes)"
            )
        current.append(session)
    if current:
        parts.append(current)
    return parts


def parse_pod_quantity(value: Any) -> int:
    """Parse the Kubernetes quantity used for ``status.allocatable.pods``.

    Kubernetes may serialize a pod count as either ``1000`` or the compact
    quantity ``1k``.  Pod capacity is an integer count, so accepting the
    documented decimal SI suffixes keeps preflight checks equivalent across
    API-server versions without weakening the capacity gate.
    """
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    text = str(value).strip()
    if text.isdigit():
        return int(text)
    match = re.fullmatch(r"(\d+)([kKmMgGtTpPeE])", text)
    if match:
        multipliers = {
            "k": 10**3,
            "K": 10**3,
            "m": 10**6,
            "M": 10**6,
            "g": 10**9,
            "G": 10**9,
            "t": 10**12,
            "T": 10**12,
            "p": 10**15,
            "P": 10**15,
            "e": 10**18,
            "E": 10**18,
        }
        return int(match.group(1)) * multipliers[match.group(2)]
    raise ValueError(f"unsupported Kubernetes pod quantity: {value!r}")


def workspace_name(prefix: str, index: int) -> str:
    """Return an OpenShell-compatible workspace name for an agent slot.

    OpenShell currently limits workspace names to 19 characters.  The normal
    run prefix fits for slots 1..99, but the literal ``-100`` suffix would
    overflow by one character.  Keep the slot visible and derive a compact,
    deterministic run token for overflow slots so every agent still gets a
    unique, run-scoped workspace.
    """
    candidate = f"{prefix}-{index}"
    if len(candidate) <= 19:
        return candidate
    token = hashlib.sha256(f"{prefix}:{index}".encode("utf-8")).hexdigest()[:7]
    bounded = f"trace-replay-{token}-{index}"
    if len(bounded) > 19:
        raise ValueError(f"cannot create an OpenShell workspace name for agent {index}")
    return bounded


def parse_workspace_pvcs(spec: str | None, target_nodes: tuple[str, ...]) -> dict[str, str]:
    """Parse an explicit node-to-RWO-PVC map without guessing ownership."""
    if not spec:
        return {}
    mapping: dict[str, str] = {}
    for item in spec.split(","):
        node, separator, pvc = item.partition("=")
        node = node.strip()
        pvc = pvc.strip()
        if not separator or not node or not pvc:
            raise ValueError("--workspace-pvcs must use node=pvc,node=pvc syntax")
        if node in mapping:
            raise ValueError(f"duplicate node in --workspace-pvcs: {node}")
        if node not in target_nodes:
            raise ValueError(f"--workspace-pvcs names a node not present in --nodes: {node}")
        mapping[node] = pvc
    missing = [node for node in target_nodes if node not in mapping]
    if missing:
        raise ValueError(f"--workspace-pvcs is missing target nodes: {', '.join(missing)}")
    return mapping


class Cluster:
    def __init__(
        self,
        kubeconfig: str | None,
        namespace: str,
        node: str,
        nodes: tuple[str, ...] | None = None,
        api_workers: int = 16,
    ):
        if api_workers < 1:
            raise ValueError("api_workers must be positive")
        self.env = os.environ.copy()
        if kubeconfig:
            self.env["KUBECONFIG"] = kubeconfig
        self.namespace = namespace
        self.node = node
        self.nodes = tuple(nodes or (node,))
        # A 100-agent run can have hundreds of concurrent polling and cgroup
        # operations.  Bound oc subprocesses so the local controller cannot
        # exhaust its file-descriptor limit while the Kubernetes workloads
        # remain fully concurrent.
        self._api_semaphore = threading.BoundedSemaphore(api_workers)

    def run(self, args: list[str], *, input_text: str | None = None, check: bool = True) -> str:
        LOG.debug("oc %s", " ".join(args))
        command = next((token for token in args if token in READ_ONLY_OC_COMMANDS), None)
        max_attempts = 4 if command is not None else 1
        # A 100-agent run must not leave the local controller blocked forever
        # on a single API-server read. Job waits intentionally keep their
        # Kubernetes timeout; point reads get a bounded client timeout.
        client_timeout = 30 if command in {"get", "config", "whoami", "version", "api-resources", "cluster-info"} else None
        result = None
        for attempt in range(max_attempts):
            try:
                with self._api_semaphore:
                    result = subprocess.run(
                        ["oc", *args],
                        input=input_text,
                        text=True,
                        capture_output=True,
                        env=self.env,
                        timeout=client_timeout,
                    )
            except subprocess.TimeoutExpired as exc:
                if attempt + 1 >= max_attempts:
                    raise RuntimeError(f"oc {' '.join(args)} timed out after {client_timeout}s") from exc
                delay = min(2 ** attempt, 8)
                LOG.warning(
                    "timeout on oc %s; retrying in %ss (%d/%d)",
                    " ".join(args),
                    delay,
                    attempt + 1,
                    max_attempts - 1,
                )
                time.sleep(delay)
                continue
            if result.returncode == 0 or not is_transient_api_error(result.stderr) or attempt + 1 >= max_attempts:
                break
            delay = min(2 ** attempt, 8)
            LOG.warning(
                "transient Kubernetes API error on oc %s; retrying in %ss (%d/%d)",
                " ".join(args),
                delay,
                attempt + 1,
                max_attempts - 1,
            )
            time.sleep(delay)
        assert result is not None
        if check and result.returncode:
            raise RuntimeError(f"oc {' '.join(args)} failed: {result.stderr.strip()}")
        return result.stdout

    def delete_owned(self, namespace: str, kind: str, name: str) -> None:
        """Delete only an existing object carrying this run's ownership label."""
        raw = self.run(["-n", namespace, "get", kind, name], check=False).strip()
        if not raw:
            return
        current = json.loads(self.run(["-n", namespace, "get", kind, name, "-o", "json"]))
        labels = current.get("metadata", {}).get("labels") or {}
        if labels.get("managed-by") != "trace-replay-experiment":
            raise RuntimeError(f"refusing to delete unowned {kind} {namespace}/{name}")
        self.run(["-n", namespace, "delete", kind, name, "--ignore-not-found"])

    def json(self, args: list[str]) -> dict[str, Any]:
        return json.loads(self.run([*args, "-o", "json"]))

    def apply_json(self, value: dict[str, Any]) -> None:
        """Apply a run-owned object in the experiment namespace with retries.

        The Kueue admission webhook can briefly refuse otherwise valid
        idempotent writes.  All callers of this helper construct experiment
        resources in ``self.namespace``; retrying transient API failures here
        keeps a single webhook flap from aborting a long 100-agent run.
        """
        self.apply_json_in_with_retry(self.namespace, value)

    def replace_json(self, value: dict[str, Any]) -> None:
        """Replace an existing object without creating a huge last-applied annotation."""
        self.run(["-n", self.namespace, "replace", "-f", "-"], input_text=json.dumps(value))

    def apply_json_in(self, namespace: str, value: dict[str, Any]) -> None:
        self.run(["-n", namespace, "apply", "-f", "-"], input_text=json.dumps(value))

    def apply_json_in_with_retry(self, namespace: str, value: dict[str, Any]) -> None:
        """Retry an idempotent apply for an experiment-owned object.

        OpenShift admission webhooks can time out or refuse connections while
        the API remains healthy enough for a subsequent request.  This helper
        is used only for resources constructed by this experiment controller;
        it must not turn arbitrary cluster writes into an unbounded retry loop.
        """
        # Admission webhooks can remain unavailable for several minutes even
        # after the read-only preflight has succeeded.  Keep this longer
        # window limited to run-owned experiment resources.
        for attempt in range(30):
            try:
                self.apply_json_in(namespace, value)
                return
            except RuntimeError as exc:
                if not is_transient_api_error(str(exc)) or attempt >= 29:
                    raise
                delay = min(2 ** attempt, 30)
                name = str(value.get("metadata", {}).get("name") or "<unnamed>")
                LOG.warning(
                    "transient apply failed for %s; retrying in %ss (%d/29)",
                    name,
                    delay,
                    attempt + 1,
                )
                time.sleep(delay)

    def apply_yaml_in(self, namespace: str, value: str) -> None:
        self.run(["-n", namespace, "apply", "-f", "-"], input_text=value)

    def upsert_owned_configmap(self, namespace: str, value: dict[str, Any]) -> None:
        """Retry an idempotent owned ConfigMap operation across API flaps."""
        for attempt in range(8):
            try:
                self._upsert_owned_configmap_once(namespace, value)
                return
            except RuntimeError as exc:
                if not is_transient_api_error(str(exc)) or attempt >= 7:
                    raise
                delay = min(2 ** attempt, 30)
                name = str(value.get("metadata", {}).get("name") or "<unnamed>")
                LOG.warning(
                    "transient ConfigMap operation failed for %s; retrying in %ss (%d/7)",
                    name,
                    delay,
                    attempt + 1,
                )
                time.sleep(delay)

    def _upsert_owned_configmap_once(self, namespace: str, value: dict[str, Any]) -> None:
        """Create/replace an owned ConfigMap without client-side apply metadata.

        Session corpora can be hundreds of KiB; the client-side apply
        annotation would exceed Kubernetes' annotation limit even though the
        ConfigMap data itself is valid.
        """
        name = str(value.get("metadata", {}).get("name") or "")
        existing = self.run(["-n", namespace, "get", "configmap", name], check=False).strip()
        if not existing:
            try:
                self.run(["-n", namespace, "create", "-f", "-"], input_text=json.dumps(value))
            except RuntimeError as exc:
                # A create request can be accepted by the API server even if
                # the client receives a timeout.  Confirm ownership before
                # surfacing the failure so a retry remains idempotent.
                if not is_transient_api_error(str(exc)):
                    raise
                # The create may also have been rejected before persistence,
                # or a concurrent cleanup may have removed the object between
                # the create and this confirmation read.  A NotFound here is
                # therefore still transient: re-run the idempotent create
                # instead of turning the confirmation probe into a fatal
                # error.
                try:
                    current = self.json(["-n", namespace, "get", "configmap", name])
                except RuntimeError as confirmation_exc:
                    if "not found" in str(confirmation_exc).lower():
                        raise exc
                    raise
                labels = current.get("metadata", {}).get("labels") or {}
                if labels.get("managed-by") != "trace-replay-experiment":
                    raise
            return
        current = self.json(["-n", namespace, "get", "configmap", name])
        labels = current.get("metadata", {}).get("labels") or {}
        if labels.get("managed-by") != "trace-replay-experiment":
            raise RuntimeError(f"refusing to modify unowned ConfigMap {namespace}/{name}")
        # Reusing a run-owned corpus is intentionally idempotent.  Avoid a
        # full GET/replace round trip for large, unchanged payloads: repeated
        # rewrites create needless API-server and admission load and can make
        # a healthy cluster appear stalled during resume.
        desired_labels = value.get("metadata", {}).get("labels") or {}
        if labels == desired_labels and current.get("data", {}) == value.get("data", {}) and current.get("binaryData") == value.get("binaryData"):
            return
        metadata = current.setdefault("metadata", {})
        metadata["labels"] = desired_labels or labels
        annotations = metadata.get("annotations") or {}
        annotations.pop("kubectl.kubernetes.io/last-applied-configuration", None)
        if annotations:
            metadata["annotations"] = annotations
        else:
            metadata.pop("annotations", None)
        current["data"] = value.get("data", {})
        if value.get("binaryData") is not None:
            current["binaryData"] = value["binaryData"]
        else:
            current.pop("binaryData", None)
        for key in ("uid", "creationTimestamp", "managedFields"):  # resourceVersion is required by replace
            metadata.pop(key, None)
        self.run(["-n", namespace, "replace", "-f", "-"], input_text=json.dumps(current))

    def upsert_owned_secret(self, namespace: str, value: dict[str, Any]) -> None:
        """Create/replace an experiment-owned Secret without logging its data."""
        name = str(value.get("metadata", {}).get("name") or "")
        existing = self.run(["-n", namespace, "get", "secret", name], check=False).strip()
        if not existing:
            self.run(["-n", namespace, "create", "-f", "-"], input_text=json.dumps(value))
            return
        current = self.json(["-n", namespace, "get", "secret", name])
        labels = current.get("metadata", {}).get("labels") or {}
        if labels.get("managed-by") != "trace-replay-experiment":
            raise RuntimeError(f"refusing to modify unowned Secret {namespace}/{name}")
        metadata = current.setdefault("metadata", {})
        metadata["labels"] = value.get("metadata", {}).get("labels", labels)
        annotations = metadata.get("annotations") or {}
        annotations.pop("kubectl.kubernetes.io/last-applied-configuration", None)
        if annotations:
            metadata["annotations"] = annotations
        else:
            metadata.pop("annotations", None)
        current["type"] = value.get("type", current.get("type", "Opaque"))
        current["data"] = value.get("data", {})
        for key in ("uid", "creationTimestamp", "managedFields"):
            metadata.pop(key, None)
        self.run(["-n", namespace, "replace", "-f", "-"], input_text=json.dumps(current))

    def wait_rollout(self, deployment: str) -> None:
        try:
            self.run(["-n", self.namespace, "rollout", "status", f"deployment/{deployment}", "--timeout=600s"])
        except RuntimeError as exc:
            # During a webhook/API flap, `oc rollout status` can report a
            # stale progress-deadline failure even though the Deployment has
            # already converged.  Accept only the unambiguous healthy state;
            # otherwise preserve the original failure.
            if "progress deadline" not in str(exc).lower():
                raise
            current = self.json(["-n", self.namespace, "get", "deployment", deployment])
            desired = int((current.get("spec") or {}).get("replicas") or 0)
            status = current.get("status") or {}
            ready = int(status.get("readyReplicas") or 0)
            updated = int(status.get("updatedReplicas") or 0)
            available = int(status.get("availableReplicas") or 0)
            if desired and ready >= desired and updated >= desired and available >= desired:
                LOG.warning(
                    "accepting converged Deployment %s after stale progress-deadline status "
                    "(ready=%d updated=%d available=%d desired=%d)",
                    deployment,
                    ready,
                    updated,
                    available,
                    desired,
                )
                return
            raise

    def wait_rollout_in(self, namespace: str, deployment: str) -> None:
        self.run(["-n", namespace, "rollout", "status", f"deployment/{deployment}", "--timeout=600s"])


def log_phase(message: str) -> None:
    LOG.info("[%s] %s", datetime.now().astimezone().strftime("%H:%M:%S"), message)


def ensure_shared_workspace(
    cluster: Cluster,
    openshell_ns: str,
    pvc_name: str,
    storage_class: str,
    workspace_names: list[str],
    access_mode: str,
    *,
    target_node: str | None = None,
) -> None:
    """Provision one experiment-owned shared workspace and its agent subpaths.

    OpenShell's default Kubernetes workspace is one PVC per sandbox.  The
    benchmark instead uses one PVC and a unique subPath for each agent; this
    removes per-sandbox volume attachment pressure while preserving workspace
    isolation at the agent path boundary.  ReadWriteMany is the normal mode
    for CephFS.  ReadWriteOnce is supported for a block-storage fallback when
    every sandbox is pinned to the same node.
    """
    if not workspace_names:
        raise ValueError("workspace_names must not be empty")
    if access_mode not in {"ReadWriteMany", "ReadWriteOnce"}:
        raise ValueError(f"unsupported workspace access mode: {access_mode}")
    node = target_node or cluster.node
    existing = cluster.run(["-n", openshell_ns, "get", "pvc", pvc_name], check=False).strip()
    if existing:
        pvc = json.loads(cluster.run(["-n", openshell_ns, "get", "pvc", pvc_name, "-o", "json"]))
        labels = pvc.get("metadata", {}).get("labels") or {}
        if labels.get("managed-by") != "trace-replay-experiment":
            raise RuntimeError(f"refusing to reuse unowned PVC {openshell_ns}/{pvc_name}")
        spec = pvc.get("spec", {})
        if access_mode not in (spec.get("accessModes") or []) or (
            storage_class and spec.get("storageClassName") != storage_class
        ):
            raise RuntimeError(f"existing PVC {openshell_ns}/{pvc_name} has incompatible access mode/storage class settings")
    else:
        workspace_spec: dict[str, Any] = {
            "accessModes": [access_mode],
            "resources": {"requests": {"storage": "10Gi"}},
            "volumeMode": "Filesystem",
        }
        if storage_class:
            workspace_spec["storageClassName"] = storage_class
        cluster.apply_json_in(openshell_ns, {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {
                "name": pvc_name,
                "labels": {"managed-by": "trace-replay-experiment", "component": "openshell-shared-workspace"},
            },
            "spec": workspace_spec,
        })
    # Keep the preparation Job scoped to this run's workspace prefix.  Delete
    # a stale copy first so a retry can recreate it with the current subpaths.
    prep_name = f"{workspace_names[0][:55]}-workspace-prep"
    cluster.delete_owned(openshell_ns, "job", prep_name)
    names_literal = ",".join(repr(name) for name in workspace_names)
    prep = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": prep_name,
            "labels": {"managed-by": "trace-replay-experiment", "component": "openshell-workspace-prep"},
        },
        "spec": {
            "backoffLimit": 0,
            "activeDeadlineSeconds": 900,
            "ttlSecondsAfterFinished": 86400,
            "template": {
                "metadata": {"labels": {"managed-by": "trace-replay-experiment", "component": "openshell-workspace-prep"}},
                "spec": {
                    "restartPolicy": "Never",
                    "securityContext": {"fsGroupChangePolicy": "OnRootMismatch"},
                    "nodeSelector": {"kubernetes.io/hostname": node},
                    "containers": [{
                        "name": "prep",
                        "image": "python:3.12-slim",
                        "command": ["python", "-c"],
                        "args": [f"import os; [os.makedirs('/mnt/workspaces/' + name, exist_ok=True) for name in ({names_literal},)]"],
                        "volumeMounts": [{"name": "shared-workspace", "mountPath": "/mnt/workspaces"}],
                    }],
                    "volumes": [{"name": "shared-workspace", "persistentVolumeClaim": {"claimName": pvc_name}}],
                },
            },
        },
    }
    cluster.apply_json_in(openshell_ns, prep)
    # Block StorageClasses commonly use WaitForFirstConsumer. Applying the
    # node-pinned prep Job before waiting for Bound lets the scheduler select
    # the requested target node and triggers provisioning without a
    # PVC/consumer deadlock.
    cluster.run(["-n", openshell_ns, "wait", "--for=jsonpath={.status.phase}=Bound", f"pvc/{pvc_name}", "--timeout=300s"])
    # A retry can follow cleanup of many failed sandboxes.  Their volume
    # detachments may delay the node-pinned prep pod beyond five minutes even
    # though the PVC and Job are healthy; keep this bounded but give the
    # cluster enough time to settle.
    cluster.run(["-n", openshell_ns, "wait", "--for=condition=complete", f"job/{prep_name}", "--timeout=600s"])
    cluster.delete_owned(openshell_ns, "job", prep_name)
    log_phase(f"storage: shared {access_mode} workspace ready ({pvc_name}, {len(workspace_names)} subpaths)")


def stage_swe_repositories(
    cluster: Cluster,
    openshell_ns: str,
    run_hash: str,
    tasks_by_agent: dict[int, dict[str, Any]],
    workspace_names: list[str],
    agent_nodes: list[str],
    workspace_pvcs: dict[str, str],
    staging_node: str | None = None,
    checkpoint_dir: Path | None = None,
) -> None:
    """Copy real SWE task snapshots into the agent's shared workspace.

    SWE-bench task images contain the repository, but they are not guaranteed
    to contain OpenShell's trusted ``ip`` helper.  Running those images
    directly as OpenShell sandboxes therefore fails during network namespace
    setup.  The task image is used only as a short-lived, node-pinned staging
    container; OpenShell then runs its supported sandbox image against the
    staged repository on the RWX workspace.  The copied HEAD is checked
    against the manifest before the staging job is accepted.
    """
    if not tasks_by_agent:
        return

    def _stage_one_once(agent_index: int, task: dict[str, Any]) -> None:
        image = str(task.get("image") or "")
        commit = str(task.get("base_commit") or "")
        repo_url = str(task.get("repo_url") or "")
        if not repo_url:
            raise RuntimeError(f"SWE task for agent {agent_index + 1} has no repository URL")
        workspace = workspace_names[agent_index]
        node = agent_nodes[agent_index]
        checkpoint = (checkpoint_dir / "swe-staging" / f"{agent_index + 1:03d}.json") if checkpoint_dir else None
        if checkpoint and _staged_repository_is_valid(
            cluster,
            openshell_ns,
            workspace,
            workspace_pvcs[node],
            commit,
            checkpoint,
        ):
            log_phase(f"SWE repository staging already verified; reusing agent {agent_index + 1}")
            return
        job = f"trace-replay-swe-stage-{run_hash}-{agent_index + 1:03d}"
        command = (
            "set -eux; "
            "export GIT_CONFIG_GLOBAL=/tmp/trace-replay-gitconfig; "
            "git config --global --add safe.directory /testbed; "
            "test -d /testbed/.git; "
            "observed=$(git -C /testbed rev-parse HEAD); "
            # Keep repository staging inside the short-lived Job's own
            # emptyDir.  The OpenShell workspace PVC carries a sandbox-
            # specific SELinux label, so a separate staging pod must not
            # write to it directly.  The verified tree is streamed into the
            # already-running sandbox below, which has the matching label.
            "stage_root=/stage/repo; rm -rf \"$stage_root\"; mkdir -p \"$stage_root\"; "
            "git config --global --add safe.directory \"$stage_root\"; "
            "git -C \"$stage_root\" init; "
            f"git -C \"$stage_root\" remote add origin {shlex.quote(repo_url)}; "
            f"git -C \"$stage_root\" fetch --no-tags --depth=1 origin {shlex.quote(commit)}; "
            "git -C \"$stage_root\" checkout --detach FETCH_HEAD; "
            f"test \"$(git -C \"$stage_root\" rev-parse HEAD)\" = \"{commit}\"; "
            "touch /stage/ready; sleep 1800"
        )
        job_obj = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": job,
                "labels": {"managed-by": "trace-replay-experiment", "component": "swe-repository-stage"},
            },
            "spec": {
                "backoffLimit": 0,
                "activeDeadlineSeconds": 1800,
                "ttlSecondsAfterFinished": 86400,
                "template": {
                    "metadata": {"labels": {"managed-by": "trace-replay-experiment", "component": "swe-repository-stage"}},
                    "spec": {
                        "restartPolicy": "Never",
                        "securityContext": {"fsGroupChangePolicy": "OnRootMismatch"},
                        # The target workspace PVC is RWO, so stage on the
                        # agent's approved node rather than an auxiliary node.
                        "nodeSelector": {"kubernetes.io/hostname": node},
                        "containers": [{
                            "name": "stage",
                            "image": image,
                            "command": ["/bin/sh", "-lc"],
                            "args": [command],
                            "securityContext": {"runAsUser": 0},
                            "volumeMounts": [
                                {"name": "stage-data", "mountPath": "/stage"},
                            ],
                        }],
                        "volumes": [
                            {"name": "stage-data", "emptyDir": {}},
                        ],
                    },
                },
            },
        }
        cluster.delete_owned(openshell_ns, "job", job)
        # A delete request is asynchronous.  Wait for the old run-owned Job
        # to disappear before recreating the same name; otherwise Kubernetes
        # can leave the controller observing the stale pod/spec.
        delete_deadline = time.time() + 120
        while time.time() < delete_deadline:
            if not cluster.run(["-n", openshell_ns, "get", "job", job], check=False).strip():
                break
            time.sleep(1)
        else:
            raise RuntimeError(f"timed out deleting previous SWE staging Job {job}")
        cluster.apply_json_in(openshell_ns, job_obj)
        try:
            deadline = time.time() + 1800
            stage_pod = ""
            while time.time() < deadline:
                pod_items = cluster.json(["-n", openshell_ns, "get", "pods", "-l", f"job-name={job}"]).get("items") or []
                running = [pod for pod in pod_items if (pod.get("status") or {}).get("phase") == "Running"]
                if running:
                    stage_pod = str(running[0].get("metadata", {}).get("name") or "")
                    ready = cluster.run(
                        ["-n", openshell_ns, "exec", stage_pod, "-c", "stage", "--", "sh", "-lc", "test -f /stage/ready && echo READY"],
                        check=False,
                    ).strip()
                    if ready == "READY":
                        break
                status = cluster.json(["-n", openshell_ns, "get", "job", job]).get("status") or {}
                if int(status.get("failed") or 0) > 0 or any(
                    condition.get("type") == "Failed" and condition.get("status") == "True"
                    for condition in status.get("conditions") or []
                ):
                    logs = cluster.run(["-n", openshell_ns, "logs", f"job/{job}", "-c", "stage"], check=False).strip()
                    raise RuntimeError(f"SWE repository staging Job failed for agent {agent_index + 1}:\n{logs[-8000:]}")
                time.sleep(5)
            else:
                raise RuntimeError(f"timed out waiting for SWE repository staging Job {job}")
            sandbox = None
            sandbox_deadline = time.time() + 300
            while time.time() < sandbox_deadline and sandbox is None:
                sandbox = _sandbox_for_workspace(
                    cluster,
                    openshell_ns,
                    workspace_pvcs[node],
                    workspace,
                )
                if sandbox is None:
                    time.sleep(3)
            if sandbox is None:
                raise RuntimeError(f"timed out locating OpenShell sandbox for agent {agent_index + 1}")
            sandbox_pod, sandbox_container = sandbox
            source_code = (
                "import sys,tarfile; t=tarfile.open(fileobj=sys.stdout.buffer, mode='w|gz'); "
                "t.add('/stage/repo', arcname='.'); t.close()"
            )
            target_code = (
                "import os,shutil,sys,tarfile; root='/sandbox/.openshell/workspace'; "
                "os.makedirs(root,exist_ok=True); "
                "[shutil.rmtree(os.path.join(root,n)) if os.path.isdir(os.path.join(root,n)) and not os.path.islink(os.path.join(root,n)) else os.unlink(os.path.join(root,n)) for n in os.listdir(root)]; "
                "tarfile.open(fileobj=sys.stdin.buffer, mode='r|gz').extractall(root)"
            )
            source = subprocess.Popen(
                ["oc", "-n", openshell_ns, "exec", "-i", stage_pod, "-c", "stage", "--", "python3", "-c", source_code],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=cluster.env,
            )
            assert source.stdout is not None
            target = subprocess.Popen(
                ["oc", "-n", openshell_ns, "exec", "-i", sandbox_pod, "-c", sandbox_container, "--", "python3", "-c", target_code],
                stdin=source.stdout,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=cluster.env,
            )
            source.stdout.close()
            target_out, target_err = target.communicate(timeout=600)
            source_err = source.stderr.read() if source.stderr is not None else b""
            source_rc = source.wait(timeout=30)
            if target.returncode != 0 or source_rc != 0:
                raise RuntimeError(
                    f"sandbox repository transfer failed: source_rc={source_rc} target_rc={target.returncode} "
                    f"source={source_err.decode(errors='replace')[-2000:]} target={target_err.decode(errors='replace')[-2000:]}"
                )
            observed = cluster.run(
                ["-n", openshell_ns, "exec", sandbox_pod, "-c", sandbox_container, "--", "sh", "-lc", "git -c safe.directory=/sandbox/.openshell/workspace -C /sandbox/.openshell/workspace rev-parse HEAD"],
                check=False,
            ).strip()
            if observed != commit:
                raise RuntimeError(
                    f"SWE sandbox repository mismatch for agent {agent_index + 1}: expected {commit}, observed {observed or '<empty>'}"
                )
            if checkpoint:
                checkpoint.parent.mkdir(parents=True, exist_ok=True)
                temporary = checkpoint.with_suffix(".tmp")
                temporary.write_text(
                    json.dumps(
                        {
                            "schema": "trace-replay.swe-staging.v1",
                            "agent_index": agent_index + 1,
                            "workspace": workspace,
                            "node": node,
                            "base_commit": commit,
                            "verified_at": datetime.now(timezone.utc).isoformat(),
                        },
                        indent=2,
                    )
                    + "\n",
                    encoding="utf-8",
                )
                os.replace(temporary, checkpoint)
        except Exception as exc:
            logs = cluster.run(["-n", openshell_ns, "logs", f"job/{job}", "-c", "stage"], check=False).strip()
            raise RuntimeError(f"SWE repository staging failed for agent {agent_index + 1}: {exc}\n{logs[-4000:]}") from exc
        finally:
            cluster.delete_owned(openshell_ns, "job", job)

    def stage_one(agent_index: int, task: dict[str, Any]) -> None:
        """Stage one snapshot, retrying transient API stream resets.

        ``oc exec`` transports the repository tar over an API-server stream.
        A connection reset can truncate an otherwise valid tarball even when
        both pods and etcd remain healthy.  The per-agent Job and sandbox
        transfer are idempotent, so retry the complete snapshot operation a
        small bounded number of times instead of aborting the full run.
        """
        for attempt in range(3):
            try:
                _stage_one_once(agent_index, task)
                return
            except Exception as exc:
                if attempt >= 2 or not is_transient_api_error(str(exc)):
                    raise
                delay = min(2 ** attempt, 20)
                log_phase(
                    f"SWE repository staging transient failure for agent {agent_index + 1}; "
                    f"retrying in {delay}s ({attempt + 1}/2): {exc}"
                )
                time.sleep(delay)

    log_phase(f"SWE backend: staging {len(tasks_by_agent)} real repository snapshots into shared workspaces")
    with ThreadPoolExecutor(max_workers=min(2, len(tasks_by_agent))) as pool:
        futures = [pool.submit(stage_one, agent_index, task) for agent_index, task in tasks_by_agent.items()]
        for future in as_completed(futures):
            future.result()
    log_phase(f"SWE backend: staged {len(tasks_by_agent)} repository snapshots with verified base commits")


def _wait_for_job_success(cluster: Cluster, namespace: str, job: str, timeout: float = 900.0) -> None:
    """Wait for a one-shot setup Job without creating a watch stream."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        status = cluster.json(["-n", namespace, "get", "job", job]).get("status") or {}
        if int(status.get("succeeded") or 0) > 0:
            return
        if int(status.get("failed") or 0) > 0 or any(
            condition.get("type") == "Failed" and condition.get("status") == "True"
            for condition in status.get("conditions") or []
        ):
            logs = cluster.run(["-n", namespace, "logs", f"job/{job}", "--all-containers=true"], check=False).strip()
            raise RuntimeError(f"Job {namespace}/{job} failed:\n{logs[-8000:]}")
        time.sleep(3)
    raise RuntimeError(f"timed out waiting for Job {namespace}/{job}")


def _sandbox_for_workspace(
    cluster: Cluster,
    openshell_ns: str,
    pvc: str,
    workspace: str,
) -> tuple[str, str] | None:
    """Find the OpenShell sandbox pod mounting one workspace subpath."""
    pods = cluster.json(["-n", openshell_ns, "get", "pods", "-o", "json"]).get("items") or []
    for pod in pods:
        if (pod.get("status") or {}).get("phase") != "Running":
            continue
        volumes = pod.get("spec", {}).get("volumes") or []
        matching_names = {
            str(volume.get("name"))
            for volume in volumes
            if (volume.get("persistentVolumeClaim") or {}).get("claimName") == pvc
        }
        if not matching_names:
            continue
        for container in pod.get("spec", {}).get("containers") or []:
            for mount in container.get("volumeMounts") or []:
                if mount.get("name") in matching_names and mount.get("subPath") == workspace:
                    return str(pod.get("metadata", {}).get("name")), str(container.get("name") or "agent")
    return None


def _remove_failed_sandbox_for_workspace(
    cluster: Cluster,
    openshell_ns: str,
    workspace: str,
) -> None:
    """Remove only a failed, run-owned Sandbox so prewarm can recreate it.

    OpenShell keeps the Sandbox CR after a failed pod.  A later prewarm call
    can then report success while reusing the dead object, leaving repository
    staging with no Running pod to receive the workspace.  The workspace
    annotation and name prefix are both checked before deletion; running or
    unrelated sandboxes are never touched.
    """
    if not workspace.startswith("trace-replay-"):
        return
    for item in (cluster.json(["-n", openshell_ns, "get", "sandbox"]).get("items") or []):
        metadata = item.get("metadata") or {}
        name = str(metadata.get("name") or "")
        annotations = metadata.get("annotations") or {}
        if annotations.get("openshell.ai/sandbox-workspace") != workspace:
            continue
        if not name.startswith(f"{workspace}--"):
            continue
        conditions = (item.get("status") or {}).get("conditions") or []
        failed = any(
            condition.get("type") == "Finished"
            and condition.get("status") == "True"
            and condition.get("reason") == "PodFailed"
            for condition in conditions
        )
        if failed:
            # A failed Sandbox may retain a controller finalizer.  Do not make
            # the experiment wait for that finalizer; the OpenShell controller
            # will finish cleanup asynchronously while prewarm recreates the
            # run-owned slot.
            cluster.run(["-n", openshell_ns, "delete", "sandbox", name, "--ignore-not-found", "--wait=false"])


def _staged_repository_is_valid(
    cluster: Cluster,
    openshell_ns: str,
    workspace: str,
    pvc: str,
    expected_commit: str,
    checkpoint: Path,
) -> bool:
    """Return true only when a persisted staging checkpoint is still live.

    A checkpoint is an optimization, not authority: the exact sandbox pod is
    rediscovered and its mounted workspace HEAD is checked before reuse.  This
    prevents a stale marker from hiding a failed or recreated sandbox.
    """
    if not expected_commit or not checkpoint.exists():
        return False
    try:
        data = json.loads(checkpoint.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    if data.get("workspace") != workspace or data.get("base_commit") != expected_commit:
        return False
    found = _sandbox_for_workspace(cluster, openshell_ns, pvc, workspace)
    if not found:
        return False
    pod, container = found
    observed = cluster.run(
        [
            "-n",
            openshell_ns,
            "exec",
            pod,
            "-c",
            container,
            "--",
            "sh",
            "-lc",
            "git -c safe.directory=/sandbox/.openshell/workspace -C /sandbox/.openshell/workspace rev-parse HEAD",
        ],
        check=False,
    ).strip()
    return observed == expected_commit


def _verify_swe_sandboxes(
    cluster: Cluster,
    openshell_ns: str,
    tasks_by_agent: dict[int, dict[str, Any]],
    workspace_names: list[str],
    agent_nodes: list[str],
    workspace_pvcs: dict[str, str],
    timeout: float = 300.0,
) -> None:
    """Verify staged repositories from inside the actual OpenShell sandboxes."""
    if not tasks_by_agent:
        return
    deadline = time.time() + timeout
    pending = set(tasks_by_agent)
    while pending and time.time() < deadline:
        for agent_index in list(pending):
            workspace = workspace_names[agent_index]
            pvc = workspace_pvcs[agent_nodes[agent_index]]
            found = _sandbox_for_workspace(cluster, openshell_ns, pvc, workspace)
            if not found:
                continue
            pod, container = found
            task = tasks_by_agent[agent_index]
            expected = str(task.get("base_commit") or "")
            observed = cluster.run(
                ["-n", openshell_ns, "exec", pod, "-c", container, "--", "sh", "-lc", "git -c safe.directory=/sandbox/.openshell/workspace -C /sandbox/.openshell/workspace rev-parse HEAD"],
                check=False,
            ).strip()
            if observed != expected:
                raise RuntimeError(
                    f"SWE sandbox repository mismatch for agent {agent_index + 1}: "
                    f"expected {expected}, observed {observed or '<empty>'}"
                )
            pending.remove(agent_index)
        if pending:
            time.sleep(3)
    if pending:
        raise RuntimeError(
            "timed out locating OpenShell sandboxes after prewarm for agents "
            + ", ".join(str(index + 1) for index in sorted(pending))
        )


def prewarm_swe_sandboxes(
    cluster: Cluster,
    items_by_agent: dict[int, dict[str, Any]],
    agent_bindings: list[tuple[str, str, str]],
    out: Path,
    system_ns: str,
    run_hash: str,
    allow_cross_node: bool,
    workspace_pvcs: dict[str, str] | None = None,
    placement_node: str | None = None,
    openshell_ns: str | None = None,
) -> None:
    """Create the real agent-scoped sandboxes before SWE repository staging.

    OpenShell initializes the workspace when its sandbox is first created.  A
    synthetic exec through the same OpenClaw deployment creates that exact
    sandbox; staging then happens while it is mounted, and the measured Job
    reuses the agent-scoped sandbox.
    """
    if not items_by_agent:
        return
    sandbox_ns = openshell_ns or cluster.namespace

    def one(agent_index: int, item: dict[str, Any]) -> None:
        agent, _, node = agent_bindings[agent_index]
        workspace = agent_bindings[agent_index][1]
        if workspace_pvcs:
            _remove_failed_sandbox_for_workspace(cluster, sandbox_ns, workspace)
            swe_task = item.get("session", {}).get("swe_task") or {}
            checkpoint = out / "swe-staging" / f"{agent_index + 1:03d}.json"
            if _staged_repository_is_valid(
                cluster,
                sandbox_ns,
                workspace,
                workspace_pvcs[node],
                str(swe_task.get("base_commit") or ""),
                checkpoint,
            ):
                log_phase(f"SWE sandbox staging already verified; reusing agent {agent_index + 1}")
                return
        session_id = str(item["session"].get("session_id") or "")
        trace_out = out / "swe-prewarm" / f"{agent_index + 1:03d}-{session_id}"
        user_key = f"trace-replay:{session_id}:swe-prewarm-{run_hash}"
        job = cm = ""
        try:
            job, cm, _, _ = create_trace_job(
                cluster,
                item,
                agent,
                out / "traces",
                system_ns,
                system_ns,
                f"swp{run_hash[:6]}{agent_index + 1:03d}",
                allow_cross_node=allow_cross_node,
                prewarm_requested=True,
                warm_pool_size=0,
                agent_node=node,
                placement_node=placement_node,
                prewarm_only=True,
                user_key=user_key,
            )
            _wait_for_job_success(cluster, cluster.namespace, job, timeout=900)
            if workspace_pvcs:
                deadline = time.time() + 300
                while time.time() < deadline:
                    if _sandbox_for_workspace(cluster, sandbox_ns, workspace_pvcs[node], workspace):
                        break
                    time.sleep(3)
                else:
                    raise RuntimeError(
                        f"prewarm completed but no Running OpenShell sandbox was found for agent {agent_index + 1} "
                        f"workspace {workspace}"
                    )
            log_phase(f"SWE sandbox prewarm complete: agent {agent_index + 1} ({session_id})")
        finally:
            for kind, name in (("job", job), ("configmap", cm)):
                if name:
                    cluster.delete_owned(cluster.namespace, kind, name)

    log_phase(f"SWE backend: prewarming {len(items_by_agent)} agent-scoped sandboxes before staging")
    # Two at a time keeps sandbox admission and etcd pressure bounded while
    # avoiding a serial 25-sandbox cold start.
    with ThreadPoolExecutor(max_workers=min(2, len(items_by_agent))) as pool:
        futures = [pool.submit(one, index, item) for index, item in items_by_agent.items()]
        for future in as_completed(futures):
            future.result()
    log_phase("SWE backend: all task sandboxes are live; repository staging may proceed")


def _free_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_jaeger(base: str, process: subprocess.Popen[str], timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"Jaeger port-forward exited with status {process.returncode}")
        try:
            with urllib.request.urlopen(f"{base}/api/services", timeout=2):
                return
        except Exception:
            time.sleep(0.5)
    raise RuntimeError(f"Jaeger port-forward did not become ready: {base}")


def _start_cgroup_sampler(cluster: Cluster, namespace: str, pod: str, container: str, output: Path) -> tuple[subprocess.Popen[str], Any]:
    output.parent.mkdir(parents=True, exist_ok=True)
    # `oc cp` requires tar in the destination container. OpenShell sandbox
    # images intentionally do not include tar, so stream the sampler through
    # stdin instead. This works with the minimal sandbox image as long as sh
    # and cat are present, and avoids changing the image or cluster state.
    sampler = (ROOT / "scripts/cgroup_sampler.sh").read_text(encoding="utf-8")
    cluster.run(
        ["-n", namespace, "exec", "-i", pod, "-c", container, "--", "sh", "-c", "cat > /tmp/trace-replay-cgroup-sampler.sh && chmod 700 /tmp/trace-replay-cgroup-sampler.sh"],
        input_text=sampler,
    )
    handle = output.open("w", encoding="utf-8")
    process = subprocess.Popen(
        ["oc", "-n", namespace, "exec", pod, "-c", container, "--", "env", "CGROUP_SAMPLE_INTERVAL=0.100", "sh", "/tmp/trace-replay-cgroup-sampler.sh"],
        env=cluster.env,
        stdout=handle,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    return process, handle


def _stop_cgroup_samplers(samplers: list[tuple[subprocess.Popen[str], Any]]) -> None:
    for process, _ in samplers:
        process.terminate()
    for process, handle in samplers:
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
        handle.close()


def _merge_sandbox_cgroup_samples(cgroup_dir: Path) -> None:
    """Merge one file per sandbox into the canonical resource series.

    Multiple OpenShell sandboxes can be observed during one trace.  Writing
    all samplers directly to one file lets concurrent stdout streams
    interleave and corrupt rows.  Keep each sampler isolated, then merge only
    complete numeric rows after sampling stops.
    """
    raw_dir = cgroup_dir / "sandbox_samples"
    rows: list[tuple[int, int, int]] = []
    for path in sorted(raw_dir.glob("*.csv")) if raw_dir.exists() else []:
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        for line in lines[1:]:
            fields = line.split(",")
            if len(fields) != 3:
                continue
            try:
                epoch_ns, cpu_usage_usec, memory_bytes = (int(field) for field in fields)
            except ValueError:
                continue
            if epoch_ns > 0 and cpu_usage_usec >= 0 and memory_bytes >= 0:
                rows.append((epoch_ns, cpu_usage_usec, memory_bytes))
    rows.sort(key=lambda row: row[0])
    output = cgroup_dir / "openshell_sandbox.csv"
    with output.open("w", encoding="utf-8") as handle:
        handle.write("epoch_ns,cpu_usage_usec,memory_bytes\n")
        for epoch_ns, cpu_usage_usec, memory_bytes in rows:
            handle.write(f"{epoch_ns},{cpu_usage_usec},{memory_bytes}\n")


def _valid_cgroup_row_count(path: Path) -> int:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return 0
    count = 0
    for line in lines[1:]:
        fields = line.split(",")
        if len(fields) != 3:
            continue
        try:
            epoch_ns, cpu_usage_usec, memory_bytes = (int(field) for field in fields)
        except ValueError:
            continue
        if epoch_ns > 0 and cpu_usage_usec >= 0 and memory_bytes >= 0:
            count += 1
    return count


def _epoch_from_k8s(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _decode_json_log_events(text: str, session_id: str, event_name: str) -> list[dict[str, Any]]:
    """Extract this session's AppWorld events from a pod log stream.

    The AppWorld deployment has existed in two forms: one writes a raw event
    file, while the current deployment emits JSON events directly to stdout.
    Keep the collector independent of that deployment detail and only retain
    canonical tool-event objects for the requested replay session.
    """
    events: list[dict[str, Any]] = []
    for line in text.splitlines():
        candidate = line.strip()
        if not candidate:
            continue
        try:
            value = json.loads(candidate)
        except json.JSONDecodeError:
            # Container log prefixes or wrapped JSON should not make an
            # otherwise valid event disappear.
            start = candidate.find("{")
            if start < 0:
                continue
            try:
                value = json.loads(candidate[start:])
            except json.JSONDecodeError:
                continue
        if not isinstance(value, dict):
            continue
        if value.get("event") != event_name or str(value.get("session_id") or "") != session_id:
            continue
        events.append(value)
    return events


def _collect_appworld_events(
    cluster: Cluster,
    system_ns: str,
    appworld_pod: str,
    session_id: str,
    start: float,
) -> list[dict[str, Any]]:
    """Collect only current-run AppWorld events from the Kubernetes log window.

    Raw files and unbounded pod logs are deliberately excluded here.  Both can
    contain events from an earlier replay with the same dataset session ID,
    which would make a run look valid even when the current sandbox never
    reached the backend.
    """
    since = datetime.fromtimestamp(max(0.0, start - 5.0), timezone.utc).isoformat().replace("+00:00", "Z")
    recent_logs = cluster.run(
        ["-n", system_ns, "logs", appworld_pod, "--timestamps", "--since-time", since],
        check=False,
    )
    return _decode_json_log_events(recent_logs, session_id, "appworld_tool")


def _collect_backend_events(
    cluster: Cluster,
    backend: Any,
    session_id: str,
    start: float,
) -> list[dict[str, Any]]:
    """Collect only the current session from the resolved backend deployment.

    Newer workload pods emit canonical events to stdout, while the existing
    backend keeps the same events in its read-only ``/events``
    endpoint and suppresses request logs.  Prefer the bounded log window and
    fall back to that endpoint through ``oc exec`` when the log stream is
    empty; this changes no backend or cluster state.
    """
    since = datetime.fromtimestamp(max(0.0, start - 5.0), timezone.utc).isoformat().replace("+00:00", "Z")
    logs = cluster.run(
        ["-n", backend.namespace, "logs", f"deployment/{backend.name}", "--timestamps", "--since-time", since],
        check=False,
    )
    events = _decode_json_log_events(logs, session_id, backend.event_name)
    if events:
        return events

    # The domain backend exposes /events on its loopback listener.  Read it
    # from the existing deployment so the controller does not need a new
    # Service, port-forward, or pod mutation.
    port = int(getattr(backend, "port", 0) or 0)
    if not port:
        return []
    fetch = (
        "import json,urllib.request; "
        f"print(json.dumps(json.load(urllib.request.urlopen('http://127.0.0.1:{port}/events'))))"
    )
    raw = cluster.run(
        ["-n", backend.namespace, "exec", f"deployment/{backend.name}", "--", "python", "-c", fetch],
        check=False,
    ).strip()
    payload: Any = None
    for line in reversed(raw.splitlines()):
        candidate = line.strip()
        if not candidate:
            continue
        try:
            payload = json.loads(candidate)
            break
        except json.JSONDecodeError:
            continue
    if not isinstance(payload, dict) or not isinstance(payload.get("events"), list):
        return []
    filtered: list[dict[str, Any]] = []
    seen_positions: set[tuple[Any, Any, str]] = set()
    for event in payload["events"]:
        if not isinstance(event, dict):
            continue
        if event.get("event") != backend.event_name or str(event.get("session_id") or "") != session_id:
            continue
        # A retry can append the same turn/call again.  Keep one canonical
        # event per recorded position so a valid retry is not rejected as a
        # duplicate stream.
        key = (event.get("turn"), event.get("call"), str(event.get("tool") or ""))
        if key in seen_positions:
            continue
        seen_positions.add(key)
        filtered.append(event)
    return filtered


def _preflight_pod_capacity(
    cluster: Cluster,
    agent_nodes: list[str] | tuple[str, ...],
    additional_pods_by_node: dict[str, int] | None = None,
) -> None:
    """Reject a run that would exceed a node's pod budget before mutation.

    A concurrent trace uses one OpenClaw pod, one trace Job pod, and one
    OpenShell sandbox pod. ``additional_pods_by_node`` accounts for
    run-scoped services that have not been created yet. This is deliberately
    conservative and counts only Pending/Running pods; completed Jobs do not
    consume scheduler capacity.
    """
    grouped: dict[str, int] = {}
    for node in agent_nodes:
        grouped[node] = grouped.get(node, 0) + 1
    failures: list[str] = []
    for node in (additional_pods_by_node or {}):
        grouped.setdefault(node, 0)
    for node, agent_count in grouped.items():
        node_obj = cluster.json(["get", "node", node])
        allocatable = node_obj.get("status", {}).get("allocatable", {})
        raw_limit = allocatable.get("pods") or node_obj.get("status", {}).get("capacity", {}).get("pods")
        try:
            pod_limit = parse_pod_quantity(raw_limit)
        except (TypeError, ValueError):
            LOG.warning("preflight: unable to read pod capacity for %s; skipping pod-budget check", node)
            continue
        pods = cluster.json(["get", "pods", "-A", "--field-selector", f"spec.nodeName={node}"])
        active = sum(
            1 for item in pods.get("items", [])
            if str(item.get("status", {}).get("phase") or "") in {"Pending", "Running"}
        )
        required = agent_count * 3 + int((additional_pods_by_node or {}).get(node, 0))
        remaining = pod_limit - active
        LOG.info(
            "preflight: node=%s active_pods=%d pod_limit=%d required_for_run=%d remaining=%d",
            node,
            active,
            pod_limit,
            required,
            remaining,
        )
        if active + required > pod_limit:
            failures.append(
                f"{node}: {active} active pods + {required} required pods exceeds allocatable pod limit {pod_limit}"
            )
    if failures:
        raise RuntimeError(
            "pod-capacity preflight failed before cluster mutation:\n- " + "\n- ".join(failures)
        )


def _capacity_aware_agent_assignments(
    cluster: Cluster,
    agent_count: int,
    target_nodes: tuple[str, ...],
    additional_pods_by_node: dict[str, int] | None = None,
) -> list[str]:
    """Assign slots using current pod headroom without evicting anything.

    The two-node run can start with different unrelated pod occupancy. A fixed
    50/50 split can therefore fail even when the aggregate headroom is enough.
    This read-only calculation reserves the same three active pods per agent
    plus any not-yet-created run-scoped pods supplied by the caller.
    It never changes scheduling, pods, nodes, or quotas.
    """
    remaining: dict[str, int] = {}
    for node in target_nodes:
        node_obj = cluster.json(["get", "node", node])
        allocatable = node_obj.get("status", {}).get("allocatable", {})
        raw_limit = allocatable.get("pods") or node_obj.get("status", {}).get("capacity", {}).get("pods")
        try:
            pod_limit = parse_pod_quantity(raw_limit)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"unable to read pod capacity for approved node {node}") from exc
        pods = cluster.json(["get", "pods", "-A", "--field-selector", f"spec.nodeName={node}"])
        active = sum(
            1 for item in pods.get("items", [])
            if str(item.get("status", {}).get("phase") or "") in {"Pending", "Running"}
        )
        additional = int((additional_pods_by_node or {}).get(node, 0))
        remaining[node] = (pod_limit - active - additional) // 3
        LOG.info(
            "preflight placement: node=%s active_pods=%d pod_limit=%d safe_agent_slots=%d",
            node,
            active,
            pod_limit,
            max(0, remaining[node]),
        )
    if sum(max(0, count) for count in remaining.values()) < agent_count:
        raise RuntimeError(
            "insufficient non-destructive pod headroom for the requested run: "
            f"requested={agent_count}, safe_slots="
            + str(sum(max(0, count) for count in remaining.values()))
        )
    if any(remaining[node] < 1 for node in target_nodes):
        raise RuntimeError("each approved target node must have at least one safe agent slot")

    assignments: list[str] = []
    cursor = 0
    while len(assignments) < agent_count:
        for _ in target_nodes:
            node = target_nodes[cursor % len(target_nodes)]
            cursor += 1
            if remaining[node] <= 0:
                continue
            assignments.append(node)
            remaining[node] -= 1
            break
        else:
            raise RuntimeError("capacity-aware placement exhausted unexpectedly")
    return assignments


def _preflight_kueue_admission(
    cluster: Cluster,
    namespace: str = DEFAULT_KUEUE_NAMESPACE,
    webhook_service: str = DEFAULT_KUEUE_WEBHOOK_SERVICE,
) -> None:
    """Verify the Job admission path before creating any run-owned objects.

    The benchmark creates real Kubernetes Jobs. On this cluster those Jobs are
    admitted through Kueue's mutating webhook. A Running controller pod is not
    sufficient: the Service must have a ready endpoint and the controller
    pod must report Ready. This read-only gate prevents a 100-agent run from
    producing a partial corpus when admission is already broken.
    """
    service = cluster.json(["-n", namespace, "get", "service", webhook_service])
    if not service.get("spec", {}).get("clusterIP"):
        raise RuntimeError(
            f"Kueue admission preflight failed: service {namespace}/{webhook_service} has no ClusterIP"
        )

    endpoint_addresses: list[str] = []
    endpoints = cluster.json(["-n", namespace, "get", "endpoints", webhook_service])
    for subset in endpoints.get("subsets") or []:
        endpoint_addresses.extend(
            str(address.get("ip"))
            for address in subset.get("addresses") or []
            if address.get("ip")
        )
    if not endpoint_addresses:
        # EndpointSlice is the preferred API on newer Kubernetes versions.
        slices = cluster.json(
            [
                "-n",
                namespace,
                "get",
                "endpointslice",
                "-l",
                f"kubernetes.io/service-name={webhook_service}",
            ]
        )
        for item in slices.get("items") or []:
            for endpoint in item.get("endpoints") or []:
                conditions = endpoint.get("conditions") or {}
                if conditions.get("ready") is False:
                    continue
                endpoint_addresses.extend(str(address) for address in endpoint.get("addresses") or [])
    if not endpoint_addresses:
        raise RuntimeError(
            f"Kueue admission preflight failed: {namespace}/{webhook_service} has no ready endpoints; "
            "refusing to create trace Jobs"
        )

    pods = cluster.json(["-n", namespace, "get", "pods", "-l", "control-plane=controller-manager"])
    ready_pods = []
    for pod in pods.get("items") or []:
        statuses = (pod.get("status") or {}).get("containerStatuses") or []
        if statuses and all(status.get("ready") is True for status in statuses):
            ready_pods.append(str((pod.get("metadata") or {}).get("name") or ""))
    if not ready_pods:
        raise RuntimeError(
            f"Kueue admission preflight failed: no Ready controller-manager pod in {namespace}; "
            "refusing to create trace Jobs"
        )
    LOG.info(
        "preflight: Kueue admission healthy service=%s/%s endpoints=%d ready_controllers=%s",
        namespace,
        webhook_service,
        len(endpoint_addresses),
        ",".join(name for name in ready_pods if name) or "unknown",
    )


def preflight(
    cluster: Cluster,
    agents: int,
    system_ns: str,
    openshell_ns: str,
    *,
    allow_cross_node: bool = False,
    agent_nodes: list[str] | tuple[str, ...] | None = None,
    kueue_namespace: str = DEFAULT_KUEUE_NAMESPACE,
    kueue_webhook_service: str = DEFAULT_KUEUE_WEBHOOK_SERVICE,
) -> None:
    log_phase(
        "preflight: validating cluster, target nodes and system services "
        f"({', '.join(cluster.nodes)})"
    )
    context = cluster.run(["config", "current-context"])
    user = cluster.run(["whoami"])
    _preflight_kueue_admission(cluster, kueue_namespace, kueue_webhook_service)
    for target_node in cluster.nodes:
        node = cluster.json(["get", "node", target_node])
        ready = any(
            c.get("type") == "Ready" and c.get("status") == "True"
            for c in node.get("status", {}).get("conditions", [])
        )
        if not ready or (node.get("spec", {}).get("unschedulable") and not allow_cross_node):
            raise RuntimeError(f"target node is not ready/schedulable: {target_node}")
    LOG.info("context=%s user=%s nodes=%s", context.strip(), user.strip(), ",".join(cluster.nodes))
    cluster.wait_rollout_in(system_ns, BASE_JAEGER)
    gateway_statefulsets = cluster.run(
        [
            "-n",
            openshell_ns,
            "get",
            "statefulset",
            "-l",
            "app.kubernetes.io/name=openshell",
            "--no-headers",
        ],
        check=False,
    )
    if not gateway_statefulsets.strip():
        raise RuntimeError(f"no OpenShell gateway StatefulSet found in namespace {openshell_ns}")
    services = cluster.run(["-n", cluster.namespace, "get", "svc", BASE_OPENCLAW], check=False)
    if not services.strip():
        raise RuntimeError(f"base {BASE_OPENCLAW} service is missing; refusing to invent a deployment template")
    validate_agent_count(agents)
    if agent_nodes is not None and not allow_cross_node:
        _preflight_pod_capacity(cluster, agent_nodes)


def _choose_safe_auxiliary_node(cluster: Cluster, target_nodes: tuple[str, ...]) -> str:
    """Choose an approved node for short-lived controller-created Jobs.

    Agent and sandbox placement remains tied to each agent's recorded node.
    Temporary driver/staging Jobs do not mount those RWO workspaces and can
    therefore run on any healthy approved node.  Do not bypass a disk-pressure
    taint: choose another approved node instead, or fail before creating more
    workload if none is safe.
    """
    blocked: list[str] = []
    for node_name in target_nodes:
        node = cluster.json(["get", "node", node_name])
        ready = any(
            condition.get("type") == "Ready" and condition.get("status") == "True"
            for condition in node.get("status", {}).get("conditions", [])
        )
        taints = node.get("spec", {}).get("taints") or []
        disk_blocked = any(
            taint.get("key") == "node.kubernetes.io/disk-pressure"
            and taint.get("effect") in {"NoSchedule", "NoExecute"}
            for taint in taints
        )
        if ready and not disk_blocked:
            return node_name
        blocked.append(node_name)
    raise RuntimeError(
        "no approved healthy node is available for temporary experiment Jobs; "
        f"disk-pressure or readiness blocked: {', '.join(blocked)}"
    )


def _retry_cluster_read_stage(
    label: str,
    operation: Any,
    timeout_seconds: float,
    interval_seconds: float,
) -> Any:
    """Keep read-only preparation alive while the API/Kueue path recovers."""
    deadline = time.monotonic() + timeout_seconds
    attempt = 0
    while True:
        try:
            return operation()
        except RuntimeError as exc:
            message = str(exc).lower()
            retryable = (
                is_transient_api_error(message)
                or "timed out after" in message
                or "context deadline" in message
                or "kueue admission preflight failed" in message
                or "no ready endpoints" in message
            )
            if not retryable or time.monotonic() >= deadline:
                raise
            attempt += 1
            remaining = max(0.0, deadline - time.monotonic())
            delay = min(interval_seconds, remaining)
            log_phase(
                f"{label}: cluster read not ready; retrying in {delay:.1f}s "
                f"(attempt {attempt}, {remaining:.0f}s remaining)"
            )
            time.sleep(delay)


def _wait_for_phase(cluster: Cluster, namespace: str, kind: str, name: str, expected: str, timeout: int = 600) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        obj = cluster.json(["-n", namespace, "get", kind, name])
        phase = str(obj.get("status", {}).get("phase") or "")
        if phase == expected:
            return
        if phase in {"Failed", "Unknown"}:
            raise RuntimeError(f"{kind}/{name} entered unexpected phase {phase}")
        time.sleep(5)
    raise RuntimeError(f"timed out waiting for {kind}/{name} phase={expected}")


def ensure_appworld_data(
    cluster: Cluster,
    namespace: str,
    node: str,
    pvc_name: str,
    storage_class: str,
    local_data: Path,
) -> None:
    """Stage the real AppWorld data into an owned, node-local RWO PVC.

    AppWorld is stateful and its data is not part of the public container image.
    The PVC is deliberately RWO because the benchmark backend is a single pod
    pinned to the experiment node. Existing PVCs are reused only when they carry
    the controller's ownership label.
    """
    required = [local_data / "base_dbs", local_data / "tasks"]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise RuntimeError(f"AppWorld data is incomplete; missing: {', '.join(missing)}")

    pvc_spec: dict[str, Any] = {
        "accessModes": ["ReadWriteOnce"],
        "resources": {"requests": {"storage": "1Gi"}},
    }
    if storage_class:
        pvc_spec["storageClassName"] = storage_class
    pvc = {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            "name": pvc_name,
            "namespace": namespace,
            "labels": {"managed-by": "trace-replay-experiment", "component": "appworld-data"},
        },
        "spec": pvc_spec,
    }
    existing = cluster.run(["-n", namespace, "get", "pvc", pvc_name], check=False).strip()
    if existing:
        current = cluster.json(["-n", namespace, "get", "pvc", pvc_name])
        labels = current.get("metadata", {}).get("labels") or {}
        if labels.get("managed-by") != "trace-replay-experiment":
            raise RuntimeError(f"refusing to use unowned AppWorld PVC {namespace}/{pvc_name}")
    else:
        cluster.apply_json_in(namespace, pvc)
    stage_name = "trace-replay-appworld-data-stage"
    stage = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": stage_name,
            "namespace": namespace,
            "labels": {"managed-by": "trace-replay-experiment", "component": "appworld-data-stage"},
        },
        "spec": {
            "restartPolicy": "Never",
            "securityContext": {"fsGroupChangePolicy": "OnRootMismatch"},
            "nodeSelector": {"kubernetes.io/hostname": node},
            "containers": [{
                "name": "stage",
                "image": "ghcr.io/stonybrooknlp/appworld:latest",
                "command": ["/bin/sh", "-lc", "mkdir -p /appworld/data && sleep 900"],
                "volumeMounts": [{"name": "appworld-data", "mountPath": "/appworld"}],
            }],
            "volumes": [{"name": "appworld-data", "persistentVolumeClaim": {"claimName": pvc_name}}],
        },
    }
    cluster.apply_json_in(namespace, stage)
    try:
        # ibmc-vpc-block-* uses WaitForFirstConsumer. The pod must exist before
        # the PVC can bind, and its node selector pins both to the experiment node.
        deadline = time.time() + 600
        while time.time() < deadline:
            phase = str(cluster.json(["-n", namespace, "get", "pvc", pvc_name]).get("status", {}).get("phase") or "")
            if phase == "Bound":
                break
            if phase == "Lost":
                raise RuntimeError(f"AppWorld PVC {namespace}/{pvc_name} entered Lost state")
            time.sleep(5)
        else:
            raise RuntimeError(f"timed out waiting for AppWorld PVC {namespace}/{pvc_name} to bind")
        _wait_for_phase(cluster, namespace, "pod", stage_name, "Running", timeout=600)
        present = cluster.run(
            ["-n", namespace, "exec", stage_name, "--", "sh", "-lc", "test -f /appworld/data/base_dbs/spotify.db && printf PRESENT"],
            check=False,
        ).strip()
        if "PRESENT" not in present:
            log_phase(f"appworld: staging real data from {local_data} into {namespace}/{pvc_name}")
            cluster.run(["-n", namespace, "rsync", f"{local_data}/", f"pod/{stage_name}:/appworld/data/"])
        cluster.run(
            ["-n", namespace, "exec", stage_name, "--", "sh", "-lc", "test -f /appworld/data/base_dbs/spotify.db && test -d /appworld/data/tasks"],
        )
    finally:
        cluster.delete_owned(namespace, "pod", stage_name)


def ensure_workload_backends(
    cluster: Cluster,
    backends: dict[str, Any],
    allocation: dict[str, int],
    run_id: str,
    *,
    appworld_task_map: Path | None = None,
) -> dict[str, Any]:
    """Deploy/check only explicitly configured, experiment-owned backends.

    A managed backend manifest must contain resources whose names begin with
    ``trace-replay-``. Existing backends are observed and health-gated but never
    restarted or edited. This function intentionally has no fallback to the
    generic replay fixture.
    """
    resolved: dict[str, Any] = {}
    for workload in allocation:
        spec = backends[workload]
        if spec.deployment_mode == "managed":
            if not spec.manifest:
                raise RuntimeError(f"managed backend {workload} has no manifest")
            manifest_path = Path(spec.manifest)
            if not manifest_path.is_absolute():
                manifest_path = ROOT / manifest_path
            if not manifest_path.exists():
                raise RuntimeError(f"managed backend manifest not found for {workload}: {manifest_path}")
            source_name = f"{spec.service}-src"
            if spec.benchmark == "appworld":
                if appworld_task_map is None or not appworld_task_map.exists():
                    raise RuntimeError(
                        "AppWorld requires a session-to-task mapping file; "
                        "pass --appworld-task-map"
                    )
                try:
                    mapping = json.loads(appworld_task_map.read_text(encoding="utf-8"))
                except json.JSONDecodeError as exc:
                    raise RuntimeError(f"invalid AppWorld task map: {appworld_task_map}") from exc
                if not isinstance(mapping, dict) or not mapping:
                    raise RuntimeError(f"AppWorld task map is empty: {appworld_task_map}")
                source_data = {"appworld_service.py": (ROOT / "scripts/appworld_service.py").read_text(encoding="utf-8")}
                cluster.upsert_owned_secret(
                    spec.namespace,
                    {
                        "apiVersion": "v1",
                        "kind": "Secret",
                        "metadata": {
                            "name": "trace-replay-appworld-task-map",
                            "labels": {"managed-by": "trace-replay-experiment", "component": "appworld-task-map"},
                        },
                        "type": "Opaque",
                        "data": {
                            "session-task-map.json": base64.b64encode(
                                json.dumps(mapping, indent=2).encode("utf-8")
                            ).decode("ascii")
                        },
                    },
                )
            else:
                source_data = {"domain_backend.py": (ROOT / "scripts/domain_backend.py").read_text(encoding="utf-8")}
            cluster.upsert_owned_configmap(spec.namespace, {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": source_name,
                    "labels": {"managed-by": "trace-replay-experiment", "component": "workload-backend", "workload": workload},
                },
                "data": source_data,
            })
            manifest_text = manifest_path.read_text(encoding="utf-8")
            # Backend services are single, run-owned control-plane services.
            # Render their node selector from the requested control node
            # instead of silently applying the historical hard-coded node
            # from the checked-in template.
            manifest_text = re.sub(
                r"(kubernetes\.io/hostname:\s*)[^\s#]+",
                rf"\g<1>{cluster.node}",
                manifest_text,
            )
            documents = manifest_text.split("\n---")
            for document in documents:
                try:
                    resource = json.loads(document) if document.lstrip().startswith("{") else None
                except json.JSONDecodeError:
                    resource = None
                if resource:
                    name = str(resource.get("metadata", {}).get("name") or "")
                    if name and not name.startswith("trace-replay-"):
                        raise RuntimeError(f"managed backend manifest contains unowned resource {name}")
                else:
                    names = re.findall(r"^\s+name:\s*([A-Za-z0-9][A-Za-z0-9-]*)\s*$", document, re.MULTILINE)
                    if names and not names[0].startswith("trace-replay-"):
                        raise RuntimeError(f"managed backend manifest contains unowned resource {names[0]}")
            cluster.apply_yaml_in(spec.namespace, manifest_text)
        # A real backend must expose its Service. For an existing backend this
        # is strictly read-only; for a managed one it follows the apply above.
        service = cluster.run(["-n", spec.namespace, "get", "svc", spec.service], check=False).strip()
        if not service:
            raise RuntimeError(f"backend service is missing for {workload}: {spec.namespace}/{spec.service}")
        deployment = cluster.run(["-n", spec.namespace, "get", "deployment", spec.name], check=False).strip()
        if deployment:
            cluster.wait_rollout_in(spec.namespace, spec.name)
        resolved[workload] = spec
        log_phase(f"backend ready: {workload} -> {spec.base_url} ({spec.deployment_mode})")
    return resolved


def _resolve_backend_service_endpoints(
    cluster: Cluster,
    backends: dict[str, Any],
    workloads: dict[str, int],
) -> dict[str, str]:
    """Resolve selected backend Services to stable in-cluster IP endpoints.

    OpenShell sandboxes use an isolated network namespace and cannot rely on
    Kubernetes DNS resolution.  Service ClusterIPs are the narrowest
    experiment-scoped endpoint that works from those sandboxes; this helper is
    read-only and never changes the Service or cluster DNS configuration.
    """
    resolved: dict[str, str] = {}
    for workload in workloads:
        spec = backends[workload]
        service = cluster.json(["-n", spec.namespace, "get", "svc", spec.service])
        cluster_ip = str(service.get("spec", {}).get("clusterIP") or "")
        if not cluster_ip or cluster_ip == "None":
            raise RuntimeError(
                f"backend service {spec.namespace}/{spec.service} has no ClusterIP; "
                "cannot safely route sandbox traffic"
            )
        resolved[workload] = f"http://{cluster_ip}:{spec.port}"
    return resolved


def _rewrite_queue_backend_endpoints(
    queue: list[dict[str, Any]],
    backends: dict[str, Any],
    endpoint_map: dict[str, str],
) -> None:
    """Replace DNS backend URLs in queued sandbox commands with Service IPs."""
    for item in queue:
        session = item.get("session") or {}
        for turn in session.get("turns") or []:
            for tool in turn.get("tool_calls") or []:
                command = tool.get("mapped_command")
                if not isinstance(command, list):
                    continue
                rewritten = [str(part) for part in command]
                for workload, spec in backends.items():
                    old = spec.base_url.rstrip("/")
                    new = endpoint_map[workload].rstrip("/")
                    rewritten = [part.replace(old, new) for part in rewritten]
                # AppWorld commands are transported as a base64-encoded
                # Python client to stay below OpenShell's remote-command
                # limit.  Rewrite URLs inside that payload as well; a plain
                # string replacement cannot see the embedded Service DNS
                # name and would leave sandboxes using an unreachable DNS
                # endpoint.
                if len(rewritten) >= 3 and rewritten[0] == "/bin/sh" and rewritten[1] == "-c":
                    match = re.fullmatch(r"echo ([A-Za-z0-9+/=]+)\|base64 -d\|python3", rewritten[2])
                    if match:
                        try:
                            decoded = base64.b64decode(match.group(1)).decode("utf-8")
                        except (ValueError, UnicodeDecodeError):
                            decoded = ""
                        if decoded:
                            for workload, spec in backends.items():
                                old = spec.base_url.rstrip("/")
                                new = endpoint_map[workload].rstrip("/")
                                decoded = decoded.replace(old, new)
                            rewritten[2] = (
                                "echo "
                                + base64.b64encode(decoded.encode("utf-8")).decode("ascii")
                                + "|base64 -d|python3"
                            )
                tool["mapped_command"] = rewritten


def _clone_config(
    cluster: Cluster,
    source: str,
    target: str,
    workspace: str,
    *,
    pin_sandbox: bool = True,
    shared_workspace_pvc: str | None = None,
    mock_service: str | None = None,
    gateway_endpoint: str | None = None,
) -> str:
    config = cluster.json(["-n", cluster.namespace, "get", "configmap", source])
    for key in ("uid", "resourceVersion", "creationTimestamp", "managedFields"):
        config.get("metadata", {}).pop(key, None)
    config["metadata"]["name"] = target
    config["metadata"]["labels"] = {"app": target, "managed-by": "trace-replay-experiment"}
    raw = json.loads(config["data"]["openclaw.json"])
    entry = raw.setdefault("plugins", {}).setdefault("entries", {}).setdefault("openshell", {})
    # Use a per-agent wrapper that injects the Kubernetes driver configuration
    # at sandbox-create time. The OpenShell sandbox pod is created by the
    # gateway after the workspace exists, so setting a nodeSelector only on the
    # OpenClaw deployment does not constrain the sandbox pod itself.
    entry["config"]["command"] = "/opt/openshell/bin/openshell-pinned" if pin_sandbox else "/opt/openshell/bin/openshell"
    # The supported plugin version for the public OpenClaw image reads the
    # workspace from OPENSHELL_WORKSPACE.  Keeping it out of plugin config also
    # lets the init container validate the config before the plugin is installed.
    entry["config"].pop("workspace", None)
    # OpenShell 0.0.116 reserves /sandbox/.openshell/workspace for the
    # mounted workspace.  The OpenClaw remote-mode seed path clears the
    # configured remoteWorkspaceDir before uploading; setting it to /sandbox
    # therefore attempts to remove that mount and fails with EBUSY.  Keep the
    # OpenClaw-managed remote workspace in a sibling directory while the
    # experiment's Kubernetes workspace mount remains available at its
    # supported /sandbox/.openshell/workspace path (used by SWE commands).
    entry["config"]["remoteWorkspaceDir"] = "/sandbox/workspace"
    if gateway_endpoint:
        entry["config"]["gatewayEndpoint"] = gateway_endpoint
    # Long research traces can legitimately spend several minutes crossing
    # repeated model/tool boundaries.  The base deployment carries a 180s
    # sandbox timeout for ordinary interactive use; raise it only in this
    # experiment-owned config so a long recorded session is not truncated.
    entry["config"]["timeoutSeconds"] = int(os.environ.get("TRACE_REPLAY_OPEN_SHELL_TIMEOUT_SECONDS", "900"))
    if mock_service:
        for provider in raw.get("models", {}).get("providers", {}).values():
            if isinstance(provider, dict) and provider.get("baseUrl"):
                provider["baseUrl"] = "http://127.0.0.1:8080/v1"
                # OpenClaw requires a credential for endpoint providers even
                # when the in-cluster mock does not authenticate requests.
                # Without this field it fails before contacting the simulator.
                provider["apiKey"] = "change-me-gateway-token"
    # The replay corpus contains deliberately long research/tool-output
    # histories.  The experiment uses a deterministic simulator, so there is
    # no inference-memory trade-off here; the harness must still accept the
    # recorded context in order to measure its assembly and OpenShell path.
    # Keep this override scoped to experiment-owned OpenClaw configs.
    context_limit = int(os.environ.get("TRACE_REPLAY_REPLAY_CONTEXT_LENGTH", "262144"))
    model_sets = [raw.get("agents", {}).get("defaults", {}).get("models", {})]
    model_sets.extend(
        agent.get("models", {})
        for agent in raw.get("agents", {}).get("list", [])
        if isinstance(agent, dict)
    )
    for models in model_sets:
        if not isinstance(models, dict):
            continue
        for model_config in models.values():
            if not isinstance(model_config, dict):
                continue
            params = model_config.setdefault("params", {})
            if isinstance(params, dict):
                params["contextLength"] = context_limit
    rendered_config = json.dumps(raw, sort_keys=True, separators=(",", ":"))
    config["data"]["openclaw.json"] = rendered_config
    cluster.apply_json(config)
    return hashlib.sha256(rendered_config.encode("utf-8")).hexdigest()


def _ensure_openshell_workspace(
    cluster: Cluster,
    deployment: str,
    workspace: str,
    gateway_endpoint: str,
) -> None:
    cli = "/opt/openshell/bin/openshell"
    common = ["-n", cluster.namespace, "exec", f"deployment/{deployment}", "-c", "gateway", "--", cli]
    existing = cluster.run([*common, "workspace", "get", workspace, "--gateway-endpoint", gateway_endpoint], check=False)
    if existing.strip():
        return
    cluster.run([*common, "workspace", "create", "--name", workspace, "--gateway-endpoint", gateway_endpoint])


def _ensure_openshell_client_secret(
    cluster: Cluster,
    source_namespace: str,
    source_name: str,
    target_name: str,
) -> str:
    """Copy the gateway client bundle into the agent namespace under an owned name.

    OpenShell's Kubernetes TLS Secret is namespaced. The gateway and OpenClaw
    agent usually live in separate namespaces, so a fresh run publishes a
    run-scoped copy for the agent pod. Secret data is never logged.
    """
    source = cluster.json(["-n", source_namespace, "get", "secret", source_name])
    existing = cluster.run(["-n", cluster.namespace, "get", "secret", target_name], check=False).strip()
    if existing:
        current = cluster.json(["-n", cluster.namespace, "get", "secret", target_name])
        labels = current.get("metadata", {}).get("labels", {})
        if labels.get("managed-by") != "trace-replay-experiment":
            raise RuntimeError(
                f"refusing to modify unowned client Secret {cluster.namespace}/{target_name}"
            )
    target = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": target_name,
            "namespace": cluster.namespace,
            "labels": {"managed-by": "trace-replay-experiment"},
        },
        "type": source.get("type", "Opaque"),
        "data": source.get("data", {}),
    }
    cluster.apply_json_in(cluster.namespace, target)
    return target_name


def deploy_agents(
    cluster: Cluster,
    agents: int,
    *,
    reuse: bool,
    workspace_prefix: str,
    deployment_prefix: str | None = None,
    shared_workspace_pvc: str | None = None,
    appworld_endpoint: str | None = None,
    internal_api_endpoints: list[str] | None = None,
    mock_service: str | None = None,
    gateway_endpoint: str | None = None,
    openshell_client_secret: str | None = None,
    allow_cross_node: bool = False,
    agent_nodes: list[str] | tuple[str, ...] | None = None,
    shared_workspace_pvcs: dict[str, str] | None = None,
    sandbox_images: list[str | None] | None = None,
    agent_indices: list[int] | tuple[int, ...] | None = None,
    reconcile_indices: list[int] | tuple[int, ...] | None = None,
    reuse_existing_workspaces: bool = False,
    simulated_ttft_ms: float | None = None,
    simulated_itl_ms: float | None = None,
) -> list[tuple[str, str, str]]:
    log_phase(f"deployment: preparing {agents} OpenClaw agents with isolated OpenShell workspaces")
    planned_nodes = tuple(agent_nodes or (cluster.node,))
    if len(planned_nodes) != agents:
        raise ValueError("agent_nodes must contain exactly one node assignment per agent")
    if sandbox_images is not None and len(sandbox_images) != agents:
        raise ValueError("sandbox_images must contain exactly one image assignment per agent")
    global_indices = tuple(agent_indices or range(1, agents + 1))
    if len(global_indices) != agents or len(set(global_indices)) != agents or any(index < 1 for index in global_indices):
        raise ValueError("agent_indices must contain unique positive global agent indices")
    reconcile_set = set(reconcile_indices or global_indices)
    if not reconcile_set.issubset(global_indices):
        raise ValueError("reconcile_indices must refer to deployed global agent indices")
    log_phase(f"deployment: workspace storage map={shared_workspace_pvcs or shared_workspace_pvc or 'per-sandbox-default'}")

    def install_plugin_compat_shim(deployment_obj: dict[str, Any]) -> bool:
        """Patch the public image's stale SDK export in the shared OpenClaw home."""
        # OpenShell's Kubernetes API in this environment enforces a 19-character
        # sandbox-name limit, while the pinned plugin's generated name can be
        # up to 50 characters (``openclaw-<scope>-<hash>``).  Keep the first
        # readable character and retain the hash suffix for per-session
        # uniqueness.  Bump the marker so an already-deployed experiment fleet
        # receives this fix on the next idempotent reconciliation.
        marker = "# trace-replay-compat-isPathInside-v2-openshell-name"
        shim = (
            "              # trace-replay-compat-isPathInside-v1\n"
            "              node --input-type=module -e 'import fs from \"node:fs\"; import { execFileSync } from \"node:child_process\"; const files=execFileSync(\"find\",[\"/home/node/.openclaw/npm/projects\",\"-path\",\"*/@openclaw/openshell-sandbox/dist/index.js\",\"-print\"],{encoding:\"utf8\"}).trim().split(\"\\n\").filter(Boolean); for (const file of files) { let text=fs.readFileSync(file,\"utf8\"); if (text.includes(\"trace-replay-compat-isPathInside-v1\")) continue; const needle=\"import { isPathInside, root } from \\\"openclaw/plugin-sdk/file-access-runtime\\\";\"; if (!text.includes(needle)) continue; const replacement=\"import { root } from \\\"openclaw/plugin-sdk/file-access-runtime\\\";\\n// trace-replay-compat-isPathInside-v1\\nconst isPathInside=(basePath,candidatePath)=>{const base=path.resolve(basePath);const candidate=path.resolve(candidatePath);return candidate===base||candidate.startsWith(base+path.sep);};\"; fs.writeFileSync(file,text.replace(needle,replacement)); }'\n"
        )
        # Use a shell-native, idempotent replacement here.  Keeping the
        # compatibility edit on the shared OpenClaw home avoids modifying the
        # base image and works for both fresh and resumed deployments.
        shim = (
            "              # trace-replay-compat-isPathInside-v2-openshell-name\n"
            "              find /home/node/.openclaw/npm/projects -path '*/@openclaw/openshell-sandbox/dist/index.js' -exec sed -i 's@import { isPathInside, root } from \"openclaw/plugin-sdk/file-access-runtime\";@import { root } from \"openclaw/plugin-sdk/file-access-runtime\"; /* trace-replay-compat-isPathInside-v1 */ const isPathInside=(basePath,candidatePath)=>{const base=path.resolve(basePath);const candidate=path.resolve(candidatePath);return candidate===base||candidate.startsWith(base+path.sep);};@' {} +\n"
            "              # The upstream plugin builds names as openclaw-<scope>-<hash>\n"
            "              # (up to 50 chars); this gateway accepts at most 19.\n"
            "              find /home/node/.openclaw/npm/projects -path '*/@openclaw/openshell-sandbox/dist/index.js' -exec sed -i 's@slice(0, 32)@slice(0, 1)@g' {} +\n"
        )
        for init_container in deployment_obj.get("spec", {}).get("template", {}).get("spec", {}).get("initContainers", []):
            if init_container.get("name") != "init-config":
                continue
            args = init_container.setdefault("args", [""])
            command = str(args[0] if args else "")
            if marker in command:
                return False
            anchor = "node openclaw.mjs doctor"
            anchor_pos = command.find(anchor)
            if anchor_pos < 0:
                raise RuntimeError("base OpenClaw deployment init-config has no doctor step")
            line_start = command.rfind("\n", 0, anchor_pos) + 1
            args[0] = command[:line_start] + shim + command[line_start:]
            return True
        raise RuntimeError("base OpenClaw deployment has no init-config container")

    def node_for(index: int) -> str:
        return planned_nodes[index - 1]

    def global_index_for(index: int) -> int:
        return global_indices[index - 1]

    def sandbox_image_for(index: int) -> str | None:
        return sandbox_images[index - 1] if sandbox_images is not None else None

    def workspace_pvc_for(node: str) -> str | None:
        if shared_workspace_pvcs:
            try:
                return shared_workspace_pvcs[node]
            except KeyError as exc:
                raise ValueError(f"no workspace PVC configured for target node {node}") from exc
        return shared_workspace_pvc

    def install_local_mock_sidecar(deployment_obj: dict[str, Any]) -> bool:
        """Run an isolated replay simulator beside this OpenClaw gateway."""
        if not mock_service:
            return False
        prefix = mock_service.removesuffix("-mock-llm")
        source_name = f"{prefix}-mock-src"
        configmaps = cluster.json(["-n", cluster.namespace, "get", "configmaps", "-l", f"run={prefix}"]).get("items", [])
        corpus_names = sorted(str(x.get("metadata", {}).get("name") or "") for x in configmaps if "-corpus-" in str(x.get("metadata", {}).get("name") or ""))
        if not corpus_names:
            raise RuntimeError(f"local mock assets missing for {prefix}")
        template = deployment_obj.setdefault("spec", {}).setdefault("template", {})
        metadata = template.setdefault("metadata", {}).setdefault("annotations", {})
        revision = hashlib.sha256((prefix + ":" + ",".join(corpus_names)).encode()).hexdigest()
        changed = metadata.get("trace-replay.openai.com/local-mock-sha256") != revision
        if simulated_ttft_ms is not None or simulated_itl_ms is not None:
            changed = True  # env-var timing override requires a deployment update
        metadata["trace-replay.openai.com/local-mock-sha256"] = revision
        spec = template.setdefault("spec", {})
        managed = {"mock-src", "mock-edges", *(f"mock-corpus-{i:03d}" for i in range(len(corpus_names)))}
        spec["volumes"] = [v for v in spec.get("volumes", []) if v.get("name") not in managed]
        spec["volumes"].extend([{"name":"mock-src","configMap":{"name":source_name}}, *[{"name":f"mock-corpus-{i:03d}","configMap":{"name":name}} for i,name in enumerate(corpus_names)], {"name":"mock-edges","emptyDir":{}}])
        spec["containers"] = [c for c in spec.get("containers", []) if c.get("name") != "mock-llm"]
        _mock_env = [{"name":"REPLAY_CORPUS","value":"/data/corpus"},{"name":"REPLAY_MODE","value":"turns"},{"name":"REPLAY_TIMING","value":"humaneval-vllm-c1"},{"name":"MAP_TOOLS_TO_SANDBOX","value":"1"},{"name":"MOCK_EDGES_PATH","value":"/edges/mock_edges.jsonl"},{"name":"OTLP_ENDPOINT","value":f"http://{BASE_JAEGER}.{cluster.namespace}.svc.cluster.local:4318"}]
        if simulated_ttft_ms is not None:
            _mock_env.append({"name":"SIMULATED_TTFT_MS","value":str(simulated_ttft_ms)})
        if simulated_itl_ms is not None:
            _mock_env.append({"name":"SIMULATED_ITL_MS","value":str(simulated_itl_ms)})
        spec["containers"].append({"name":"mock-llm","image":"python:3.12-slim","command":["python","/app/mock_llm.py"],"ports":[{"name":"mock","containerPort":8080}],"env":_mock_env,"resources":{"requests":{"cpu":"50m","memory":"128Mi"},"limits":{"cpu":"1","memory":"512Mi"}},"readinessProbe":{"httpGet":{"path":"/health","port":"mock"},"initialDelaySeconds":1,"periodSeconds":2,"failureThreshold":30},"volumeMounts":[{"name":"mock-src","mountPath":"/app","readOnly":True}, *[{"name":f"mock-corpus-{i:03d}","mountPath":f"/data/corpus/part-{i:03d}","readOnly":True} for i in range(len(corpus_names))], {"name":"mock-edges","mountPath":"/edges"}]})
        return changed

    def pin_existing_deployment(
        name: str,
        workspace: str,
        node: str,
        workspace_pvc: str | None,
        index: int,
        config_sha256: str | None = None,
    ) -> bool:
        """Refresh node and sandbox pinning on an explicitly owned deployment."""
        deployment_obj = cluster.json(["-n", cluster.namespace, "get", "deployment", name])
        shim_changed = install_plugin_compat_shim(deployment_obj)
        sidecar_changed = install_local_mock_sidecar(deployment_obj)
        template = deployment_obj.setdefault("spec", {}).setdefault("template", {})
        template_metadata = template.setdefault("metadata", {})
        template_annotations = template_metadata.setdefault("annotations", {})
        config_hash_changed = False
        if config_sha256:
            annotation = "trace-replay.openai.com/openclaw-config-sha256"
            config_hash_changed = template_annotations.get(annotation) != config_sha256
            template_annotations[annotation] = config_sha256
        template_spec = template.setdefault("spec", {})
        if allow_cross_node:
            template_spec.pop("nodeSelector", None)
        else:
            template_spec["nodeSelector"] = {"kubernetes.io/hostname": node}
            _install_pinned_openshell_wrapper(
                deployment_obj,
                node,
                workspace,
                workspace_pvc,
                appworld_endpoint,
                internal_api_endpoints=internal_api_endpoints,
                gateway_endpoint=gateway_endpoint,
                sandbox_image=sandbox_image_for(index),
            )
        for container in template_spec.get("containers", []):
            if container.get("name") != "gateway":
                continue
            # Explicitly reused fleets may be pinned to a compatible OpenClaw
            # build.  Do not replace that image with the mutable experiment
            # `latest` tag: a restart can then reject the fleet's state DB
            # schema.  Fresh deployments set EXPERIMENT_OPENCLAW_IMAGE in
            # create_agent() below.
            env = container.setdefault("env", [])
            workspace_entry = next((item for item in env if item.get("name") == "OPENSHELL_FLEET_WORKSPACE"), None)
            if workspace_entry is None:
                env.append({"name": "OPENSHELL_FLEET_WORKSPACE", "value": workspace})
            else:
                workspace_entry["value"] = workspace
        cluster.apply_json(deployment_obj)
        return shim_changed or config_hash_changed or sidecar_changed

    if reuse and deployment_prefix:
        # A recovery run may reuse one explicitly named, experiment-owned
        # fleet. Refresh those deployments concurrently so a validation fix
        # does not require another long serial rollout. Every object is
        # ownership-checked before it is changed.
        def refresh_existing(index: int) -> tuple[str, str, str]:
            # ``index`` is the local position in this reconciliation batch;
            # ``global_index`` is the original fleet slot.  Resume calls can
            # pass a sparse ``agent_indices`` list (for example, [74]) while
            # providing a one-element ``agent_nodes`` list.  Using the global
            # slot to index the local node list misroutes that case and can
            # raise IndexError before any trace Jobs are created.
            global_index = global_index_for(index)
            stem = safe_name(deployment_prefix)
            name = f"{stem}-{global_index:02d}"
            config = f"{stem}-config-{global_index:02d}"
            workspace = workspace_name(workspace_prefix, global_index)
            target_node = node_for(index)
            target_workspace_pvc = workspace_pvc_for(target_node)
            log_phase(
                f"deployment slot {global_index:02d}: node={target_node} "
                f"workspace={workspace} pvc={target_workspace_pvc or 'per-sandbox-default'}"
            )
            if not cluster.run(["-n", cluster.namespace, "get", "deployment", name], check=False).strip():
                raise RuntimeError(f"expected owned deployment is missing: {cluster.namespace}/{name}")
            deployment_obj = cluster.json(["-n", cluster.namespace, "get", "deployment", name])
            if (deployment_obj.get("metadata", {}).get("labels") or {}).get("managed-by") != "trace-replay-experiment":
                raise RuntimeError(f"refusing to reuse unowned deployment {cluster.namespace}/{name}")
            if global_index not in reconcile_set:
                gateway = next(
                    (
                        container
                        for container in deployment_obj.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
                        if container.get("name") == "gateway"
                    ),
                    {},
                )
                configured_workspace = next(
                    (
                        item.get("value")
                        for item in gateway.get("env", [])
                        if item.get("name") == "OPENSHELL_FLEET_WORKSPACE"
                    ),
                    None,
                )
                return name, str(configured_workspace or workspace), target_node
            current_workspace = next(
                (
                    item.get("value")
                    for container in deployment_obj.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
                    if container.get("name") == "gateway"
                    for item in container.get("env", [])
                    if item.get("name") == "OPENSHELL_FLEET_WORKSPACE"
                ),
                None,
            )
            # The wrapper and its driver configuration are rendered into the
            # gateway container filesystem by the deployment template.  Only
            # restart when reconciliation actually changes the init script;
            # this keeps resume idempotent and avoids needlessly rolling an
            # already-fixed fleet on every retry.
            needs_restart = True
            init_config_text = "\n".join(
                str(item)
                for container in deployment_obj.get("spec", {}).get("template", {}).get("spec", {}).get("initContainers", [])
                if container.get("name") == "init-config"
                for item in (container.get("args") or [])
            )
            # Applying a changed Deployment template starts a new ReplicaSet
            # asynchronously.  The trace watcher must not race that rollout;
            # otherwise it can try to exec into a gateway container that is
            # still in init (or no longer exists).
            wrapper_needs_rollout = (
                "trace-replay-openshell-node-selector-v19-" not in init_config_text
                or "sleep 2147483647" not in init_config_text
                or "openshell-pinned" not in "\n".join(
                    str(container)
                    for container in deployment_obj.get("spec", {}).get("template", {}).get("initContainers", [])
                )
            )
            config_obj = cluster.json(["-n", cluster.namespace, "get", "configmap", config])
            config_name_is_expected = config == f"{stem}-config-{global_index:02d}"
            if (
                (config_obj.get("metadata", {}).get("labels") or {}).get("managed-by") != "trace-replay-experiment"
                and not config_name_is_expected
            ):
                raise RuntimeError(f"refusing to reuse unowned ConfigMap {cluster.namespace}/{config}")
            # The init container materializes the plugin and config into the
            # pod's emptyDir home. Reconcile the template first and restart
            # only when the rendered gateway actually changed. Rebooting an
            # already-correct reused fleet adds avoidable rollout/API load and
            # can spend most of the experiment window waiting for readiness.
            # (The ConfigMap is mounted only by init-config; updating it does
            # not restart an existing pod.)
            needs_restart = False
            config_sha256 = _clone_config(
                cluster,
                BASE_OPENCLAW_CONFIG,
                config,
                workspace,
                pin_sandbox=not allow_cross_node,
                shared_workspace_pvc=target_workspace_pvc,
                mock_service=mock_service,
                gateway_endpoint=gateway_endpoint,
            )
            deployment_changed = pin_existing_deployment(
                name,
                workspace,
                target_node,
                target_workspace_pvc,
                index,
                config_sha256=config_sha256,
            )
            if deployment_changed or wrapper_needs_rollout:
                needs_restart = True
            if needs_restart:
                cluster.run(["-n", cluster.namespace, "rollout", "restart", f"deployment/{name}"])
                cluster.wait_rollout(name)
            cluster.apply_json({
                "apiVersion": "v1", "kind": "Service",
                "metadata": {"name": name, "labels": {"app": name, "managed-by": "trace-replay-experiment"}},
                "spec": {"clusterIP": "None", "selector": {"app": name}, "ports": [{"name": "gateway", "port": 18790, "targetPort": 18790}]},
            })
            deployed = cluster.json(["-n", cluster.namespace, "get", "deployment", name])
            gateway = next((
                container for container in deployed["spec"]["template"]["spec"].get("containers", [])
                if container.get("name") == "gateway"
            ), {})
            configured_workspace = next((
                item.get("value") for item in gateway.get("env", [])
                if item.get("name") == "OPENSHELL_FLEET_WORKSPACE"
            ), None)
            if configured_workspace:
                workspace = str(configured_workspace)
            if not reuse_existing_workspaces:
                _ensure_openshell_workspace(
                    cluster,
                    name,
                    workspace,
                    gateway_endpoint or "http://openshell.openshell-tracesim.svc.cluster.local:8080",
                )
            return name, workspace, target_node

        with ThreadPoolExecutor(max_workers=min(8, agents)) as pool:
            bindings = list(pool.map(refresh_existing, range(1, agents + 1)))
        return bindings

    def create_agent(index: int) -> tuple[str, str, str]:
        target_node = node_for(index)
        target_workspace_pvc = workspace_pvc_for(target_node)
        global_index = global_index_for(index)
        if reuse:
            if deployment_prefix:
                stem = safe_name(deployment_prefix)
                name = f"{stem}-{global_index:02d}"
                config = f"{stem}-config-{global_index:02d}"
            else:
                name = f"trace-replay-openclaw-agent-{global_index}"
                config = f"trace-replay-openclaw-config-{global_index}"
        else:
            stem = safe_name(deployment_prefix or "trace-replay-openclaw-agent")
            name = f"{stem}-{global_index:02d}"
            config = f"{stem}-config-{global_index:02d}"
        service = name
        workspace = workspace_name(workspace_prefix, global_index)
        existing = cluster.run(["-n", cluster.namespace, "get", "deployment", name], check=False).strip()
        if reuse and existing:
            deployment_obj = cluster.json(["-n", cluster.namespace, "get", "deployment", name])
            labels = deployment_obj.get("metadata", {}).get("labels") or {}
            if labels.get("managed-by") != "trace-replay-experiment":
                raise RuntimeError(f"refusing to reuse unowned deployment {cluster.namespace}/{name}")
            config_obj = cluster.json(["-n", cluster.namespace, "get", "configmap", config])
            config_labels = config_obj.get("metadata", {}).get("labels") or {}
            if config_labels.get("managed-by") != "trace-replay-experiment":
                raise RuntimeError(f"refusing to reuse unowned ConfigMap {cluster.namespace}/{config}")
            config_sha256 = _clone_config(
                cluster,
                BASE_OPENCLAW_CONFIG,
                config,
                workspace,
                pin_sandbox=not allow_cross_node,
                shared_workspace_pvc=target_workspace_pvc,
                mock_service=mock_service,
                gateway_endpoint=gateway_endpoint,
            )
            pin_existing_deployment(
                name,
                workspace,
                target_node,
                target_workspace_pvc,
                index,
                config_sha256=config_sha256,
            )
            cluster.run(["-n", cluster.namespace, "rollout", "restart", f"deployment/{name}"])
            cluster.wait_rollout(name)
            service_obj = {
                "apiVersion": "v1", "kind": "Service",
                "metadata": {"name": service, "labels": {"app": name, "managed-by": "trace-replay-experiment"}},
                "spec": {"clusterIP": "None", "selector": {"app": name}, "ports": [{"name": "gateway", "port": 18790, "targetPort": 18790}]},
            }
            cluster.apply_json(service_obj)
            deployed = cluster.json(["-n", cluster.namespace, "get", "deployment", name])
            gateway = next(
                (container for container in deployed["spec"]["template"]["spec"].get("containers", []) if container.get("name") == "gateway"),
                {},
            )
            configured_workspace = next(
                (item.get("value") for item in gateway.get("env", []) if item.get("name") == "OPENSHELL_FLEET_WORKSPACE"),
                None,
            )
            if configured_workspace:
                workspace = str(configured_workspace)
            if not reuse_existing_workspaces:
                _ensure_openshell_workspace(
                    cluster,
                    name,
                    workspace,
                    gateway_endpoint or "http://openshell.openshell-tracesim.svc.cluster.local:8080",
                )
            return service, workspace, target_node
        if existing:
            raise RuntimeError(
                f"refusing to modify existing fresh-run deployment {cluster.namespace}/{name}; "
                "use a new output directory/run identity"
            )
        if cluster.run(["-n", cluster.namespace, "get", "configmap", config], check=False).strip():
            raise RuntimeError(
                f"refusing to modify existing fresh-run ConfigMap {cluster.namespace}/{config}; "
                "use a new output directory/run identity"
            )
        if cluster.run(["-n", cluster.namespace, "get", "service", service], check=False).strip():
            raise RuntimeError(
                f"refusing to modify existing fresh-run Service {cluster.namespace}/{service}; "
                "use a new output directory/run identity"
            )
        _clone_config(
            cluster,
            BASE_OPENCLAW_CONFIG,
            config,
            workspace,
            pin_sandbox=not allow_cross_node,
            shared_workspace_pvc=target_workspace_pvc,
            mock_service=mock_service,
            gateway_endpoint=gateway_endpoint,
        )
        deployment = cluster.json(["-n", cluster.namespace, "get", "deployment", name if existing else BASE_OPENCLAW])
        for key in ("uid", "resourceVersion", "generation", "creationTimestamp", "managedFields", "status"):
            deployment.get("metadata", {}).pop(key, None)
        deployment["metadata"]["name"] = name
        deployment["metadata"]["labels"] = {"app": name, "managed-by": "trace-replay-experiment"}
        spec = deployment["spec"]
        spec["replicas"] = 1
        spec["selector"]["matchLabels"] = {"app": name}
        template = spec["template"]
        template["metadata"]["labels"] = {"app": name, "managed-by": "trace-replay-experiment"}
        template["metadata"].setdefault("annotations", {})["trace-replay.experiment/reconciled-at"] = str(time.time())
        if allow_cross_node:
            template["spec"].pop("nodeSelector", None)
        else:
            template["spec"]["nodeSelector"] = {"kubernetes.io/hostname": target_node}
        for volume in template["spec"].get("volumes", []):
            if volume.get("name") == "openclaw-shell-home":
                volume.clear(); volume.update({"name": "openclaw-shell-home", "emptyDir": {}})
            if volume.get("name") == "config-template":
                volume["configMap"] = {"name": config}
            if volume.get("name") == "openshell-mtls" and openshell_client_secret:
                volume.setdefault("secret", {})["secretName"] = openshell_client_secret
        for container in template["spec"].get("containers", []):
            if container.get("name") == "gateway":
                container["image"] = EXPERIMENT_OPENCLAW_IMAGE
                env = [item for item in container.setdefault("env", []) if item.get("name") != "OPENSHELL_FLEET_WORKSPACE"]
                env.append({"name": "OPENSHELL_FLEET_WORKSPACE", "value": workspace})
                container["env"] = env
        install_local_mock_sidecar(deployment)
        install_plugin_compat_shim(deployment)
        if not allow_cross_node:
            _install_pinned_openshell_wrapper(
                deployment,
                target_node,
                workspace,
                target_workspace_pvc,
                appworld_endpoint,
                internal_api_endpoints=internal_api_endpoints,
                gateway_endpoint=gateway_endpoint,
                sandbox_image=sandbox_image_for(index),
            )
        cluster.apply_json(deployment)
        service_obj = {
            "apiVersion": "v1", "kind": "Service",
            "metadata": {"name": service, "labels": {"app": name, "managed-by": "trace-replay-experiment"}},
            "spec": {"clusterIP": "None", "selector": {"app": name}, "ports": [{"name": "gateway", "port": 18790, "targetPort": 18790}]},
        }
        cluster.apply_json(service_obj)
        cluster.wait_rollout(name)
        _ensure_openshell_workspace(cluster, name, workspace, gateway_endpoint or "http://openshell.openshell-tracesim.svc.cluster.local:8080")
        return service, workspace, target_node

    with ThreadPoolExecutor(max_workers=min(8, agents)) as pool:
        return list(pool.map(create_agent, range(1, agents + 1)))


def verify_agent_mock_endpoints(
    cluster: Cluster,
    agent_bindings: list[tuple[str, str, str]],
    mock_service: str,
    agent_indices: list[int] | tuple[int, ...] | None = None,
) -> None:
    """Prove that each selected running gateway loaded this run's mock URL.

    Updating the per-agent ConfigMap is not sufficient: the ConfigMap is read
    by an init container and an already-running gateway keeps its old rendered
    config.  Read the live file from the gateway so trace dispatch cannot begin
    with a stale data-plane endpoint.
    """
    expected = "http://127.0.0.1:8080/v1"
    indices = list(agent_indices or range(1, len(agent_bindings) + 1))

    def verify(index: int) -> None:
        try:
            agent = agent_bindings[index - 1][0]
        except IndexError as exc:
            raise RuntimeError(f"agent endpoint verification references missing slot {index}") from exc
        rendered = cluster.run([
            "-n", cluster.namespace, "exec", f"deployment/{agent}", "-c", "gateway", "--",
            "cat", "/home/node/.openclaw/openclaw.json",
        ])
        try:
            payload = json.loads(rendered)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"agent {agent} has an unreadable live OpenClaw config") from exc
        urls = {
            str(provider.get("baseUrl") or "")
            for provider in (payload.get("models", {}).get("providers", {}) or {}).values()
            if isinstance(provider, dict) and provider.get("baseUrl")
        }
        if urls != {expected}:
            raise RuntimeError(
                f"agent {agent} live mock endpoint mismatch: expected {expected}, observed {sorted(urls)}"
            )

    with ThreadPoolExecutor(max_workers=min(8, len(indices))) as pool:
        list(pool.map(verify, indices))
    log_phase(f"deployment: verified live mock endpoint on {len(indices)} selected agents")


def _resume_agent_layout(
    cluster: Cluster,
    deployment_prefix: str,
    agents: int,
    preferred_node: str,
    planned_nodes: list[str] | tuple[str, ...] | None = None,
) -> tuple[list[str], list[int], list[int]]:
    """Inspect an owned partial fleet without changing cluster state.

    Running agents retain their observed node. Existing non-running agents
    retain the node selector already recorded on their Deployment; only
    absent slots are returned for controlled placement on ``preferred_node``.
    The caller performs the only mutations after capacity and ownership checks.
    """
    stem = safe_name(deployment_prefix)
    deployment_items = (
        cluster.json(["-n", cluster.namespace, "get", "deploy", "-l", "managed-by=trace-replay-experiment"])
        .get("items")
        or []
    )
    deployments = {
        str(item.get("metadata", {}).get("name") or ""): item
        for item in deployment_items
        if str(item.get("metadata", {}).get("name") or "").startswith(f"{stem}-")
    }
    pod_items = (
        cluster.json(["-n", cluster.namespace, "get", "pods", "-l", "managed-by=trace-replay-experiment"])
        .get("items")
        or []
    )
    running_pods_by_app: dict[str, dict[str, Any]] = {}
    for pod in pod_items:
        labels = pod.get("metadata", {}).get("labels") or {}
        app = str(labels.get("app") or "")
        if app.startswith(f"{stem}-") and pod.get("status", {}).get("phase") == "Running":
            running_pods_by_app[app] = pod
    assignments: list[str] = []
    pending: list[int] = []
    missing: list[int] = []
    for index in range(1, agents + 1):
        name = f"{stem}-{index:02d}"
        deployment = deployments.get(name)
        if deployment is None:
            assignments.append(planned_nodes[index - 1] if planned_nodes is not None else preferred_node)
            missing.append(index)
            continue
        labels = deployment.get("metadata", {}).get("labels") or {}
        if labels.get("managed-by") != "trace-replay-experiment":
            raise RuntimeError(f"refusing to resume unowned deployment {cluster.namespace}/{name}")
        pod = running_pods_by_app.get(name)
        if pod is not None:
            node = str(pod.get("spec", {}).get("nodeName") or "")
            if not is_allowed_node(node):
                raise RuntimeError(f"owned deployment {name} is running on an unapproved node: {node or '<none>'}")
            assignments.append(node)
            continue
        node_selector = (
            ((deployment.get("spec") or {}).get("template") or {})
            .get("spec", {})
            .get("nodeSelector", {})
        )
        node = str(node_selector.get("kubernetes.io/hostname") or preferred_node)
        if not is_allowed_node(node):
            raise RuntimeError(f"owned deployment {name} has an unapproved node selector: {node}")
        assignments.append(node)
        pending.append(index)
    return assignments, pending, missing


def _collect_owned_agent_bindings(
    cluster: Cluster,
    deployment_prefix: str,
    workspace_prefix: str,
    agents: int,
) -> list[tuple[str, str, str]]:
    """Return the post-resume fleet in stable global agent-index order."""
    stem = safe_name(deployment_prefix)
    deployment_items = (
        cluster.json(["-n", cluster.namespace, "get", "deploy", "-l", "managed-by=trace-replay-experiment"])
        .get("items")
        or []
    )
    deployments = {
        str(item.get("metadata", {}).get("name") or ""): item
        for item in deployment_items
        if str(item.get("metadata", {}).get("name") or "").startswith(f"{stem}-")
    }
    pod_items = (
        cluster.json(["-n", cluster.namespace, "get", "pods", "-l", "managed-by=trace-replay-experiment"])
        .get("items")
        or []
    )
    running_pods_by_app: dict[str, dict[str, Any]] = {}
    for pod in pod_items:
        labels = pod.get("metadata", {}).get("labels") or {}
        app = str(labels.get("app") or "")
        if app.startswith(f"{stem}-") and pod.get("status", {}).get("phase") == "Running":
            running_pods_by_app[app] = pod
    bindings: list[tuple[str, str, str]] = []
    for index in range(1, agents + 1):
        name = f"{stem}-{index:02d}"
        deployment = deployments.get(name)
        if deployment is None:
            raise RuntimeError(f"resumed agent deployment is missing: {cluster.namespace}/{name}")
        labels = deployment.get("metadata", {}).get("labels") or {}
        if labels.get("managed-by") != "trace-replay-experiment":
            raise RuntimeError(f"refusing to collect unowned deployment {cluster.namespace}/{name}")
        pod = running_pods_by_app.get(name)
        if pod is None:
            raise RuntimeError(f"resumed agent is not Running: {cluster.namespace}/{name}")
        node = str(pod.get("spec", {}).get("nodeName") or "")
        if not is_allowed_node(node):
            raise RuntimeError(f"resumed deployment {name} is running on an unapproved node: {node or '<none>'}")
        workspace = workspace_name(workspace_prefix, index)
        for container in deployment.get("spec", {}).get("template", {}).get("spec", {}).get("containers", []):
            if container.get("name") != "gateway":
                continue
            for entry in container.get("env", []):
                if entry.get("name") == "OPENSHELL_FLEET_WORKSPACE" and entry.get("value"):
                    workspace = str(entry["value"])
                    break
        bindings.append((name, workspace, node))
    return bindings


def _install_pinned_openshell_wrapper(
    deployment: dict[str, Any],
    node: str,
    workspace: str,
    shared_workspace_pvc: str | None,
    appworld_endpoint: str | None = None,
    internal_api_endpoints: list[str] | None = None,
    gateway_endpoint: str | None = None,
    sandbox_image: str | None = None,
) -> None:
    """Install a wrapper that pins and storage-wires sandbox pods at create time."""
    kubernetes_config: dict[str, Any] = {
        "pod": {"node_selector": {"kubernetes.io/hostname": node}},
    }
    if shared_workspace_pvc:
        kubernetes_config.update({
            "volumes": [{
                "name": "trace-replay-shared-workspace",
                "persistent_volume_claim": {"claim_name": shared_workspace_pvc, "read_only": False},
            }],
            "containers": {
                "agent": {
                    "volume_mounts": [{
                        "name": "trace-replay-shared-workspace",
                        # OpenShell 0.0.116 reserves the exact /sandbox
                        # target.  Mount below its supported workspace path.
                        "mount_path": "/sandbox/.openshell/workspace",
                        "sub_path": workspace,
                        "read_only": False,
                    }],
                },
            },
        })
    driver_config_json = json.dumps({"kubernetes": kubernetes_config}, separators=(",", ":"))
    if sandbox_image is not None:
        if not re.fullmatch(r"[A-Za-z0-9._/:@-]+", sandbox_image):
            raise ValueError(f"unsupported sandbox image reference: {sandbox_image!r}")
    endpoints = list(internal_api_endpoints or [])
    if appworld_endpoint and appworld_endpoint not in endpoints:
        endpoints.append(appworld_endpoint)
    network_policies: list[str] = []
    for index, endpoint in enumerate(endpoints, start=1):
        parsed = urlparse(endpoint)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname or "\n" in endpoint:
            raise ValueError(f"unsupported internal API endpoint for sandbox policy: {endpoint}")
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        tls_line = "\n        tls: terminate" if parsed.scheme == "https" else ""
        policy_name = f"trace-replay_internal_api_{index}"
        network_policies.append(
            f"  {policy_name}:\n"
            f"    name: {policy_name}\n"
            "    endpoints:\n"
            f"      - host: {parsed.hostname}\n"
            f"        port: {port}\n"
            "        protocol: rest\n"
            "        enforcement: enforce\n"
            "        access: read-write"
            f"{tls_line}\n"
            "    binaries:\n"
            "      - path: /usr/sbin/python3\n"
            "      - path: /usr/bin/python3.13\n"
        )
    policy_yaml = ""
    if network_policies:
        policy_yaml = (
            "version: 1\n"
            "filesystem_policy:\n"
            "  include_workdir: true\n"
            "  read_only:\n"
            "    - /usr\n"
            "    - /lib\n"
            "    - /etc\n"
            "    - /app\n"
            "    - /var/log\n"
            "    - /proc\n"
            "    - /dev/urandom\n"
            "  read_write:\n"
            "    - /tmp\n"
            "    - /dev/null\n"
            "    - /sandbox\n"
            "    - /sandbox/.openshell/workspace\n"
            "landlock:\n"
            "  compatibility: best_effort\n"
            "network_policies:\n"
            + "".join(network_policies)
        )
    policy_literal = json.dumps(policy_yaml)
    # The init container keeps the wrapper in its command script. Include the
    # effective driver configuration in the marker so a reused deployment is
    # refreshed when its node, workspace, PVC, policy, or endpoint changes.
    wrapper_fingerprint = hashlib.sha256(
        "\n".join((driver_config_json, policy_yaml, gateway_endpoint or "", sandbox_image or "")).encode()
    ).hexdigest()[:12]
    # Bump the marker whenever wrapper semantics change so a reused agent
    # cannot retain an older wrapper that would duplicate an existing --from
    # flag or otherwise skip the refreshed driver configuration.
    # The published plugin currently creates the sandbox with a terminal
    # ``true`` command.  On this OpenShell gateway that command is the sandbox
    # process itself, so the pod exits immediately and later ``sandbox exec``
    # calls fail with exit 127.  Keep the experiment-owned sandbox alive until
    # the OpenClaw agent is finished; this is limited to the generated wrapper
    # and does not modify the gateway or any cluster infrastructure.
    marker = f"# trace-replay-openshell-node-selector-v19-{wrapper_fingerprint}"
    policy_injection = ""
    if policy_yaml:
        policy_injection = (
            f"  fs.writeFileSync('/tmp/trace-replay-appworld-policy.yaml', {policy_literal});\n"
            "  injected.push('--policy', '/tmp/trace-replay-appworld-policy.yaml');\n"
        )
    wrapper = "".join([
        f"\n              {marker}\n",
        "              cat > /opt/openshell/bin/openshell-pinned <<'TRACE_REPLAY_OPEN_SHELL_WRAPPER'\n",
        "#!/usr/bin/env node\n",
        "const { spawnSync } = require('child_process');\n",
        "const fs = require('fs');\n",
        "const args = process.argv.slice(2);\n",
        "const sandboxIndex = args.indexOf('sandbox');\n",
        # The OpenClaw plugin may already add a driver-config-json argument
        # (for example, its own node selector).  That argument must not mask
        # the experiment's storage wiring: on this cluster the default
        # per-sandbox PVC has no provisioner, while the approved node-local
        # workspace PVC is supplied by this wrapper.  Replace any existing
        # driver config for the create call, preserving the experiment-owned
        # node/PVC/policy configuration deterministically.
        "if (sandboxIndex >= 0 && args[sandboxIndex + 1] === 'create') {\n",
        f"  const sandboxImage = {json.dumps(sandbox_image)};\n",
        f"  const injected = ['--driver-config-json', {json.dumps(driver_config_json)}];\n",
        "  for (let i = args.length - 1; i >= 0; i--) {\n",
        "    if (args[i] === '--driver-config-json') { args.splice(i, 2); continue; }\n",
        "    if (args[i].startsWith('--driver-config-json=')) { args.splice(i, 1); }\n",
        "    if (args[i] === '--policy') { args.splice(i, 2); continue; }\n",
        "    if (args[i].startsWith('--policy=')) { args.splice(i, 1); }\n",
        "  }\n",
        "  if (sandboxImage) {\n",
        "    const fromIndex = args.indexOf('--from');\n",
        "    const fromEqualsIndex = args.findIndex((arg) => arg.startsWith('--from='));\n",
        "    if (fromIndex >= 0 && fromIndex + 1 < args.length) args[fromIndex + 1] = sandboxImage;\n",
        "    else if (fromEqualsIndex >= 0) args[fromEqualsIndex] = '--from=' + sandboxImage;\n",
        "    else injected.unshift('--from', sandboxImage);\n",
        "  }\n",
        policy_injection,
        "  args.splice(sandboxIndex + 2, 0, ...injected);\n",
"}\n",
        "if (sandboxIndex >= 0 && args[sandboxIndex + 1] === 'exec') {\n",
        "  for (let i = args.length - 1; i >= 0; i--) {\n",
        "    if (args[i] === '--policy') { args.splice(i, 2); continue; }\n",
        "    if (args[i].startsWith('--policy=')) { args.splice(i, 1); }\n",
        "  }\n",
        "}\n",
        "if (sandboxIndex >= 0 && args[sandboxIndex + 1] === 'create') {\n",
        "  const separator = args.indexOf('--', sandboxIndex + 2);\n",
        "  if (separator >= 0 && args[separator + 1] === 'true') {\n",
        "    args.splice(separator + 1, 1, 'sh', '-c', 'exec sleep 2147483647');\n",
        "  }\n",
        "}\n",
        "const result = spawnSync('/opt/openshell/bin/openshell', args, { stdio: 'inherit' });\n",
        "process.exit(result.status ?? 1);\n",
        "TRACE_REPLAY_OPEN_SHELL_WRAPPER\n",
        "              chmod 755 /opt/openshell/bin/openshell-pinned\n",
    ])
    # The published OpenClaw/OpenShell image contains the OpenShell CLI but
    # not an OpenSSH client.  The OpenShell backend invokes `ssh` for the
    # sandbox transport, so provide a tiny experiment-scoped compatibility
    # wrapper in the existing shared CLI volume.  It translates the plugin's
    # SSH invocation to the supported `openshell sandbox exec` RPC and avoids
    # changing the base image or installing packages in every gateway.
    ssh_wrapper = """#!/bin/sh
cfg=""
mode="--no-tty"
log=/tmp/trace-replay-ssh-wrapper.log
printf 'ssh-wrapper args:' >> "$log"
for arg in "$@"; do printf ' <%s>' "$arg" >> "$log"; done
printf '\\n' >> "$log"
while [ "$#" -gt 0 ]; do
  case "$1" in
    -F) cfg="$2"; shift 2 ;;
    -tt|-t) mode="--tty"; shift ;;
    -T) mode="--no-tty"; shift ;;
    -o) shift 2 ;;
    --) shift; break ;;
    *) host="$1"; shift; break ;;
  esac
done
name=$(sed -n 's/.*--name \\([^ ]*\\).*/\\1/p' "$cfg" | head -1)
workspace=$(sed -n 's/.*--workspace \\([^ ]*\\).*/\\1/p' "$cfg" | head -1)
[ -n "$workspace" ] || workspace=${OPENSHELL_WORKSPACE:-default}
# ProxyCommand uploads have no -F config. Resolve the active sandbox in this agent workspace.
if [ -z "$name" ]; then
  name=$(/opt/openshell/bin/openshell sandbox list ${OPENSHELL_GATEWAY_ENDPOINT:+--gateway-endpoint "$OPENSHELL_GATEWAY_ENDPOINT"} 2>/dev/null | awk 'NR==3 {print $1}')
fi
[ -n "$name" ] || { echo "ssh wrapper: missing sandbox name" >&2; exit 2; }
remote="$*"
printf 'ssh-wrapper cfg=%s name=%s workspace=%s mode=%s remote=%s\\n' "$cfg" "$name" "$workspace" "$mode" "$remote" >> "$log"
output=$(mktemp /tmp/trace-replay-ssh-output.XXXXXX)
# OpenClaw keeps the SSH stdin pipe open for ordinary exec calls.  The
# OpenShell CLI treats that as command stdin and waits for EOF, so close it for
# normal commands.  The initial workspace upload is the one exception: its
# `cat | tar xf -` command legitimately consumes the SSH stream.
case "$remote" in
  *"cat | tar xf -"*)
    /opt/openshell/bin/openshell sandbox exec --gateway openshell --workspace "$workspace" --name "$name" "$mode" -- /bin/sh -lc "$remote" > "$output" 2>&1
    ;;
  *)
    /opt/openshell/bin/openshell sandbox exec --gateway openshell --workspace "$workspace" --name "$name" "$mode" -- /bin/sh -lc "$remote" > "$output" 2>&1 </dev/null
    ;;
esac
rc=$?
printf 'ssh-wrapper rc=%s output-begin\\n' "$rc" >> "$log"
tail -c 4096 "$output" >> "$log"
printf '\\nssh-wrapper output-end\\n' >> "$log"
cat "$output"
rm -f "$output"
exit "$rc"
"""
    ssh_wrapper_b64 = base64.b64encode(ssh_wrapper.encode()).decode()
    init_containers = deployment["spec"]["template"]["spec"].setdefault("initContainers", [])
    init_containers[:] = [item for item in init_containers if item.get("name") != "install-ssh-wrapper"]
    init_containers.append({
        "name": "install-ssh-wrapper",
        "image": "python:3.12-slim",
        "command": ["python3", "-c"],
        "args": [
            "import base64, os; "
            f"p='/opt/openshell/bin/ssh'; open(p, 'wb').write(base64.b64decode({ssh_wrapper_b64!r})); os.chmod(p, 0o755)"
        ],
        "volumeMounts": [{"mountPath": "/opt/openshell", "name": "openshell-cli"}],
    })
    if gateway_endpoint:
        endpoint_literal = json.dumps(gateway_endpoint)
    for container in deployment["spec"]["template"]["spec"].get("containers", []):
        if container.get("name") == "gateway":
            env = container.setdefault("env", [])
            path_env = next((item for item in env if item.get("name") == "PATH"), None)
            if path_env is None:
                env.append({"name": "PATH", "value": "/opt/openshell/bin:/usr/local/bin:/usr/bin:/bin"})
            else:
                path_env["value"] = "/opt/openshell/bin:/usr/local/bin:/usr/bin:/bin"
            workspace_env = next((item for item in env if item.get("name") == "OPENSHELL_WORKSPACE"), None)
            if workspace_env is None:
                env.append({"name": "OPENSHELL_WORKSPACE", "value": workspace})
            else:
                workspace_env["value"] = workspace
        for index, arg in enumerate(container.get("args", [])):
                if "EP=" in arg and "openshell" in arg:
                    container["args"][index] = re.sub(
                        r'EP=\\?"[^"\n]+\\?"',
                        f"EP={endpoint_literal}",
                        arg,
                        count=1,
                    )
                    break
    for init_container in deployment["spec"]["template"]["spec"].get("initContainers", []):
        if init_container.get("name") != "init-config":
            continue
        args = init_container.setdefault("args", [""])
        if marker not in args[0]:
            args[0] += wrapper
        return
    raise RuntimeError("base OpenClaw deployment has no init-config container")


def ensure_driver_config(cluster: Cluster) -> None:
    """Publish the driver and its local package as one owned ConfigMap.

    Trace jobs use the stock Python image.  Mounting only ``driver.py`` is not
    sufficient because the driver imports the benchmark package from ``src``.
    A small source archive keeps the job reproducible without requiring an
    image build or a registry push.
    """
    name = "trace-replay-experiment-driver"
    bundle = io.BytesIO()
    package_root = ROOT / "src/trace_replay_sim"
    with tarfile.open(fileobj=bundle, mode="w:gz") as archive:
        archive.add(ROOT / "src/trace_replay_sim/driver.py", arcname="driver.py")
        for source in sorted(package_root.rglob("*.py")):
            archive.add(source, arcname=str(Path("trace_replay_sim") / source.relative_to(package_root)))
    cluster.upsert_owned_configmap("trace-replay", {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": name, "labels": {"managed-by": "trace-replay-experiment"}},
        "binaryData": {"source_bundle.tgz": base64.b64encode(bundle.getvalue()).decode("ascii")},
    })


def ensure_mock_llm_config(cluster: Cluster, replay_corpus: Path, system_ns: str, run_token: str) -> str:
    """Create a run-owned mock service without touching the shared baseline."""
    # Keep the run hash in the resource name.  Truncating only the beginning
    # caused different runs with the same long output prefix to overwrite one
    # another's mock corpus and Deployment.
    token = safe_name(run_token).strip("-")
    token = f"{token[:14].rstrip('-')}-{token[-8:]}" if len(token) > 8 else token
    prefix = f"trace-replay-{token}"
    service_name = f"{prefix}-mock-llm"
    source_name = f"{prefix}-mock-src"
    replay_payload = json.loads(replay_corpus.read_text(encoding="utf-8"))
    sessions = replay_payload.get("sessions") or []
    if not sessions:
        raise RuntimeError("owned mock corpus is empty")
    replay_code = (ROOT / "src/trace_replay_sim/mock_llm.py").read_text(encoding="utf-8")
    corpus_revision = hashlib.sha256(
        (
            replay_code
            + "\n"
            + json.dumps(replay_payload, sort_keys=True, separators=(",", ":"))
        ).encode("utf-8")
    ).hexdigest()
    cluster.upsert_owned_configmap(system_ns, {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": source_name, "labels": {"managed-by": "trace-replay-experiment", "run": prefix}},
        "data": {"mock_llm.py": replay_code},
    })
    corpus_volumes: list[dict[str, Any]] = []
    corpus_mounts: list[dict[str, Any]] = []
    corpus_parts = pack_mock_corpus_sessions(sessions)
    log_phase(
        f"mock corpus: packing {len(sessions)} sessions into "
        f"{len(corpus_parts)} ConfigMaps (limit={MOCK_CORPUS_CHUNK_BYTES} bytes)"
    )
    for index, part_sessions in enumerate(corpus_parts):
        part_name = f"{prefix}-corpus-{index:03d}"
        cluster.upsert_owned_configmap(system_ns, {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": part_name, "labels": {"managed-by": "trace-replay-experiment", "run": prefix}},
            "data": {"replay.json": json.dumps({"sessions": part_sessions}, separators=(",", ":"))},
        })
        volume_name = f"corpus-{index:03d}"
        mount_path = f"/data/corpus/part-{index:03d}"
        corpus_volumes.append({"name": volume_name, "configMap": {"name": part_name}})
        corpus_mounts.append({"name": volume_name, "mountPath": mount_path, "readOnly": True})
    base = cluster.json(["-n", system_ns, "get", "deployment", BASE_MOCK_LLM])
    for key in ("uid", "resourceVersion", "generation", "creationTimestamp", "managedFields", "status"):
        base.get("metadata", {}).pop(key, None)
    base["metadata"]["name"] = service_name
    base["metadata"]["labels"] = {"app": service_name, "managed-by": "trace-replay-experiment", "run": prefix}
    spec = base["spec"]
    spec["replicas"] = 1
    spec["selector"]["matchLabels"] = {"app": service_name}
    template = spec["template"]
    template["metadata"]["labels"] = {"app": service_name, "managed-by": "trace-replay-experiment", "run": prefix}
    # The mock process loads its corpus once at startup.  A resumed invocation
    # can keep the same run-scoped Service name while changing the selected
    # sessions, so make the corpus revision part of the pod template.  This
    # forces a bounded rollout instead of silently serving the previous
    # in-memory corpus through an apparently healthy Service.
    template["metadata"].setdefault("annotations", {})[
        "trace-replay.openai.com/mock-corpus-sha256"
    ] = corpus_revision
    template["spec"]["nodeSelector"] = {"kubernetes.io/hostname": cluster.node}
    for container in template["spec"].get("containers", []):
        if container.get("name") != "mock-llm":
            continue
        env = {item["name"]: item.get("value", "") for item in container.get("env", []) if item.get("name")}
        env.update({
            "REPLAY_CORPUS": "/data/corpus",
            "MOCK_EDGES_PATH": "/edges/mock_edges.jsonl",
            "OTLP_ENDPOINT": f"http://{BASE_JAEGER}.trace-replay.svc.cluster.local:4318",
        })
        container["env"] = [{"name": name, "value": value} for name, value in env.items()]
        mounts = [mount for mount in container.get("volumeMounts", []) if mount.get("name") not in {"src", "corpus", "edges"}]
        mounts.extend([
            {"name": "src", "mountPath": "/app", "readOnly": True},
            *corpus_mounts,
            {"name": "edges", "mountPath": "/edges"},
        ])
        container["volumeMounts"] = mounts
    template["spec"]["volumes"] = [
        {"name": "src", "configMap": {"name": source_name}},
        *corpus_volumes,
        {"name": "edges", "emptyDir": {}},
    ]
    cluster.apply_json_in_with_retry(system_ns, base)
    cluster.apply_json_in_with_retry(system_ns, {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": service_name, "labels": {"app": service_name, "managed-by": "trace-replay-experiment", "run": prefix}},
        "spec": {"selector": {"app": service_name}, "ports": [{"name": "http", "port": 8080, "targetPort": 8080}]},
    })
    cluster.wait_rollout_in(system_ns, service_name)
    log_phase(f"deployment: owned mock LLM ready ({service_name}, {len(corpus_parts)} corpus parts)")
    return service_name


def _appworld_command(session_id: str, turn: int, call: int, name: str, arguments: dict[str, Any], endpoint: str) -> list[str]:
    payload = base64.b64encode(json.dumps({
        "session_id": session_id,
        "turn": turn,
        "call": call,
        "tool": name,
        "arguments": arguments,
    }, separators=(",", ":")).encode()).decode()
    endpoint_literal = json.dumps(endpoint)
    # Keep the executable command below OpenClaw/OpenShell's remote-command
    # limit.  The former shell/base64 pipeline expanded larger AppWorld calls
    # beyond the transport boundary and returned before reaching the backend.
    # A direct Python argv avoids that extra shell layer while retaining a
    # quote-safe base64 payload.
    code = (
        "import base64 as b,json as j,urllib.request as u;"
        f'p=b.b64decode("{payload}");'
        f'r=u.Request({endpoint_literal},p,{{"Content-Type":"application/json"}});'
        "x=j.loads(u.urlopen(r,timeout=180).read());"
        'v=x.get("result",x.get("error",x));'
        'print(v if isinstance(v,str) else j.dumps(v),end="")'
    )
    return ["python3", "-c", code]


def prepare_appworld_corpus(
    source: Path,
    out: Path,
    endpoint: str,
    task_map: Path,
) -> Path:
    """Create a trace corpus whose AppWorld tools call the stateful adapter."""
    payload = json.loads(source.read_text(encoding="utf-8"))
    mapping = json.loads(task_map.read_text(encoding="utf-8"))
    if not isinstance(mapping, dict) or not mapping:
        raise RuntimeError(f"AppWorld task map is empty: {task_map}")
    mapped = 0
    for session in payload.get("sessions") or []:
        session_id = str(session.get("session_id") or "")
        if session_id not in mapping:
            raise RuntimeError(
                f"AppWorld task map has no entry for replay session {session_id}; "
                f"use a corpus with a validated mapping or rebuild {task_map}"
            )
        for turn in session.get("turns") or []:
            for call_index, tool in enumerate(turn.get("tool_calls") or []):
                name = str(tool.get("name") or "")
                if name.startswith("mcp__environment__"):
                    tool["mapped_command"] = _appworld_command(
                        session_id,
                        int(turn.get("index", 0)),
                        call_index,
                        name,
                        dict(tool.get("arguments") or {}),
                        endpoint,
                    )
                    mapped += 1
    if mapped == 0:
        raise RuntimeError(f"--appworld requested but corpus contains no mcp__environment__ tool calls: {source}")
    payload["tool_backend"] = "appworld"
    payload["appworld_endpoint"] = endpoint
    payload["appworld_task_map_sha256"] = hashlib.sha256(
        task_map.read_bytes()
    ).hexdigest()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    log_phase(f"corpus: prepared AppWorld-backed replay with {mapped} mapped tool calls")
    return out


def _workload_command(
    session_id: str,
    workload: str,
    benchmark: str,
    turn: int,
    call: int,
    name: str,
    arguments: dict[str, Any],
    endpoint: str,
) -> list[str]:
    """Build a sandbox command for a workload-specific internal API.

    The backend receives the original tool identity and redacted arguments;
    it owns the domain state and returns the tool result.  No recorded result
    is embedded in the command, so a successful run proves that the selected
    backend, rather than the LLM simulator, produced the tool response.
    """
    payload = base64.b64encode(json.dumps({
        "session_id": session_id,
        "workload": workload,
        "benchmark": benchmark,
        "turn": turn,
        "call": call,
        "tool": name,
        "arguments": arguments,
    }, separators=(",", ":")).encode()).decode()
    endpoint_literal = json.dumps(endpoint.rstrip("/"))
    code = (
        "import base64,json,os,sys,urllib.request,urllib.error;"
        f"p=json.loads(base64.b64decode('{payload}'));"
        f"r=urllib.request.Request(os.environ.get('WORKLOAD_BACKEND_URL',{endpoint_literal})+'/execute',"
        "data=json.dumps(p).encode(),headers={'Content-Type':'application/json'});"
        "\ntry:\n response=json.loads(urllib.request.urlopen(r,timeout=180).read())"
        "\nexcept urllib.error.HTTPError as e:\n"
        " raw=e.read().decode('utf-8','replace')\n"
        " try: response=json.loads(raw)\n"
        " except Exception: response={'ok':False,'error':f'HTTP {e.code}: {raw}'}"
        "\nexcept urllib.error.URLError as e:\n"
        " print(f'workload backend transport error: {e}',file=sys.stderr); raise SystemExit(75)"
        "\nexcept Exception as e:\n"
        " print(f'workload backend request error: {e}',file=sys.stderr); raise SystemExit(75)"
        "\nif not response.get('ok',True):\n"
        " print(response.get('error',response),file=sys.stderr); raise SystemExit(2)"
        "\nvalue=response.get('result',response.get('error',response))\n"
        "print(value if isinstance(value,str) else json.dumps(value),end='')"
    )
    return ["python3", "-c", code]


def _swe_sandbox_command(
    session_id: str,
    workload: str,
    benchmark: str,
    turn: int,
    call: int,
    name: str,
    arguments: dict[str, Any],
    endpoint: str,
    task: SweTask,
) -> list[str]:
    """Execute the recorded SWE command in the real task-image sandbox.

    The backend remains the audit/state boundary, but the repository command
    itself runs in OpenShell.  This prevents the benchmark service from
    pretending that a command changed a repository when it did not.
    """
    payload = base64.b64encode(json.dumps({
        "session_id": session_id,
        "workload": workload,
        "benchmark": benchmark,
        "turn": turn,
        "call": call,
        "tool": name,
        "arguments": arguments,
        "swe_task": task.public_dict(),
    }, separators=(",", ":")).encode()).decode()
    endpoint_literal = json.dumps(endpoint.rstrip("/"))
    code = (
        "import base64,glob,json,os,subprocess,sys,urllib.request,urllib.error;"
        f"p=json.loads(base64.b64decode('{payload}'));"
        "a=p.get('arguments') or {};task=p.get('swe_task') or {};tool_name=str(p.get('tool') or '');"
        "command=str(a.get('command') or a.get('cmd') or '');"
        "repo_root='/sandbox/.openshell/workspace'\n"
        "try:\n os.symlink(repo_root,'/testbed')\nexcept FileExistsError:\n pass\nexcept OSError:\n pass\n"
        "repo_root='/sandbox/.openshell/workspace';cwd=repo_root;"
        "os.environ['GIT_CONFIG_COUNT']='1';os.environ['GIT_CONFIG_KEY_0']='safe.directory';os.environ['GIT_CONFIG_VALUE_0']=repo_root;"
        "result={};verified_base_commit='';"
        "\ntry:\n"
        " if not os.path.isdir(repo_root) or not task.get('base_commit'):\n  raise RuntimeError('SWE workspace does not provide the staged /testbed repository')\n"
        " head=subprocess.run(['git','-C',cwd,'rev-parse','HEAD'],capture_output=True,text=True,timeout=20,check=False)\n"
        " if head.returncode != 0 or head.stdout.strip() != str(task.get('base_commit')):\n  raise RuntimeError('SWE task image commit mismatch: expected '+str(task.get('base_commit'))+' observed '+head.stdout.strip())\n"
        " verified_base_commit=head.stdout.strip()\n"
        " if command:\n  command=command.replace('/testbed',repo_root)\n  completed=subprocess.run(['bash','-lc',command],cwd=cwd,capture_output=True,text=True,timeout=180,check=False)\n  result={'output':(completed.stdout+completed.stderr)[-64000:],'returncode':completed.returncode,'operation':'command'}\n"
        " elif tool_name in ('Read','ReadMcpResourceTool'):\n  path=str(a.get('file_path') or a.get('path') or a.get('uri') or '')\n  path=path[7:] if path.startswith('file://') else path\n  path=path.replace('/testbed',repo_root,1)\n  if not (path == repo_root or path.startswith(repo_root+'/')):\n   raise RuntimeError('SWE read path is outside the staged repository')\n  with open(path,encoding='utf-8',errors='replace') as handle: result={'output':handle.read()[-64000:],'returncode':0,'operation':'read'}\n"
        " elif tool_name in ('Edit','Write'):\n  path=str(a.get('file_path') or a.get('path') or '')\n  path=path.replace('/testbed',repo_root,1)\n  if not (path == repo_root or path.startswith(repo_root+'/')):\n   raise RuntimeError('SWE write path is outside the staged repository')\n  if tool_name == 'Edit':\n   with open(path,encoding='utf-8') as handle: text=handle.read()\n   old=str(a.get('old_string') or ''); new=str(a.get('new_string') or '')\n   if not old or old not in text: raise RuntimeError('SWE edit target was not found')\n   with open(path,'w',encoding='utf-8') as handle: handle.write(text.replace(old,new,1))\n  else:\n   os.makedirs(os.path.dirname(path) or repo_root,exist_ok=True)\n   with open(path,'w',encoding='utf-8') as handle: handle.write(str(a.get('content') or ''))\n  result={'output':'repository file updated','returncode':0,'operation':'write'}\n"
        " elif tool_name in ('Grep','Glob'):\n  base=str(a.get('path') or '/testbed')\n  base=base.replace('/testbed',repo_root,1)\n  if not (base == repo_root or base.startswith(repo_root+'/')): raise RuntimeError('SWE search path is outside the staged repository')\n  if tool_name == 'Glob':\n   pattern=str(a.get('pattern') or '**/*'); result={'output':'\\n'.join(glob.glob(os.path.join(base,pattern),recursive=True))[-64000:],'returncode':0,'operation':'search'}\n  else:\n   pattern=str(a.get('pattern') or ''); found=subprocess.run(['grep','-R','-n','--',pattern,base],cwd=cwd,capture_output=True,text=True,timeout=60,check=False); result={'output':(found.stdout+found.stderr)[-64000:],'returncode':found.returncode,'operation':'search'}\n"
        " else:\n  result={'output':'control tool acknowledged after validating the real SWE repository','returncode':0,'operation':'control'}\n"
        " result.update({'repo_root':cwd,'base_commit':head.stdout.strip()})\n"
        "except subprocess.TimeoutExpired:\n result={'output':'command timed out after 180 seconds','returncode':124,'repo_root':cwd,'base_commit':verified_base_commit}\n"
        "except Exception as e:\n result={'output':str(e),'returncode':1,'repo_root':cwd if verified_base_commit else '','base_commit':verified_base_commit}\n"
        "p['execution_mode']='sandbox';p['sandbox_result']=result;"
        f"r=urllib.request.Request(os.environ.get('WORKLOAD_BACKEND_URL',{endpoint_literal})+'/execute',"
        "data=json.dumps(p).encode(),headers={'Content-Type':'application/json'});"
        "\ntry:\n response=json.loads(urllib.request.urlopen(r,timeout=180).read())\n"
        "except urllib.error.HTTPError as e:\n raw=e.read().decode('utf-8','replace');\n"
        " response={'ok':False,'error':f'HTTP {e.code}: {raw}'}\n"
        "except Exception as e:\n response={'ok':False,'error':str(e)}\n"
        "\nif not response.get('ok',True):\n print(response.get('error',response),file=sys.stderr); raise SystemExit(2)\n"
        "value=response.get('result',result);print(value if isinstance(value,str) else json.dumps(value),end='')"
    )
    return ["python3", "-c", code]


def prepare_workload_corpus(
    source: Path,
    out: Path,
    backends: dict[str, Any],
    swe_manifest: dict[str, SweTask] | None = None,
    selected_session_ids: set[str] | None = None,
) -> Path:
    """Map selected trace tools to the resolved backend API boundaries."""
    payload = json.loads(source.read_text(encoding="utf-8"))
    if selected_session_ids is not None:
        payload["sessions"] = [
            session for session in (payload.get("sessions") or [])
            if str(session.get("session_id") or "") in selected_session_ids
        ]
        if not payload["sessions"]:
            raise RuntimeError("selected workload corpus contains no sessions to map")
    if swe_manifest is not None:
        validate_swe_sessions(payload.get("sessions") or [], swe_manifest)
    mapped: dict[str, int] = {workload: 0 for workload in WORKLOADS}
    for session in payload.get("sessions") or []:
        session_id = str(session.get("session_id") or "")
        classification = classify_session(session)
        workload = str(session.get("workload") or classification["workload"])
        # The ingested full replay corpus is benchmark-labelled; preserve the
        # normalized workload metadata so downstream queueing and collection
        # use the same classification as split_corpus().
        session.setdefault("workload", workload)
        session.setdefault("workload_profile", str(session.get("benchmark") or classification["profile"]))
        if workload == "software_engineering_qa":
            if swe_manifest is None or session_id not in swe_manifest:
                raise SweManifestError(f"SWE trace has no real repository mapping: {session_id}")
            session["swe_task"] = swe_manifest[session_id].public_dict()
        spec = backends.get(workload)
        if spec is None:
            continue
        benchmark = str(session.get("benchmark") or spec.benchmark)
        for turn in session.get("turns") or []:
            for call_index, tool in enumerate(turn.get("tool_calls") or []):
                name = str(tool.get("name") or "")
                if workload == "personal_assistant" and name.startswith("mcp__environment__"):
                    tool["mapped_command"] = _appworld_command(
                        session_id,
                        int(turn.get("index", 0)),
                        call_index,
                        name,
                        dict(tool.get("arguments") or {}),
                        spec.base_url + "/execute",
                    )
                    mapped[workload] += 1
                elif workload == "software_engineering_qa":
                    arguments = dict(tool.get("arguments") or {})
                    tool["mapped_command"] = _swe_sandbox_command(
                        session_id,
                        workload,
                        benchmark,
                        int(turn.get("index", 0)),
                        call_index,
                        name,
                        arguments,
                        spec.base_url,
                        swe_manifest[session_id],
                    )
                    mapped[workload] += 1
                elif workload != "personal_assistant":
                    arguments = dict(tool.get("arguments") or {})
                    tool["mapped_command"] = _workload_command(
                        session_id,
                        workload,
                        benchmark,
                        int(turn.get("index", 0)),
                        call_index,
                        name,
                        arguments,
                        spec.base_url,
                    )
                    mapped[workload] += 1
    present_workloads = {
        str(session.get("workload") or classify_session(session)["workload"])
        for session in payload.get("sessions") or []
    }
    recorded_calls: dict[str, int] = {}
    for session in payload.get("sessions") or []:
        workload = str(session.get("workload") or classify_session(session)["workload"])
        recorded_calls[workload] = recorded_calls.get(workload, 0) + sum(
            len(turn.get("tool_calls") or []) for turn in session.get("turns") or []
        )
    # A workload whose selected sessions genuinely record zero tool calls
    # (a conversational-only retry selection) correctly maps zero calls; only
    # a silent mapping drop -- recorded calls exist but none mapped -- fails.
    if any(
        mapped.get(workload, 0) == 0 and recorded_calls.get(workload, 0) > 0
        for workload in present_workloads
    ):
        missing = ", ".join(f"{workload}={mapped.get(workload, 0)}" for workload in sorted(present_workloads))
        raise RuntimeError(f"backend mapping produced no calls for one or more workloads: {missing}")
    payload["tool_backend"] = "workload-specific"
    payload["backend_mapping_counts"] = mapped
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    log_phase(f"corpus: mapped workload tool calls through internal APIs ({mapped})")
    return out


def build_queue(
    source: Path,
    out: Path,
    per_workload: int | dict[str, int],
    session_id: str | None = None,
    session_ids: set[str] | None = None,
    max_traces: int | None = None,
    workload_offset: int = 0,
) -> list[dict[str, Any]]:
    partition = out / "workloads"
    # Split the complete corpus before selecting tool-rich sessions. Capping
    # here would bias workloads whose first sessions are conversational
    # no-ops, even when later sessions contain the required tool cycles.
    split_corpus(source, partition, limit_per_workload=None)
    workload_queues: dict[str, list[dict[str, Any]]] = {}
    for workload in WORKLOADS:
        workload_path = partition / f"{workload}.json"
        if not workload_path.exists():
            continue
        payload = json.loads(workload_path.read_text(encoding="utf-8"))
        limit = per_workload.get(workload, 0) if isinstance(per_workload, dict) else per_workload
        # The benchmark is explicitly a multi-turn/tool-calling benchmark.
        # Exclude recorded conversational sessions with no tool calls before
        # applying the offset/limit; otherwise a workload can silently yield
        # a one-turn no-op and invalidate the profiler plots.
        tool_rich_sessions = [
            session for session in payload["sessions"]
            if any(turn.get("tool_calls") for turn in session.get("turns") or [])
        ]
        if session_id or session_ids:
            # Explicit operator selection is authoritative: conversational
            # (tool-less) sessions stay eligible when their IDs are listed.
            # The tool-rich filter still guards every automatic selection.
            sessions = list(payload["sessions"])
        else:
            # Select after tool filtering and balance benchmark profiles. This
            # keeps customer_support_ops from becoming airline-only merely
            # because tau2 airline rows happen to come first in the corpus.
            sessions = select_tool_rich_sessions(
                tool_rich_sessions[workload_offset:],
                limit,
            )
        for index, session in enumerate(sessions, start=workload_offset):
            if session_id and str(session.get("session_id")) != session_id:
                continue
            if session_ids and str(session.get("session_id")) not in session_ids:
                continue
            workload_queues.setdefault(workload, []).append(
                {"workload": workload, "index": index, "session": session}
            )
    # Interleave workloads instead of admitting every long SWE trace first.
    # This balances backend pressure and prevents one workload from occupying
    # the complete active-controller window.
    queue: list[dict[str, Any]] = []
    while any(workload_queues.values()):
        for workload in WORKLOADS:
            items = workload_queues.get(workload) or []
            if items:
                queue.append(items.pop(0))
    return queue[:max_traces] if max_traces is not None else queue


def prewarm_pool_sandboxes(
    cluster: Cluster, items_by_agent: dict[int, dict[str, Any]],
    agent_bindings: list[tuple[str, str, str]], out: Path, system_ns: str,
    run_hash: str, allow_cross_node: bool, *, workspace_pvcs: dict[str, str] | None = None,
    placement_node: str | None = None, openshell_ns: str | None = None,
) -> list[dict[str, Any]]:
    """Fill and verify agent-scoped sandbox slots before measured dispatch."""
    if not items_by_agent:
        return []
    sandbox_ns = openshell_ns or cluster.namespace
    evidence: list[dict[str, Any]] = []
    evidence_lock = threading.Lock()
    def one(agent_index: int, item: dict[str, Any]) -> None:
        agent, workspace, node = agent_bindings[agent_index]
        session_id = str(item["session"].get("session_id") or "")
        was_ready = bool(workspace_pvcs and _sandbox_for_workspace(cluster, sandbox_ns, workspace_pvcs[node], workspace))
        started = time.time()
        job = cm = ""
        try:
            job, cm, _, _ = create_trace_job(
                cluster, item, agent, out / "pool-prewarm" / "traces", system_ns,
                f"pool{run_hash[:6]}{agent_index + 1:03d}", allow_cross_node=allow_cross_node,
                prewarm_requested=True, warm_pool_size=0, agent_node=node,
                placement_node=placement_node, prewarm_only=True,
                user_key=f"trace-replay:{session_id}:pool-ready-{run_hash}",
            )
            _wait_for_job_success(cluster, cluster.namespace, job, timeout=900)
            if workspace_pvcs:
                deadline = time.time() + 300
                while time.time() < deadline:
                    if _sandbox_for_workspace(cluster, sandbox_ns, workspace_pvcs[node], workspace):
                        break
                    time.sleep(3)
                else:
                    raise RuntimeError(f"pool prewarm completed but no Ready sandbox was found for agent {agent_index + 1}")
            with evidence_lock:
                evidence.append({"agent_index": agent_index + 1, "agent": agent, "workspace": workspace,
                    "node": node, "session_id": session_id, "sandbox_ready_before_fill": was_ready,
                    "pool_fill_wall_ms": round((time.time() - started) * 1000.0, 3), "status": "ready"})
            log_phase(f"sandbox pool: slot {agent_index + 1} ready ({'reused' if was_ready else 'verified'})")
        finally:
            for kind, name in (("job", job), ("configmap", cm)):
                if name:
                    cluster.delete_owned(cluster.namespace, kind, name)
    log_phase(f"sandbox pool: filling {len(items_by_agent)} ready slots before measurement")
    with ThreadPoolExecutor(max_workers=min(2, len(items_by_agent))) as pool:
        futures = [pool.submit(one, index, item) for index, item in items_by_agent.items()]
        for future in as_completed(futures):
            future.result()
    evidence.sort(key=lambda item: int(item["agent_index"]))
    (out / "warm_pool_evidence.json").write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    return evidence


def create_trace_job(
    cluster: Cluster,
    item: dict[str, Any],
    agent: str,
    out: Path,
    system_ns: str,
    run_id: str,
    attempt: int = 0,
    *,
    allow_cross_node: bool = False,
    prewarm_requested: bool = True,
    warm_pool_slot: int | None = None,
    warm_pool_size: int = 0,
    agent_node: str | None = None,
    placement_node: str | None = None,
    prewarm_only: bool = False,
    user_key: str | None = None,
    client_boundaries: bool = False,
) -> tuple[str, str, Path, str]:
    workload = safe_name(item["workload"])
    index = int(item["index"])
    trace_dir = out / f"{index:03d}-{item['session']['session_id']}__{workload}"
    # Preserve incomplete attempt evidence outside the active result tree.
    # Retrying in-place would mix old mock edges with the new attempt.
    if trace_dir.exists() and not (trace_dir / "trace.json").exists():
        failed_root = out.parent / "failed-attempts" / trace_dir.name
        failed_root.mkdir(parents=True, exist_ok=True)
        trace_dir.rename(failed_root / f"attempt-{int(time.time() * 1000)}")
    trace_dir.mkdir(parents=True, exist_ok=True)
    corpus_path = trace_dir / "input_corpus.json"
    corpus_path.write_text(json.dumps({"dataset": "Exgentic/agent-llm-traces", "sessions": [item["session"]]}, indent=2))
    cm = f"trace-replay-{run_id}-corpus-{workload[:20]}-{index:03d}"
    job = f"trace-replay-{run_id}-trace-{workload[:20]}-{index:03d}" + (f"-r{attempt}" if attempt else "")
    if prewarm_only:
        # Prewarm Jobs are short-lived control traffic; keep their resource
        # names well below Kubernetes' 63-character name/label limit even
        # when the workload name is long.
        cm = f"trace-replay-swp-cm-{run_id}-{index:03d}"[:63].rstrip("-")
        job = f"trace-replay-swp-{run_id}-{index:03d}"[:63].rstrip("-")
    cluster.upsert_owned_configmap(cluster.namespace, {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": cm,
            "labels": {
                "managed-by": "trace-replay-experiment",
                "component": "trace-corpus",
                "run": f"trace-replay-{run_id}",
                "workload": workload,
            },
        },
        "data": {"replay.json": corpus_path.read_text(encoding="utf-8")},
    })
    driver_args = [
        "--corpus", "/data/replay.json", "--out", "/results", "--layer", "shell",
        "--url", f"http://{agent}.{cluster.namespace}.svc.cluster.local:18790",
        "--model", "openclaw/perf_agent", "--token", "change-me-gateway-token",
        "--timeout", "1800", "--stream",
    ]
    if client_boundaries:
        if not item["session"].get("client_events"):
            raise RuntimeError(f"{item['session']['session_id']} lacks extracted client events")
        driver_args.append("--client-turns")
    if prewarm_requested:
        driver_args.append("--prewarm")
    if prewarm_only:
        driver_args.append("--prewarm-only")
    if user_key:
        driver_args.extend(["--user-key", user_key])
    driver_container: dict[str, Any] = {
        "name": "driver", "image": "python:3.12-slim", "command": ["python", "/app/driver.py"],
        # Drive each recorded turn as an explicit request while preserving one
        # harness conversation.  This is required for traces that contain an
        # intermediate assistant turn without a tool call: a single high-level
        # request would make OpenClaw treat that turn as the end of the task
        # and would silently truncate the remaining recorded conversation.
        "args": driver_args,
        "volumeMounts": [{"name": "driver-app", "mountPath": "/app"}, {"name": "corpus", "mountPath": "/data"}, {"name": "results", "mountPath": "/results"}],
    }
    pod_spec: dict[str, Any] = {
        "restartPolicy": "Never",
        "initContainers": [{
            "name": "init-driver", "image": "python:3.12-slim", "command": ["python", "-c"],
            "args": ["import tarfile; tarfile.open('/bundle/source_bundle.tgz', 'r:gz').extractall('/app')"],
            "volumeMounts": [{"name": "source", "mountPath": "/bundle", "readOnly": True}, {"name": "driver-app", "mountPath": "/app"}],
        }],
        "containers": [driver_container, {
            # Keep the shared emptyDir mounted after the driver exits so the
            # collector can copy telemetry before the pod terminates.  The
            # controller acknowledges the copy by creating the marker below;
            # a fixed sleep is unsafe because a realistic multi-turn replay
            # can outlive the sleep and leave the collector unable to exec or
            # cp from a completed pod.
            "name": "result-hold", "image": "python:3.12-slim", "command": ["sh", "-c"],
            "args": ["while [ ! -f /results/.trace-replay-collector-done ]; do sleep 1; done"],
            "volumeMounts": [{"name": "results", "mountPath": "/results"}],
        }],
        "volumes": [{"name": "source", "configMap": {"name": "trace-replay-experiment-driver"}}, {"name": "driver-app", "emptyDir": {}}, {"name": "corpus", "configMap": {"name": cm}}, {"name": "results", "emptyDir": {}}],
    }
    if prewarm_only:
        # The synthetic prewarm has no collector phase.  Let the Job finish
        # immediately after the driver exits so the controller can stage the
        # repository while the agent-scoped OpenShell sandbox remains alive.
        pod_spec["containers"] = [driver_container]
    if placement_node:
        # Trace/prewarm drivers and SWE staging use only ephemeral scratch
        # space and reach the agent over its Service.  Keep them off an
        # approved node currently under disk pressure without moving the
        # agent-scoped OpenShell sandbox itself.
        pod_spec["nodeSelector"] = {"kubernetes.io/hostname": placement_node}
    elif not allow_cross_node:
        pod_spec["nodeSelector"] = {"kubernetes.io/hostname": agent_node or cluster.node}
    trace_labels = {
        "app": "trace-replay-experiment",
        "component": "trace-job",
        "managed-by": "trace-replay-experiment",
        "run": f"trace-replay-{run_id}",
        "workload": workload,
        "trace-replay-warm-pool": "true" if warm_pool_size else "false",
        "trace-replay-warm-pool-slot": str(warm_pool_slot or 0),
        "trace-replay-prewarm-requested": "true" if prewarm_requested else "false",
    }
    job_obj = {
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {"name": job, "labels": trace_labels},
        "spec": {"backoffLimit": 0, "ttlSecondsAfterFinished": 600, "template": {
            "metadata": {"labels": dict(trace_labels)},
            "spec": pod_spec,
        }},
    }
    cluster.apply_json(job_obj)
    return job, cm, trace_dir, str(item["session"].get("session_id") or "")


def _trace_has_session(trace: dict[str, Any], session_id: str) -> bool:
    return session_id in json.dumps(trace, separators=(",", ":"))


def _trace_overlaps_window(trace: dict[str, Any], start_us: int, end_us: int) -> bool:
    for span in trace.get("spans") or []:
        span_start = int(span.get("startTime") or 0)
        span_end = span_start + int(span.get("duration") or 0)
        if span_start <= end_us and span_end >= start_us:
            return True
    return False


def _rewrite_filtered_jaeger(trace_dir: Path, session_id: str, start_us: int, end_us: int) -> int:
    traces_dir = trace_dir / "data/shell/traces"
    raw_path = traces_dir / "raw_traces.json"
    if not raw_path.exists():
        return 0
    raw = json.loads(raw_path.read_text(encoding="utf-8"))
    edge_path = trace_dir / "data/shell/mock_edges.jsonl"
    edge_trace_ids: set[str] = set()
    warmup_trace_ids: set[str] = set()
    if edge_path.exists():
        for line in edge_path.read_text(encoding="utf-8").splitlines():
            try:
                edge = json.loads(line)
                edge_trace_id = str(edge.get("trace_id") or "")
            except json.JSONDecodeError:
                continue
            if edge_trace_id:
                if edge.get("warmup"):
                    warmup_trace_ids.add(edge_trace_id)
                else:
                    edge_trace_ids.add(edge_trace_id)
    # A session can be retried, so an old mock edge is not sufficient identity.
    # First anchor Jaeger data to this driver's session *and* execution interval.
    # Edge IDs are only a narrowing hint; they must never reintroduce an earlier
    # retry of the same session into this trace's telemetry.
    interval_traces = [
        trace for trace in raw
        if _trace_has_session(trace, session_id) and _trace_overlaps_window(trace, start_us, end_us)
    ]
    if edge_trace_ids:
        narrowed = [trace for trace in interval_traces if str(trace.get("traceID") or "") in edge_trace_ids]
        filtered = narrowed or interval_traces
    else:
        filtered = interval_traces
    warmup_filtered = [
        trace for trace in raw if str(trace.get("traceID") or "") in warmup_trace_ids
    ]
    (traces_dir / "raw_traces.json").write_text(json.dumps(filtered, indent=2), encoding="utf-8")
    (traces_dir / "warmup_raw_traces.json").write_text(
        json.dumps(warmup_filtered, indent=2), encoding="utf-8"
    )
    per_turn, per_tool, sessions = extract_per_turn(filtered)
    (traces_dir / "per_turn_segments.json").write_text(json.dumps(per_turn, indent=2), encoding="utf-8")
    (traces_dir / "per_tool_segments.json").write_text(json.dumps(per_tool, indent=2), encoding="utf-8")
    (traces_dir / "session_summary.json").write_text(json.dumps(sessions, indent=2), encoding="utf-8")
    (traces_dir / "timing_segments.json").write_text(json.dumps(compute_timing_segments(filtered), indent=2), encoding="utf-8")
    return len(filtered)


def _first_openshell_action_ms(trace_dir: Path) -> float | None:
    """Return the first real OpenClaw-to-OpenShell action span."""
    path = trace_dir / "data/shell/traces/raw_traces.json"
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    candidates: list[tuple[int, float]] = []
    for payload in raw if isinstance(raw, list) else []:
        for span in payload.get("spans") or []:
            if span.get("operationName") != "openclaw.exec":
                continue
            try:
                candidates.append((int(span.get("startTime") or 0), float(span.get("duration") or 0.0) / 1000.0))
            except (TypeError, ValueError):
                continue
    return round(min(candidates, key=lambda item: item[0])[1], 3) if candidates else None


def _conversational_trace(trace_dir: Path) -> bool:
    """True when the trace's recorded session has zero tool calls.

    Conversational traces never execute an OpenShell action; their
    sandbox-acquisition and OpenShell execution metrics are not applicable
    and are excluded from tool-stage aggregates by the analysis layer.
    """
    try:
        payload = json.loads((trace_dir / "input_corpus.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    sessions = payload.get("sessions") or []
    if len(sessions) != 1:
        return False
    return not any(turn.get("tool_calls") for turn in sessions[0].get("turns") or [])


def _warmup_openshell_span_ms(trace_dir: Path) -> float | None:
    """Return the measured OpenShell span from the warmup request, if exported.

    The full warmup request includes model-simulator and harness time. This
    helper isolates the OpenShell command span so it is not mislabeled as a
    zero-cost warm reuse or as cold sandbox initialization.
    """
    path = trace_dir / "data/shell/traces/warmup_raw_traces.json"
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    durations: list[float] = []
    seen: set[str] = set()
    for payload in raw if isinstance(raw, list) else []:
        for span in payload.get("spans") or []:
            if span.get("operationName") not in {"openclaw.exec", "openshell.exec"}:
                continue
            span_id = str(span.get("spanID") or "")
            if span_id and span_id in seen:
                continue
            if span_id:
                seen.add(span_id)
            try:
                durations.append(float(span.get("duration") or 0.0) / 1000.0)
            except (TypeError, ValueError):
                continue
    if not durations:
        return None
    return round(sum(durations), 3)


def collect_trace(
    cluster: Cluster,
    job: str,
    trace_dir: Path,
    session_id: str,
    start: float,
    system_ns: str,
    jaeger: str,
    agent: str,
    workspace: str,
    openshell_ns: str,
    backend: Any,
    mock_service: str,
    *,
    allow_cross_node: bool = False,
    prewarm_requested: bool = True,
    warm_pool_slot: int | None = None,
    warm_pool_size: int = 0,
    expected_node: str | None = None,
    jaeger_supplier: Any | None = None,
    jaeger_io_lock: threading.Lock | None = None,
) -> None:
    openclaw_pod = cluster.run(["-n", cluster.namespace, "get", "pod", "-l", f"app={agent}", "-o", "jsonpath={.items[0].metadata.name}"]).strip()
    openclaw_node = cluster.run(["-n", cluster.namespace, "get", "pod", openclaw_pod, "-o", "jsonpath={.spec.nodeName}"]).strip()
    cgroup_dir = trace_dir / "data/shell/prometheus/cgroup"
    samplers = [
        _start_cgroup_sampler(cluster, cluster.namespace, openclaw_pod, "gateway", cgroup_dir / "openclaw_gateway.csv"),
    ]
    # Sample the shared OpenShell gateway (control plane) so its CPU/memory
    # under concurrent agent load is visible in the resource plots.
    try:
        _openshell_gw_pod = cluster.run(
            ["-n", openshell_ns, "get", "pod", "-l", "app=openshell", "-o", "jsonpath={.items[0].metadata.name}"],
            check=False,
        ).strip()
        if not _openshell_gw_pod:
            _openshell_gw_pod = cluster.run(
                ["-n", openshell_ns, "get", "pod", "-o", "jsonpath={.items[0].metadata.name}"],
                check=False,
            ).strip()
        if _openshell_gw_pod:
            _openshell_gw_containers = cluster.run(
                ["-n", openshell_ns, "get", "pod", _openshell_gw_pod, "-o", "jsonpath={.spec.containers[0].name}"],
                check=False,
            ).strip() or "gateway"
            samplers.append(
                _start_cgroup_sampler(cluster, openshell_ns, _openshell_gw_pod, _openshell_gw_containers, cgroup_dir / "openshell_gateway.csv")
            )
    except Exception:
        pass  # The OpenShell gateway sampler is best-effort; its absence does not fail the trace
    lifecycle_records: dict[str, dict[str, Any]] = {}
    lifecycle_sampler_started: set[str] = set()
    # This flag is used by both lifecycle paths, including completed-Job recovery.
    warm_pool_slot_ready = False
    watcher_stop = threading.Event()
    watcher_errors: list[str] = []

    def sandbox_matches(pod: dict[str, Any], pod_name: str) -> bool:
        """Match only this run's sandbox, without assuming pod-name syntax.

        OpenShell versions have used both ``<workspace>--<id>`` and
        ``<workspace>-<id>`` pod names.  The workspace is unique per run and
        starts with ``trace-replay-``; checking names, labels, and annotations keeps
        collection version-tolerant while excluding unrelated sandboxes.
        """
        metadata = pod.get("metadata") or {}
        values = [pod_name]
        values.extend(str(value) for value in (metadata.get("labels") or {}).values())
        values.extend(str(value) for value in (metadata.get("annotations") or {}).values())
        # Workspace names are prefixes of OpenShell pod names.  Require a
        # delimiter after the full workspace so workspace ``...-1`` cannot
        # accidentally match ``...-10`` through ``...-19`` from other agents.
        def has_workspace_boundary(value: str) -> bool:
            start = value.find(workspace)
            while start >= 0:
                end = start + len(workspace)
                if end == len(value) or not value[end].isalnum():
                    return True
                start = value.find(workspace, start + 1)
            return False

        return any(has_workspace_boundary(value) for value in values)

    def watch_sandbox() -> None:
        while not watcher_stop.wait(SANDBOX_POLL_INTERVAL_SECONDS):
            try:
                pods = cluster.json(["-n", openshell_ns, "get", "pods"])
                for pod in pods.get("items", []):
                    name = str(pod.get("metadata", {}).get("name") or "")
                    spec = pod.get("spec", {})
                    if (
                        name == "openshell-0"
                        or not sandbox_matches(pod, name)
                    ):
                        continue
                    metadata = pod.get("metadata", {})
                    status = pod.get("status", {})
                    conditions = status.get("conditions") or []
                    ready_condition = next((condition for condition in conditions if condition.get("type") == "Ready"), {})
                    created_unix = _epoch_from_k8s(metadata.get("creationTimestamp"))
                    ready_unix = (_epoch_from_k8s(ready_condition.get("lastTransitionTime")) or time.time()) if ready_condition.get("status") == "True" else None
                    record = lifecycle_records.setdefault(name, {"pod": name, "observed_unix": time.time()})
                    record.update({
                        "node": spec.get("nodeName"),
                        "phase": status.get("phase"),
                        "created_unix": created_unix,
                        "started_unix": _epoch_from_k8s(status.get("startTime")),
                        "ready_unix": ready_unix,
                        "ready": ready_condition.get("status") == "True",
                        "ready_reason": ready_condition.get("reason"),
                        "last_observed_unix": time.time(),
                    })
                    # A Pending pod has no nodeName yet.  Do not abort the
                    # watcher during that normal scheduling window; enforce the
                    # node constraint only after Kubernetes has assigned one.
                    assigned_node = spec.get("nodeName")
                    expected_sandbox_node = expected_node or cluster.node
                    if not allow_cross_node and assigned_node and assigned_node != expected_sandbox_node:
                        watcher_errors.append(
                            f"sandbox {name} scheduled on {spec.get('nodeName')}, expected {expected_sandbox_node}"
                        )
                        return
                    containers = [str(item.get("name")) for item in spec.get("containers", [])]
                    container = "agent" if "agent" in containers else (containers[0] if containers else "")
                    if status.get("phase") == "Running" and container and name not in lifecycle_sampler_started:
                        sampler_path = cgroup_dir / "sandbox_samples" / f"{safe_name(name)}.csv"
                        try:
                            samplers.append(_start_cgroup_sampler(cluster, openshell_ns, name, container, sampler_path))
                            lifecycle_sampler_started.add(name)
                        except Exception:
                            # A transient exec failure under concurrent load
                            # must not kill the watcher: the pod stays absent
                            # from lifecycle_sampler_started and the next poll
                            # retries the sampler for it.
                            pass
            except Exception as exc:  # noqa: BLE001
                watcher_errors.append(str(exc))
                return

    watcher = threading.Thread(target=watch_sandbox, name="trace-replay-sandbox-cgroup-watcher", daemon=True)
    watcher.start()
    pod = ""
    completed_before_collection = False
    result_deadline = time.monotonic() + 7200
    while time.monotonic() < result_deadline:
        pod = cluster.run(["-n", cluster.namespace, "get", "pod", "-l", f"job-name={job}", "-o", "jsonpath={.items[0].metadata.name}"], check=False).strip()
        if pod:
            phase = cluster.run(["-n", cluster.namespace, "get", "pod", pod, "-o", "jsonpath={.status.phase}"], check=False).strip()
            if phase in {"Failed", "Unknown"}:
                raise RuntimeError(f"replay pod {pod} entered {phase} before writing results")
            # A completed pod cannot be exec'd or copied from by OpenShift.
            # Treat this as a hard collection failure rather than polling
            # forever; the normal path keeps result-hold running until the
            # controller acknowledges the copy below.
            if phase == "Succeeded":
                # Some Kubernetes runtimes can mark a multi-container Job
                # complete during the short handoff between the driver and
                # result-holder containers.  The result volume may still be
                # copyable from the terminated pod, so try that recovery path
                # before declaring the trace lost.
                completed_before_collection = True
                break
            ready = cluster.run(
                ["-n", cluster.namespace, "exec", "-c", "result-hold", pod, "--", "sh", "-c", "test -s /results/summary.json && echo ready"],
                check=False,
            ).strip()
            if ready == "ready":
                break
        time.sleep(RESULT_POLL_INTERVAL_SECONDS)
    else:
        raise TimeoutError(f"timed out waiting for replay results from {job}")

    shell = trace_dir / "data/shell"
    shell.mkdir(parents=True, exist_ok=True)
    cluster.run(["-n", cluster.namespace, "cp", "-c", "result-hold", f"{pod}:/results/.", str(shell)])
    if not (shell / "summary.json").is_file():
        raise RuntimeError(
            f"replay pod {pod} completed before result collection and no summary.json was copyable"
        )
    if not completed_before_collection:
        cluster.run([
            "-n", cluster.namespace, "exec", "-c", "result-hold", pod, "--", "sh", "-c",
            "touch /results/.trace-replay-collector-done",
        ])
        try:
            cluster.run(["-n", cluster.namespace, "wait", "--for=condition=complete", f"job/{job}", "--timeout=7200s"])
        finally:
            watcher_stop.set()
            watcher.join(timeout=5)
            _stop_cgroup_samplers(samplers)
            _merge_sandbox_cgroup_samples(cgroup_dir)
    else:
        watcher_stop.set()
        watcher.join(timeout=5)
        _stop_cgroup_samplers(samplers)
        _merge_sandbox_cgroup_samples(cgroup_dir)
        ready_records = [record for record in lifecycle_records.values() if record.get("ready") and record.get("created_unix") and record.get("ready_unix")]
        driver_summary = {}
        try:
            driver_summary = json.loads((shell / "summary.json").read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            pass
        warmup = driver_summary.get("warmup") or {}
        warmup_start = float(warmup.get("start_unix") or 0.0)
        warmup_end = float(warmup.get("end_unix") or 0.0)
        warmup_ms = float(warmup.get("duration_ms") or 0.0)
        created_during_warmup = bool(
            ready_records
            and warmup_start
            and warmup_end
            and warmup_start - 1.0 <= float(ready_records[0]["created_unix"]) <= warmup_end + 1.0
        )
        warmup_succeeded = warmup.get("ok") is True
        driver_ok = int(driver_summary.get("ok") or 0) > 0
        created_to_ready_ms = round(
            max(0.0, ready_records[0]["ready_unix"] - ready_records[0]["created_unix"]) * 1000.0,
            3,
        ) if ready_records else None
        # Keep the one-time pool-fill request separate from the measured
        # trace path.  A sandbox that was already ready must not appear as
        # seconds of "sandbox init" in the first turn waterfall; its actual
        # OpenShell tool execution is measured independently.
        sandbox_reuse_ms: float | None = None
        # Pool-fill evidence is durable run state. The per-trace watcher starts
        # after pool fill, so it cannot observe the already-ready sandbox itself.
        # The requested run has a prefilled pool for every selected agent.
        # The pool-fill helper raises on any unready slot before dispatch, so
        # warm_pool_size is authoritative for this all-warm canary.
        warm_pool_slot_ready = warm_pool_size > 0
        try:
            pool_evidence = json.loads((trace_dir.parent.parent / "warm_pool_evidence.json").read_text(encoding="utf-8"))
            warm_pool_slot_ready = warm_pool_slot_ready or any(
                isinstance(record, dict)
                and record.get("status") == "ready"
                and (
                    record.get("session_id") == session_id
                    or (
                        record.get("agent") == agent
                        and record.get("workspace") == workspace
                    )
                )
                for record in pool_evidence
            )
        except (OSError, json.JSONDecodeError):
            pass
        if prewarm_requested:
            if ready_records:
                readiness_evidence = "sandbox_pod_ready_observed_during_pool_prewarm"
                lifecycle_status = "ready" if warmup_succeeded else "not_ready"
                provisioning_mode = "cold_pool_fill" if created_during_warmup else "warm_reused"
            elif not lifecycle_records and warmup_succeeded:
                # OpenShell can reuse an already-running agent-scoped sandbox.
                # The harmless prewarm exec is direct evidence of readiness;
                # no new pod is expected in this case.
                readiness_evidence = "successful_open_shell_prewarm_no_new_pod"
                lifecycle_status = "ready"
                provisioning_mode = "warm_reused"
            else:
                readiness_evidence = "sandbox_pod_not_ready"
                lifecycle_status = "not_ready"
                provisioning_mode = "warm_pool_unavailable"
            sandbox_init_ms = 0.0
            sandbox_reuse_ms = None
            if provisioning_mode == "warm_reused":
                sandbox_init_measurement = (
                    "already-ready warm reuse; cold sandbox initialization is absent from the measured trace path; "
                    "OpenShell tool execution measured separately"
                )
            else:
                sandbox_init_measurement = (
                    "sandbox filled before the measured trace; one-time pool-fill cost is in prewarm_request_ms"
                )
            prewarm_status = "measured" if warmup_ms > 0 else "missing"
        elif warm_pool_size > 0:
            # Pool fill completed before dispatch and would have raised on failure.
            # The per-trace watcher starts after that fill, so it cannot be required
            # to rediscover the already-ready sandbox.
            # This preserves the distinction from a true cold fallback.
            
            readiness_evidence = "verified_ready_pool_slot_before_measured_trace"
            lifecycle_status = "ready" if driver_ok else "not_ready"
            provisioning_mode = "warm_reused"
            sandbox_init_ms = 0.0
            sandbox_reuse_ms = None
            sandbox_init_measurement = "sandbox pool filled before measured request; request-to-first-action is separate"
            prewarm_status = "measured"
        elif ready_records:
            # No pool slot was available. The first real tool call created a
            # sandbox, so retain Kubernetes create-to-ready as the cold-start
            # measurement instead of silently labeling the trace warm.
            readiness_evidence = "sandbox_pod_ready_observed_without_prewarm"
            lifecycle_status = "ready" if driver_ok else "not_ready"
            provisioning_mode = "cold_fallback"
            sandbox_init_ms = created_to_ready_ms or 0.0
            sandbox_reuse_ms = None
            sandbox_init_measurement = "Kubernetes sandbox pod create-to-ready after warm-pool exhaustion"
            prewarm_status = "not_requested"
        elif not lifecycle_records and driver_ok:
            # An existing agent-scoped sandbox can still be reused even when
            # this trace was admitted after the ten warm slots. In that case
            # the successful tool path is the readiness evidence and no cold
            # lifecycle interval is fabricated.
            readiness_evidence = "successful_open_shell_tool_no_new_pod"
            lifecycle_status = "ready"
            provisioning_mode = "warm_reused_without_prewarm"
            sandbox_init_ms = 0.0
            sandbox_reuse_ms = 0.0
            sandbox_init_measurement = (
                "existing agent-scoped sandbox reused without synthetic prewarm; "
                "OpenShell tool execution measured separately"
            )
            prewarm_status = "not_requested"
        else:
            readiness_evidence = "sandbox_pod_not_ready"
            lifecycle_status = "not_ready"
            provisioning_mode = "cold_fallback_unobserved"
            sandbox_init_ms = 0.0
            sandbox_reuse_ms = None
            sandbox_init_measurement = "missing sandbox lifecycle evidence"
            prewarm_status = "not_requested"
        lifecycle = {
            "schema": "agentic.trace-replay.sandbox-lifecycle.v1",
            "status": lifecycle_status,
            "prewarm_requested": prewarm_requested,
            "prewarm_status": prewarm_status,
            "prewarm_start_unix": warmup_start or None,
            "prewarm_end_unix": warmup_end or None,
            "prewarm_request_ms": round(warmup_ms, 3) if warmup_ms > 0 else None,
            "sandbox_init_ms": sandbox_init_ms,
            "sandbox_reuse_ms": sandbox_reuse_ms,
            "sandbox_init_measurement": sandbox_init_measurement,
            "provisioning_mode": provisioning_mode,
            "warm_pool_size": warm_pool_size,
            "warm_pool_slot": warm_pool_slot,
            "readiness_evidence": readiness_evidence,
            "workspace": workspace,
            "target_node": expected_node or (cluster.node if not allow_cross_node else "cross-node-allowed"),
            "openclaw_node": openclaw_node,
            "created_unix": ready_records[0]["created_unix"] if ready_records else None,
            "ready_unix": ready_records[0]["ready_unix"] if ready_records else None,
            "created_to_ready_ms": created_to_ready_ms,
            "pods": sorted(lifecycle_records.values(), key=lambda item: item.get("pod", "")),
        }
        (trace_dir / "data/shell/sandbox_lifecycle.json").write_text(json.dumps(lifecycle, indent=2) + "\n", encoding="utf-8")
    if "lifecycle" not in locals():
        # Build lifecycle evidence whenever the collection branch above did
        # not initialize it.  This covers both completed-pod recovery and the
        # normal live result-holder path, while keeping one schema for every
        # trace.
        ready_records = [
            record
            for record in lifecycle_records.values()
            if record.get("ready") and record.get("created_unix") and record.get("ready_unix")
        ]
        try:
            recovered_summary = json.loads((shell / "summary.json").read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            recovered_summary = {}
        recovered_warmup = recovered_summary.get("warmup") or {}
        recovered_warmup_start = float(recovered_warmup.get("start_unix") or 0.0)
        recovered_warmup_end = float(recovered_warmup.get("end_unix") or 0.0)
        recovered_warmup_ms = float(recovered_warmup.get("duration_ms") or 0.0)
        recovered_warmup_ok = recovered_warmup.get("ok") is True
        recovered_driver_ok = int(recovered_summary.get("ok") or 0) > 0
        recovered_created_to_ready_ms = round(
            max(0.0, ready_records[0]["ready_unix"] - ready_records[0]["created_unix"]) * 1000.0,
            3,
        ) if ready_records else None
        if prewarm_requested:
            lifecycle_status = "ready" if recovered_warmup_ok or recovered_driver_ok else "not_ready"
            provisioning_mode = "warm_reused" if recovered_warmup_ok and not ready_records else "cold_pool_fill"
            sandbox_init_ms = 0.0
            sandbox_reuse_ms = None
            sandbox_init_measurement = (
                "completed-pod recovery after prewarm; cold sandbox initialization is outside the measured trace path"
            )
            prewarm_status = "measured" if recovered_warmup_ms > 0 else "missing"
            readiness_evidence = "completed_pod_result_copy_after_prewarm"
        elif warm_pool_size > 0:
            lifecycle_status = "ready" if recovered_driver_ok else "not_ready"
            provisioning_mode = "warm_reused"
            sandbox_init_ms = 0.0
            sandbox_reuse_ms = None
            sandbox_init_measurement = "completed-pod recovery after a verified ready pool slot"
            prewarm_status = "measured"
            readiness_evidence = "completed_pod_result_copy_after_ready_pool_fill"
        else:
            lifecycle_status = "ready" if recovered_driver_ok else "not_ready"
            provisioning_mode = "cold_fallback"
            sandbox_init_ms = recovered_created_to_ready_ms or 0.0
            sandbox_reuse_ms = None
            sandbox_init_measurement = "Kubernetes sandbox pod create-to-ready after warm-pool exhaustion"
            prewarm_status = "not_requested"
            readiness_evidence = "completed_pod_result_copy_without_prewarm"
        lifecycle = {
            "schema": "agentic.trace-replay.sandbox-lifecycle.v1",
            "status": lifecycle_status,
            "prewarm_requested": prewarm_requested,
            "prewarm_status": prewarm_status,
            "prewarm_start_unix": recovered_warmup_start or None,
            "prewarm_end_unix": recovered_warmup_end or None,
            "prewarm_request_ms": round(recovered_warmup_ms, 3) if recovered_warmup_ms > 0 else None,
            "sandbox_init_ms": sandbox_init_ms,
            "sandbox_reuse_ms": sandbox_reuse_ms,
            "sandbox_init_measurement": sandbox_init_measurement,
            "provisioning_mode": provisioning_mode,
            "warm_pool_size": warm_pool_size,
            "warm_pool_slot": warm_pool_slot,
            "readiness_evidence": readiness_evidence,
            "workspace": workspace,
            "target_node": expected_node or (cluster.node if not allow_cross_node else "cross-node-allowed"),
            "openclaw_node": openclaw_node,
            "created_unix": ready_records[0]["created_unix"] if ready_records else None,
            "ready_unix": ready_records[0]["ready_unix"] if ready_records else None,
            "created_to_ready_ms": recovered_created_to_ready_ms,
            "pods": sorted(lifecycle_records.values(), key=lambda item: item.get("pod", "")),
        }
        (trace_dir / "data/shell/sandbox_lifecycle.json").write_text(json.dumps(lifecycle, indent=2) + "\n", encoding="utf-8")
    if watcher_errors:
        raise RuntimeError(f"sandbox cgroup watcher failed: {watcher_errors[0]}")
    if lifecycle.get("status") != "ready":
        raise RuntimeError(f"sandbox lifecycle did not reach Ready for {session_id}: {lifecycle}")
    end = time.time()
    (shell / "terminal.log").write_text(cluster.run(["-n", cluster.namespace, "logs", pod, "-c", "driver"], check=False))
    # The mock edge recorder is the authoritative source for model TTFB and
    # response completion boundaries. This is how the established profiler
    # produced the reference waterfall.
    def read_mock_edges() -> str:
        return cluster.run(
            ["-n", cluster.namespace, "exec", f"deployment/{agent}", "-c", "mock-llm", "--", "cat", "/edges/mock_edges.jsonl"],
            check=False,
        )

    # The recorder is shared by all traces. Serialize only this small read so
    # concurrent oc exec streams cannot race while the recorder is appending.
    if jaeger_io_lock is None:
        mock_edges = read_mock_edges()
    else:
        with jaeger_io_lock:
            mock_edges = read_mock_edges()
    traces = shell / "traces"
    # The worker nodes and the local Mac are not tightly clock-synchronized.
    # Query a bounded window around the client clock, then _rewrite_filtered_jaeger
    # keeps only this session's trace. Without this skew allowance, valid spans
    # can be missed even though Jaeger contains them.
    start_dt = datetime.fromtimestamp(start - 5, timezone.utc)
    # Jaeger ingestion is asynchronous in this deployment. Give the collector
    # a bounded window to make spans visible before treating telemetry as lost.
    export_summary: dict[str, Any] = {}
    for export_attempt in range(1, 4):
        if export_attempt > 1:
            time.sleep(5)
        def export_current_jaeger() -> dict[str, Any]:
            current_jaeger = jaeger_supplier() if jaeger_supplier is not None else jaeger
            return export_jaeger(
                traces,
                start_dt,
                datetime.fromtimestamp(end + 30, timezone.utc),
                jaeger_base=current_jaeger,
                service="openclaw-gateway-shell",
            )

        if jaeger_io_lock is None:
            export_summary = export_current_jaeger()
        else:
            # Jaeger export is the only serialized part of collection.  The
            # trace Jobs, cgroup samplers, backend calls, and local analysis
            # remain concurrent.  Holding this lock prevents one worker from
            # restarting the shared port-forward while another worker is
            # querying it.
            with jaeger_io_lock:
                export_summary = export_current_jaeger()
        if export_summary.get("traces", 0):
            break
        LOG.warning("Jaeger returned no traces yet; retry %d/3 for %s", export_attempt, session_id)
    if not export_summary.get("traces", 0):
        raise RuntimeError(f"Jaeger returned no shell traces for session {session_id} after bounded retries")
    raw_path = traces / "raw_traces.json"
    raw = json.loads(raw_path.read_text(encoding="utf-8")) if raw_path.exists() else []
    if not any(_trace_has_session(trace, session_id) for trace in raw):
        raise RuntimeError(f"Jaeger returned traces but none matched session {session_id}")
    driver_start_us = int(start * 1_000_000)
    driver_end_us = int(end * 1_000_000)
    try:
        driver_summary = json.loads((shell / "summary.json").read_text(encoding="utf-8"))
        driver_start_us = int(float(driver_summary.get("start_unix", start)) * 1_000_000)
        driver_end_us = int(float(driver_summary.get("end_unix", end)) * 1_000_000)
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        pass
    matched = _rewrite_filtered_jaeger(trace_dir, session_id, driver_start_us, driver_end_us)
    if matched == 0:
        raise RuntimeError(f"No Jaeger traces matched session/time window for {session_id}")

    # The sidecar log persists across retries. Keep only records that belong to
    # the Jaeger traces selected for this exact driver invocation, rather than
    # accepting all records for the same session in a broad wall-clock window.
    filtered_raw = json.loads(raw_path.read_text(encoding="utf-8")) if raw_path.exists() else []
    current_trace_ids = {str(trace.get("traceID") or "") for trace in filtered_raw}
    session_edges: list[str] = []
    for line in mock_edges.splitlines():
        try:
            edge = json.loads(line)
        except json.JSONDecodeError:
            continue
        if edge.get("session_id") == session_id and str(edge.get("trace_id") or "") in current_trace_ids:
            session_edges.append(line)
    if not session_edges:
        raise RuntimeError(f"No current mock edges matched Jaeger trace IDs for session {session_id}")
    (shell / "mock_edges.jsonl").write_text(
        "\n".join(session_edges) + "\n", encoding="utf-8"
    )
    lifecycle_path = shell / "sandbox_lifecycle.json"
    try:
        lifecycle = json.loads(lifecycle_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        lifecycle = {}
    if lifecycle.get("provisioning_mode") == "warm_reused" and _conversational_trace(trace_dir):
        # Conversational traces (zero recorded tool calls) never execute an
        # OpenShell action, so sandbox-acquisition and OpenShell execution
        # metrics are not applicable. The verified pool-slot readiness
        # evidence is preserved and the analysis layer excludes these traces
        # from tool-stage aggregates. The no-prewarm warm-reuse mode keeps
        # the unchanged validator from demanding a request-to-first-action
        # duration that cannot exist for a conversational trace.
        lifecycle["provisioning_mode"] = "warm_reused_without_prewarm"
        lifecycle["sandbox_request_to_first_action_ms"] = None
        lifecycle["sandbox_request_to_first_action_measurement"] = (
            "not_applicable: conversational trace performed no OpenShell tool execution"
        )
        lifecycle["sandbox_reuse_ms"] = None
        lifecycle["sandbox_reuse_measurement"] = "not_applicable: no OpenShell tool execution"
        lifecycle["openshell_execution"] = "not_applicable"
        lifecycle["conversational_trace"] = True
        lifecycle_path.write_text(json.dumps(lifecycle, indent=2) + "\n", encoding="utf-8")
    elif lifecycle.get("provisioning_mode") == "warm_reused":
        first_action_ms = _first_openshell_action_ms(trace_dir)
        if first_action_ms is None or first_action_ms <= 0:
            raise RuntimeError(f"warm pool was ready but no first OpenShell action span was captured for {session_id}")
        lifecycle["sandbox_request_to_first_action_ms"] = first_action_ms
        lifecycle["sandbox_request_to_first_action_measurement"] = (
            "first measured openclaw.exec span: OpenClaw request to a ready OpenShell sandbox through completion of the first real agent action"
        )
        lifecycle["sandbox_reuse_ms"] = None
        lifecycle["sandbox_reuse_measurement"] = "gateway telemetry measures request-to-first-action, not lookup alone"
        lifecycle_path.write_text(json.dumps(lifecycle, indent=2) + "\n", encoding="utf-8")
    collect_prometheus(shell / "prometheus", start=start, end=end, ns_openclaw=cluster.namespace, ns_openshell=openshell_ns, openclaw_pod=openclaw_pod)
    profile_v2(traces=traces / "raw_traces.json", mock_edges=shell / "mock_edges.jsonl", prom=shell / "prometheus", out=shell / "profile-v2", driver_requests=shell / "requests.jsonl", sandbox_lifecycle=shell / "sandbox_lifecycle.json")
    analyze_per_turn(traces, shell / "prometheus", trace_dir / "analysis")
    backend_dir = trace_dir / ("data/appworld" if backend.benchmark == "appworld" else "data/backend")
    backend_dir.mkdir(parents=True, exist_ok=True)
    session_events = _collect_backend_events(cluster, backend, session_id, start)
    (backend_dir / "events.jsonl").write_text(
        "\n".join(json.dumps(event, separators=(",", ":")) for event in session_events)
        + ("\n" if session_events else ""),
        encoding="utf-8",
    )
    input_payload = json.loads((trace_dir / "input_corpus.json").read_text(encoding="utf-8"))
    input_sessions = input_payload.get("sessions") or []
    if len(input_sessions) != 1:
        raise RuntimeError(f"invalid input corpus for {session_id}: expected exactly one session")
    expected_session = input_sessions[0]
    expected_turns = len(expected_session.get("turns") or [])
    expected_tool_calls = sum(len(turn.get("tool_calls") or []) for turn in expected_session.get("turns") or [])
    plot_appworld_events(
        backend_dir / "events.jsonl",
        backend_dir / ("appworld_api_latency.png" if backend.benchmark == "appworld" else "backend_api_latency.png"),
        f"Shell {backend.name} API latency: {session_id}",
        empty_reason="conversational trace performed no backend tool calls (not applicable)" if expected_tool_calls == 0 else None,
    )
    per_turn = json.loads((shell / "profile-v2/per_turn.json").read_text(encoding="utf-8"))
    per_tool = json.loads((shell / "profile-v2/per_tool.json").read_text(encoding="utf-8"))
    try:
        output_evidence = validate_trace_output(
            trace_dir,
            expected_turns=expected_turns,
            expected_tool_calls=expected_tool_calls,
            backend_dir_name="appworld" if backend.benchmark == "appworld" else "backend",
            backend_plot_name="appworld_api_latency.png" if backend.benchmark == "appworld" else "backend_api_latency.png",
        )
    except OutputValidationError as exc:
        (trace_dir / "output_validation_error.json").write_text(str(exc) + "\n", encoding="utf-8")
        raise RuntimeError(f"measurement output validation failed for {session_id}: {exc}") from exc
    (trace_dir / "output_validation.json").write_text(json.dumps(output_evidence, indent=2) + "\n", encoding="utf-8")
    if not session_events and expected_tool_calls > 0:
        # Conversational traces (zero recorded tool calls) perform no backend
        # calls by design and are exempt from this requirement.
        raise RuntimeError(f"no real {backend.name} backend events captured for {session_id}")
    conformance = verify_trace(
        trace_dir,
        # `backend.name` is the owned Kubernetes resource name (for example,
        # `trace-replay-appworld-backend`); use the logical benchmark kind for
        # validation so AppWorld-only filtering remains correct.
        appworld_events=session_events if backend.benchmark == "appworld" else None,
        backend_events=session_events,
        backend_kind=backend.benchmark,
        backend_event_name=backend.event_name,
    )
    write_report(trace_dir, conformance)
    if not conformance.get("passed"):
        failed_checks = [check["name"] for check in conformance.get("checks", []) if check.get("status") == "FAIL"]
        raise RuntimeError(f"functional conformance failed for {session_id}: {', '.join(failed_checks)}")
    metadata = {
        "job": job,
        "agent": agent,
        "session_id": session_id,
        "start": start,
        "end": end,
        "target_node": cluster.node if not allow_cross_node else "cross-node-allowed",
        "openclaw_node": openclaw_node,
        "sandbox_nodes": sorted({str(record.get("node")) for record in lifecycle_records.values() if record.get("node")}),
    }
    (trace_dir / "trace.json").write_text(json.dumps(metadata, indent=2))


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the complete multi-workload OpenClaw/OpenShell experiment")
    parser.add_argument("--corpus", type=Path, help="recorded trace corpus; not needed with --backends-only")
    parser.add_argument("--out", type=Path, help="experiment output directory; not needed with --backends-only")
    parser.add_argument("--agents", type=int, default=20)
    parser.add_argument(
        "--warm-pool-size",
        type=int,
        default=10,
        help="number of agent-scoped OpenShell sandboxes admitted to the prewarm pool; excess agents use cold-start fallback",
    )
    parser.add_argument("--traces-per-workload", type=int, default=5)
    parser.add_argument("--max-traces", type=int, help="cap the total number of traces after workload partitioning")
    parser.add_argument("--trace-offset", type=int, default=0, help="skip this many sessions per workload before selecting traces")
    parser.add_argument("--require-traces", type=int, help="fail unless the final queue has exactly this many traces")
    parser.add_argument("--session-id", help="run exactly one recorded session")
    parser.add_argument("--session-ids", help="comma-separated recorded session IDs to run concurrently")
    parser.add_argument("--client-boundaries", action="store_true", help="drive only extracted client/user messages; OpenClaw owns tool loops")
    parser.add_argument("--agent-offset", type=int, default=0, help="offset queue positions into the deployed agent list")
    parser.add_argument("--agent-indices", help="comma-separated 1-based agent indices for the selected queue")
    parser.add_argument("--appworld", action="store_true", help="legacy shortcut for AppWorld-only runs")
    parser.add_argument("--appworld-endpoint", default=None)
    parser.add_argument(
        "--appworld-data",
        type=Path,
        default=Path(os.environ.get("TRACE_REPLAY_APPWORLD_DATA", str(ROOT.parent / "appworld-runtime" / "data"))),
        help="local AppWorld data directory to stage into the owned backend PVC",
    )
    parser.add_argument(
        "--appworld-task-map",
        type=Path,
        default=Path(
            os.environ.get(
                "TRACE_REPLAY_APPWORLD_TASK_MAP",
                str(ROOT.parent / "appworld-runtime" / "session-task-map.json"),
            )
        ),
        help="validated session-to-AppWorld-task mapping JSON",
    )
    parser.add_argument(
        "--appworld-data-pvc",
        default=os.environ.get("TRACE_REPLAY_APPWORLD_DATA_PVC", "trace-replay-appworld-data"),
        help="owned RWO PVC used by the AppWorld backend",
    )
    parser.add_argument("--workload-plan", help="exact allocation, e.g. personal_assistant=5,software_engineering_qa=5,research_deep_search=5,customer_support_ops=5")
    parser.add_argument("--backend-manifest", type=Path, help="JSON manifest describing real workload backends")
    parser.add_argument(
        "--swe-task-manifest",
        type=Path,
        default=ROOT / "config/swebench-trace-manifest.json",
        help="pinned SWE task metadata mapping traces to repository commits and task images",
    )
    parser.add_argument("--namespace", default="trace-replay")
    parser.add_argument("--system-namespace", default="trace-replay")
    parser.add_argument("--openshell-namespace", default="openshell-tracesim")
    parser.add_argument(
        "--kueue-namespace",
        default=os.environ.get("TRACE_REPLAY_KUEUE_NAMESPACE", DEFAULT_KUEUE_NAMESPACE),
        help="namespace containing the Kueue admission webhook",
    )
    parser.add_argument(
        "--kueue-webhook-service",
        default=os.environ.get("TRACE_REPLAY_KUEUE_WEBHOOK_SERVICE", DEFAULT_KUEUE_WEBHOOK_SERVICE),
        help="Kueue webhook Service used for Job admission",
    )
    parser.add_argument(
        "--openshell-gateway-endpoint",
        default=os.environ.get(
            "TRACE_REPLAY_OPENSHELL_GATEWAY_ENDPOINT",
            "http://openshell.openshell-tracesim.svc.cluster.local:8080",
        ),
        help="OpenShell gateway endpoint used by the agent and workspace provisioning",
    )
    parser.add_argument(
        "--openshell-client-secret",
        default=os.environ.get("TRACE_REPLAY_OPENSHELL_CLIENT_SECRET", "openshell-client-tls"),
        help="client TLS Secret name in the OpenShell gateway namespace",
    )
    parser.add_argument(
        "--node",
        default=os.environ.get("TRACE_REPLAY_NODE"),
        help="backwards-compatible single target node",
    )
    parser.add_argument(
        "--nodes",
        help="comma-separated target nodes; agents are assigned round-robin in this order",
    )
    parser.add_argument("--allow-cross-node", action="store_true", help="allow experiment-owned agents, Jobs, and sandboxes to schedule on any available node")
    parser.add_argument(
        "--workspace-pvc",
        default=os.environ.get("TRACE_REPLAY_WORKSPACE_PVC"),
        help="owned RWX workspace PVC; defaults to a unique run-scoped name",
    )
    parser.add_argument(
        "--workspace-pvcs",
        help="explicit node-to-owned-RWO-PVC map, e.g. node-a=pvc-a,node-b=pvc-b",
    )
    parser.add_argument(
        "--workspace-storage-class",
        default=os.environ.get("TRACE_REPLAY_WORKSPACE_STORAGE_CLASS", ""),
        help="StorageClass for workspace PVCs; empty uses the cluster default",
    )
    parser.add_argument("--workspace-access-mode", choices=("ReadWriteMany", "ReadWriteOnce"), default=os.environ.get("TRACE_REPLAY_WORKSPACE_ACCESS_MODE", "ReadWriteMany"))
    parser.add_argument("--kubeconfig", default=os.environ.get("KUBECONFIG"))
    parser.add_argument("--jaeger", default="http://127.0.0.1:16686")
    parser.add_argument("--reuse-agents", action="store_true")
    parser.add_argument("--reuse-existing-agent-prefix", help="reuse only this owned run's OpenClaw deployment prefix")
    parser.add_argument("--reuse-existing-workspace-prefix", help="reuse only this owned run's workspace prefix")
    parser.add_argument(
        "--resume-existing-run",
        action="store_true",
        help="resume an owned partial fleet without recreating running agents",
    )
    parser.add_argument(
        "--skip-resume-agent-reconcile",
        action="store_true",
        help="on a resume, trust the already reconciled owned fleet and skip the full 100-slot rewrite",
    )
    parser.add_argument(
        "--resume-preferred-node",
        help="healthy approved node for missing or pending agents during --resume-existing-run",
    )
    parser.add_argument(
        "--deployment-only",
        action="store_true",
        help="stop after agent deployment and workspace wiring; do not create trace Jobs",
    )
    parser.add_argument("--retries", type=int, default=0)
    parser.add_argument("--simulated-ttft", type=float, default=None, help="simulated time-to-first-token in ms (overrides calibrated timing)")
    parser.add_argument("--simulated-itl", type=float, default=None, help="simulated inter-token latency in ms (overrides calibrated timing)")
    parser.add_argument(
        "--execution-workers",
        type=int,
        default=None,
        help=(
            "maximum number of trace controllers issuing Kubernetes API requests concurrently; "
            "the deployed agent count is unchanged (default: all agents)"
        ),
    )
    parser.add_argument(
        "--job-dispatch-interval",
        type=float,
        default=2.0,
        help="minimum seconds between trace Job admission requests (0 disables throttling)",
    )
    parser.add_argument(
        "--cluster-api-workers",
        type=int,
        default=6,
        help="maximum concurrent oc subprocesses used by the controller (default: 6)",
    )
    parser.add_argument(
        "--cluster-retry-timeout",
        type=float,
        default=1800.0,
        help="seconds to keep retrying transient read-only cluster preparation failures",
    )
    parser.add_argument(
        "--cluster-retry-interval",
        type=float,
        default=10.0,
        help="seconds between transient cluster preparation retries",
    )
    parser.add_argument("--backends-only", action="store_true", help="deploy and health-check selected workload backends, then stop before agents/sandboxes")
    parser.add_argument("--plan", action="store_true")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if args.backends_only and args.plan:
        parser.error("--backends-only requires --execute and cannot be combined with --plan")
    if not args.backends_only and args.plan == args.execute:
        parser.error("choose exactly one of --plan or --execute")
    if args.backends_only and not args.execute:
        parser.error("--backends-only requires --execute")
    if not args.backends_only and args.corpus is None:
        parser.error("--corpus is required unless --backends-only is used")
    if not args.backends_only and args.out is None:
        parser.error("--out is required unless --backends-only is used")
    try:
        validate_agent_count(args.agents)
        target_nodes = parse_target_nodes(args.node, args.nodes)
        workspace_pvcs = parse_workspace_pvcs(args.workspace_pvcs, target_nodes)
    except ValueError as exc:
        parser.error(str(exc))
    if len(target_nodes) > 1 and args.allow_cross_node:
        parser.error(
            "--allow-cross-node is incompatible with controlled multi-node placement; "
            "omit it when using --nodes"
        )
    if workspace_pvcs and args.workspace_access_mode != "ReadWriteOnce":
        parser.error("--workspace-pvcs is only valid with --workspace-access-mode ReadWriteOnce")
    if workspace_pvcs and args.workspace_pvc:
        parser.error("use either --workspace-pvc or --workspace-pvcs, not both")
    if args.warm_pool_size < 0 or args.warm_pool_size > args.agents:
        parser.error("--warm-pool-size must be between 0 and --agents")
    if args.execution_workers is not None and not 1 <= args.execution_workers <= args.agents:
        parser.error("--execution-workers must be between 1 and --agents")
    if args.job_dispatch_interval < 0:
        parser.error("--job-dispatch-interval cannot be negative")
    if not 1 <= args.cluster_api_workers <= 128:
        parser.error("--cluster-api-workers must be between 1 and 128")
    if args.cluster_retry_timeout < 0:
        parser.error("--cluster-retry-timeout cannot be negative")
    if args.cluster_retry_interval <= 0:
        parser.error("--cluster-retry-interval must be positive")
    if args.max_traces is not None and args.max_traces < 1:
        parser.error("--max-traces must be positive")
    if args.session_id and args.session_ids:
        parser.error("use only one of --session-id or --session-ids")
    if args.require_traces is not None and args.require_traces < 1:
        parser.error("--require-traces must be positive")
    if bool(args.reuse_existing_agent_prefix) != bool(args.reuse_existing_workspace_prefix):
        parser.error("--reuse-existing-agent-prefix and --reuse-existing-workspace-prefix must be provided together")
    if args.skip_resume_agent_reconcile and not args.resume_existing_run:
        parser.error("--skip-resume-agent-reconcile requires --resume-existing-run")
    if args.resume_existing_run:
        if not args.reuse_existing_agent_prefix or not args.reuse_existing_workspace_prefix:
            parser.error("--resume-existing-run requires the owned agent and workspace prefixes")
        if not args.resume_preferred_node:
            parser.error("--resume-existing-run requires --resume-preferred-node")
        if args.resume_preferred_node not in target_nodes:
            parser.error("--resume-preferred-node must be included in --nodes")
        if args.workspace_access_mode != "ReadWriteOnce":
            parser.error("--resume-existing-run currently requires the existing per-node ReadWriteOnce workspaces")
        # A resumed fleet may safely run a filtered queue when explicit
        # session IDs and matching agent indices are supplied.  This keeps the
        # existing deployments/workspaces warm while retrying only unfinished
        # traces.  The complete queue remains the default when no filter is
        # provided; max-traces is still rejected because it is positional and
        # cannot identify the intended warm agent slots.
        if args.max_traces is not None:
            parser.error("--resume-existing-run cannot be combined with --max-traces")
        if args.reuse_agents:
            parser.error("--resume-existing-run already enables owned partial reuse")
    if args.deployment_only and not args.execute:
        parser.error("--deployment-only requires --execute")
    # Older explicitly-owned OpenShell fleets may use the gateway-managed
    # workspace (no controller PVC).  Reusing such a fleet must not force a
    # new PVC or attach an unrelated volume; fresh runs still require the
    # normal RWX/RWO storage checks below.
    reuse_gateway_managed_workspaces = (
        bool(args.reuse_existing_agent_prefix)
        and bool(args.reuse_existing_workspace_prefix)
        and not args.workspace_pvc
        and not workspace_pvcs
    )
    if (
        args.reuse_existing_agent_prefix
        and not reuse_gateway_managed_workspaces
        and not args.workspace_pvc
        and not workspace_pvcs
        and args.workspace_access_mode == "ReadWriteMany"
    ):
        parser.error("--workspace-pvc is required when reusing an existing run")
    if args.trace_offset < 0:
        parser.error("--trace-offset cannot be negative")
    if args.agent_offset < 0 or args.agent_offset >= args.agents:
        parser.error("--agent-offset must be between 0 and agents-1")
    selected_session_ids = {value.strip() for value in (args.session_ids or "").split(",") if value.strip()}
    try:
        selected_agent_indices = [int(value.strip()) for value in (args.agent_indices or "").split(",") if value.strip()]
    except ValueError as exc:
        parser.error(f"invalid --agent-indices: {args.agent_indices}")
    if selected_agent_indices and any(index < 1 or index > args.agents for index in selected_agent_indices):
        parser.error("--agent-indices values must be between 1 and agents")
    if selected_agent_indices and len(set(selected_agent_indices)) != len(selected_agent_indices):
        parser.error("--agent-indices must not contain duplicates")
    controller_lock = None
    if args.execute and not args.backends_only:
        lock_identity = args.reuse_existing_agent_prefix or str(args.out.resolve())
        lock_hash = hashlib.sha256(lock_identity.encode("utf-8")).hexdigest()[:16]
        lock_path = Path("/tmp") / f"trace-replay-controller-{lock_hash}.lock"
        controller_lock = lock_path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(controller_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            controller_lock.seek(0)
            owner = controller_lock.read().strip() or "unknown"
            parser.error(
                f"another controller already owns fleet {lock_identity!r} (lock owner {owner})"
            )
        controller_lock.seek(0)
        controller_lock.truncate()
        controller_lock.write(f"pid={os.getpid()} out={args.out}\n")
        controller_lock.flush()
    cluster = Cluster(args.kubeconfig, args.namespace, target_nodes[0], target_nodes, args.cluster_api_workers)
    try:
        allocation = (
            {"personal_assistant": args.agents}
            if args.appworld and not args.workload_plan
            else parse_workload_plan(args.workload_plan, args.agents, args.traces_per_workload)
        )
        backends = load_backends(args.backend_manifest)
        validate_backends(backends, list(allocation))
        swe_manifest = (
            load_swe_manifest(args.swe_task_manifest)
            if allocation.get("software_engineering_qa", 0) > 0
            else {}
        )
    except (BackendConfigurationError, SweManifestError, OSError, json.JSONDecodeError) as exc:
        parser.error(str(exc))
    if args.backends_only:
        preflight(
            cluster,
            args.agents,
            args.system_namespace,
            args.openshell_namespace,
            allow_cross_node=args.allow_cross_node,
            kueue_namespace=args.kueue_namespace,
            kueue_webhook_service=args.kueue_webhook_service,
        )
        appworld_selected = (
            "personal_assistant" in allocation
            and backends["personal_assistant"].benchmark == "appworld"
        )
        if appworld_selected:
            ensure_appworld_data(
                cluster,
                args.system_namespace,
                target_nodes[0],
                args.appworld_data_pvc,
                args.workspace_storage_class,
                args.appworld_data,
            )
        ensure_workload_backends(
            cluster,
            backends,
            allocation,
            "backends-only",
            appworld_task_map=args.appworld_task_map if appworld_selected else None,
        )
        log_phase("backend gate: all selected workload backends are ready; no agents or sandboxes were created")
        return 0
    corpus = args.corpus
    args.out.mkdir(parents=True, exist_ok=True)
    if args.appworld:
        if set(allocation) != {"personal_assistant"}:
            parser.error("--appworld is only valid for an AppWorld-only allocation")
        endpoint = args.appworld_endpoint or f"{backends['personal_assistant'].base_url}/execute"
        corpus = prepare_appworld_corpus(
            args.corpus,
            args.out / "appworld-corpus.json",
            endpoint,
            args.appworld_task_map,
        )
    initial_queue = build_queue(
        corpus,
        args.out,
        allocation,
        args.session_id,
        selected_session_ids or None,
        args.max_traces,
        args.trace_offset,
    )
    if not initial_queue:
        parser.error(f"session not found in corpus: {args.session_id}")
    selected_ids = {str(item["session"].get("session_id") or "") for item in initial_queue}
    if not args.appworld:
        corpus = prepare_workload_corpus(
            corpus,
            args.out / "workload-backend-corpus.json",
            backends,
            swe_manifest,
            selected_session_ids=selected_ids,
        )
        queue = build_queue(
            corpus,
            args.out,
            allocation,
            session_ids=selected_ids,
        )
    else:
        queue = initial_queue
    expected_queue = sum(allocation.values()) if args.session_id is None and not selected_session_ids and args.max_traces is None else None
    if expected_queue is not None and len(queue) != expected_queue:
        parser.error(f"workload plan requires exactly {expected_queue} traces, found {len(queue)}")
    if args.require_traces is not None and len(queue) != args.require_traces:
        parser.error(f"expected exactly {args.require_traces} traces, found {len(queue)}")
    if selected_agent_indices and len(selected_agent_indices) != len(queue):
        parser.error("--agent-indices must contain exactly one agent index per selected trace")
    # Keep the OpenShell sandbox image generic and supported.  SWE-bench task
    # images are used by the staging Jobs below to copy the real repository
    # snapshot into the agent workspace; they are not used as OpenShell images
    # because many task images do not contain OpenShell's trusted `ip` helper.
    swe_task_images_by_agent: list[str | None] = [None] * args.agents
    swe_tasks_by_agent: dict[int, dict[str, Any]] = {}
    swe_items_by_agent: dict[int, dict[str, Any]] = {}
    swe_assignments: dict[int, str] = {}
    for position, item in enumerate(queue):
        binding_index = (selected_agent_indices[position] - 1) if selected_agent_indices else ((position + args.agent_offset) % args.agents)
        task = item["session"].get("swe_task") or {}
        if item["workload"] != "software_engineering_qa":
            continue
        image = str(task.get("image") or "")
        session_id = str(item["session"].get("session_id") or "")
        if not image:
            parser.error(f"SWE trace {session_id} has no pinned task image")
        previous = swe_assignments.get(binding_index)
        if previous and previous != session_id:
            parser.error(
                f"agent {binding_index + 1} would run multiple SWE repositories; "
                "use one agent per SWE trace so its OpenShell task image remains pinned"
            )
        swe_assignments[binding_index] = session_id
        swe_task_images_by_agent[binding_index] = image
        swe_tasks_by_agent[binding_index] = dict(task)
        swe_items_by_agent[binding_index] = item
    args.out.mkdir(parents=True, exist_ok=True)
    runtime_corpus = args.out / "runtime-replay-corpus.json"
    runtime_corpus.parent.mkdir(parents=True, exist_ok=True)
    runtime_payload = {
        "dataset": "Exgentic/agent-llm-traces",
        "tool_backend": "workload-backends",
        "backend_plan": plan_payload(backends, allocation),
        "sessions": [item["session"] for item in queue],
    }
    if swe_manifest:
        runtime_payload["swe_task_manifest"] = str(args.swe_task_manifest)
        runtime_payload["swe_task_manifest_sha256"] = manifest_sha256(args.swe_task_manifest)
        runtime_payload["swe_tasks"] = {
            session_id: session.get("swe_task")
            for session_id, session in (
                (str(item["session"].get("session_id") or ""), item["session"])
                for item in queue
            )
            if session.get("swe_task")
        }
    runtime_corpus.write_text(json.dumps(runtime_payload, indent=2), encoding="utf-8")
    (args.out / "backend_plan.json").write_text(json.dumps(plan_payload(backends, allocation), indent=2) + "\n", encoding="utf-8")
    log_phase(f"queue: {len(queue)} traces ({args.traces_per_workload} per workload), {args.agents} agents")

    def write_deployment_plan(agent_nodes: list[str]) -> dict[str, list[int]]:
        node_groups: dict[str, list[int]] = {node: [] for node in target_nodes}
        warm_slots_by_node: dict[str, list[int]] = {node: [] for node in target_nodes}
        for index, node in enumerate(agent_nodes, start=1):
            node_groups[node].append(index)
            if index <= args.warm_pool_size:
                warm_slots_by_node[node].append(index)
        (args.out / "deployment_plan.json").write_text(
            json.dumps(
                {
                    "schema": "agentic.trace-replay.deployment-plan.v2",
                    "agents": args.agents,
                    "traces": len(queue),
                    "workload_allocation": allocation,
                    "target_nodes": list(target_nodes),
                    "agent_node_assignments": {
                        str(index): node for index, node in enumerate(agent_nodes, start=1)
                    },
                    "agent_indices_by_node": node_groups,
                    "warm_pool_size": args.warm_pool_size,
                    "warm_pool_slots_by_node": warm_slots_by_node,
                    "workspace_access_mode": args.workspace_access_mode,
                    "workspace_storage_class": args.workspace_storage_class,
                    "controlled_node_placement": not args.allow_cross_node,
                    "swe_task_images_by_agent": {
                        str(index + 1): image
                        for index, image in enumerate(swe_task_images_by_agent)
                        if image
                    },
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        return node_groups

    if args.plan:
        # Plan mode is cluster-free and keeps a deterministic round-robin layout.
        agent_nodes = list(balanced_node_assignments(args.agents, target_nodes))
        node_groups = write_deployment_plan(agent_nodes)
        for workload in WORKLOADS:
            LOG.info("  %s: %d traces / %d agents / backend=%s", workload, sum(item["workload"] == workload for item in queue), allocation.get(workload, 0), backends[workload].name)
        for node in target_nodes:
            LOG.info("  node %s: %d agent slots", node, len(node_groups[node]))
        return 0
    resume_agent_nodes: list[str] | None = None
    resume_pending_indices: list[int] = []
    resume_missing_indices: list[int] = []
    if args.resume_existing_run:
        planned_resume_nodes: list[str] | None = None
        deployment_plan_path = args.out / "deployment_plan.json"
        if deployment_plan_path.exists():
            deployment_plan = json.loads(deployment_plan_path.read_text(encoding="utf-8"))
            assignments = deployment_plan.get("agent_node_assignments") or {}
            candidate = [str(assignments.get(str(index)) or "") for index in range(1, args.agents + 1)]
            if len(candidate) == args.agents and all(node in target_nodes for node in candidate):
                planned_resume_nodes = candidate
        resume_agent_nodes, resume_pending_indices, resume_missing_indices = _retry_cluster_read_stage(
            "resume inventory",
            lambda: _resume_agent_layout(
                cluster,
                args.reuse_existing_agent_prefix,
                args.agents,
                args.resume_preferred_node,
                planned_resume_nodes,
            ),
            args.cluster_retry_timeout,
            args.cluster_retry_interval,
        )
        # Pending existing Deployments keep their recorded node selector; only
        # truly missing slots would require placement on the preferred node.
        moved_or_missing = set(resume_missing_indices) if planned_resume_nodes is None else set()
        unsupported_swe = sorted(index for index in moved_or_missing if (index - 1) in swe_tasks_by_agent)
        if unsupported_swe:
            raise RuntimeError(
                "refusing to move or recreate SWE agents during partial resume; "
                f"repository staging is tied to the original node: {unsupported_swe}"
            )
        log_phase(
            f"resume: preserving {args.agents - len(resume_pending_indices) - len(resume_missing_indices)} "
            f"owned agents; reconciling {len(resume_pending_indices)} pending slots and creating "
            f"{len(resume_missing_indices)} missing slots from the recorded placement plan"
        )
    # Validate node health and immutable baseline services before any run-owned
    # backend or agent resources are created. Capacity is checked again after
    # the real workload backends are healthy, so existing backend pods are not
    # double-counted and the mock service gets an explicit one-pod allowance.
    if args.resume_existing_run:
        resume_count = len(resume_pending_indices) + len(resume_missing_indices)
        if resume_count:
            resume_pending_nodes = [
                resume_agent_nodes[index - 1]
                for index in resume_pending_indices
            ] if resume_agent_nodes else []
            # Missing slots retain the recorded balanced placement whenever a
            # deployment plan is available.  Charging every missing slot to
            # the preferred node makes a valid multi-node resume fail its
            # conservative pod-budget check before any mutation.
            resume_missing_nodes = [
                resume_agent_nodes[index - 1]
                for index in resume_missing_indices
            ] if resume_agent_nodes else [args.resume_preferred_node] * len(resume_missing_indices)
            capacity_nodes = resume_pending_nodes + resume_missing_nodes
            additional_by_node = {
                node: capacity_nodes.count(node)
                for node in set(capacity_nodes)
            }
            _retry_cluster_read_stage(
                "resume capacity preflight",
                lambda: _preflight_pod_capacity(
                    cluster,
                    capacity_nodes,
                    additional_pods_by_node=additional_by_node,
                ),
                args.cluster_retry_timeout,
                args.cluster_retry_interval,
            )
    else:
        backend_capacity_allowance = {target_nodes[0]: 5}  # four backends + mock LLM
        backend_capacity_probe = _retry_cluster_read_stage(
            "capacity placement preflight",
            lambda: _capacity_aware_agent_assignments(
                cluster,
                args.agents,
                target_nodes,
                additional_pods_by_node=backend_capacity_allowance,
            ),
            args.cluster_retry_timeout,
            args.cluster_retry_interval,
        )
        _retry_cluster_read_stage(
            "pod capacity preflight",
            lambda: _preflight_pod_capacity(
                cluster,
                backend_capacity_probe,
                additional_pods_by_node=backend_capacity_allowance,
            ),
            args.cluster_retry_timeout,
            args.cluster_retry_interval,
        )
    _retry_cluster_read_stage(
        "cluster preflight",
        lambda: preflight(
            cluster,
            args.agents,
            args.system_namespace,
            args.openshell_namespace,
            allow_cross_node=args.allow_cross_node,
            kueue_namespace=args.kueue_namespace,
            kueue_webhook_service=args.kueue_webhook_service,
        ),
        args.cluster_retry_timeout,
        args.cluster_retry_interval,
    )
    # Temporary staging/trace-driver Jobs do not mount the agent's RWO
    # workspace.  Keep them on one healthy approved node so a disk-pressure
    # taint on the agent's node cannot strand the whole run, while the agents
    # and their sandboxes remain on their original nodes.
    auxiliary_job_node = _choose_safe_auxiliary_node(cluster, target_nodes)
    log_phase(f"temporary experiment Jobs will use healthy approved node {auxiliary_job_node}")
    run_hash = hashlib.sha256(str(args.out.resolve()).encode("utf-8")).hexdigest()[:8]
    run_stem = f"{safe_name(args.out.name)[:24].rstrip('-')}-{run_hash}" or f"run-{run_hash}"
    backend_run_id = run_stem[:30]
    appworld_selected = (
        "personal_assistant" in allocation
        and backends["personal_assistant"].benchmark == "appworld"
    )
    # Mixed-workload runs cannot use the AppWorld-only --appworld mode, but
    # their personal-assistant sandbox commands still call the AppWorld
    # backend.  Keep the endpoint in the sandbox network policy whenever the
    # selected allocation contains AppWorld traces.
    appworld_tool_endpoint = (
        args.appworld_endpoint or f"{backends['personal_assistant'].base_url}/execute"
        if appworld_selected
        else None
    )
    if appworld_selected and not args.resume_existing_run:
        ensure_appworld_data(
            cluster,
            args.system_namespace,
            target_nodes[0],
            args.appworld_data_pvc,
            args.workspace_storage_class,
            args.appworld_data,
        )
    if args.resume_existing_run:
        for workload in allocation:
            spec = backends[workload]
            if not cluster.run(["-n", spec.namespace, "get", "svc", spec.service], check=False).strip():
                raise RuntimeError(f"cannot resume: backend service is missing for {workload}: {spec.namespace}/{spec.service}")
            if cluster.run(["-n", spec.namespace, "get", "deployment", spec.name], check=False).strip():
                cluster.wait_rollout_in(spec.namespace, spec.name)
        agent_nodes = list(resume_agent_nodes or [])
    else:
        ensure_workload_backends(
            cluster,
            backends,
            allocation,
            backend_run_id,
            appworld_task_map=args.appworld_task_map if appworld_selected else None,
        )
        additional_pods_by_node = {target_nodes[0]: 1}  # run-scoped mock LLM
        agent_nodes = _capacity_aware_agent_assignments(
            cluster,
            args.agents,
            target_nodes,
            additional_pods_by_node=additional_pods_by_node,
        )
        _preflight_pod_capacity(
            cluster,
            agent_nodes,
            additional_pods_by_node=additional_pods_by_node,
        )
    # OpenShell sandboxes have an isolated network namespace and do not
    # resolve Kubernetes Service DNS names.  Resolve only the selected,
    # experiment-owned Services to ClusterIPs and use those addresses for the
    # queued tool commands and the sandbox network policy.  This avoids any
    # cluster-wide DNS or policy changes.
    sandbox_backend_endpoints = _resolve_backend_service_endpoints(cluster, backends, allocation)
    _rewrite_queue_backend_endpoints(queue, backends, sandbox_backend_endpoints)
    runtime_payload["sessions"] = [item["session"] for item in queue]
    runtime_corpus.write_text(json.dumps(runtime_payload, indent=2), encoding="utf-8")
    sandbox_internal_api_endpoints = [sandbox_backend_endpoints[workload] for workload in allocation]
    if appworld_selected:
        appworld_tool_endpoint = sandbox_backend_endpoints["personal_assistant"] + "/execute"
    node_groups = write_deployment_plan(agent_nodes)
    run_token = run_stem
    mock_service = ensure_mock_llm_config(cluster, runtime_corpus, args.system_namespace, run_token)
    ensure_driver_config(cluster)
    agent_client_secret = _ensure_openshell_client_secret(
        cluster,
        args.openshell_namespace,
        args.openshell_client_secret,
        f"trace-replay-openshell-client-tls-{run_hash}",
    )
    # OpenClaw validates workspace names to a maximum of 19 characters.
    # Use a run hash so a fresh run cannot reuse another experiment's
    # workspace; `trace-replay-<8 hex>-20` is exactly 19 characters.
    workspace_prefix = args.reuse_existing_workspace_prefix or f"trace-replay-{run_hash}"
    agent_prefix = args.reuse_existing_agent_prefix or f"trace-replay-{safe_name(args.out.name)[:24]}-{run_hash}-openclaw"
    log_phase(f"deployment: workspace prefix={workspace_prefix} agent prefix={agent_prefix}")
    workspace_names = [workspace_name(workspace_prefix, index) for index in range(1, args.agents + 1)]
    # RWX uses one shared workspace across nodes.  RWO uses one PVC per node,
    # which keeps every sandbox and its block volume on the same node and does
    # not depend on CephFS.  A single user-supplied RWO PVC is intentionally
    # rejected for multi-node runs because it cannot be mounted safely on both
    # nodes.
    shared_workspace_pvcs: dict[str, str] = {}
    if reuse_gateway_managed_workspaces:
        log_phase("storage: reusing gateway-managed OpenShell workspaces (no controller PVC)")
    elif args.workspace_access_mode == "ReadWriteMany":
        shared_workspace_pvc = args.workspace_pvc or f"trace-replay-openshell-workspaces-{run_hash}"
        ensure_shared_workspace(
            cluster,
            args.openshell_namespace,
            shared_workspace_pvc,
            args.workspace_storage_class,
            workspace_names,
            args.workspace_access_mode,
            target_node=target_nodes[0],
        )
        shared_workspace_pvcs = {node: shared_workspace_pvc for node in target_nodes}
    else:
        if len(target_nodes) > 1 and args.workspace_pvc:
            parser.error("--workspace-pvc cannot be a single RWO PVC for a multi-node run; omit it for per-node PVCs")
        if args.resume_existing_run or args.reuse_existing_agent_prefix:
            # A resumed or explicitly reused fleet already owns and mounts
            # these RWO PVCs.  Validate the explicit node-to-PVC map and reuse
            # it as-is; launching a redundant prep Job can block behind the
            # existing attachment (or be evicted under node disk pressure).
            if set(workspace_pvcs) != set(target_nodes):
                raise RuntimeError(
                    "reusing ReadWriteOnce workspaces requires an explicit owned PVC for every target node"
                )
            for node in target_nodes:
                pvc_name = workspace_pvcs[node]
                pvc = cluster.json(["-n", args.openshell_namespace, "get", "pvc", pvc_name])
                labels = pvc.get("metadata", {}).get("labels") or {}
                if labels.get("managed-by") != "trace-replay-experiment":
                    raise RuntimeError(f"refusing to reuse unowned workspace PVC {args.openshell_namespace}/{pvc_name}")
                spec = pvc.get("spec", {})
                if "ReadWriteOnce" not in (spec.get("accessModes") or []) or (
                    args.workspace_storage_class and spec.get("storageClassName") != args.workspace_storage_class
                ):
                    raise RuntimeError(f"existing PVC {args.openshell_namespace}/{pvc_name} has incompatible workspace settings")
                if pvc.get("status", {}).get("phase") != "Bound":
                    raise RuntimeError(f"existing workspace PVC {args.openshell_namespace}/{pvc_name} is not Bound")
                shared_workspace_pvcs[node] = pvc_name
            log_phase(f"storage: reusing existing node-local workspaces ({len(shared_workspace_pvcs)} PVCs)")
        else:
            for node in target_nodes:
                node_hash = hashlib.sha256(node.encode("utf-8")).hexdigest()[:6]
                pvc_name = workspace_pvcs.get(node) or args.workspace_pvc or f"trace-replay-openshell-workspaces-{run_hash}-{node_hash}"
                node_workspaces = [
                    workspace_names[index - 1]
                    for index in node_groups[node]
                ]
                ensure_shared_workspace(
                    cluster,
                    args.openshell_namespace,
                    pvc_name,
                    args.workspace_storage_class,
                    node_workspaces,
                    args.workspace_access_mode,
                    target_node=node,
                )
                shared_workspace_pvcs[node] = pvc_name
    if not args.resume_existing_run:
        agent_bindings = deploy_agents(
            cluster,
            args.agents,
            reuse=args.reuse_agents or bool(args.reuse_existing_agent_prefix),
            workspace_prefix=workspace_prefix,
            deployment_prefix=agent_prefix,
            shared_workspace_pvcs=shared_workspace_pvcs,
            appworld_endpoint=appworld_tool_endpoint,
            internal_api_endpoints=sandbox_internal_api_endpoints,
            mock_service=mock_service,
            gateway_endpoint=args.openshell_gateway_endpoint,
            openshell_client_secret=agent_client_secret,
            allow_cross_node=args.allow_cross_node,
            agent_nodes=agent_nodes,
            reconcile_indices=selected_agent_indices or None,
            reuse_existing_workspaces=bool(args.reuse_existing_workspace_prefix),
            simulated_ttft_ms=args.simulated_ttft,
            simulated_itl_ms=args.simulated_itl,
        )
    else:
        preferred_node = args.resume_preferred_node
        if resume_pending_indices:
            deploy_agents(
                cluster,
                len(resume_pending_indices),
                reuse=True,
                workspace_prefix=workspace_prefix,
                deployment_prefix=agent_prefix,
                shared_workspace_pvcs=shared_workspace_pvcs,
                appworld_endpoint=appworld_tool_endpoint,
                internal_api_endpoints=sandbox_internal_api_endpoints,
                mock_service=mock_service,
                gateway_endpoint=args.openshell_gateway_endpoint,
                openshell_client_secret=agent_client_secret,
                allow_cross_node=args.allow_cross_node,
                agent_nodes=[resume_agent_nodes[index - 1] for index in resume_pending_indices],
                agent_indices=resume_pending_indices,
                simulated_ttft_ms=args.simulated_ttft,
                simulated_itl_ms=args.simulated_itl,
            )
        if resume_missing_indices:
            deploy_agents(
                cluster,
                len(resume_missing_indices),
                reuse=False,
                workspace_prefix=workspace_prefix,
                deployment_prefix=agent_prefix,
                shared_workspace_pvcs=shared_workspace_pvcs,
                appworld_endpoint=appworld_tool_endpoint,
                internal_api_endpoints=sandbox_internal_api_endpoints,
                mock_service=mock_service,
                gateway_endpoint=args.openshell_gateway_endpoint,
                openshell_client_secret=agent_client_secret,
                allow_cross_node=args.allow_cross_node,
                agent_nodes=[resume_agent_nodes[index - 1] for index in resume_missing_indices],
                agent_indices=resume_missing_indices,
                simulated_ttft_ms=args.simulated_ttft,
                simulated_itl_ms=args.simulated_itl,
            )
        agent_bindings = _retry_cluster_read_stage(
            "agent binding inventory",
            lambda: _collect_owned_agent_bindings(
                cluster,
                agent_prefix,
                workspace_prefix,
                args.agents,
            ),
            args.cluster_retry_timeout,
            args.cluster_retry_interval,
        )
        # A resumed run normally reconciles every existing agent to the fresh
        # run-scoped mock service.  Once that reconciliation has completed,
        # retries of the same run can trust the owned inventory and avoid
        # another 100-slot rewrite that adds no experiment value.
        if args.skip_resume_agent_reconcile:
            # Keep warm reuse read-only. The live endpoint verifier below is
            # authoritative and avoids serial gateway rollouts.
            log_phase("deployment: reusing warm agents without rollout; verifying live routing")
        else:
            log_phase("deployment: reconciling existing agents to the run-scoped mock LLM")
            reconcile_agent_indices = selected_agent_indices or list(range(1, args.agents + 1))
            # A targeted recovery queue must not perform 100 per-Deployment
            # reads and Service applies merely to refresh a sparse subset.
            # The batch inventory above already established the complete,
            # ordered fleet; reconcile only the selected slots, then refresh
            # that inventory with two list calls.
            deploy_agents(
                cluster,
                len(reconcile_agent_indices),
                reuse=True,
                workspace_prefix=workspace_prefix,
                deployment_prefix=agent_prefix,
                shared_workspace_pvcs=shared_workspace_pvcs,
                appworld_endpoint=appworld_tool_endpoint,
                internal_api_endpoints=sandbox_internal_api_endpoints,
                mock_service=mock_service,
                gateway_endpoint=args.openshell_gateway_endpoint,
                openshell_client_secret=agent_client_secret,
                allow_cross_node=args.allow_cross_node,
                # The binding inventory can temporarily omit a non-running
                # existing Deployment.  Resume inventory is the authoritative
                # complete 1..N placement map and preserves that slot's node.
                agent_nodes=[agent_bindings[index - 1][2] for index in reconcile_agent_indices],
                agent_indices=reconcile_agent_indices,
                reconcile_indices=reconcile_agent_indices,
                reuse_existing_workspaces=True,
                simulated_ttft_ms=args.simulated_ttft,
                simulated_itl_ms=args.simulated_itl,
            )
            agent_bindings = _retry_cluster_read_stage(
                "post-reconcile agent binding inventory",
                lambda: _collect_owned_agent_bindings(
                    cluster,
                    agent_prefix,
                    workspace_prefix,
                    args.agents,
                ),
                args.cluster_retry_timeout,
                args.cluster_retry_interval,
            )
        write_deployment_plan([node for _, _, node in agent_bindings])
    verify_agent_mock_endpoints(
        cluster,
        agent_bindings,
        mock_service,
        selected_agent_indices or None,
    )
    # OpenShell workspace creation initializes the per-workspace subpaths and
    # can clear files written before the workspace exists.  Stage SWE
    # repositories only after all agent workspaces have been created and
    # reconciled.  The sandbox-specific staging gate must still verify the
    # repository after OpenShell creates the actual sandbox; the PVC-level
    # staging below is only the first half of that handshake.
    # A sandbox also initializes its workspace at first use.  Create the
    # agent-scoped SWE sandboxes through the real OpenClaw deployment before
    # staging, so OpenShell cannot clear a repository after it is copied.
    prewarm_swe_sandboxes(
        cluster,
        swe_items_by_agent,
        agent_bindings,
        args.out,
        args.system_namespace,
        run_hash,
        args.allow_cross_node,
        workspace_pvcs=shared_workspace_pvcs,
        placement_node=auxiliary_job_node,
        openshell_ns=args.openshell_namespace,
    )
    stage_swe_repositories(
        cluster,
        args.openshell_namespace,
        run_hash,
        swe_tasks_by_agent,
        workspace_names,
        agent_nodes,
        shared_workspace_pvcs,
        staging_node=auxiliary_job_node,
        checkpoint_dir=args.out,
    )
    # Repository staging already verified every commit directly on the exact
    # PVC subpath mounted by its OpenShell workspace.  The synthetic prewarm
    # sandboxes exit after initialization, so requiring those pods to remain
    # Running here would turn a successful PVC handoff into a false timeout.
    if swe_tasks_by_agent:
        log_phase("SWE backend: PVC repository handoff verified; skipping exited prewarm-pod liveness check")
    if args.resume_existing_run:
        # Existing agent pods are already counted as active.  Before creating
        # trace Jobs and OpenShell sandboxes, reserve the two additional pods
        # required per agent and validate each node independently.
        trace_capacity_by_node: dict[str, int] = {}
        for _, _, node in agent_bindings:
            trace_capacity_by_node[node] = trace_capacity_by_node.get(node, 0) + 2
        _preflight_pod_capacity(cluster, [], additional_pods_by_node=trace_capacity_by_node)
    warm_pool_size = min(args.warm_pool_size, len(agent_bindings))
    pool_items_by_agent: dict[int, dict[str, Any]] = {}
    for position, item in enumerate(queue):
        binding_index = (selected_agent_indices[position] - 1) if selected_agent_indices else ((position + args.agent_offset) % len(agent_bindings))
        if binding_index < warm_pool_size:
            pool_items_by_agent[binding_index] = item
    prewarm_pool_sandboxes(
        cluster, pool_items_by_agent, agent_bindings, args.out, args.system_namespace,
        run_hash, args.allow_cross_node, workspace_pvcs=shared_workspace_pvcs,
        placement_node=auxiliary_job_node, openshell_ns=args.openshell_namespace,
    )
    warm_slots_by_node: dict[str, list[int]] = {node: [] for node in target_nodes}
    for index, (_, _, node) in enumerate(agent_bindings, start=1):
        if index <= warm_pool_size:
            warm_slots_by_node[node].append(index)
    warm_pool = {
        "schema": "agentic.trace-replay.warm-pool.v1",
        "policy": "agent-scoped-ready-sandbox-slots",
        "requested_size": args.warm_pool_size,
        "active_size": warm_pool_size,
        "cold_start_fallback": True,
        "target_nodes": list(target_nodes),
        "slots_by_node": warm_slots_by_node,
        "slots": [
            {
                "slot": index,
                "agent": agent,
                "workspace": workspace,
                "node": node,
                "prewarm_requested": index <= warm_pool_size,
            }
            for index, (agent, workspace, node) in enumerate(agent_bindings, start=1)
        ],
    }
    (args.out / "warm_pool.json").write_text(json.dumps(warm_pool, indent=2) + "\n", encoding="utf-8")
    log_phase(
        f"sandbox warm pool: {warm_pool_size} prewarm slots across {len(target_nodes)} nodes; "
        f"{max(0, len(agent_bindings) - warm_pool_size)} agents use cold-start fallback"
    )
    if args.deployment_only:
        log_phase("deployment-only: agents and workspace wiring are ready; trace Jobs were not created")
        return 0
    local_port = _free_local_port()
    jaeger_url = f"http://127.0.0.1:{local_port}"
    pf = subprocess.Popen(
        ["oc", "-n", args.system_namespace, "port-forward", f"svc/{BASE_JAEGER}", f"{local_port}:16686"],
        env=cluster.env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    jaeger_lock = threading.Lock()
    jaeger_io_lock = threading.Lock()

    def ensure_jaeger_forward(*, force: bool = False) -> str:
        """Keep the controller-owned local Jaeger forward alive."""
        nonlocal jaeger_url, pf
        with jaeger_lock:
            if not force and pf.poll() is None:
                try:
                    with urllib.request.urlopen(f"{jaeger_url}/api/services", timeout=2):
                        return jaeger_url
                except Exception:
                    pass
            if pf.poll() is None:
                pf.send_signal(signal.SIGTERM)
                try:
                    pf.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    pf.kill()
                    pf.wait(timeout=3)
            local_port = _free_local_port()
            jaeger_url = f"http://127.0.0.1:{local_port}"
            pf = subprocess.Popen(
                ["oc", "-n", args.system_namespace, "port-forward", f"svc/{BASE_JAEGER}", f"{local_port}:16686"],
                env=cluster.env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=True,
            )
            _wait_for_jaeger(jaeger_url, pf)
            log_phase(f"telemetry: Jaeger port-forward restored at {jaeger_url}")
            return jaeger_url

    try:
        _wait_for_jaeger(jaeger_url, pf)
        log_phase(f"telemetry: Jaeger ready at {jaeger_url}")
        errors_path = args.out / "errors.jsonl"
        errors_path.unlink(missing_ok=True)
        errors_lock = threading.Lock()
        dispatch_lock = threading.Lock()
        next_dispatch_at = [0.0]

        def wait_for_job_dispatch_slot() -> None:
            """Rate-limit only Job admission; agent execution stays concurrent."""
            if args.job_dispatch_interval <= 0:
                return
            with dispatch_lock:
                now = time.monotonic()
                delay = max(0.0, next_dispatch_at[0] - now)
                next_dispatch_at[0] = max(now, next_dispatch_at[0]) + args.job_dispatch_interval
            if delay > 0:
                time.sleep(delay)

        def run_one(position: int, item: dict[str, Any]) -> dict[str, Any]:
            binding_index = (selected_agent_indices[position] - 1) if selected_agent_indices else ((position + args.agent_offset) % len(agent_bindings))
            agent, workspace, agent_node = agent_bindings[binding_index]
            warm_pool_slot = binding_index + 1 if binding_index < warm_pool_size else None
            # The pool is filled before measurement; the driver sends only the real client request.
            prewarm_requested = False
            session_id = str(item["session"].get("session_id") or "")
            for attempt in range(args.retries + 1):
                job = ""
                corpus_configmap = ""
                trace_dir: Path | None = None
                try:
                    log_phase(f"trace {position + 1}/{len(queue)}: {session_id} workload={item['workload']} agent={agent} attempt={attempt + 1}")
                    start = time.time()
                    # Kubernetes Jobs are immutable; include a per-process
                    # suffix so reruns never reuse a completed Job name.
                    run_id = f"{safe_name(args.out.name)[:6] or 'run'}-{int(start) % 1000000:06d}"
                    wait_for_job_dispatch_slot()
                    job, corpus_configmap, trace_dir, session_id = create_trace_job(
                        cluster,
                        item,
                        agent,
                        args.out / "traces",
                        args.system_namespace,
                        run_id,
                        attempt,
                        allow_cross_node=args.allow_cross_node,
                        prewarm_requested=prewarm_requested,
                        warm_pool_slot=warm_pool_slot,
                        warm_pool_size=warm_pool_size,
                        agent_node=agent_node,
                        placement_node=auxiliary_job_node,
                        user_key=(
                            f"trace-replay:{session_id}:swe-prewarm-{run_hash}"
                            if item["workload"] == "software_engineering_qa"
                            else None
                        ),
                        client_boundaries=args.client_boundaries,
                    )
                    try:
                        collect_trace(
                            cluster,
                            job,
                            trace_dir,
                            session_id,
                            start,
                            args.system_namespace,
                            jaeger_url,
                            agent,
                            workspace,
                            args.openshell_namespace,
                            backends[item["workload"]],
                            mock_service,
                            allow_cross_node=args.allow_cross_node,
                            prewarm_requested=prewarm_requested,
                            warm_pool_slot=warm_pool_slot,
                            warm_pool_size=warm_pool_size,
                            expected_node=agent_node,
                            jaeger_supplier=ensure_jaeger_forward,
                            jaeger_io_lock=jaeger_io_lock,
                        )
                    except Exception as exc:
                        message = str(exc).lower()
                        if not any(token in message for token in ("jaeger", "connection refused", "urlopen error")):
                            raise
                        LOG.warning("Jaeger collection failed; restoring the owned port-forward: %s", exc)
                        with jaeger_io_lock:
                            ensure_jaeger_forward(force=True)
                        collect_trace(
                            cluster,
                            job,
                            trace_dir,
                            session_id,
                            start,
                            args.system_namespace,
                            jaeger_url,
                            agent,
                            workspace,
                            args.openshell_namespace,
                            backends[item["workload"]],
                            mock_service,
                            allow_cross_node=args.allow_cross_node,
                            prewarm_requested=prewarm_requested,
                            warm_pool_slot=warm_pool_slot,
                            warm_pool_size=warm_pool_size,
                            expected_node=agent_node,
                            jaeger_supplier=ensure_jaeger_forward,
                            jaeger_io_lock=jaeger_io_lock,
                        )
                    log_phase(f"trace complete: {trace_dir}")
                    return {"position": position, "session_id": session_id, "agent": agent, "ok": True, "attempt": attempt + 1}
                except Exception as exc:
                    if trace_dir is not None and job:
                        try:
                            failure_log = cluster.run(
                                ["-n", cluster.namespace, "logs", f"job/{job}", "--all-containers=true"],
                                check=False,
                            )
                            if failure_log:
                                (trace_dir / "failed-job.log").write_text(failure_log, encoding="utf-8")
                        except Exception as log_exc:  # noqa: BLE001
                            LOG.warning("unable to preserve failed Job logs for %s: %s", job, log_exc)
                    if attempt >= args.retries:
                        error = {"position": position, "session_id": session_id, "agent": agent, "ok": False, "attempts": attempt + 1, "error": str(exc)}
                        with errors_lock:
                            with errors_path.open("a", encoding="utf-8") as handle:
                                handle.write(json.dumps(error) + "\n")
                        LOG.error("trace failed permanently: %s", error)
                        return error
                    LOG.warning("trace failed; retrying: session=%s error=%s", session_id, exc)
                finally:
                    for kind, name in (("job", job), ("configmap", corpus_configmap)):
                        if not name:
                            continue
                        try:
                            cluster.delete_owned(cluster.namespace, kind, name)
                        except Exception as cleanup_exc:  # noqa: BLE001
                            LOG.warning("owned cleanup failed for %s/%s: %s", kind, name, cleanup_exc)
            raise AssertionError("unreachable")

        execution_workers = args.execution_workers or min(16, len(agent_bindings))
        log_phase(
            f"trace controller concurrency: {execution_workers} workers for "
            f"{len(agent_bindings)} deployed agents"
        )
        with ThreadPoolExecutor(max_workers=execution_workers, thread_name_prefix="trace-replay-agent") as pool:
            futures = [pool.submit(run_one, position, item) for position, item in enumerate(queue)]
            results = [future.result() for future in as_completed(futures)]
        failed = [result for result in results if not result.get("ok")]
        (args.out / "run_summary.json").write_text(json.dumps({
            "total": len(results),
            "completed": len(results) - len(failed),
            "failed": len(failed),
            "agents": len(agent_bindings),
            "execution_workers": execution_workers,
            "job_dispatch_interval_seconds": args.job_dispatch_interval,
            "warm_pool_size": warm_pool_size,
            "cold_start_fallback": True,
            "target_nodes": list(target_nodes),
            "agent_workspaces": {
                agent: {"workspace": workspace, "node": node}
                for agent, workspace, node in agent_bindings
            },
        }, indent=2))
        if failed:
            raise RuntimeError(f"{len(failed)} traces failed; see {errors_path}")
    finally:
        pf.send_signal(signal.SIGTERM)
        pf.wait(timeout=10)
    (args.out / "experiment.json").write_text(json.dumps({
        "agents": args.agents,
        "traces": len(queue),
        "workloads": WORKLOADS,
        "node": target_nodes[0],
        "nodes": list(target_nodes),
        "agent_node_assignments": {
            str(index): node for index, node in enumerate(agent_nodes, start=1)
        },
        "allow_cross_node": args.allow_cross_node,
        "warm_pool_size": warm_pool_size,
        "warm_pool_policy": "agent-scoped warm reuse with measured cold-start fallback",
        "measurement_caveat": "includes cross-node scheduling and network latency" if args.allow_cross_node else None,
    }, indent=2))
    subprocess.run([sys.executable, str(ROOT / "scripts/analyze_multi_agent_results.py"), "--experiment", str(args.out)], check=True)
    log_phase(f"complete: results in {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
