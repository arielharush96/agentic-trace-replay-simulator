#!/usr/bin/env python3
"""Single entrypoint for the workload-partitioned OpenClaw benchmark.

The controller deliberately keeps deployment, execution, collection and
analysis in one trace-first lifecycle. It creates only ``aharush-*``
resources, never deletes resources, and requires ``--execute`` for cluster
mutation. ``--plan`` is safe and is useful for validating the corpus first.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.request
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

from trace_replay_sim.collect import collect as collect_prometheus  # noqa: E402
from trace_replay_sim.jaeger_export import compute_timing_segments, extract_per_turn, export as export_jaeger  # noqa: E402
from trace_replay_sim.per_turn_analysis import analyze as analyze_per_turn  # noqa: E402
from trace_replay_sim.profiler_v2 import profile as profile_v2  # noqa: E402
from trace_replay_sim.audit_corpus import classify_session  # noqa: E402
from trace_replay_sim.workloads import WORKLOADS, split_corpus  # noqa: E402
from generate_appworld_plots import plot_events as plot_appworld_events  # noqa: E402

LOG = logging.getLogger("run-experiment")


def safe_name(value: str) -> str:
    return "".join(char if char.isalnum() or char == "-" else "-" for char in value.lower()).strip("-")


class Cluster:
    def __init__(self, kubeconfig: str | None, namespace: str, node: str):
        self.env = os.environ.copy()
        if kubeconfig:
            self.env["KUBECONFIG"] = kubeconfig
        self.namespace = namespace
        self.node = node

    def run(self, args: list[str], *, input_text: str | None = None, check: bool = True) -> str:
        LOG.debug("oc %s", " ".join(args))
        result = subprocess.run(["oc", *args], input=input_text, text=True,
                                capture_output=True, env=self.env)
        if check and result.returncode:
            raise RuntimeError(f"oc {' '.join(args)} failed: {result.stderr.strip()}")
        return result.stdout

    def json(self, args: list[str]) -> dict[str, Any]:
        return json.loads(self.run([*args, "-o", "json"]))

    def apply_json(self, value: dict[str, Any]) -> None:
        self.run(["-n", self.namespace, "apply", "-f", "-"], input_text=json.dumps(value))

    def wait_rollout(self, deployment: str) -> None:
        self.run(["-n", self.namespace, "rollout", "status", f"deployment/{deployment}", "--timeout=600s"])

    def wait_rollout_in(self, namespace: str, deployment: str) -> None:
        self.run(["-n", namespace, "rollout", "status", f"deployment/{deployment}", "--timeout=600s"])


def log_phase(message: str) -> None:
    LOG.info("[%s] %s", datetime.now().astimezone().strftime("%H:%M:%S"), message)


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
        ["-n", namespace, "exec", "-i", pod, "-c", container, "--", "sh", "-c", "cat > /tmp/aharush-cgroup-sampler.sh && chmod 700 /tmp/aharush-cgroup-sampler.sh"],
        input_text=sampler,
    )
    handle = output.open("w", encoding="utf-8")
    process = subprocess.Popen(
        ["oc", "-n", namespace, "exec", pod, "-c", container, "--", "env", "CGROUP_SAMPLE_INTERVAL=0.100", "sh", "/tmp/aharush-cgroup-sampler.sh"],
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


def preflight(cluster: Cluster, agents: int, system_ns: str, openshell_ns: str, appworld: bool) -> None:
    log_phase("preflight: validating cluster, target node and system services")
    context = cluster.run(["config", "current-context"])
    user = cluster.run(["whoami"])
    node = cluster.json(["get", "node", cluster.node])
    ready = any(c.get("type") == "Ready" and c.get("status") == "True" for c in node.get("status", {}).get("conditions", []))
    if not ready or node.get("spec", {}).get("unschedulable"):
        raise RuntimeError(f"target node is not ready/schedulable: {cluster.node}")
    LOG.info("context=%s user=%s node=%s", context.strip(), user.strip(), cluster.node)
    deployments = ["mock-llm", "jaeger"]
    if appworld:
        deployments.append("appworld")
    for deployment in deployments:
        cluster.wait_rollout_in(system_ns, deployment)
    cluster.run(["-n", openshell_ns, "get", "statefulset/openshell"])
    services = cluster.run(["-n", cluster.namespace, "get", "svc", "openclaw-shell"], check=False)
    if not services.strip():
        raise RuntimeError("base openclaw-shell service is missing; refusing to invent a deployment template")
    if agents < 1 or agents > 9:
        raise ValueError("--agents must be between 1 and 9")


def _clone_config(cluster: Cluster, source: str, target: str, workspace: str) -> None:
    config = cluster.json(["-n", cluster.namespace, "get", "configmap", source])
    for key in ("uid", "resourceVersion", "creationTimestamp", "managedFields"):
        config.get("metadata", {}).pop(key, None)
    config["metadata"]["name"] = target
    raw = json.loads(config["data"]["openclaw.json"])
    entry = raw.setdefault("plugins", {}).setdefault("entries", {}).setdefault("openshell", {})
    # Use a per-agent wrapper that injects the Kubernetes driver configuration
    # at sandbox-create time. The OpenShell sandbox pod is created by the
    # gateway after the workspace exists, so setting a nodeSelector only on the
    # OpenClaw deployment does not constrain the sandbox pod itself.
    entry["config"]["command"] = "/opt/openshell/bin/openshell-pinned"
    entry["config"]["workspace"] = workspace
    config["data"]["openclaw.json"] = json.dumps(raw)
    cluster.apply_json(config)


def _ensure_openshell_workspace(cluster: Cluster, deployment: str, workspace: str, openshell_ns: str) -> None:
    cli = "/opt/openshell/bin/openshell"
    endpoint = f"http://openshell.{openshell_ns}.svc.cluster.local:8080"
    common = ["-n", cluster.namespace, "exec", f"deployment/{deployment}", "-c", "gateway", "--", cli]
    existing = cluster.run([*common, "workspace", "get", workspace, "--gateway-endpoint", endpoint], check=False)
    if existing.strip():
        return
    cluster.run([*common, "workspace", "create", "--name", workspace, "--gateway-endpoint", endpoint])


def _configured_workspace(cluster: Cluster, agent: str) -> str:
    """Read the workspace actually loaded by an existing aharush agent."""
    suffix = agent.rsplit("-", 1)[-1]
    config_name = f"aharush-openclaw-config-{suffix}"
    try:
        config = cluster.json(["-n", cluster.namespace, "get", "configmap", config_name])
        raw = json.loads((config.get("data") or {}).get("openclaw.json") or "{}")
        value = (((raw.get("plugins") or {}).get("entries") or {}).get("openshell") or {}).get("config", {}).get("workspace")
        return str(value or "")
    except (json.JSONDecodeError, TypeError, ValueError, RuntimeError):
        return ""


def deploy_agents(cluster: Cluster, agents: int, *, reuse: bool, workspace_prefix: str, openshell_ns: str) -> list[str]:
    log_phase(f"deployment: preparing {agents} OpenClaw agents with isolated OpenShell workspaces")
    names: list[str] = []
    for index in range(1, agents + 1):
        name = f"aharush-openclaw-agent-{index}"
        service = name
        config = f"aharush-openclaw-config-{index}"
        workspace = f"{workspace_prefix}-{index}"
        names.append(service)
        existing = cluster.run(["-n", cluster.namespace, "get", "deployment", name], check=False).strip()
        if reuse and existing:
            # Reuse the already-loaded agent configuration and persistent
            # OpenShell workspace; do not invent a new workspace that the
            # already-running deployment will not use.
            workspace = _configured_workspace(cluster, name) or workspace
            cluster.wait_rollout(name)
            service_obj = {
                "apiVersion": "v1", "kind": "Service",
                "metadata": {"name": service, "labels": {"app": name, "managed-by": "aharush-experiment"}},
                "spec": {"clusterIP": "None", "selector": {"app": name}, "ports": [{"name": "gateway", "port": 18790, "targetPort": 18790}]},
            }
            cluster.apply_json(service_obj)
            _ensure_openshell_workspace(cluster, name, workspace, openshell_ns)
            continue
        _clone_config(cluster, "openclaw-shell-config", config, workspace)
        deployment = cluster.json(["-n", cluster.namespace, "get", "deployment", name if existing else "openclaw-shell"])
        for key in ("uid", "resourceVersion", "generation", "creationTimestamp", "managedFields", "status"):
            deployment.get("metadata", {}).pop(key, None)
        deployment["metadata"]["name"] = name
        deployment["metadata"]["labels"] = {"app": name, "managed-by": "aharush-experiment"}
        spec = deployment["spec"]
        spec["selector"]["matchLabels"] = {"app": name}
        template = spec["template"]
        template["metadata"]["labels"] = {"app": name, "managed-by": "aharush-experiment"}
        template["metadata"].setdefault("annotations", {})["aharush.experiment/reconciled-at"] = str(time.time())
        template["spec"]["nodeSelector"] = {"kubernetes.io/hostname": cluster.node}
        for volume in template["spec"].get("volumes", []):
            if volume.get("name") == "openclaw-shell-home":
                volume.clear()
                volume.update({"name": "openclaw-shell-home", "emptyDir": {}})
            if volume.get("name") == "config-template":
                volume["configMap"] = {"name": config}
        for container in template["spec"].get("containers", []):
            if container.get("name") == "gateway":
                env = [item for item in container.setdefault("env", []) if item.get("name") != "OPENSHELL_FLEET_WORKSPACE"]
                env.append({"name": "OPENSHELL_FLEET_WORKSPACE", "value": workspace})
                container["env"] = env
        _install_pinned_openshell_wrapper(deployment, cluster.node)
        cluster.apply_json(deployment)
        service_obj = {
            "apiVersion": "v1", "kind": "Service",
            "metadata": {"name": service, "labels": {"app": name, "managed-by": "aharush-experiment"}},
            "spec": {"clusterIP": "None", "selector": {"app": name}, "ports": [{"name": "gateway", "port": 18790, "targetPort": 18790}]},
        }
        cluster.apply_json(service_obj)
        cluster.wait_rollout(name)
        _ensure_openshell_workspace(cluster, name, workspace, openshell_ns)
    return names


def _install_pinned_openshell_wrapper(deployment: dict[str, Any], node: str) -> None:
    """Install a wrapper that pins sandbox pods at sandbox-create time."""
    selector_json = json.dumps(
        {"kubernetes": {"pod": {"node_selector": {"kubernetes.io/hostname": node}}}},
        separators=(",", ":"),
    )
    # Version the marker so an already-deployed experiment agent receives the
    # corrected wrapper on the next reconciliation. Older runs installed a
    # shell wrapper that only matched `sandbox create` when it was argv[0:2],
    # but OpenClaw supplies global flags (including --workspace) first.
    marker = "# aharush-openshell-node-selector-v2"
    wrapper = (
        f"\n              {marker}\n"
        "              cat > /opt/openshell/bin/openshell-pinned <<'AHARUSH_OPEN_SHELL_WRAPPER'\n"
        "#!/usr/bin/env node\n"
        "const { spawnSync } = require('child_process');\n"
        "const args = process.argv.slice(2);\n"
        "const sandboxIndex = args.indexOf('sandbox');\n"
        "if (sandboxIndex >= 0 && args[sandboxIndex + 1] === 'create' && !args.includes('--driver-config-json')) {\n"
        f"  args.splice(sandboxIndex + 2, 0, '--driver-config-json', '{selector_json}');\n"
        "}\n"
        "const result = spawnSync('/opt/openshell/bin/openshell', args, { stdio: 'inherit' });\n"
        "process.exit(result.status ?? 1);\n"
        "AHARUSH_OPEN_SHELL_WRAPPER\n"
        "              chmod 755 /opt/openshell/bin/openshell-pinned\n"
    )
    for init_container in deployment["spec"]["template"]["spec"].get("initContainers", []):
        if init_container.get("name") != "init-config":
            continue
        args = init_container.setdefault("args", [""])
        if marker not in args[0]:
            args[0] += wrapper
        return
    raise RuntimeError("base OpenClaw deployment has no init-config container")


def ensure_driver_config(cluster: Cluster) -> None:
    name = "aharush-experiment-driver"
    data = (ROOT / "src/trace_replay_sim/driver.py").read_text(encoding="utf-8")
    cluster.apply_json({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": name, "labels": {"managed-by": "aharush-experiment"}},
        "data": {"driver.py": data},
    })


def build_queue(
    source: Path,
    out: Path,
    per_workload: int,
    session_id: str | None = None,
    max_traces: int | None = None,
) -> list[dict[str, Any]]:
    partition = out / "workloads"
    split_corpus(source, partition, limit_per_workload=None if session_id else per_workload)
    queue: list[dict[str, Any]] = []
    for workload in WORKLOADS:
        workload_path = partition / f"{workload}.json"
        if not workload_path.exists():
            continue
        payload = json.loads(workload_path.read_text(encoding="utf-8"))
        for index, session in enumerate(payload["sessions"]):
            if session_id and str(session.get("session_id")) != session_id:
                continue
            # Keep the execution queue limited to deterministic, single-agent
            # sessions. Nested/subagent traces and sessions without complete
            # recorded tool results cannot be reproduced faithfully by this
            # harness and must not silently enter a controlled run.
            if classify_session(session)["status"] != "controlled_replay_candidate":
                continue
            queue.append({"workload": workload, "index": index, "session": session})
    return queue[:max_traces] if max_traces is not None else queue


def create_trace_job(cluster: Cluster, item: dict[str, Any], agent: str, out: Path, system_ns: str, run_id: str, attempt: int = 0) -> tuple[str, Path, str]:
    workload = safe_name(item["workload"])
    index = int(item["index"])
    trace_dir = out / f"{index:03d}-{item['session']['session_id']}__{workload}"
    trace_dir.mkdir(parents=True, exist_ok=True)
    corpus_path = trace_dir / "input_corpus.json"
    corpus_path.write_text(json.dumps({"dataset": "Exgentic/agent-llm-traces", "sessions": [item["session"]]}, indent=2))
    cm = f"aharush-{run_id}-corpus-{workload[:20]}-{index:03d}"
    job = f"aharush-{run_id}-trace-{workload[:20]}-{index:03d}" + (f"-r{attempt}" if attempt else "")
    if not cluster.run(["-n", cluster.namespace, "get", "configmap", cm], check=False).strip():
        cluster.run(["-n", cluster.namespace, "create", "configmap", cm, f"--from-file=replay.json={corpus_path}"])
    driver_env: list[dict[str, Any]] = []
    token_secret = os.environ.get("OPENCLAW_TOKEN_SECRET")
    if token_secret:
        driver_env.append({
            "name": "OPENCLAW_TOKEN",
            "valueFrom": {
                "secretKeyRef": {
                    "name": token_secret,
                    "key": os.environ.get("OPENCLAW_TOKEN_SECRET_KEY", "token"),
                }
            },
        })
    job_obj = {
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {"name": job, "labels": {"app": "aharush-experiment", "workload": workload}},
        "spec": {"backoffLimit": 0, "ttlSecondsAfterFinished": 86400, "template": {
            "metadata": {"labels": {"app": "aharush-experiment", "workload": workload}},
            "spec": {"restartPolicy": "Never", "nodeSelector": {"kubernetes.io/hostname": cluster.node}, "containers": [{
                "name": "driver", "image": "python:3.12-slim", "command": ["python", "/app/driver.py"],
                # Match the established profiler-v2 replay semantics exactly.
                "args": ["--corpus", "/data/replay.json", "--out", "/results", "--layer", "shell", "--url", f"http://{agent}.{cluster.namespace}.svc.cluster.local:18790", "--model", "openclaw/perf_agent", "--timeout", "1800", "--stream", "--prewarm"],
                "env": driver_env,
                "volumeMounts": [{"name": "source", "mountPath": "/app"}, {"name": "corpus", "mountPath": "/data"}, {"name": "results", "mountPath": "/results"}],
            }], "volumes": [{"name": "source", "configMap": {"name": "aharush-experiment-driver"}}, {"name": "corpus", "configMap": {"name": cm}}, {"name": "results", "emptyDir": {}}]}
        }},
    }
    cluster.apply_json(job_obj)
    return job, trace_dir, str(item["session"].get("session_id") or "")


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
    if edge_path.exists():
        for line in edge_path.read_text(encoding="utf-8").splitlines():
            try:
                edge = json.loads(line)
                edge_trace_id = str(edge.get("trace_id") or "")
            except json.JSONDecodeError:
                continue
            if edge_trace_id and not edge.get("warmup"):
                edge_trace_ids.add(edge_trace_id)
    if edge_trace_ids:
        filtered = [trace for trace in raw if str(trace.get("traceID") or "") in edge_trace_ids]
    else:
        filtered = [
            trace for trace in raw
            if _trace_has_session(trace, session_id) and _trace_overlaps_window(trace, start_us, end_us)
        ]
    (traces_dir / "raw_traces.json").write_text(json.dumps(filtered, indent=2), encoding="utf-8")
    per_turn, per_tool, sessions = extract_per_turn(filtered)
    (traces_dir / "per_turn_segments.json").write_text(json.dumps(per_turn, indent=2), encoding="utf-8")
    (traces_dir / "per_tool_segments.json").write_text(json.dumps(per_tool, indent=2), encoding="utf-8")
    (traces_dir / "session_summary.json").write_text(json.dumps(sessions, indent=2), encoding="utf-8")
    (traces_dir / "timing_segments.json").write_text(json.dumps(compute_timing_segments(filtered), indent=2), encoding="utf-8")
    return len(filtered)


def collect_trace(
    cluster: Cluster,
    job: str,
    trace_dir: Path,
    session_id: str,
    start: float,
    system_ns: str,
    openshell_ns: str,
    jaeger: str,
    agent: str,
    workspace: str,
    appworld: bool,
) -> None:
    openclaw_pod = cluster.run(["-n", cluster.namespace, "get", "pod", "-l", f"app={agent}", "-o", "jsonpath={.items[0].metadata.name}"]).strip()
    cgroup_dir = trace_dir / "data/shell/prometheus/cgroup"
    samplers = [
        _start_cgroup_sampler(cluster, cluster.namespace, openclaw_pod, "gateway", cgroup_dir / "openclaw_gateway.csv"),
    ]
    watcher_stop = threading.Event()
    watcher_errors: list[str] = []

    def watch_sandbox() -> None:
        while not watcher_stop.wait(0.25):
            try:
                pods = cluster.json(["-n", openshell_ns, "get", "pods"])
                for pod in pods.get("items", []):
                    name = str(pod.get("metadata", {}).get("name") or "")
                    spec = pod.get("spec", {})
                    if (
                        name == "openshell-0"
                        or not name.startswith(f"{workspace}--")
                        or pod.get("status", {}).get("phase") != "Running"
                    ):
                        continue
                    if spec.get("nodeName") != cluster.node:
                        watcher_errors.append(f"sandbox {name} scheduled on {spec.get('nodeName')}, expected {cluster.node}")
                        return
                    containers = [str(item.get("name")) for item in spec.get("containers", [])]
                    container = "agent" if "agent" in containers else (containers[0] if containers else "")
                    if container:
                        samplers.append(_start_cgroup_sampler(cluster, openshell_ns, name, container, cgroup_dir / "openshell_sandbox.csv"))
                        return
            except Exception as exc:  # noqa: BLE001
                watcher_errors.append(str(exc))
                return

    watcher = threading.Thread(target=watch_sandbox, name="aharush-sandbox-cgroup-watcher", daemon=True)
    watcher.start()
    try:
        cluster.run(["-n", cluster.namespace, "wait", "--for=condition=complete", f"job/{job}", "--timeout=7200s"])
    finally:
        watcher_stop.set()
        watcher.join(timeout=5)
        _stop_cgroup_samplers(samplers)
    if watcher_errors:
        raise RuntimeError(f"sandbox cgroup watcher failed: {watcher_errors[0]}")
    end = time.time()
    pod = cluster.run(["-n", cluster.namespace, "get", "pod", "-l", f"job-name={job}", "-o", "jsonpath={.items[0].metadata.name}"]).strip()
    shell = trace_dir / "data/shell"
    shell.mkdir(parents=True, exist_ok=True)
    (shell / "terminal.log").write_text(cluster.run(["-n", cluster.namespace, "logs", pod], check=False))
    cluster.run(["-n", cluster.namespace, "cp", f"{pod}:/results/.", str(shell)], check=False)
    # The mock edge recorder is the authoritative source for model TTFB and
    # response completion boundaries. This is how the established profiler
    # produced the reference waterfall.
    mock_edges = cluster.run(
        ["-n", system_ns, "exec", "deployment/mock-llm", "--", "cat", "/edges/mock_edges.jsonl"],
        check=False,
    )
    session_edges: list[str] = []
    for line in mock_edges.splitlines():
        try:
            edge = json.loads(line)
            edge_wall_s = float(edge.get("t_recv_wall_ns") or 0) / 1_000_000_000
        except (json.JSONDecodeError, TypeError, ValueError):
            continue
        if edge.get("session_id") == session_id and start <= edge_wall_s <= end + 30:
            session_edges.append(line)
    (shell / "mock_edges.jsonl").write_text("\n".join(session_edges) + ("\n" if session_edges else ""), encoding="utf-8")
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
        export_summary = export_jaeger(
            traces,
            start_dt,
            datetime.fromtimestamp(end + 30, timezone.utc),
            jaeger_base=jaeger,
            service="openclaw-gateway-shell",
        )
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
        driver_summary = json.loads((shell / "terminal.log").read_text(encoding="utf-8"))
        driver_start_us = int(float(driver_summary.get("start_unix", start)) * 1_000_000)
        driver_end_us = int(float(driver_summary.get("end_unix", end)) * 1_000_000)
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        pass
    matched = _rewrite_filtered_jaeger(trace_dir, session_id, driver_start_us, driver_end_us)
    if matched == 0:
        raise RuntimeError(f"No Jaeger traces matched session/time window for {session_id}")
    collect_prometheus(shell / "prometheus", start=start, end=end, ns_openclaw=cluster.namespace, ns_openshell=openshell_ns, openclaw_pod=openclaw_pod)
    profile_v2(traces=traces / "raw_traces.json", mock_edges=shell / "mock_edges.jsonl", prom=shell / "prometheus", out=shell / "profile-v2", driver_requests=shell / "terminal.log")
    analyze_per_turn(traces, shell / "prometheus", trace_dir / "analysis")
    session_events: list[str] = []
    if appworld:
        appworld_pod = cluster.run(["-n", system_ns, "get", "pod", "-l", "app=appworld", "-o", "jsonpath={.items[0].metadata.name}"]).strip()
        app_dir = trace_dir / "data/appworld"
        app_dir.mkdir(parents=True, exist_ok=True)
        events = cluster.run(["-n", system_ns, "exec", appworld_pod, "--", "cat", "/tmp/appworld-service-raw.log"], check=False)
        session_events = [line for line in events.splitlines() if session_id in line]
        (app_dir / "events.jsonl").write_text("\n".join(session_events) + ("\n" if session_events else ""))
        plot_appworld_events(
            app_dir / "events.jsonl",
            app_dir / "appworld_api_latency.png",
            f"Shell AppWorld API latency: {session_id}",
        )
    per_turn = json.loads((shell / "profile-v2/per_turn.json").read_text(encoding="utf-8"))
    per_tool = json.loads((shell / "profile-v2/per_tool.json").read_text(encoding="utf-8"))
    waterfall = shell / "profile-v2/diagrams/latency_waterfall.png"
    tool_errors = sum(row.get("error_category") not in (None, "") for row in per_tool)
    cgroup_rows = {path.name: max(0, sum(1 for _ in path.open(encoding="utf-8")) - 1) for path in cgroup_dir.glob("*.csv")}
    if not per_turn or not per_tool or tool_errors or any(count < 2 for count in cgroup_rows.values()) or len(cgroup_rows) != 2 or not waterfall.exists() or waterfall.stat().st_size < 1000:
        raise RuntimeError(
            f"invalid profiler output for {session_id}: turns={len(per_turn)} tools={len(per_tool)} tool_errors={tool_errors} cgroup_rows={cgroup_rows} waterfall_bytes={waterfall.stat().st_size if waterfall.exists() else 0}"
        )
    if appworld and not session_events:
        raise RuntimeError(f"no real AppWorld events captured for {session_id}")
    metadata = {
        "job": job,
        "agent": agent,
        "session_id": session_id,
        "start": start,
        "end": end,
        "target_node": cluster.node,
        "appworld": appworld,
        "openshell_namespace": openshell_ns,
    }
    (trace_dir / "trace.json").write_text(json.dumps(metadata, indent=2))


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the complete multi-workload OpenClaw/OpenShell experiment")
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--agents", type=int, default=9)
    parser.add_argument("--traces-per-workload", type=int, default=25)
    parser.add_argument("--max-traces", type=int, help="cap the total number of traces after workload partitioning")
    parser.add_argument("--require-traces", type=int, help="fail unless the final queue has exactly this many traces")
    parser.add_argument("--session-id", help="run exactly one recorded session")
    parser.add_argument("--namespace", default="trace-replay")
    parser.add_argument("--system-namespace", default="trace-replay")
    parser.add_argument("--openshell-namespace", default="openshell-tracesim")
    parser.add_argument("--appworld", action="store_true", help="require the optional AppWorld service and collect real AppWorld events")
    parser.add_argument("--node", default=os.environ.get("TARGET_NODE"))
    parser.add_argument("--kubeconfig", default=os.environ.get("KUBECONFIG"))
    parser.add_argument("--jaeger", default="http://127.0.0.1:16686")
    parser.add_argument("--reuse-agents", action="store_true")
    parser.add_argument("--retries", type=int, default=1)
    parser.add_argument("--plan", action="store_true")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if args.plan == args.execute:
        parser.error("choose exactly one of --plan or --execute")
    if args.agents < 1 or args.agents > 9:
        parser.error("cluster limit is 9 agents")
    if args.max_traces is not None and args.max_traces < 1:
        parser.error("--max-traces must be positive")
    if args.require_traces is not None and args.require_traces < 1:
        parser.error("--require-traces must be positive")
    cluster = Cluster(args.kubeconfig, args.namespace, args.node)
    queue = build_queue(args.corpus, args.out, args.traces_per_workload, args.session_id, args.max_traces)
    if not queue:
        parser.error(f"session not found in corpus: {args.session_id}")
    if args.require_traces is not None and len(queue) != args.require_traces:
        parser.error(f"expected exactly {args.require_traces} traces, found {len(queue)}")
    log_phase(f"queue: {len(queue)} traces ({args.traces_per_workload} per workload), {args.agents} agents")
    if args.plan:
        for workload in WORKLOADS:
            LOG.info("  %s: %d", workload, sum(item["workload"] == workload for item in queue))
        return 0
    preflight(cluster, args.agents, args.system_namespace, args.openshell_namespace, args.appworld)
    ensure_driver_config(cluster)
    # OpenClaw validates workspace names to a maximum of 19 characters.
    # Keep the per-run timestamp component while leaving room for `-<index>`.
    run_token = safe_name(args.out.name).split("-", 1)[1][:6] if "-" in args.out.name else safe_name(args.out.name)[:6]
    workspace_prefix = f"aharush-{run_token}"
    log_phase(f"deployment: workspace prefix={workspace_prefix}")
    agents = deploy_agents(cluster, args.agents, reuse=args.reuse_agents, workspace_prefix=workspace_prefix, openshell_ns=args.openshell_namespace)
    workspace_by_agent = {
        agent: _configured_workspace(cluster, agent) or f"{workspace_prefix}-{index}"
        for index, agent in enumerate(agents, start=1)
    }
    local_port = _free_local_port()
    jaeger_url = f"http://127.0.0.1:{local_port}"
    pf = subprocess.Popen(
        ["oc", "-n", args.system_namespace, "port-forward", "svc/jaeger", f"{local_port}:16686"],
        env=cluster.env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    try:
        _wait_for_jaeger(jaeger_url, pf)
        log_phase(f"telemetry: Jaeger ready at {jaeger_url}")
        errors_path = args.out / "errors.jsonl"
        errors_path.unlink(missing_ok=True)

        def run_one(position: int, item: dict[str, Any]) -> dict[str, Any]:
            agent = agents[position % len(agents)]
            session_id = str(item["session"].get("session_id") or "")
            for attempt in range(args.retries + 1):
                try:
                    log_phase(f"trace {position + 1}/{len(queue)}: {session_id} workload={item['workload']} agent={agent} attempt={attempt + 1}")
                    start = time.time()
                    # Kubernetes Jobs are immutable; include a per-process
                    # suffix so reruns never reuse a completed Job name.
                    run_id = f"{safe_name(args.out.name)[:6] or 'run'}-{int(start) % 1000000:06d}"
                    job, trace_dir, session_id = create_trace_job(cluster, item, agent, args.out / "traces", args.system_namespace, run_id, attempt)
                    workspace = workspace_by_agent[agent]
                    collect_trace(cluster, job, trace_dir, session_id, start, args.system_namespace, args.openshell_namespace, jaeger_url, agent, workspace, args.appworld)
                    log_phase(f"trace complete: {trace_dir}")
                    return {"position": position, "session_id": session_id, "agent": agent, "ok": True, "attempt": attempt + 1}
                except Exception as exc:
                    if attempt >= args.retries:
                        error = {"position": position, "session_id": session_id, "agent": agent, "ok": False, "attempts": attempt + 1, "error": str(exc)}
                        with errors_path.open("a", encoding="utf-8") as handle:
                            handle.write(json.dumps(error) + "\n")
                        LOG.error("trace failed permanently: %s", error)
                        return error
                    LOG.warning("trace failed; retrying: session=%s error=%s", session_id, exc)
            raise AssertionError("unreachable")

        with ThreadPoolExecutor(max_workers=len(agents), thread_name_prefix="aharush-agent") as pool:
            futures = [pool.submit(run_one, position, item) for position, item in enumerate(queue)]
            results = [future.result() for future in as_completed(futures)]
        failed = [result for result in results if not result.get("ok")]
        (args.out / "run_summary.json").write_text(json.dumps({"total": len(results), "completed": len(results) - len(failed), "failed": len(failed), "agents": len(agents)}, indent=2))
        if failed:
            raise RuntimeError(f"{len(failed)} traces failed; see {errors_path}")
    finally:
        pf.send_signal(signal.SIGTERM)
        pf.wait(timeout=10)
    (args.out / "experiment.json").write_text(json.dumps({
        "agents": args.agents,
        "traces": len(queue),
        "workloads": WORKLOADS,
        "node": args.node,
        "appworld": args.appworld,
        "openshell_namespace": args.openshell_namespace,
    }, indent=2))
    subprocess.run([sys.executable, str(ROOT / "scripts/analyze_multi_agent_results.py"), "--experiment", str(args.out)], check=True)
    log_phase(f"complete: results in {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
