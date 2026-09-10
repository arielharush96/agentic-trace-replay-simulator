"""Collect Prometheus metrics for the trace-replay experiment.

Two resolution tiers:
  1s  — cAdvisor container CPU/memory (additional scrape job)
  1s  — OpenClaw application metrics scraped via additionalScrapeConfigs job

Fallback: if Thanos returns empty for app metrics, directly scrape OpenClaw's
/api/diagnostics/prometheus endpoint and parse the exposition format.
"""

from __future__ import annotations

import json
import os
import re
import ssl
import subprocess
import sys
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


DEFAULT_THANOS = ""


def oc_token() -> str:
    """Get auth token for Thanos queries."""
    token = os.environ.get("PROM_TOKEN")
    if token:
        return token
    result = subprocess.run(["oc", "whoami", "-t"], capture_output=True, text=True)
    if result.stdout.strip():
        return result.stdout.strip()
    result = subprocess.run(
        ["oc", "create", "token", "prometheus-k8s", "-n", "openshift-monitoring", "--duration=1h"],
        capture_output=True, text=True,
    )
    return result.stdout.strip()


def cadvisor_queries(ns_openclaw: str, ns_openshell: str, openclaw_pod: str | None = None) -> dict[str, str]:
    """cAdvisor container metrics — queried at the configured 1s scrape step."""
    # The namespace contains both plain OpenClaw and OpenClaw+OpenShell pods.
    # The profiler measures the shell gateway, so do not accidentally select
    # the plain openclaw deployment with the broader openclaw.* matcher.
    openclaw_selector = f'pod="{openclaw_pod}"' if openclaw_pod else 'pod=~"openclaw-shell.*"'
    cpu = f'rate(container_cpu_usage_seconds_total{{namespace="{ns_openclaw}",{openclaw_selector},container="gateway"}}[1m])'
    mem = f'container_memory_working_set_bytes{{namespace="{ns_openclaw}",{openclaw_selector},container="gateway"}}'
    return {
        "cpu_openclaw": cpu,
        "memory_openclaw": mem,
        "cpu_openshell": f'rate(container_cpu_usage_seconds_total{{namespace="{ns_openshell}",pod!~"default--oc-.*",container!~"POD|"}}[1m])',
        "memory_openshell": f'container_memory_working_set_bytes{{namespace="{ns_openshell}",pod!~"default--oc-.*",container!~"POD|"}}',
        # OpenShell sandbox pods are named default--oc-<id> on this cluster;
        # do not assume the word "sandbox" appears in the generated name.
        "cpu_sandbox": f'rate(container_cpu_usage_seconds_total{{namespace="{ns_openshell}",pod=~"default--oc-.*",container!~"POD|"}}[1m])',
        "memory_sandbox": f'container_memory_working_set_bytes{{namespace="{ns_openshell}",pod=~"default--oc-.*",container!~"POD|"}}',
        "cpu_mock_llm": f'rate(container_cpu_usage_seconds_total{{namespace="{ns_openclaw}",pod=~"mock-llm.*",container!~"POD|"}}[1m])',
        "memory_mock_llm": f'container_memory_working_set_bytes{{namespace="{ns_openclaw}",pod=~"mock-llm.*",container!~"POD|"}}',
    }


def openclaw_app_queries() -> dict[str, str]:
    """OpenClaw application metrics — scraped at 1s via additionalScrapeConfigs job."""
    return {
        "oc_tool_exec_p50": 'histogram_quantile(0.50, rate(openclaw_tool_execution_duration_seconds_bucket[10s]))',
        "oc_tool_exec_p95": 'histogram_quantile(0.95, rate(openclaw_tool_execution_duration_seconds_bucket[10s]))',
        "oc_model_call_p50": 'histogram_quantile(0.50, rate(openclaw_model_call_duration_seconds_bucket[10s]))',
        "oc_model_call_p95": 'histogram_quantile(0.95, rate(openclaw_model_call_duration_seconds_bucket[10s]))',
        "oc_queue_wait_p50": 'histogram_quantile(0.50, rate(openclaw_queue_lane_wait_seconds_bucket[10s]))',
        "oc_queue_wait_p95": 'histogram_quantile(0.95, rate(openclaw_queue_lane_wait_seconds_bucket[10s]))',
        "oc_queue_depth": 'openclaw_queue_lane_size',
        "oc_session_state": 'rate(openclaw_session_state_total[10s])',
        "oc_tool_exec_rate": 'rate(openclaw_tool_execution_total[10s])',
        "oc_model_call_rate": 'rate(openclaw_model_call_total[10s])',
        "oc_memory_rss": 'openclaw_memory_bytes{kind="rss"}',
        "oc_memory_heap": 'openclaw_memory_bytes{kind="heapUsed"}',
        "oc_harness_p50": 'histogram_quantile(0.50, rate(openclaw_harness_run_duration_seconds_bucket[10s]))',
        "oc_harness_p95": 'histogram_quantile(0.95, rate(openclaw_harness_run_duration_seconds_bucket[10s]))',
    }


def query_range(token: str, host: str, query: str, start: float, end: float, step: str = "15s") -> dict[str, Any] | None:
    params = urllib.parse.urlencode(
        {"query": query, "start": str(int(start)), "end": str(int(end)), "step": step}
    )
    url = f"https://{host}/api/v1/query_range?{params}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
            return json.loads(resp.read())
    except Exception as exc:
        return {"status": "error", "error": str(exc), "query": query}


def _has_data(result: dict | None) -> bool:
    """Check if a Prometheus query result actually has data points."""
    if not result or result.get("status") == "error":
        return False
    series = ((result.get("data") or {}).get("result") or [])
    return any(len(s.get("values") or []) > 0 for s in series)


def direct_scrape_openclaw(openclaw_url: str, api_key: str) -> str | None:
    """Directly scrape OpenClaw's /api/diagnostics/prometheus endpoint."""
    url = f"{openclaw_url}/api/diagnostics/prometheus"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {api_key}"})
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=10) as resp:
            return resp.read().decode("utf-8")
    except Exception as exc:
        print(f"  WARNING: direct scrape failed: {exc}", file=sys.stderr)
        return None


def parse_prometheus_text(text: str) -> dict[str, list[dict]]:
    """Parse Prometheus exposition format into structured data."""
    metrics: dict[str, list[dict]] = {}
    for line in text.split("\n"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = re.match(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(.+?)(\s+\d+)?$', line)
        if match:
            name = match.group(1)
            labels = match.group(2) or ""
            value = match.group(3)
            metrics.setdefault(name, []).append({"labels": labels, "value": value})
    return metrics


def discover_metrics(openclaw_url: str, api_key: str) -> list[str]:
    """Discover which metric names OpenClaw actually exports."""
    text = direct_scrape_openclaw(openclaw_url, api_key)
    if not text:
        return []
    metrics = parse_prometheus_text(text)
    return sorted(metrics.keys())


def collect(
    out_dir: Path,
    *,
    start: float,
    end: float,
    ns_openclaw: str = "trace-replay",
    ns_openshell: str = "openshell-tracesim",
    thanos_host: str | None = None,
    openclaw_pod: str | None = None,
    openclaw_url: str | None = None,
    openclaw_api_key: str = "",
) -> dict[str, Any]:
    token = oc_token()
    if not token:
        print("WARNING: could not obtain oc token for Thanos", file=sys.stderr)
    host = thanos_host or os.environ.get("THANOS_HOST", DEFAULT_THANOS)
    out_dir.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {"host": host, "start": start, "end": end, "metrics": [], "warnings": []}

    # cAdvisor container metrics at 1s. The v2 monitoring manifest installs a
    # dedicated 1-second scrape job; do not silently downsample this source.
    print("Collecting cAdvisor metrics (1s step)...")
    cadvisor_success = 0
    # Query a buffer so local counter-delta calculation has samples around the
    # workload boundaries.
    for name, query in cadvisor_queries(ns_openclaw, ns_openshell, openclaw_pod).items():
        result = query_range(token, host, query, max(0, start - 300), end + 60, step="1s") if token else None
        if _has_data(result):
            cadvisor_success += 1
        path = out_dir / f"{name}.json"
        path.write_text(json.dumps(result, indent=2), encoding="utf-8")
        report["metrics"].append(name)
    print(f"  {cadvisor_success}/{len(cadvisor_queries(ns_openclaw, ns_openshell, openclaw_pod))} queries returned data")

    # OpenClaw application metrics at 1s
    print("Collecting OpenClaw app metrics (1s step)...")
    oc_dir = out_dir / "openclaw_app_1s"
    oc_dir.mkdir(exist_ok=True)
    app_success = 0
    for name, query in openclaw_app_queries().items():
        result = query_range(token, host, query, start, end, step="1s") if token else None
        if _has_data(result):
            app_success += 1
        path = oc_dir / f"{name}.json"
        path.write_text(json.dumps(result, indent=2), encoding="utf-8")
        report["metrics"].append(f"openclaw_app_1s/{name}")

    print(f"  {app_success}/{len(openclaw_app_queries())} queries returned data")

    # Fallback: direct scrape if Thanos app metrics are empty
    if app_success == 0 and openclaw_url:
        print("  Thanos app metrics empty — trying direct scrape fallback...")
        text = direct_scrape_openclaw(openclaw_url, openclaw_api_key)
        if text:
            (oc_dir / "direct_scrape.txt").write_text(text, encoding="utf-8")
            metrics = parse_prometheus_text(text)
            (oc_dir / "discovered_metrics.json").write_text(
                json.dumps(sorted(metrics.keys()), indent=2), encoding="utf-8"
            )
            report["direct_scrape_metrics"] = len(metrics)
            print(f"  Direct scrape: {len(metrics)} metric families discovered")
            report["warnings"].append("Thanos app metrics empty; using direct scrape snapshot")
        else:
            report["warnings"].append("Both Thanos and direct scrape failed for app metrics")

    (out_dir / "collect_manifest.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report
