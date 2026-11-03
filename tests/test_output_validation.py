import csv
import json
import struct
import zlib
from pathlib import Path

import pytest

from trace_replay_sim.output_validation import OutputValidationError, validate_trace_output


def _png(path: Path, width: int = 160, height: int = 120) -> None:
    def chunk(kind: bytes, payload: bytes) -> bytes:
        return struct.pack(">I", len(payload)) + kind + payload + struct.pack(">I", zlib.crc32(kind + payload) & 0xFFFFFFFF)

    # Use a deterministic varied image rather than a solid white fixture.  A
    # solid image compresses below the validator's minimum artifact size and
    # would not exercise the real "non-empty plot" check.
    rows = []
    for y in range(height):
        row = bytearray(b"\x00")
        for x in range(width):
            row.extend(((x * 17 + y * 31) % 256, (x * 7 + y * 13) % 256, (x * 3 + y * 19) % 256))
        rows.append(bytes(row))
    pixels = b"".join(rows)
    raw = b"\x89PNG\r\n\x1a\n"
    raw += chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
    raw += chunk(b"IDAT", zlib.compress(pixels))
    raw += chunk(b"IEND", b"")
    path.write_bytes(raw)


def _valid_trace(tmp_path: Path) -> Path:
    trace = tmp_path / "trace"
    diagrams = trace / "data/shell/profile-v2/diagrams"
    cgroup = trace / "data/shell/prometheus/cgroup"
    backend = trace / "data/backend"
    diagrams.mkdir(parents=True)
    cgroup.mkdir(parents=True)
    backend.mkdir(parents=True)
    (trace / "data/shell/traces").mkdir(parents=True)
    (trace / "data/shell/prometheus").mkdir(parents=True, exist_ok=True)
    (trace / "data/shell/traces/raw_traces.json").write_text("[{\"traceID\":\"t\",\"spans\":[]}]", encoding="utf-8")
    (trace / "data/shell/mock_edges.jsonl").write_text("{\"session_id\":\"s\"}\n", encoding="utf-8")
    (trace / "data/shell/sandbox_lifecycle.json").write_text(json.dumps({
        "status": "ready",
        "prewarm_requested": True,
        "prewarm_status": "measured",
        "provisioning_mode": "warm_reused",
        "sandbox_init_ms": 0.0,
        "sandbox_request_to_first_action_ms": 200.0,
        "readiness_evidence": "successful_open_shell_prewarm_no_new_pod",
    }), encoding="utf-8")
    (trace / "data/shell/prometheus/collect_manifest.json").write_text(json.dumps({"cadvisor_success": 2, "app_success": 1}), encoding="utf-8")
    for name in ("openclaw_gateway.csv", "openshell_sandbox.csv"):
        (cgroup / name).write_text(
            "epoch_ns,cpu_usage_usec,memory_bytes\n1000000000,1,2\n1100000000,2,3\n",
            encoding="utf-8",
        )
    (trace / "data/shell/profile-v2/per_turn.json").write_text("[{\"turn\":0},{\"turn\":1}]", encoding="utf-8")
    (trace / "data/shell/profile-v2/per_tool.json").write_text("[{\"tool\":\"a\"}]", encoding="utf-8")
    with (diagrams / "per_turn_efficiency.csv").open("w", newline="", encoding="utf-8") as handle:
        fields = ["turn", "sandbox_init_ms", "prompt_boundary_ms", "response_processing_ms", "tool_dispatch_overhead_ms", "openshell_exec_ms", "tool_failure_ms"]
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerow({"turn": 0, "sandbox_init_ms": 2, "prompt_boundary_ms": 5, "response_processing_ms": 0, "tool_dispatch_overhead_ms": 0, "openshell_exec_ms": 0, "tool_failure_ms": 0})
        writer.writerow({"turn": 1, "sandbox_init_ms": 0, "prompt_boundary_ms": 3, "response_processing_ms": 4, "tool_dispatch_overhead_ms": 1, "openshell_exec_ms": 6, "tool_failure_ms": 0})
    for name in ("latency_waterfall.png", "openshell_execution_latency_ms.png", "resource_memory_mib.png", "resource_cpu_cores.png", "resource_timeseries.png"):
        _png(diagrams / name)
    _png(backend / "backend_api_latency.png")
    return trace


def test_output_validation_accepts_complete_evidence(tmp_path):
    evidence = validate_trace_output(
        _valid_trace(tmp_path),
        expected_turns=2,
        expected_tool_calls=1,
        backend_dir_name="backend",
        backend_plot_name="backend_api_latency.png",
    )
    assert evidence["passed"] is True
    assert evidence["cgroup_rows"]["openclaw_gateway.csv"] == 2


def test_output_validation_rejects_missing_first_turn_invariant(tmp_path):
    trace = _valid_trace(tmp_path)
    path = trace / "data/shell/profile-v2/diagrams/per_turn_efficiency.csv"
    text = path.read_text(encoding="utf-8").replace("0,2,5,0,0,0,0", "0,2,5,9,0,0,0", 1)
    path.write_text(text, encoding="utf-8")
    with pytest.raises(OutputValidationError, match="turn 0 must not contain response_processing_ms"):
        validate_trace_output(
            trace,
            expected_turns=2,
            expected_tool_calls=1,
            backend_dir_name="backend",
            backend_plot_name="backend_api_latency.png",
        )


def test_output_validation_rejects_zero_warm_reuse_measurement(tmp_path):
    trace = _valid_trace(tmp_path)
    path = trace / "data/shell/sandbox_lifecycle.json"
    lifecycle = json.loads(path.read_text(encoding="utf-8"))
    lifecycle["sandbox_request_to_first_action_ms"] = 0.0
    path.write_text(json.dumps(lifecycle), encoding="utf-8")
    with pytest.raises(OutputValidationError, match="warm reuse must have a positive"):
        validate_trace_output(
            trace,
            expected_turns=2,
            expected_tool_calls=1,
            backend_dir_name="backend",
            backend_plot_name="backend_api_latency.png",
        )
