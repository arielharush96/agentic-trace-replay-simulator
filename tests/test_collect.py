import json
from pathlib import Path

from trace_replay_sim.collect import collect, skipped_report, write_report


def test_collect_without_thanos_writes_a_skipped_manifest(tmp_path, monkeypatch):
    monkeypatch.delenv("THANOS_HOST", raising=False)
    monkeypatch.setattr(
        "trace_replay_sim.collect.oc_token",
        lambda: (_ for _ in ()).throw(AssertionError("oc should not be called without a host")),
    )

    report = collect(tmp_path / "prometheus", start=10.0, end=20.0, openclaw_pod="openclaw-0")

    assert report["skipped"] is True
    assert report["host"] is None
    assert report["cadvisor_success"] == 0
    assert report["app_success"] == 0
    assert report["warnings"]
    manifest = json.loads((tmp_path / "prometheus" / "collect_manifest.json").read_text(encoding="utf-8"))
    assert manifest == report


def test_skipped_report_keeps_the_normal_manifest_schema(tmp_path):
    report = skipped_report(start=1.0, end=2.0, reason="telemetry unavailable", ns_openclaw="agents")

    assert report["schema"] == "agentic.trace-replay.prometheus-collection.v1"
    assert report["skip_reason"] == "telemetry unavailable"
    assert report["cadvisor_queries"] > 0
    assert report["app_queries"] > 0


def test_write_report_creates_the_telemetry_directory(tmp_path):
    out = tmp_path / "nested" / "prometheus"
    report = skipped_report(start=1.0, end=2.0, reason="test")

    write_report(out, report)

    assert Path(out / "collect_manifest.json").is_file()
