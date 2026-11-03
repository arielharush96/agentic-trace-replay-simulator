import base64
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / "scripts"))

from run_experiment import _swe_sandbox_command
from trace_replay_sim.swe_manifest import SweManifestError, load_swe_manifest, validate_swe_sessions


MANIFEST = Path(__file__).parents[1] / "config/swebench-trace-manifest.json"


def test_checked_in_manifest_has_real_pinned_task_environments():
    tasks = load_swe_manifest(MANIFEST)
    assert len(tasks) == 25
    task = tasks["e3bfdef4aca3_061b9f48"]
    assert task.repo == "astropy/astropy"
    assert len(task.base_commit) == 40
    assert task.image.startswith("swebench/")


def test_swe_sessions_fail_closed_without_a_repository_mapping():
    tasks = load_swe_manifest(MANIFEST)
    with pytest.raises(SweManifestError, match="no real SWE environment mapping"):
        validate_swe_sessions([{"session_id": "missing", "benchmark": "swebench"}], tasks)


def test_generated_swe_command_carries_pinned_task_identity():
    tasks = load_swe_manifest(MANIFEST)
    task = tasks["e3bfdef4aca3_061b9f48"]
    command = _swe_sandbox_command(
        "s",
        "software_engineering_qa",
        "swebench",
        0,
        0,
        "mcp__environment__bash",
        {"command": "git status --short"},
        "http://backend",
        task,
    )
    encoded = command[2].split("b64decode('", 1)[1].split("'", 1)[0]
    payload = json.loads(base64.b64decode(encoded))
    assert payload["swe_task"]["base_commit"] == task.base_commit
    assert payload["swe_task"]["repo"] == task.repo
    compile(command[2], "<generated-swe-command>", "exec")


def test_manifest_round_trip_is_json_serializable():
    tasks = load_swe_manifest(MANIFEST)
    payload = {session_id: task.public_dict() for session_id, task in tasks.items()}
    assert json.loads(json.dumps(payload))["e3bfdef4aca3_061b9f48"]["base_commit"]


def test_manifest_rejects_unpinned_or_mismatched_metadata(tmp_path):
    payload = {
        "schema": "agentic.trace-replay.swe-task-manifest.v1",
        "tasks": [{
            "session_id": "s1",
            "instance_id": "owner__repo-1",
            "repo": "owner/repo",
            "repo_url": "https://github.com/owner/other.git",
            "base_commit": "not-a-commit",
            "image": "swebench/image:latest",
        }],
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(SweManifestError):
        load_swe_manifest(path)
