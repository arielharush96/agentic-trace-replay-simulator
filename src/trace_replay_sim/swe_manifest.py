"""Pinned SWE-bench task environments for trace replay.

The trace corpus contains the issue conversation but not the repository
checkout.  This manifest supplies the missing environment identity without
including solution patches or secrets.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any


class SweManifestError(ValueError):
    """Raised when a trace cannot be mapped to a real SWE environment."""


_COMMIT_RE = re.compile(r"^[0-9a-fA-F]{40}$")
_IMAGE_RE = re.compile(r"^[A-Za-z0-9._/:@-]+$")
_REPO_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")


@dataclass(frozen=True)
class SweTask:
    session_id: str
    instance_id: str
    repo: str
    repo_url: str
    base_commit: str
    environment_setup_commit: str
    image: str
    version: str

    def public_dict(self) -> dict[str, Any]:
        return asdict(self)


def load_swe_manifest(path: Path) -> dict[str, SweTask]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except OSError as exc:
        raise SweManifestError(f"cannot read SWE manifest: {path}") from exc
    except json.JSONDecodeError as exc:
        raise SweManifestError(f"invalid SWE manifest JSON: {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise SweManifestError("SWE manifest root must be an object")
    if payload.get("schema") != "agentic.trace-replay.swe-task-manifest.v1":
        raise SweManifestError(f"unsupported SWE manifest schema: {payload.get('schema')!r}")
    entries = payload.get("tasks")
    if not isinstance(entries, list):
        raise SweManifestError("SWE manifest tasks must be a list")
    result: dict[str, SweTask] = {}
    instance_ids: set[str] = set()
    for raw in entries:
        if not isinstance(raw, dict):
            raise SweManifestError("SWE manifest task entries must be objects")
        required = ("session_id", "instance_id", "repo", "repo_url", "base_commit", "image")
        missing = [key for key in required if not str(raw.get(key) or "")]
        if missing:
            raise SweManifestError(f"SWE manifest entry is missing: {', '.join(missing)}")
        if not str(raw["repo_url"]).startswith("https://github.com/"):
            raise SweManifestError(f"SWE repository URL is not an HTTPS GitHub URL: {raw['repo_url']!r}")
        session_id = str(raw["session_id"])
        instance_id = str(raw["instance_id"])
        repo = str(raw["repo"])
        repo_url = str(raw["repo_url"])
        base_commit = str(raw["base_commit"])
        environment_setup_commit = str(raw.get("environment_setup_commit") or "")
        image = str(raw["image"])
        if session_id in result:
            raise SweManifestError(f"duplicate SWE session mapping: {session_id}")
        if instance_id in instance_ids:
            raise SweManifestError(f"duplicate SWE instance mapping: {instance_id}")
        if not _REPO_RE.fullmatch(repo):
            raise SweManifestError(f"invalid SWE repository name: {repo!r}")
        if repo_url.rstrip("/") != f"https://github.com/{repo}.git":
            raise SweManifestError(f"repository URL does not match repo {repo!r}: {repo_url!r}")
        if not _COMMIT_RE.fullmatch(base_commit):
            raise SweManifestError(f"invalid SWE base commit for {session_id}: {base_commit!r}")
        if environment_setup_commit and not _COMMIT_RE.fullmatch(environment_setup_commit):
            raise SweManifestError(f"invalid SWE environment commit for {session_id}: {environment_setup_commit!r}")
        if not _IMAGE_RE.fullmatch(image) or "/" not in image or any(char.isspace() for char in image):
            raise SweManifestError(f"invalid SWE task image for {session_id}: {image!r}")
        instance_ids.add(instance_id)
        result[session_id] = SweTask(
            session_id=session_id,
            instance_id=instance_id,
            repo=repo,
            repo_url=repo_url,
            base_commit=base_commit,
            environment_setup_commit=environment_setup_commit,
            image=image,
            version=str(raw.get("version") or ""),
        )
    if not result:
        raise SweManifestError(f"SWE manifest contains no tasks: {path}")
    return result


def validate_swe_sessions(sessions: list[dict[str, Any]], manifest: dict[str, SweTask]) -> None:
    required = {
        str(session.get("session_id") or "")
        for session in sessions
        if str(session.get("benchmark") or "") == "swebench"
    }
    missing = sorted(session_id for session_id in required if session_id not in manifest)
    if missing:
        raise SweManifestError("no real SWE environment mapping for: " + ", ".join(missing))
    invalid = sorted(
        session_id
        for session_id in required
        if session_id and manifest[session_id].session_id != session_id
    )
    if invalid:
        raise SweManifestError("SWE manifest session identity mismatch: " + ", ".join(invalid))


def manifest_sha256(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()
