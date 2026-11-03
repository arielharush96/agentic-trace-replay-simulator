"""Harness adapters used by the replay driver.

The benchmark measures an agent harness, not one particular HTTP route.  This
module keeps transport and conversation-continuity details at the edge of the
driver so the corpus, timing recorder, and analysis pipeline can be reused by
different harnesses.

OpenClaw uses the Responses API and ``previous_response_id`` for transcript
continuity.  A generic OpenAI-compatible adapter is included so another
harness can be tested without adding a new driver implementation.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin


@dataclass(frozen=True)
class HarnessAdapter:
    """Describe the externally visible protocol of one agent harness."""

    name: str
    display_name: str
    api_style: str
    default_model: str
    path: str
    otel_service_names: tuple[str, ...] = ()

    def endpoint(self, base_url: str) -> str:
        """Return the request URL from a service root or an existing API URL."""
        base = base_url.rstrip("/") + "/"
        return urljoin(base, self.path.lstrip("/"))

    def build_request(
        self,
        *,
        base_url: str,
        model: str,
        prompt: str,
        stream: bool,
        previous_response_id: str | None,
    ) -> tuple[str, dict[str, Any]]:
        """Build ``(url, JSON body)`` for one replay request."""
        if self.api_style == "responses":
            payload: dict[str, Any] = {
                "model": model,
                "input": prompt,
                "stream": stream,
            }
            if previous_response_id:
                payload["previous_response_id"] = previous_response_id
        elif self.api_style == "chat-completions":
            payload = {
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "stream": stream,
            }
        else:  # pragma: no cover - registry prevents invalid adapters.
            raise ValueError(f"unsupported harness API style: {self.api_style}")
        return self.endpoint(base_url), payload


_HARNESSES: dict[str, HarnessAdapter] = {
    "openclaw": HarnessAdapter(
        name="openclaw",
        display_name="OpenClaw",
        api_style="responses",
        default_model="openclaw/perf_agent",
        path="/v1/responses",
        otel_service_names=("openclaw-gateway", "openclaw-gateway-shell"),
    ),
    "openai-compatible": HarnessAdapter(
        name="openai-compatible",
        display_name="OpenAI-compatible harness",
        api_style="chat-completions",
        default_model="agent",
        path="/v1/chat/completions",
    ),
}

_ALIASES = {
    "generic": "openai-compatible",
    "openai": "openai-compatible",
}


def available_harnesses() -> tuple[str, ...]:
    """Return names accepted by the CLI (aliases are intentionally omitted)."""
    return tuple(_HARNESSES)


def get_harness(name: str | None) -> HarnessAdapter:
    normalized = str(name or "openclaw").strip().lower()
    normalized = _ALIASES.get(normalized, normalized)
    try:
        return _HARNESSES[normalized]
    except KeyError as exc:
        choices = ", ".join(available_harnesses())
        raise ValueError(f"unknown harness {name!r}; choose one of: {choices}") from exc
