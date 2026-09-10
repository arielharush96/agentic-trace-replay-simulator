"""Normalized replay session produced from Exgentic OTEL traces."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class ToolCall:
    id: str
    name: str
    arguments: dict[str, Any] = field(default_factory=dict)
    mapped_command: list[str] = field(default_factory=list)
    recorded_result: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ToolCall:
        return cls(
            id=str(data.get("id") or ""),
            name=str(data.get("name") or ""),
            arguments=dict(data.get("arguments") or {}),
            mapped_command=list(data.get("mapped_command") or []),
            recorded_result=str(data.get("recorded_result") or ""),
        )


@dataclass
class LLMTurn:
    index: int
    span_id: str = ""
    duration_ms: float = 0.0
    input_tokens: int = 0
    output_tokens: int = 0
    model: str = ""
    content: str = ""
    finish_reason: str = ""
    tool_calls: list[ToolCall] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "span_id": self.span_id,
            "duration_ms": self.duration_ms,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "model": self.model,
            "content": self.content,
            "finish_reason": self.finish_reason,
            "tool_calls": [t.to_dict() for t in self.tool_calls],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LLMTurn:
        return cls(
            index=int(data.get("index") or 0),
            span_id=str(data.get("span_id") or ""),
            duration_ms=float(data.get("duration_ms") or 0.0),
            input_tokens=int(data.get("input_tokens") or 0),
            output_tokens=int(data.get("output_tokens") or 0),
            model=str(data.get("model") or ""),
            content=str(data.get("content") or ""),
            finish_reason=str(data.get("finish_reason") or ""),
            tool_calls=[ToolCall.from_dict(t) for t in (data.get("tool_calls") or [])],
        )


@dataclass
class ReplaySession:
    session_id: str
    harness: str = ""
    benchmark: str = ""
    models: list[str] = field(default_factory=list)
    user_prompt: str = ""
    total_tokens: int = 0
    llm_calls: int = 0
    tool_call_count: int = 0
    recorded_duration_ms: float = 0.0
    turns: list[LLMTurn] = field(default_factory=list)

    @property
    def first_turn(self) -> LLMTurn | None:
        return self.turns[0] if self.turns else None

    def gateway_text(self) -> str:
        """Text OpenClaw should receive when tools are disabled (baseline-style)."""
        for turn in self.turns:
            text = (turn.content or "").strip()
            if text:
                return text
        if self.turns and self.turns[0].tool_calls:
            names = ", ".join(t.name for t in self.turns[0].tool_calls)
            return f"I would call: {names}"
        return "ok"

    def to_dict(self) -> dict[str, Any]:
        first = self.first_turn
        return {
            "session_id": self.session_id,
            "harness": self.harness,
            "benchmark": self.benchmark,
            "models": list(self.models),
            "user_prompt": self.user_prompt,
            "total_tokens": self.total_tokens,
            "llm_calls": self.llm_calls,
            "tool_call_count": self.tool_call_count,
            "recorded_duration_ms": self.recorded_duration_ms,
            "input_tokens_first": first.input_tokens if first else 0,
            "output_tokens_first": first.output_tokens if first else 0,
            "first_llm_duration_ms": first.duration_ms if first else 0.0,
            "gateway_text": self.gateway_text(),
            "turns": [t.to_dict() for t in self.turns],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ReplaySession:
        turns = [LLMTurn.from_dict(t) for t in (data.get("turns") or [])]
        return cls(
            session_id=str(data.get("session_id") or ""),
            harness=str(data.get("harness") or ""),
            benchmark=str(data.get("benchmark") or ""),
            models=[str(m) for m in (data.get("models") or [])],
            user_prompt=str(data.get("user_prompt") or ""),
            total_tokens=int(data.get("total_tokens") or 0),
            llm_calls=int(data.get("llm_calls") or len(turns)),
            tool_call_count=int(data.get("tool_call_count") or 0),
            recorded_duration_ms=float(data.get("recorded_duration_ms") or 0.0),
            turns=turns,
        )


def replay_corpus_dict(sessions: list[ReplaySession], *, dataset: str, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "dataset": dataset,
        "session_count": len(sessions),
        "sessions": [s.to_dict() for s in sessions],
    }
    if extra:
        payload.update(extra)
    return payload
