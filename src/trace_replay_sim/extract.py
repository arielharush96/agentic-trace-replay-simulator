"""Turn Exgentic OTEL session rows into compact ReplaySession objects."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from trace_replay_sim.models import LLMTurn, ReplaySession, ToolCall
from trace_replay_sim.toolmap import map_tool, map_tool_with_replay


CHAT_NAMES = ("chat", "completion", "responses", "generate")


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value is None:
        return []
    return [value]


def _parse_json(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not text or text[0] not in "[{":
        return value
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return value


def _attr(span: dict[str, Any], *keys: str, default: Any = None) -> Any:
    attrs = span.get("attributes") or span.get("attrs") or {}
    if not isinstance(attrs, dict):
        return default
    for key in keys:
        if key in attrs and attrs[key] not in (None, ""):
            return attrs[key]
    return default


def _parse_time(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        number = float(value)
        if number > 1e16:
            number /= 1e9
        elif number > 1e14:
            number /= 1e6
        elif number > 1e11:
            number /= 1e3
        return datetime.fromtimestamp(number, tz=timezone.utc)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return None


def _duration_ms(span: dict[str, Any]) -> float:
    start = _parse_time(span.get("start_time") or span.get("startTime") or span.get("startTimeUnixNano"))
    end = _parse_time(span.get("end_time") or span.get("endTime") or span.get("endTimeUnixNano"))
    if start and end:
        return max(0.0, (end - start).total_seconds() * 1000.0)
    for key in ("duration_ms", "durationMs"):
        if key in span:
            try:
                return float(span[key])
            except (TypeError, ValueError):
                pass
    return 0.0


def _is_llm_span(span: dict[str, Any]) -> bool:
    name = str(span.get("name") or "").lower()
    op = str(_attr(span, "gen_ai.operation.name", "gen_ai.operation.name", default="") or "").lower()
    if op in {"chat", "completion", "generate_content", "text_completion"}:
        return True
    if any(token in name for token in CHAT_NAMES):
        return True
    if _attr(span, "gen_ai.usage.input_tokens", "gen_ai.usage.prompt_tokens") is not None:
        return True
    return False


def _message_text(message: dict[str, Any]) -> str:
    parts = message.get("parts") or message.get("content") or message.get("contents")
    if isinstance(parts, str):
        return parts
    chunks: list[str] = []
    for part in _as_list(parts):
        if isinstance(part, str):
            chunks.append(part)
            continue
        if not isinstance(part, dict):
            continue
        ptype = str(part.get("type") or part.get("kind") or "").lower()
        if ptype in {"tool_call", "function_call"}:
            continue
        for key in ("content", "text", "value"):
            if part.get(key):
                chunks.append(str(part[key]))
                break
    if chunks:
        return "\n".join(chunks)
    for key in ("content", "text"):
        value = message.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return ""


def _extract_tool_results(input_messages: list[Any]) -> dict[str, str]:
    """Return {tool_call_id: result_text} from a span's input message list.

    Handles both OpenAI format (role=tool) and Anthropic format
    (role=user with type=tool_result parts).
    """
    results: dict[str, str] = {}
    for msg in input_messages:
        if not isinstance(msg, dict):
            continue
        role = str(msg.get("role") or "").lower()
        # OpenAI format: {"role": "tool", "tool_call_id": "...", "content": "..."}
        if role == "tool":
            call_id = str(msg.get("tool_call_id") or msg.get("id") or "")
            content = msg.get("content") or ""
            if isinstance(content, list):
                content = "\n".join(
                    p.get("text", "") if isinstance(p, dict) else str(p)
                    for p in content
                )
            results[call_id] = str(content)
            continue
        # Anthropic format: {"role": "user", "content": [{"type": "tool_result", ...}]}
        # claude_code harness: {"role": "user", "parts": [{"type": "tool_call_response", "id": ..., "result": ...}]}
        if role in ("user", "tool_result"):
            parts = _as_list(msg.get("parts") or msg.get("content") or [])
            for part in parts:
                if not isinstance(part, dict):
                    continue
                ptype = str(part.get("type") or "").lower()
                if ptype in ("tool_result", "tool_call_response"):
                    call_id = str(
                        part.get("tool_use_id") or part.get("tool_call_id")
                        or part.get("id") or ""
                    )
                    inner = (
                        part.get("content") or part.get("output")
                        or part.get("result") or ""
                    )
                    if isinstance(inner, list):
                        inner = "\n".join(
                            p.get("text", "") if isinstance(p, dict) else str(p)
                            for p in inner
                        )
                    results[call_id] = str(inner)
    return results


def _tool_calls_from_message(message: dict[str, Any]) -> list[ToolCall]:
    found: list[ToolCall] = []
    raw_calls = message.get("tool_calls") or message.get("function_call")
    for item in _as_list(_parse_json(raw_calls)):
        if not isinstance(item, dict):
            continue
        fn = item.get("function") if isinstance(item.get("function"), dict) else item
        name = str(fn.get("name") or item.get("name") or "")
        arguments = _parse_json(fn.get("arguments") or item.get("arguments") or {})
        if not isinstance(arguments, dict):
            arguments = {"input": arguments}
        found.append(
            ToolCall(
                id=str(item.get("id") or fn.get("id") or ""),
                name=name,
                arguments=arguments,
                mapped_command=map_tool(name, arguments),
            )
        )

    for part in _as_list(message.get("parts")):
        if not isinstance(part, dict):
            continue
        ptype = str(part.get("type") or "").lower()
        if ptype not in {"tool_call", "function_call"}:
            continue
        name = str(part.get("name") or "")
        arguments = _parse_json(part.get("arguments") or {})
        if not isinstance(arguments, dict):
            arguments = {"input": arguments}
        found.append(
            ToolCall(
                id=str(part.get("id") or ""),
                name=name,
                arguments=arguments,
                mapped_command=map_tool(name, arguments),
            )
        )
    return found


def _first_user_prompt(messages: list[Any]) -> str:
    for message in messages:
        if not isinstance(message, dict):
            continue
        if str(message.get("role") or "").lower() != "user":
            continue
        text = _message_text(message).strip()
        if text:
            return text
    return ""


def _int_attr(span: dict[str, Any], *keys: str) -> int:
    value = _attr(span, *keys, default=0)
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def extract_session(row: dict[str, Any]) -> ReplaySession | None:
    spans = row.get("spans") or []
    if isinstance(spans, str):
        spans = _parse_json(spans)
    spans = [_as_dict(span) for span in _as_list(spans)]
    llm_spans = [span for span in spans if _is_llm_span(span)]
    llm_spans.sort(
        key=lambda span: _parse_time(span.get("start_time") or span.get("startTime"))
        or datetime.min.replace(tzinfo=timezone.utc)
    )

    turns: list[LLMTurn] = []
    user_prompt = ""
    tool_call_count = 0
    recorded_ms = 0.0

    for index, span in enumerate(llm_spans):
        inputs = _parse_json(_attr(span, "gen_ai.input.messages", "gen_ai.prompt", default=[]))
        outputs = _parse_json(_attr(span, "gen_ai.output.messages", "gen_ai.completion", default=[]))
        input_messages = _as_list(inputs)
        output_messages = _as_list(outputs)
        if not user_prompt:
            user_prompt = _first_user_prompt(input_messages)

        content_chunks: list[str] = []
        tool_calls: list[ToolCall] = []
        finish = ""
        for message in output_messages:
            if not isinstance(message, dict):
                continue
            finish = str(message.get("finish_reason") or message.get("finishReason") or finish)
            content_chunks.append(_message_text(message))
            tool_calls.extend(_tool_calls_from_message(message))

        duration_ms = _duration_ms(span)
        recorded_ms += duration_ms
        tool_call_count += len(tool_calls)
        turns.append(
            LLMTurn(
                index=index,
                span_id=str(span.get("span_id") or span.get("spanId") or ""),
                duration_ms=duration_ms,
                input_tokens=_int_attr(span, "gen_ai.usage.input_tokens", "gen_ai.usage.prompt_tokens"),
                output_tokens=_int_attr(span, "gen_ai.usage.output_tokens", "gen_ai.usage.completion_tokens"),
                model=str(_attr(span, "gen_ai.request.model", "gen_ai.response.model", default="") or ""),
                content="\n".join(chunk for chunk in content_chunks if chunk).strip(),
                finish_reason=finish,
                tool_calls=tool_calls,
            )
        )

    if not turns:
        return None

    # --- attach recorded tool results from next span's input messages ---
    session_id_str = str(row.get("session_id") or row.get("sessionId") or (turns[0].span_id if turns else ""))
    for i, span in enumerate(llm_spans[1:], start=1):
        if i - 1 >= len(turns):
            break
        next_inputs = _parse_json(_attr(span, "gen_ai.input.messages", "gen_ai.prompt", default=[]))
        tool_results = _extract_tool_results(_as_list(next_inputs))
        if not tool_results:
            continue
        for call_idx, tc in enumerate(turns[i - 1].tool_calls):
            result = tool_results.get(tc.id, "")
            tc.recorded_result = result
            # rebuild mapped_command now that we have session context + result
            tc.mapped_command = map_tool_with_replay(
                tc.name,
                tc.arguments,
                session_id=session_id_str,
                turn_idx=i - 1,
                call_idx=call_idx,
            )

    models = row.get("models") or []
    if isinstance(models, str):
        models = [models]

    return ReplaySession(
        session_id=str(row.get("session_id") or row.get("sessionId") or turns[0].span_id),
        harness=str(row.get("harness") or ""),
        benchmark=str(row.get("benchmark") or ""),
        models=[str(model) for model in _as_list(models)],
        user_prompt=user_prompt or "Continue the recorded task.",
        total_tokens=int(row.get("total_tokens") or 0),
        llm_calls=len(turns),
        tool_call_count=tool_call_count,
        recorded_duration_ms=recorded_ms,
        turns=turns,
    )


def extract_sessions(rows: list[dict[str, Any]]) -> list[ReplaySession]:
    sessions: list[ReplaySession] = []
    for row in rows:
        session = extract_session(_as_dict(row))
        if session is not None:
            sessions.append(session)
    return sessions
