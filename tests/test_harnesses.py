import json
from unittest.mock import patch

from trace_replay_sim.driver import run_one
from trace_replay_sim.harnesses import get_harness


def test_openclaw_uses_responses_api_and_previous_response_id():
    adapter = get_harness("openclaw")
    url, body = adapter.build_request(
        base_url="http://openclaw:18790",
        model="openclaw/perf_agent",
        prompt="hello",
        stream=True,
        previous_response_id="resp-1",
    )
    assert url == "http://openclaw:18790/v1/responses"
    assert body["input"] == "hello"
    assert body["previous_response_id"] == "resp-1"


def test_generic_openai_compatible_alias_is_available():
    adapter = get_harness("generic")
    assert adapter.name == "openai-compatible"
    assert adapter.default_model == "agent"
    url, body = adapter.build_request(
        base_url="http://agent:9000",
        model="agent",
        prompt="hello",
        stream=False,
        previous_response_id="ignored-by-chat",
    )
    assert url == "http://agent:9000/v1/chat/completions"
    assert body["messages"] == [{"role": "user", "content": "hello"}]
    assert "previous_response_id" not in body


class _Response:
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def read(self):
        return b'{"id":"chatcmpl-1","choices":[{"message":{"content":"ok"}}]}'


def test_driver_sends_chat_completions_request():
    session = {"session_id": "s1", "user_prompt": "hello"}
    with patch("trace_replay_sim.driver.urllib.request.urlopen", return_value=_Response()) as open_url:
        result = run_one(
            session,
            layer="shell",
            harness="openai-compatible",
            url="http://agent:9000",
            model=None,
            token="test-key",
            timeout=2,
            stream=False,
            export_spans=False,
        )
    request = open_url.call_args.args[0]
    assert request.full_url == "http://agent:9000/v1/chat/completions"
    assert request.get_header("Authorization") == "Bearer test-key"
    assert json.loads(request.data)["model"] == "agent"
    assert result.ok and result.harness == "openai-compatible"
