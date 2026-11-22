import json
import os
from typing import Any

import httpx2
import pytest
from anthropic import AsyncAnthropic, DefaultAsyncHttpxClient

from double_agent.anthropic_model import AnthropicModel
from double_agent.model import Message, ToolSpec

TOOL = ToolSpec(name="act", description="Do something.", input_schema={"type": "object"})
HELLO = [Message(role="user", content=[{"type": "text", "text": "hello"}])]


def _reply(content: list[dict[str, Any]], stop_reason: str = "tool_use") -> dict[str, Any]:
    return {
        "id": "msg_1",
        "type": "message",
        "role": "assistant",
        "model": "claude-opus-5-5",
        "content": content,
        "stop_reason": stop_reason,
        "stop_sequence": None,
        "usage": {
            "input_tokens": 10,
            "output_tokens": 5,
            "cache_read_input_tokens": 3,
            "cache_creation_input_tokens": None,
        },
    }


def _stubbed(reply: dict[str, Any]) -> tuple[AnthropicModel, list[httpx2.Request]]:
    requests: list[httpx2.Request] = []

    def handle(request: httpx2.Request) -> httpx2.Response:
        requests.append(request)
        return httpx2.Response(200, json=reply)

    client = AsyncAnthropic(
        api_key="test",
        max_retries=0,
        http_client=DefaultAsyncHttpxClient(transport=httpx2.MockTransport(handle)),
    )
    return AnthropicModel(client=client), requests


async def test_request_shape() -> None:
    model, requests = _stubbed(_reply([{"type": "text", "text": "hi"}], "end_turn"))
    await model.complete("Be brief.", HELLO, [TOOL])

    (request,) = requests
    body = json.loads(request.content)
    assert body["model"] == "claude-opus-5-5"
    assert body["system"] == "Be brief."
    assert body["messages"] == [{"role": "user", "content": [{"type": "text", "text": "hello"}]}]
    assert body["tools"] == [TOOL.model_dump()]
    assert body["thinking"] == {"type": "adaptive"}
    assert body["output_config"] == {"effort": "high"}
    assert body["cache_control"] == {"type": "ephemeral"}
    assert body["fallbacks"] == "default"
    assert "server-side-fallback-2026-07-01" in request.headers["anthropic-beta"]


async def test_empty_system_and_tools_are_left_out() -> None:
    model, requests = _stubbed(_reply([{"type": "text", "text": "hi"}], "end_turn"))
    await model.complete("", HELLO, [])
    body = json.loads(requests[0].content)
    assert "system" not in body and "tools" not in body


async def test_response_is_converted() -> None:
    thinking = {"type": "thinking", "thinking": "", "signature": "sig"}
    tool_use = {"type": "tool_use", "id": "t1", "name": "act", "input": {"n": 1}}
    model, _ = _stubbed(_reply([thinking, {"type": "text", "text": "ok"}, tool_use]))

    response = await model.complete("", HELLO, [TOOL])

    assert response.stop_reason == "tool_use"
    types = [b["type"] for b in response.content]
    assert types == ["thinking", "text", "tool_use"]
    assert response.content[0]["signature"] == "sig"
    assert response.content[2]["input"] == {"n": 1}
    assert response.usage.model_dump() == {
        "input_tokens": 10,
        "output_tokens": 5,
        "cache_read_input_tokens": 3,
        "cache_creation_input_tokens": 0,
    }


async def test_declined_attempt_before_fallback_is_not_echoed() -> None:
    declined: list[dict[str, Any]] = [
        {"type": "thinking", "thinking": "", "signature": "a"},
        {"type": "text", "text": "partial "},
        {"type": "tool_use", "id": "t0", "name": "act", "input": {}},
    ]
    fallback = {
        "type": "fallback",
        "from": {"type": "model", "model": "claude-opus-5-5"},
        "to": {"type": "model", "model": "claude-opus-4-8"},
    }
    after: list[dict[str, Any]] = [
        {"type": "thinking", "thinking": "", "signature": "b"},
        {"type": "tool_use", "id": "t1", "name": "act", "input": {}},
    ]
    model, _ = _stubbed(_reply([*declined, fallback, *after]))

    response = await model.complete("", HELLO, [TOOL])

    kept = [
        (b["type"], b.get("signature") or b.get("id") or b.get("text")) for b in response.content
    ]
    assert kept == [("text", "partial "), ("fallback", None), ("thinking", "b"), ("tool_use", "t1")]


@pytest.mark.skipif(
    os.environ.get("RUN_LIVE_TESTS") != "1", reason="set RUN_LIVE_TESTS=1 to call the real API"
)
async def test_live_round_trip() -> None:
    model = AnthropicModel(effort="low", max_tokens=1024)
    ask = [Message(role="user", content=[{"type": "text", "text": "Call act with n set to 7."}])]
    first = await model.complete("Use the act tool when asked.", ask, [TOOL])
    assert first.stop_reason == "tool_use"
    tool_use = next(b for b in first.content if b["type"] == "tool_use")
    assert tool_use["input"] == {"n": 7}

    result = {"type": "tool_result", "tool_use_id": tool_use["id"], "content": "done"}
    follow_up = [
        *ask,
        Message(role="assistant", content=first.content),
        Message(role="user", content=[result]),
    ]
    second = await model.complete("Use the act tool when asked.", follow_up, [TOOL])
    assert second.stop_reason == "end_turn"
