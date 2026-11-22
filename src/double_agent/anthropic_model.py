from typing import Any, Literal, cast

from anthropic import AsyncAnthropic, omit

from double_agent.model import Message, ModelResponse, ToolSpec, Usage

DEFAULT_MODEL = "claude-opus-5-5"

Effort = Literal["low", "medium", "high", "xhigh", "max"]

# Blocks that must not be sent back when they appear before a fallback marker.
_DROPPED_BEFORE_FALLBACK = {"thinking", "redacted_thinking", "tool_use"}


def _echoable(content: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return content safe to send back on the next turn.

    After a refusal fallback, thinking and tool-use blocks from before the last `fallback` block
    belong to the declined attempt and must be left out. Everything else is kept unchanged,
    since thinking blocks have to be sent back exactly as they came.
    """
    last = max((i for i, b in enumerate(content) if b.get("type") == "fallback"), default=-1)
    return [
        b
        for i, b in enumerate(content)
        if i > last or b.get("type") not in _DROPPED_BEFORE_FALLBACK
    ]


class AnthropicModel:
    """`Model` implementation on the Anthropic Messages API."""

    def __init__(
        self,
        model: str = DEFAULT_MODEL,
        effort: Effort = "high",
        max_tokens: int = 16000,
        client: AsyncAnthropic | None = None,
    ) -> None:
        self.model = model
        self.effort: Effort = effort
        self.max_tokens = max_tokens
        self._client = client or AsyncAnthropic()

    async def complete(
        self, system: str, messages: list[Message], tools: list[ToolSpec]
    ) -> ModelResponse:
        response = await self._client.beta.messages.create(
            model=self.model,
            max_tokens=self.max_tokens,
            system=system or omit,
            messages=cast(Any, [m.model_dump() for m in messages]),
            tools=cast(Any, [t.model_dump() for t in tools]) if tools else omit,
            thinking={"type": "adaptive"},
            output_config={"effort": self.effort},
            # Caches the stable prefix (tools, system, earlier turns) across calls in a run.
            cache_control={"type": "ephemeral"},
            # On a safety refusal, retry server-side on Anthropic's recommended fallback model.
            betas=["server-side-fallback-2026-07-01"],
            fallbacks="default",
        )
        usage = response.usage
        content = [b.model_dump(mode="json", exclude_none=True) for b in response.content]
        return ModelResponse(
            content=_echoable(content),
            stop_reason=cast(Any, response.stop_reason),
            usage=Usage(
                input_tokens=usage.input_tokens,
                output_tokens=usage.output_tokens,
                cache_read_input_tokens=usage.cache_read_input_tokens or 0,
                cache_creation_input_tokens=usage.cache_creation_input_tokens or 0,
            ),
        )
