from typing import Any, Literal, Protocol

from pydantic import BaseModel, Field


class Message(BaseModel):
    role: Literal["user", "assistant"]
    content: list[dict[str, Any]]


class ToolSpec(BaseModel):
    name: str
    description: str
    input_schema: dict[str, Any]


class Usage(BaseModel):
    input_tokens: int = 0
    output_tokens: int = 0


class ModelResponse(BaseModel):
    content: list[dict[str, Any]]
    stop_reason: Literal["end_turn", "tool_use", "max_tokens", "refusal"]
    usage: Usage = Field(default_factory=Usage)


class Model(Protocol):
    async def complete(self, messages: list[Message], tools: list[ToolSpec]) -> ModelResponse: ...


class FakeModel:
    """Returns scripted responses in order and records each request."""

    def __init__(self, responses: list[ModelResponse]) -> None:
        self._responses = list(responses)
        self.requests: list[list[Message]] = []

    async def complete(self, messages: list[Message], tools: list[ToolSpec]) -> ModelResponse:
        self.requests.append(messages)
        return self._responses.pop(0)
