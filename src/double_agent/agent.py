from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any

from double_agent.model import Model, ToolSpec
from double_agent.outbox import Channel


@dataclass(frozen=True)
class ToolCall:
    """Which agent, run and step a tool is being called from."""

    agent_id: str
    run_id: str
    step: int
    # The subject of the event the run is handling, such as "task:<id>".
    subject: str | None = None
    # Where that event came from, such as "user", "webhook" or "timer".
    source: str | None = None


@dataclass
class Tool:
    name: str
    description: str
    input_schema: dict[str, Any]
    fn: Callable[[dict[str, Any]], Awaitable[str]]
    # When True, each call waits for a human to approve it before it runs. A function decides
    # per call, from the call's input and where it is being made.
    needs_approval: bool | Callable[[dict[str, Any], ToolCall], bool] = False

    def spec(self) -> ToolSpec:
        return ToolSpec(
            name=self.name, description=self.description, input_schema=self.input_schema
        )


# Set by the worker while a tool runs, for built-in tools that need to know where they are.
current_tool_call: ContextVar[ToolCall] = ContextVar("current_tool_call")


@dataclass(frozen=True)
class Schedule:
    """A recurring wake-up defined in the agent's configuration."""

    cron: str
    tz: str = "UTC"
    note: str = ""


@dataclass
class Agent:
    id: str
    model: Model
    system: str = ""
    tools: list[Tool] = field(default_factory=lambda: [])
    schedules: list[Schedule] = field(default_factory=lambda: [])
    # Where the agent can send messages, by name. The agent picks a name; it never sees a URL.
    channels: dict[str, Channel] = field(default_factory=lambda: {})
    # Which of `channels` is told when a tool call needs approval. None sends no notification.
    approval_channel: str | None = None
    # Event sources whose runs may write memory without approval. Writes during runs started by
    # anything else, such as webhooks carrying outside content, wait for a human.
    trusted_sources: set[str] = field(default_factory=lambda: {"user"})
