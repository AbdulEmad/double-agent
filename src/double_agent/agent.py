from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from double_agent.model import Model, ToolSpec


@dataclass
class Tool:
    name: str
    description: str
    input_schema: dict[str, Any]
    fn: Callable[[dict[str, Any]], Awaitable[str]]

    def spec(self) -> ToolSpec:
        return ToolSpec(
            name=self.name, description=self.description, input_schema=self.input_schema
        )


@dataclass
class Agent:
    id: str
    model: Model
    tools: list[Tool] = field(default_factory=lambda: [])
