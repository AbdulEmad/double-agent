"""Agents for test_serve.py, loaded by `double-agent serve serve_agents:agents`."""

import os
from pathlib import Path
from typing import Any

from double_agent.agent import Agent, Tool
from double_agent.model import FakeModel, ModelResponse

DONE = ModelResponse(content=[{"type": "text", "text": "done"}], stop_reason="end_turn")
DEPLOY = ModelResponse(
    content=[{"type": "tool_use", "id": "t1", "name": "deploy", "input": {"env": "prod"}}],
    stop_reason="tool_use",
)


async def deploy(input: dict[str, Any]) -> str:
    with Path(os.environ["DEPLOY_LOG"]).open("a") as f:
        f.write(input["env"] + "\n")
    return "deployed"


agents = [
    Agent(id="a1", model=FakeModel([DONE] * 10)),
    Agent(
        id="deployer",
        model=FakeModel([DEPLOY, DONE]),
        tools=[Tool("deploy", "Deploy.", {"type": "object"}, deploy, needs_approval=True)],
    ),
]
