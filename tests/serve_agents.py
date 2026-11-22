"""Agents for test_serve.py, loaded by `double-agent serve serve_agents:agents`."""

from double_agent.agent import Agent
from double_agent.model import FakeModel, ModelResponse

DONE = ModelResponse(content=[{"type": "text", "text": "done"}], stop_reason="end_turn")

agents = [Agent(id="a1", model=FakeModel([DONE] * 10))]
