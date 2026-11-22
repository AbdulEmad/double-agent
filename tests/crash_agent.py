"""Agent process used by test_crash_recovery.py.

Run as `python crash_agent.py <dir>`. It adds one event (deduplicated across restarts), then
runs the agent until its inbox is empty. The scripted model asks for two `act` tool calls and
then finishes. On the first attempt, the second tool call writes a marker file and hangs, so the
test can SIGKILL the process mid-run.
"""

import asyncio
import sys
from pathlib import Path
from typing import Any

from double_agent.agent import Agent, Tool
from double_agent.model import Message, ModelResponse, ToolSpec
from double_agent.sqlite_storage import SqliteStorage
from double_agent.types import Event
from double_agent.worker import Worker


def _append(path: Path, line: str) -> None:
    with path.open("a") as f:
        f.write(line + "\n")


class ScriptedModel:
    """Picks its reply from how many turns the conversation has, so it works after a restart."""

    def __init__(self, log: Path) -> None:
        self.log = log

    async def complete(self, messages: list[Message], tools: list[ToolSpec]) -> ModelResponse:
        turn = sum(1 for m in messages if m.role == "assistant")
        _append(self.log, str(turn))
        if turn < 2:
            block = {"type": "tool_use", "id": f"t{turn}", "name": "act", "input": {"n": turn + 1}}
            return ModelResponse(content=[block], stop_reason="tool_use")
        return ModelResponse(content=[{"type": "text", "text": "done"}], stop_reason="end_turn")


async def main(root: Path) -> None:
    actions, marker = root / "actions.log", root / "hang.marker"

    async def act(input: dict[str, Any]) -> str:
        if input["n"] == 2 and not marker.exists():
            marker.touch()
            await asyncio.Event().wait()  # Hang until killed.
        _append(actions, str(input["n"]))
        return "ok"

    agent = Agent(id="a1", tools=[Tool(name="act", description="", input_schema={}, fn=act)])
    storage = await SqliteStorage.open(root / "db.sqlite")
    await storage.add_event(Event(agent_id="a1", source="test", type="start", idempotency_key="e1"))
    await Worker(agent, storage, ScriptedModel(root / "model_calls.log")).run_pending()
    await storage.close()


if __name__ == "__main__":
    asyncio.run(main(Path(sys.argv[1])))
