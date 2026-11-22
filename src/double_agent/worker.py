import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from double_agent.agent import Agent
from double_agent.model import Message, ModelResponse
from double_agent.storage import Storage
from double_agent.types import Run, Step, StepKind

log = logging.getLogger(__name__)


class Worker:
    """Runs one agent's events, one run at a time, resuming any run a crash interrupted."""

    def __init__(self, agent: Agent, storage: Storage) -> None:
        self.agent = agent
        self.storage = storage
        self._wake = asyncio.Event()

    async def run_pending(self) -> int:
        """Finish any interrupted run, then run every pending event. Returns runs completed."""
        completed = 0
        if run := await self.storage.unfinished_run(self.agent.id):
            await self.execute(run)
            completed += 1
        while run := await self.storage.start_next_run(self.agent.id):
            await self.execute(run)
            completed += 1
        return completed

    def wake(self) -> None:
        """Check the inbox now instead of waiting for the next poll."""
        self._wake.set()

    async def run_forever(self, poll_interval: float = 1.0) -> None:
        while True:
            self._wake.clear()
            try:
                await self.run_pending()
            except Exception:
                # The run stays unfinished and is retried on the next pass.
                log.exception("Run failed for agent %s", self.agent.id)
            try:
                await asyncio.wait_for(self._wake.wait(), poll_interval)
            except TimeoutError:
                pass

    async def execute(self, run: Run) -> None:
        """Drive a run to completion.

        Every model call and tool call is saved as a step before the run moves on. On resume,
        saved steps are replayed from storage instead of being repeated, so a completed model
        call or tool call never happens twice.
        """
        event = await self.storage.get_event(run.event_id)
        assert event is not None
        saved = {s.index: s for s in await self.storage.load_steps(run.id)}
        index = 0

        async def step(kind: StepKind, do: Callable[[], Awaitable[dict[str, Any]]]) -> Any:
            nonlocal index
            if index in saved:
                data = saved[index].data
            else:
                data = await do()
                await self.storage.save_step(Step(run_id=run.id, index=index, kind=kind, data=data))
            index += 1
            return data

        tools = {t.name: t for t in self.agent.tools}
        specs = [t.spec() for t in self.agent.tools]
        event_text = json.dumps(
            {
                "source": event.source,
                "type": event.type,
                "subject": event.subject,
                "payload": event.payload,
            }
        )
        messages = [Message(role="user", content=[{"type": "text", "text": event_text}])]

        async def call_model() -> dict[str, Any]:
            return (await self.agent.model.complete(list(messages), specs)).model_dump()

        while True:
            response = ModelResponse.model_validate(await step(StepKind.MODEL_CALL, call_model))
            messages.append(Message(role="assistant", content=response.content))
            if response.stop_reason != "tool_use":
                break

            results: list[dict[str, Any]] = []
            for block in response.content:
                if block.get("type") != "tool_use":
                    continue

                async def call_tool(block: dict[str, Any] = block) -> dict[str, Any]:
                    tool = tools.get(block["name"])
                    if tool is None:
                        return {"output": f"Unknown tool: {block['name']}", "is_error": True}
                    try:
                        return {"output": await tool.fn(block["input"]), "is_error": False}
                    except Exception as exc:
                        return {"output": f"{type(exc).__name__}: {exc}", "is_error": True}

                result = await step(StepKind.TOOL_CALL, call_tool)
                results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": block["id"],
                        "content": result["output"],
                        "is_error": result["is_error"],
                    }
                )
            messages.append(Message(role="user", content=results))

        await self.storage.finish_run(run.id)
