import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from double_agent.agent import Agent, Tool, ToolCall, current_tool_call
from double_agent.model import Message, ModelResponse
from double_agent.storage import Storage
from double_agent.types import Approval, ApprovalStatus, OutboxItem, Run, Step, StepKind

log = logging.getLogger(__name__)


class _Paused(Exception):
    """Raised inside a run when a tool call is waiting for approval."""


class Worker:
    """Runs one agent's events, one run at a time, resuming any run a crash interrupted.

    `builtins` are tools every agent gets on top of its own, such as the timer tools.
    `on_queue` is called after the worker queues a message, such as an approval request.
    """

    def __init__(
        self,
        agent: Agent,
        storage: Storage,
        builtins: list[Tool] | None = None,
        on_queue: Callable[[], None] = lambda: None,
    ) -> None:
        self.agent = agent
        self.storage = storage
        self.tools = [*agent.tools, *(builtins or [])]
        self.on_queue = on_queue
        self._wake = asyncio.Event()

    async def run_pending(self) -> int:
        """Work through everything ready to run. Returns how many runs it worked on.

        In order: a run a crash interrupted, paused runs whose approval has been answered, and
        new events. A run that pauses for approval counts, and is picked up again once answered.
        """
        handled = 0
        while run := (
            await self.storage.unfinished_run(self.agent.id)
            or await self.storage.resume_answered_run(self.agent.id)
            or await self.storage.start_next_run(self.agent.id)
        ):
            await self.execute(run)
            handled += 1
        return handled

    def wake(self) -> None:
        """Check the inbox now. The API and the scheduler call this when they add an event."""
        self._wake.set()

    async def run_forever(self, min_retry: float = 1.0, max_retry: float = 300.0) -> None:
        """Run events as they arrive. While idle, wait for a wake-up without polling.

        A failed run stays unfinished and is retried after a delay that doubles from
        `min_retry` up to `max_retry` seconds, and resets once a pass succeeds.
        """
        retry: float | None = None
        while True:
            self._wake.clear()
            try:
                await self.run_pending()
                retry = None
            except Exception:
                retry = min_retry if retry is None else min(retry * 2, max_retry)
                log.exception("Run failed for agent %s; retrying in %ss", self.agent.id, retry)
            try:
                await asyncio.wait_for(self._wake.wait(), retry)
            except TimeoutError:
                pass

    async def execute(self, run: Run) -> None:
        """Drive a run until it finishes or pauses for approval.

        Every model call and tool call is saved as a step before the run moves on. On resume,
        saved steps are replayed from storage instead of being repeated, so a completed model
        call or tool call never happens twice.
        """
        try:
            await self._execute(run)
        except _Paused:
            await self.storage.pause_run(run.id)
            return
        await self.storage.finish_run(run.id)

    async def _request_approval(self, run: Run, step: int, block: dict[str, Any]) -> Approval:
        """Get the approval for a tool call, creating and announcing the request if it is new."""
        approval_id = f"{run.id}:{step}"
        if approval := await self.storage.get_approval(approval_id):
            return approval
        approval = Approval(
            id=approval_id,
            agent_id=self.agent.id,
            run_id=run.id,
            tool_name=block["name"],
            input=block["input"],
        )
        await self.storage.add_approval(approval)
        if channel := self.agent.approval_channel:
            body = (
                f"Agent {self.agent.id} wants to run {approval.tool_name} with input "
                f"{json.dumps(approval.input)}. Answer with: double-agent approvals approve "
                f"{approval_id}, or: double-agent approvals deny {approval_id}"
            )
            item = OutboxItem(
                id=f"approval:{approval_id}", agent_id=self.agent.id, channel=channel, body=body
            )
            await self.storage.add_outbox_item(item)
            self.on_queue()
        return approval

    async def _execute(self, run: Run) -> None:
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

        tools = {t.name: t for t in self.tools}
        specs = [t.spec() for t in self.tools]
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
            return (
                await self.agent.model.complete(self.agent.system, list(messages), specs)
            ).model_dump()

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
                    if tool.needs_approval:
                        approval = await self._request_approval(run, index, block)
                        if approval.status == ApprovalStatus.PENDING:
                            raise _Paused
                        if approval.status == ApprovalStatus.DENIED:
                            reason = f": {approval.reason}" if approval.reason else "."
                            return {"output": f"Denied by the user{reason}", "is_error": True}
                    token = current_tool_call.set(ToolCall(self.agent.id, run.id, index))
                    try:
                        return {"output": await tool.fn(block["input"]), "is_error": False}
                    except Exception as exc:
                        return {"output": f"{type(exc).__name__}: {exc}", "is_error": True}
                    finally:
                        current_tool_call.reset(token)

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
