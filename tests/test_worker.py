from typing import Any

from double_agent.agent import Agent, Tool
from double_agent.model import FakeModel, ModelResponse
from double_agent.storage import InMemoryStorage
from double_agent.types import Event, Step, StepKind
from double_agent.worker import Worker


def _event(key: str) -> Event:
    return Event(agent_id="a1", source="webhook", type="ping", idempotency_key=key)


def _tool_use(tool_id: str, name: str = "act", **input: Any) -> ModelResponse:
    block = {"type": "tool_use", "id": tool_id, "name": name, "input": input}
    return ModelResponse(content=[block], stop_reason="tool_use")


DONE = ModelResponse(content=[{"type": "text", "text": "done"}], stop_reason="end_turn")


def _recording_tool(calls: list[dict[str, Any]]) -> Tool:
    async def act(input: dict[str, Any]) -> str:
        calls.append(input)
        return f"did {input['n']}"

    return Tool(name="act", description="Do something.", input_schema={"type": "object"}, fn=act)


async def test_run_without_tools_finishes() -> None:
    storage = InMemoryStorage()
    model = FakeModel([DONE])
    await storage.add_event(_event("e1"))

    assert await Worker(Agent(id="a1"), storage, model).run_pending() == 1
    assert await storage.unfinished_run("a1") is None
    assert len(model.requests) == 1
    assert '"type": "ping"' in model.requests[0][0].content[0]["text"]


async def test_tool_result_is_sent_back_to_model() -> None:
    storage = InMemoryStorage()
    calls: list[dict[str, Any]] = []
    model = FakeModel([_tool_use("t1", n=1), DONE])
    await storage.add_event(_event("e1"))

    await Worker(Agent(id="a1", tools=[_recording_tool(calls)]), storage, model).run_pending()

    assert calls == [{"n": 1}]
    result = model.requests[1][-1].content[0]
    assert result == {
        "type": "tool_result",
        "tool_use_id": "t1",
        "content": "did 1",
        "is_error": False,
    }


async def test_failing_and_unknown_tools_return_errors() -> None:
    async def broken(input: dict[str, Any]) -> str:
        raise ValueError("boom")

    tool = Tool(name="act", description="", input_schema={}, fn=broken)
    storage = InMemoryStorage()
    model = FakeModel([_tool_use("t1"), _tool_use("t2", name="nope"), DONE])
    await storage.add_event(_event("e1"))

    await Worker(Agent(id="a1", tools=[tool]), storage, model).run_pending()

    errors = [model.requests[i][-1].content[0] for i in (1, 2)]
    assert [(e["content"], e["is_error"]) for e in errors] == [
        ("ValueError: boom", True),
        ("Unknown tool: nope", True),
    ]


async def test_interrupted_run_resumes_without_repeating_steps() -> None:
    storage = InMemoryStorage()
    await storage.add_event(_event("e1"))
    run = await storage.start_next_run("a1")
    assert run is not None
    # Simulate a crash after the first model call and first tool call were saved.
    first = _tool_use("t1", n=1)
    await storage.save_step(
        Step(run_id=run.id, index=0, kind=StepKind.MODEL_CALL, data=first.model_dump())
    )
    await storage.save_step(
        Step(
            run_id=run.id,
            index=1,
            kind=StepKind.TOOL_CALL,
            data={"output": "did 1", "is_error": False},
        )
    )

    calls: list[dict[str, Any]] = []
    model = FakeModel([_tool_use("t2", n=2), DONE])
    worker = Worker(Agent(id="a1", tools=[_recording_tool(calls)]), storage, model)
    assert await worker.run_pending() == 1

    assert calls == [{"n": 2}]
    assert len(model.requests) == 2
    # The resumed request still carries the replayed first turn.
    assert model.requests[0][1].content == first.content
    assert len(await storage.load_steps(run.id)) == 5
    assert await storage.unfinished_run("a1") is None


async def test_event_arriving_mid_run_waits_for_next_run() -> None:
    storage = InMemoryStorage()
    active = 0
    max_active = 0
    order: list[int] = []

    async def act(input: dict[str, Any]) -> str:
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        order.append(input["n"])
        if input["n"] == 1:
            await storage.add_event(_event("e2"))
        active -= 1
        return "ok"

    tool = Tool(name="act", description="", input_schema={}, fn=act)
    model = FakeModel([_tool_use("t1", n=1), DONE, _tool_use("t2", n=2), DONE])
    await storage.add_event(_event("e1"))

    assert await Worker(Agent(id="a1", tools=[tool]), storage, model).run_pending() == 2
    assert order == [1, 2]
    assert max_active == 1
