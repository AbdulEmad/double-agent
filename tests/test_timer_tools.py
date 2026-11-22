import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest

from double_agent.agent import Agent, Schedule, Tool, ToolCall, current_tool_call
from double_agent.model import FakeModel, ModelResponse
from double_agent.scheduler import Scheduler, once, schedule_timer_id, sync_schedules
from double_agent.sqlite_storage import SqliteStorage
from double_agent.storage import InMemoryStorage, Storage
from double_agent.timer_tools import timer_tools
from double_agent.types import Event
from double_agent.worker import Worker

T0 = datetime(2026, 3, 2, 8, 0, tzinfo=UTC)
DONE = ModelResponse(content=[{"type": "text", "text": "done"}], stop_reason="end_turn")


def _tools(storage: Storage, on_change: Any = lambda: None) -> dict[str, Tool]:
    return {t.name: t for t in timer_tools(storage, on_change=on_change, clock=lambda: T0)}


async def _call(
    tool: Tool, input: dict[str, Any], agent_id: str = "a1", run_id: str = "r1", step: int = 1
) -> str:
    token = current_tool_call.set(ToolCall(agent_id, run_id, step))
    try:
        return await tool.fn(input)
    finally:
        current_tool_call.reset(token)


def _set_timer(**input: Any) -> ModelResponse:
    block = {"type": "tool_use", "id": "t1", "name": "set_timer", "input": input}
    return ModelResponse(content=[block], stop_reason="tool_use")


# The tools


async def test_set_timer_once_after_a_delay() -> None:
    storage, changes = InMemoryStorage(), list[str]()
    tools = _tools(storage, on_change=lambda: changes.append("changed"))

    result = await _call(tools["set_timer"], {"delay_minutes": 90, "note": "check PR #42"})

    (timer,) = await storage.list_timers("a1")
    assert (timer.id, timer.due_at, timer.note, timer.cron) == (
        "r1:1",
        T0 + timedelta(minutes=90),
        "check PR #42",
        None,
    )
    assert changes == ["changed"]
    assert result == "Timer r1:1 set for 2026-03-02T09:30:00+00:00."


async def test_set_timer_at_a_time_and_on_a_cron_schedule() -> None:
    storage = InMemoryStorage()
    tools = _tools(storage)
    await _call(tools["set_timer"], {"at": "2026-03-05T12:00:00+01:00", "note": "a"}, step=1)
    result = await _call(
        tools["set_timer"], {"cron": "0 9 * * 1-5", "tz": "Europe/London", "note": "b"}, step=2
    )

    cron, at = await storage.list_timers("a1")  # Earliest first.
    assert at.due_at == datetime(2026, 3, 5, 11, tzinfo=UTC)
    assert (cron.cron, cron.tz, cron.due_at) == (
        "0 9 * * 1-5",
        "Europe/London",
        datetime(2026, 3, 2, 9, tzinfo=UTC),
    )
    assert "repeating on '0 9 * * 1-5' (Europe/London)" in result


@pytest.mark.parametrize(
    "input",
    [
        {"note": "no time given"},
        {"delay_minutes": 5, "cron": "0 9 * * *", "note": "two times given"},
        {"at": "2026-03-05T12:00:00", "note": "no UTC offset"},
        {"cron": "every day", "note": "bad cron"},
    ],
)
async def test_set_timer_rejects_bad_input(input: dict[str, Any]) -> None:
    storage = InMemoryStorage()
    with pytest.raises(ValueError):
        await _call(_tools(storage)["set_timer"], input)
    assert await storage.list_timers("a1") == []


async def test_set_timer_repeated_by_a_resumed_run_creates_one_timer() -> None:
    # A crash after the timer was stored but before the step was saved re-runs the tool.
    storage = InMemoryStorage()
    set_timer = _tools(storage)["set_timer"]
    first = await _call(set_timer, {"delay_minutes": 5, "note": "x"})
    second = await _call(set_timer, {"delay_minutes": 5, "note": "x"})
    assert first == second
    assert len(await storage.list_timers("a1")) == 1


async def test_list_and_cancel_only_see_the_agents_own_timers() -> None:
    storage = InMemoryStorage()
    tools = _tools(storage)
    mine, theirs = once("a1", T0, note="mine"), once("a2", T0, note="theirs")
    await storage.add_timer(mine)
    await storage.add_timer(theirs)

    listed = json.loads(await _call(tools["list_timers"], {}))
    assert [t["note"] for t in listed] == ["mine"]
    with pytest.raises(ValueError):
        await _call(tools["cancel_timer"], {"timer_id": theirs.id})
    assert (
        await _call(tools["cancel_timer"], {"timer_id": mine.id}) == f"Timer {mine.id} cancelled."
    )
    assert await _call(tools["list_timers"], {}) == "No active timers."
    assert len(await storage.list_timers("a2")) == 1


# End to end


async def test_follow_up_set_by_the_agent_fires_after_a_restart(tmp_path: Path) -> None:
    path = tmp_path / "db.sqlite"
    storage = await SqliteStorage.open(path)
    model = FakeModel([_set_timer(delay_minutes=60, note="check PR #42"), DONE])
    worker = Worker(Agent(id="a1", model=model), storage, list(_tools(storage).values()))
    await storage.add_event(Event(agent_id="a1", source="webhook", type="pr", idempotency_key="e1"))

    assert await worker.run_pending() == 1
    assert model.requests[1][-1].content[0]["content"].startswith("Timer ")
    assert await storage.unfinished_run("a1") is None  # Nothing open while waiting.
    await storage.close()

    storage = await SqliteStorage.open(path)
    woken: list[str] = []
    scheduler = Scheduler(storage, on_event=woken.append, clock=lambda: T0 + timedelta(minutes=59))
    assert await scheduler.run_due() == 0
    scheduler.clock = lambda: T0 + timedelta(minutes=61)
    assert await scheduler.run_due() == 1
    assert woken == ["a1"]

    follow_up = FakeModel([DONE])
    assert await Worker(Agent(id="a1", model=follow_up), storage).run_pending() == 1
    event = json.loads(follow_up.requests[0][0].content[0]["text"])
    assert (event["source"], event["payload"]["note"]) == ("timer", "check PR #42")
    await storage.close()


async def test_idle_worker_does_not_touch_storage() -> None:
    calls: list[str] = []
    inner = InMemoryStorage()

    class Counting:
        def __getattr__(self, name: str) -> Any:
            calls.append(name)
            return getattr(inner, name)

    worker = Worker(Agent(id="a1", model=FakeModel([])), cast(Storage, Counting()))
    task = asyncio.create_task(worker.run_forever())
    await asyncio.sleep(0.05)
    after_startup = len(calls)
    assert after_startup > 0  # One inbox check at startup.
    await asyncio.sleep(0.5)
    task.cancel()
    assert len(calls) == after_startup


# Schedules in agent config


async def test_sync_schedules_adds_keeps_and_cancels(storage: Storage) -> None:
    daily, weekly = Schedule("0 9 * * *", note="daily"), Schedule("0 9 * * 1", note="weekly")
    agent = Agent(id="a1", model=FakeModel([]), schedules=[daily, weekly])
    await storage.add_timer(once("a1", T0 + timedelta(days=9), note="set by the agent"))

    await sync_schedules(storage, [agent], now=T0)
    assert sorted(t.note for t in await storage.list_timers("a1")) == [
        "daily",
        "set by the agent",
        "weekly",
    ]

    # A restart keeps an unchanged schedule's next fire time.
    first_due = {t.id: t.due_at for t in await storage.list_timers("a1")}
    await sync_schedules(storage, [agent], now=T0 + timedelta(hours=3))
    assert {t.id: t.due_at for t in await storage.list_timers("a1")} == first_due

    # Removing one schedule and changing the other.
    agent.schedules = [Schedule("0 10 * * *", note="daily")]
    await sync_schedules(storage, [agent], now=T0)
    timers = {t.id: t for t in await storage.list_timers("a1")}
    assert schedule_timer_id("a1", daily) not in timers
    assert schedule_timer_id("a1", weekly) not in timers
    assert timers[schedule_timer_id("a1", agent.schedules[0])].due_at == T0 + timedelta(hours=2)
    assert "set by the agent" in [t.note for t in timers.values()]
