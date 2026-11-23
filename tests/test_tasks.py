import sqlite3
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest

from double_agent.agent import Agent, Tool, ToolCall, current_tool_call
from double_agent.api import create_app
from double_agent.model import Message, ModelResponse, ToolSpec
from double_agent.scheduler import Scheduler, once
from double_agent.sqlite_storage import SqliteStorage
from double_agent.storage import Storage
from double_agent.tasks import LOG_ENTRIES_SHOWN, render_task, task_tools
from double_agent.timer_tools import timer_tools
from double_agent.types import (
    Event,
    ItemStatus,
    ItemUpdate,
    Task,
    TaskItem,
    TaskLogEntry,
    TaskStatus,
    TaskUpdate,
    TaskView,
)
from double_agent.worker import Worker

T0 = datetime(2026, 3, 2, 8, 0, tzinfo=UTC)
DONE = ModelResponse(content=[{"type": "text", "text": "done"}], stop_reason="end_turn")
ITEMS = [
    TaskItem(key="a", description="Thing a", party="x"),
    TaskItem(key="b", description="Thing b", party="y"),
    TaskItem(key="c", description="Thing c", party="z"),
]


def _task(task_id: str = "collect") -> Task:
    return Task(id=task_id, agent_id="a1", title="Collect a, b and c")


def _calls(*calls: tuple[str, dict[str, Any]]) -> ModelResponse:
    blocks = [
        {"type": "tool_use", "id": f"t{n}", "name": name, "input": input}
        for n, (name, input) in enumerate(calls)
    ]
    return ModelResponse(content=blocks, stop_reason="tool_use")


async def _tool(
    tool: Tool, input: dict[str, Any], step: int = 1, subject: str | None = None
) -> str:
    token = current_tool_call.set(ToolCall("a1", "r1", step, subject))
    try:
        return await tool.fn(input)
    finally:
        current_tool_call.reset(token)


# Storage


async def test_create_get_and_list(storage: Storage) -> None:
    view = await storage.create_task(_task(), ITEMS, op_id="op1")
    assert [i.key for i in view.items] == ["a", "b", "c"]
    assert await storage.get_task("a1", "collect") == view
    assert await storage.get_task("a2", "collect") is None  # Tasks belong to one agent.
    assert [t.id for t in await storage.list_tasks("a1", TaskStatus.OPEN)] == ["collect"]
    assert await storage.list_tasks("a1", TaskStatus.DONE) == []


async def test_create_is_applied_once_per_op(storage: Storage) -> None:
    first = await storage.create_task(_task(), ITEMS, op_id="op1")
    again = await storage.create_task(_task("other-id"), [], op_id="op1")
    assert again == first
    with pytest.raises(ValueError):
        await storage.create_task(_task(), [], op_id="op2")  # Same id, different call.
    assert len(await storage.list_tasks(None, None)) == 1


async def test_update_applies_everything_together(storage: Storage) -> None:
    await storage.create_task(_task(), ITEMS, op_id="op1")
    update = TaskUpdate(
        items=[ItemUpdate(key="b", status=ItemStatus.DONE, note="Arrived as INV-2231")],
        new_items=[TaskItem(key="d", description="Thing d")],
        notes="x is slow to reply",
        log="y sent b",
    )
    view = await storage.update_task("a1", "collect", update, op_id="op2")

    b = next(i for i in view.items if i.key == "b")
    assert (b.status, b.note) == (ItemStatus.DONE, "Arrived as INV-2231")
    assert [i.key for i in view.items] == ["a", "b", "c", "d"]
    assert view.task.notes == "x is slow to reply"
    assert [e.text for e in view.log] == ["y sent b"]
    assert await storage.get_task("a1", "collect") == view


async def test_update_is_applied_once_per_op(storage: Storage) -> None:
    await storage.create_task(_task(), ITEMS, op_id="op1")
    update = TaskUpdate(log="followed up", new_items=[TaskItem(key="d", description="d")])
    await storage.update_task("a1", "collect", update, op_id="op2")
    view = await storage.update_task("a1", "collect", update, op_id="op2")
    assert [e.text for e in view.log] == ["followed up"]
    assert len(view.items) == 4


async def test_invalid_update_changes_nothing(storage: Storage) -> None:
    await storage.create_task(_task(), ITEMS, op_id="op1")
    for bad in (
        TaskUpdate(items=[ItemUpdate(key="a", status=ItemStatus.DONE), ItemUpdate(key="zz")]),
        TaskUpdate(log="x", new_items=[TaskItem(key="a", description="duplicate")]),
    ):
        with pytest.raises(ValueError):
            await storage.update_task("a1", "collect", bad, op_id="bad")
    with pytest.raises(ValueError):
        await storage.update_task("a1", "missing", TaskUpdate(log="x"), op_id="op3")

    view = await storage.get_task("a1", "collect")
    assert view is not None
    assert all(i.status == ItemStatus.TODO for i in view.items) and view.log == []


async def test_closing_a_task_cancels_its_timers(storage: Storage) -> None:
    await storage.create_task(_task(), ITEMS, op_id="op1")
    follow_up = once("a1", T0, note="chase x").model_copy(update={"subject": "task:collect"})
    unrelated = once("a1", T0, note="something else")
    await storage.add_timer(follow_up)
    await storage.add_timer(unrelated)

    await storage.update_task("a1", "collect", TaskUpdate(status=TaskStatus.DONE), op_id="op2")
    assert [t.note for t in await storage.list_timers("a1")] == ["something else"]


async def test_old_database_gains_the_timer_subject_column(tmp_path: Path) -> None:
    path = tmp_path / "old.sqlite"
    with closing(sqlite3.connect(path)) as db:
        db.execute(
            "CREATE TABLE timers (id TEXT PRIMARY KEY, agent_id TEXT NOT NULL,"
            " due_at TEXT NOT NULL, cron TEXT, tz TEXT NOT NULL, note TEXT NOT NULL,"
            " status TEXT NOT NULL)"
        )
    storage = await SqliteStorage.open(path)
    timer = once("a1", T0).model_copy(update={"subject": "task:collect"})
    await storage.add_timer(timer)
    assert [t.subject for t in await storage.list_timers("a1")] == ["task:collect"]
    await storage.close()


# Rendering


def test_render_task() -> None:
    view = TaskView(
        task=_task().model_copy(update={"goal": "Have all three", "notes": "x is slow"}),
        items=[ITEMS[0].model_copy(update={"note": "a | b"}), ITEMS[1]],
        log=[TaskLogEntry(at=T0, text="asked x and y")],
    )
    text = render_task(view)
    assert text.startswith("# Task collect: Collect a, b and c\n\nStatus: open\n\nHave all three")
    assert "| a | Thing a | x | todo | a / b |" in text  # Pipes can't break the table.
    assert "## Notes\n\nx is slow" in text
    assert "- 2026-03-02 08:00 UTC: asked x and y" in text


def test_render_shows_only_recent_log_entries() -> None:
    log = [TaskLogEntry(at=T0, text=f"entry {n}") for n in range(LOG_ENTRIES_SHOWN + 5)]
    text = render_task(TaskView(task=_task(), items=[], log=log))
    assert "(5 earlier entries not shown)" in text
    assert "entry 4\n" not in text and f"entry {LOG_ENTRIES_SHOWN + 4}" in text


# Tools


async def test_task_tools_validate_input(storage: Storage) -> None:
    tools = {t.name: t for t in task_tools(storage)}
    created = await _tool(
        tools["create_task"],
        {"title": "Collect", "items": [{"key": "a", "description": "Thing a", "party": "x"}]},
    )
    task_id = created.split(":")[0].removeprefix("# Task ")
    assert task_id.startswith("task-")

    for bad in (
        {"title": "x", "task_id": "Has Spaces"},
        {"title": "x", "items": [{"key": "A!", "description": "bad key"}]},
        {
            "title": "x",
            "items": [{"key": "a", "description": "1"}, {"key": "a", "description": "2"}],
        },
        {"title": "x", "goal": "use sk-ant-api03-abcdefghijklmnop to log in"},
    ):
        with pytest.raises(ValueError):
            await _tool(tools["create_task"], bad, step=2)
    with pytest.raises(ValueError):
        await _tool(tools["update_task"], {"task_id": task_id, "log": "token ghp_" + "a" * 30})
    assert len(await storage.list_tasks("a1", None)) == 1


async def test_set_timer_in_a_task_run_is_attached_to_the_task(storage: Storage) -> None:
    set_timer = timer_tools(storage)[0]
    await _tool(set_timer, {"delay_minutes": 5, "note": "attached"}, step=1, subject="task:t1")
    await _tool(set_timer, {"delay_minutes": 5, "note": "explicit", "task_id": "t2"}, step=2)
    await _tool(set_timer, {"delay_minutes": 5, "note": "not a task"}, step=3, subject="pr:42")
    subjects = {t.note: t.subject for t in await storage.list_timers("a1")}
    assert subjects == {"attached": "task:t1", "explicit": "task:t2", "not a task": None}


# End to end: collect a, b and c from x, y and z, following up until done.


class ScriptedModel:
    """Replies in turn, and keeps every first message so the test can see what each run saw."""

    def __init__(self, replies: list[ModelResponse]) -> None:
        self.replies = replies
        self.first_messages: list[str] = []

    async def complete(
        self, system: str, messages: list[Message], tools: list[ToolSpec]
    ) -> ModelResponse:
        if len(messages) == 1:
            self.first_messages.append("\n".join(b["text"] for b in messages[0].content))
        return self.replies.pop(0)


async def test_collecting_from_several_parties_until_done(tmp_path: Path) -> None:
    storage = await SqliteStorage.open(tmp_path / "db.sqlite")
    clock = [T0]
    scheduler = Scheduler(storage, clock=lambda: clock[0])
    new_items = [
        {"key": k, "description": f"Thing {k}", "party": p}
        for k, p in zip("abc", "xyz", strict=True)
    ]
    model = ScriptedModel(
        [
            # Run 1, the instruction: create the task, ask everyone, set a follow-up.
            _calls(
                (
                    "create_task",
                    {"task_id": "collect", "title": "Collect a, b, c", "items": new_items},
                ),
            ),
            _calls(
                (
                    "update_task",
                    {
                        "task_id": "collect",
                        "log": "Asked x, y and z",
                        "items": [{"key": k, "status": "waiting"} for k in "abc"],
                    },
                ),
                (
                    "set_timer",
                    {
                        "delay_minutes": 60,
                        "note": "Chase whoever hasn't replied",
                        "task_id": "collect",
                    },
                ),
            ),
            DONE,
            # Run 2, y replies with b.
            _calls(
                (
                    "update_task",
                    {
                        "task_id": "collect",
                        "log": "y sent b",
                        "items": [{"key": "b", "status": "done"}],
                    },
                )
            ),
            DONE,
            # Run 3, the follow-up fires: chase x and z only, then both reply and it's done.
            _calls(
                (
                    "update_task",
                    {
                        "task_id": "collect",
                        "log": "x and z replied",
                        "items": [{"key": "a", "status": "done"}, {"key": "c", "status": "done"}],
                        "status": "done",
                    },
                )
            ),
            DONE,
        ]
    )
    builtins = [*timer_tools(storage, clock=lambda: clock[0]), *task_tools(storage)]
    worker = Worker(Agent(id="a1", model=model), storage, builtins)

    await storage.add_event(
        Event(
            agent_id="a1",
            source="user",
            type="instruction",
            payload={"text": "Get a, b and c from x, y and z"},
            idempotency_key="e1",
        )
    )
    await worker.run_pending()
    assert [t.subject for t in await storage.list_timers("a1")] == ["task:collect"]

    await storage.add_event(
        Event(
            agent_id="a1",
            source="email",
            type="reply",
            subject="task:collect",
            payload={"from": "y"},
            idempotency_key="e2",
        )
    )
    await worker.run_pending()
    assert "| b | Thing b | y | waiting |" in model.first_messages[1]  # Run 2 loaded the task.

    clock[0] += timedelta(minutes=61)
    assert await scheduler.run_due() == 1
    await worker.run_pending()
    seen_by_follow_up = model.first_messages[2]
    assert "Chase whoever hasn't replied" in seen_by_follow_up
    assert "| b | Thing b | y | done |" in seen_by_follow_up  # Knows b is done; won't chase y.
    assert "| a | Thing a | x | waiting |" in seen_by_follow_up

    view = await storage.get_task("a1", "collect")
    assert view is not None and view.task.status == TaskStatus.DONE
    assert [e.text for e in view.log] == ["Asked x, y and z", "y sent b", "x and z replied"]
    assert await storage.list_timers("a1") == []  # Nothing left to fire.
    await storage.close()


async def test_resumed_run_sees_the_task_as_it_was_when_the_run_started(storage: Storage) -> None:
    await storage.create_task(_task(), ITEMS, op_id="op1")
    await storage.add_event(
        Event(agent_id="a1", source="t", type="reply", subject="task:collect", idempotency_key="e1")
    )

    class CrashAfterFirstCall(ScriptedModel):
        async def complete(
            self, system: str, messages: list[Message], tools: list[ToolSpec]
        ) -> ModelResponse:
            if self.first_messages:
                raise ConnectionError("crash")
            return await super().complete(system, messages, tools)

    crashing = CrashAfterFirstCall([_calls(("get_task", {"task_id": "collect"})), DONE])
    with pytest.raises(ConnectionError):
        await Worker(Agent(id="a1", model=crashing), storage, task_tools(storage)).run_pending()

    # The task changes before the run resumes.
    update = TaskUpdate(items=[ItemUpdate(key="a", status=ItemStatus.DONE)])
    await storage.update_task("a1", "collect", update, op_id="op2")

    resumed = ScriptedModel([DONE])
    seen: list[list[Message]] = []
    original = resumed.complete

    async def record(system: str, messages: list[Message], tools: list[ToolSpec]) -> ModelResponse:
        seen.append(messages)
        return await original(system, messages, tools)

    resumed.complete = record  # type: ignore[method-assign]
    await Worker(Agent(id="a1", model=resumed), storage, task_tools(storage)).run_pending()
    first_text = "\n".join(b["text"] for b in seen[0][0].content)
    assert first_text == crashing.first_messages[0]
    assert "| a | Thing a | x | todo |" in first_text


# API


async def test_task_api(storage: Storage) -> None:
    await storage.create_task(_task(), ITEMS, op_id="op1")
    app = create_app(storage, {})
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        listed = (await client.get("/tasks", params={"agent_id": "a1"})).json()
        assert [t["id"] for t in listed] == ["collect"]
        assert (await client.get("/tasks", params={"status": "done"})).json() == []
        shown = (await client.get("/tasks/a1/collect")).json()["markdown"]
        assert shown.startswith("# Task collect")
        assert (await client.get("/tasks/a1/missing")).status_code == 404
