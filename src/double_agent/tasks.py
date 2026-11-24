import re
from typing import Any
from uuid import uuid4

from double_agent.agent import Tool, current_tool_call
from double_agent.safety import check_text
from double_agent.storage import Storage
from double_agent.types import (
    ItemStatus,
    ItemUpdate,
    Task,
    TaskItem,
    TaskStatus,
    TaskUpdate,
    TaskView,
)

MAX_ITEMS = 50
MAX_NOTES_CHARS = 4000
MAX_LOG_CHARS = 500
LOG_ENTRIES_SHOWN = 30

_ID = re.compile(r"^[a-z0-9][a-z0-9-]{0,39}$")


def render_task(view: TaskView) -> str:
    """The task as markdown, the way the agent and the CLI see it."""
    task = view.task
    lines = [f"# Task {task.id}: {task.title}", "", f"Status: {task.status}"]
    if task.goal:
        lines += ["", task.goal]
    lines += ["", "## Items", ""]
    if view.items:
        lines += ["| Key | Item | Party | Status | Note |", "| --- | --- | --- | --- | --- |"]
        for i in view.items:
            cells = [i.key, i.description, i.party or "", i.status, i.note or ""]
            lines.append("| " + " | ".join(c.replace("|", "/") for c in cells) + " |")
    else:
        lines.append("No items.")
    if task.notes:
        lines += ["", "## Notes", "", task.notes]
    lines += ["", "## Log", ""]
    hidden = len(view.log) - LOG_ENTRIES_SHOWN
    if hidden > 0:
        lines.append(f"({hidden} earlier entries not shown)")
    for entry in view.log[-LOG_ENTRIES_SHOWN:]:
        lines.append(f"- {entry.at.strftime('%Y-%m-%d %H:%M')} UTC: {entry.text}")
    if not view.log:
        lines.append("Nothing logged yet.")
    return "\n".join(lines)


def _items(raw: list[dict[str, Any]]) -> list[TaskItem]:
    items = [TaskItem.model_validate(i) for i in raw]
    for item in items:
        if not _ID.match(item.key):
            raise ValueError(f"Item key {item.key!r} must be lowercase letters, digits and dashes.")
        check_text(f"Item {item.key}", item.description, MAX_LOG_CHARS)
        check_text(f"Item {item.key} note", item.note, MAX_LOG_CHARS)
    if len({i.key for i in items}) != len(items):
        raise ValueError("Item keys must be unique.")
    return items


_ITEM_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "key": {"type": "string", "description": "Short id, for example 'contract-x'."},
        "description": {"type": "string", "description": "What this item is."},
        "party": {"type": "string", "description": "Who it depends on, if anyone."},
    },
    "required": ["key", "description"],
}


def task_tools(storage: Storage) -> list[Tool]:
    """The built-in tools an agent uses to track goals that take several runs."""

    async def create_task(input: dict[str, Any]) -> str:
        call = current_tool_call.get()
        task_id = input.get("task_id") or f"task-{uuid4().hex[:6]}"
        if not _ID.match(task_id):
            raise ValueError("task_id must be lowercase letters, digits and dashes.")
        items = _items(input.get("items", []))
        if len(items) > MAX_ITEMS:
            raise ValueError(f"A task can have at most {MAX_ITEMS} items.")
        check_text("Goal", input.get("goal"), MAX_NOTES_CHARS)
        task = Task(
            id=task_id, agent_id=call.agent_id, title=input["title"], goal=input.get("goal", "")
        )
        view = await storage.create_task(task, items, op_id=f"{call.run_id}:{call.step}")
        return render_task(view)

    async def get_task(input: dict[str, Any]) -> str:
        view = await storage.get_task(current_tool_call.get().agent_id, input["task_id"])
        if view is None:
            raise ValueError(f"No task {input['task_id']!r}.")
        return render_task(view)

    async def list_tasks(input: dict[str, Any]) -> str:
        status = TaskStatus(input.get("status", "open"))
        tasks = await storage.list_tasks(current_tool_call.get().agent_id, status)
        if not tasks:
            return f"No {status} tasks."
        return "\n".join(f"- {t.id}: {t.title} ({t.status})" for t in tasks)

    async def update_task(input: dict[str, Any]) -> str:
        call = current_tool_call.get()
        check_text("Notes", input.get("notes"), MAX_NOTES_CHARS)
        check_text("Log entry", input.get("log"), MAX_LOG_CHARS)
        changes = [ItemUpdate.model_validate(i) for i in input.get("items", [])]
        for change in changes:
            check_text(f"Item {change.key} note", change.note, MAX_LOG_CHARS)
        update = TaskUpdate(
            items=changes,
            new_items=_items(input.get("new_items", [])),
            notes=input.get("notes"),
            log=input.get("log"),
            status=TaskStatus(input["status"]) if input.get("status") else None,
        )
        current = await storage.get_task(call.agent_id, input["task_id"])
        if current and len(current.items) + len(update.new_items) > MAX_ITEMS:
            raise ValueError(f"A task can have at most {MAX_ITEMS} items.")
        view = await storage.update_task(
            call.agent_id, input["task_id"], update, op_id=f"{call.run_id}:{call.step}"
        )
        return render_task(view)

    statuses = [s.value for s in ItemStatus]
    return [
        Tool(
            name="create_task",
            description="Start tracking a goal that will take more than one run, such as "
            "collecting several things from several people. Break it into items. To have later "
            "runs pick it up, set timers during runs about the task; they are attached to it "
            "automatically.",
            input_schema={
                "type": "object",
                "properties": {
                    "task_id": {
                        "type": "string",
                        "description": "Optional short id, for example 'vendor-docs'.",
                    },
                    "title": {"type": "string"},
                    "goal": {"type": "string", "description": "What done looks like."},
                    "items": {"type": "array", "items": _ITEM_SCHEMA},
                },
                "required": ["title"],
            },
            fn=create_task,
        ),
        Tool(
            name="get_task",
            description="Show a task with its items, notes and log.",
            input_schema={
                "type": "object",
                "properties": {"task_id": {"type": "string"}},
                "required": ["task_id"],
            },
            fn=get_task,
        ),
        Tool(
            name="list_tasks",
            description="List your tasks. Shows open tasks unless you ask for another status.",
            input_schema={
                "type": "object",
                "properties": {"status": {"type": "string", "enum": [s.value for s in TaskStatus]}},
            },
            fn=list_tasks,
        ),
        Tool(
            name="update_task",
            description="Record progress on a task, all in one call: change item statuses, add "
            "items, replace the notes, add a log entry, or close the task. Log every action you "
            "take on the task. Never act again on an item marked done. Closing a task (done or "
            "cancelled) cancels its timers.",
            input_schema={
                "type": "object",
                "properties": {
                    "task_id": {"type": "string"},
                    "items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "key": {"type": "string"},
                                "status": {"type": "string", "enum": statuses},
                                "note": {"type": "string"},
                            },
                            "required": ["key"],
                        },
                    },
                    "new_items": {"type": "array", "items": _ITEM_SCHEMA},
                    "notes": {
                        "type": "string",
                        "description": "Replaces the task's notes. Keep pointers to other "
                        "systems, not copies of their data.",
                    },
                    "log": {"type": "string", "description": "What you just did or learned."},
                    "status": {"type": "string", "enum": [s.value for s in TaskStatus]},
                },
                "required": ["task_id"],
            },
            fn=update_task,
        ),
    ]
