import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from double_agent.agent import Tool, current_tool_call
from double_agent.scheduler import once, recurring
from double_agent.storage import Storage, task_subject

_SET_TIMER_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "delay_minutes": {
            "type": "number",
            "description": "Fire once, this many minutes from now.",
        },
        "at": {
            "type": "string",
            "description": "Fire once at this time: ISO 8601 with a UTC offset, "
            "for example 2026-11-23T09:00:00+00:00.",
        },
        "cron": {
            "type": "string",
            "description": "Fire repeatedly on this cron schedule, for example '0 9 * * 1-5'.",
        },
        "tz": {
            "type": "string",
            "description": "Time zone for `cron`, for example 'Europe/London'. Defaults to UTC.",
        },
        "note": {
            "type": "string",
            "description": "What to do when the timer fires. The run it starts sees only this "
            "note, not this conversation, so include everything it will need.",
        },
        "task_id": {
            "type": "string",
            "description": "Attach the timer to this task, so the run it starts loads the task. "
            "Defaults to the task this run is about, if any.",
        },
    },
    "required": ["note"],
}


def timer_tools(
    storage: Storage,
    on_change: Callable[[], None] = lambda: None,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> list[Tool]:
    """The built-in tools an agent uses to manage its own timers.

    `on_change` is called after a timer is added, so the scheduler can recheck its next wake-up.
    """

    async def set_timer(input: dict[str, Any]) -> str:
        call = current_tool_call.get()
        when = [k for k in ("delay_minutes", "at", "cron") if input.get(k) is not None]
        if len(when) != 1:
            raise ValueError("Give exactly one of delay_minutes, at or cron.")
        note = input["note"]
        if "cron" in when:
            timer = recurring(call.agent_id, input["cron"], input.get("tz", "UTC"), note, clock())
        elif "at" in when:
            timer = once(call.agent_id, datetime.fromisoformat(input["at"]), note)
        else:
            due = clock() + timedelta(minutes=float(input["delay_minutes"]))
            timer = once(call.agent_id, due, note)
        subject = task_subject(input["task_id"]) if input.get("task_id") else call.subject
        if subject is not None and not subject.startswith("task:"):
            subject = None
        # Named after the run and step, so a resumed run can't create the same timer twice.
        timer = timer.model_copy(update={"id": f"{call.run_id}:{call.step}", "subject": subject})
        await storage.add_timer(timer)
        on_change()
        repeat = f", then repeating on '{timer.cron}' ({timer.tz})" if timer.cron else ""
        return f"Timer {timer.id} set for {timer.due_at.astimezone(UTC).isoformat()}{repeat}."

    async def list_timers(input: dict[str, Any]) -> str:
        timers = await storage.list_timers(current_tool_call.get().agent_id)
        if not timers:
            return "No active timers."
        return json.dumps(
            [
                {
                    "id": t.id,
                    "next_fire": t.due_at.astimezone(UTC).isoformat(),
                    "cron": t.cron,
                    "tz": t.tz,
                    "note": t.note,
                }
                for t in timers
            ]
        )

    async def cancel_timer(input: dict[str, Any]) -> str:
        timer_id = input["timer_id"]
        if not await storage.cancel_timer(current_tool_call.get().agent_id, timer_id):
            raise ValueError(f"No active timer {timer_id}.")
        return f"Timer {timer_id} cancelled."

    return [
        Tool(
            name="set_timer",
            description="Schedule a future wake-up for yourself, once or on a cron schedule. "
            "When it fires, a new run starts with the note as its event.",
            input_schema=_SET_TIMER_SCHEMA,
            fn=set_timer,
        ),
        Tool(
            name="list_timers",
            description="List your active timers.",
            input_schema={"type": "object", "properties": {}},
            fn=list_timers,
        ),
        Tool(
            name="cancel_timer",
            description="Cancel one of your active timers by id.",
            input_schema={
                "type": "object",
                "properties": {"timer_id": {"type": "string"}},
                "required": ["timer_id"],
            },
            fn=cancel_timer,
        ),
    ]
