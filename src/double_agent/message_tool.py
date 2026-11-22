from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from double_agent.agent import Tool, current_tool_call
from double_agent.storage import Storage
from double_agent.types import OutboxItem


def send_message_tool(
    channel_names: list[str],
    storage: Storage,
    on_queue: Callable[[], None] = lambda: None,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> Tool:
    """The built-in tool an agent uses to send a message through one of its channels.

    The agent can only name channels from its own configuration; where each one delivers is
    set there, not by the agent. `on_queue` is called after a message is queued, so the
    dispatcher can deliver it.
    """

    async def send_message(input: dict[str, Any]) -> str:
        call = current_tool_call.get()
        channel = input["channel"]
        if channel not in channel_names:
            raise ValueError(
                f"Unknown channel {channel!r}. Use one of: {', '.join(channel_names)}."
            )
        if input.get("deliver_after_minutes") is not None and input.get("deliver_at") is not None:
            raise ValueError("Give at most one of deliver_after_minutes and deliver_at.")
        deliver = clock()
        if input.get("deliver_after_minutes") is not None:
            deliver += timedelta(minutes=float(input["deliver_after_minutes"]))
        elif input.get("deliver_at") is not None:
            deliver = datetime.fromisoformat(input["deliver_at"])
            if deliver.tzinfo is None:
                raise ValueError("deliver_at needs a UTC offset, for example +00:00.")
        # Named after the run and step, so a resumed run can't queue the same message twice.
        item = OutboxItem(
            id=f"{call.run_id}:{call.step}",
            agent_id=call.agent_id,
            channel=channel,
            body=input["body"],
            next_attempt_at=deliver,
        )
        await storage.add_outbox_item(item)
        on_queue()
        when = "now" if deliver <= clock() else f"at {deliver.astimezone(UTC).isoformat()}"
        return f"Message {item.id} queued on {channel}, to be delivered {when}."

    return Tool(
        name="send_message",
        description="Send a message to people through one of your channels. Nothing you write "
        "outside this tool is delivered, so use it only when there is something worth saying. "
        "Optionally delay delivery.",
        input_schema={
            "type": "object",
            "properties": {
                "channel": {"type": "string", "enum": channel_names},
                "body": {"type": "string", "description": "The message to send."},
                "deliver_after_minutes": {
                    "type": "number",
                    "description": "Deliver this many minutes from now instead of right away.",
                },
                "deliver_at": {
                    "type": "string",
                    "description": "Deliver at this time instead of right away: ISO 8601 with a "
                    "UTC offset.",
                },
            },
            "required": ["channel", "body"],
        },
        fn=send_message,
    )
