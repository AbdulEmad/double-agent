from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import uuid4

from pydantic import AwareDatetime, BaseModel, Field


def _new_id() -> str:
    return uuid4().hex


def _now() -> datetime:
    return datetime.now(UTC)


class EventStatus(StrEnum):
    PENDING = "pending"
    CLAIMED = "claimed"
    DONE = "done"


class Event(BaseModel):
    id: str = Field(default_factory=_new_id)
    agent_id: str
    subject: str | None = None
    source: str
    type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str
    status: EventStatus = EventStatus.PENDING
    created_at: datetime = Field(default_factory=_now)


class RunStatus(StrEnum):
    RUNNING = "running"
    # Paused on a tool call that needs a human's approval. Does not block the agent's other runs.
    AWAITING_APPROVAL = "awaiting_approval"
    FINISHED = "finished"


class Run(BaseModel):
    id: str = Field(default_factory=_new_id)
    agent_id: str
    event_id: str
    status: RunStatus = RunStatus.RUNNING
    started_at: datetime = Field(default_factory=_now)


class TimerStatus(StrEnum):
    ACTIVE = "active"
    DONE = "done"
    CANCELLED = "cancelled"


class Timer(BaseModel):
    """A future event for an agent: one-off, or recurring when `cron` is set."""

    id: str = Field(default_factory=_new_id)
    agent_id: str
    due_at: AwareDatetime
    cron: str | None = None
    tz: str = "UTC"
    note: str = ""
    # Copied onto each event the timer fires, for example "task:<id>" to load that task.
    subject: str | None = None
    status: TimerStatus = TimerStatus.ACTIVE


class StepKind(StrEnum):
    # Context loaded at the start of a run, such as the task it is about. Saved so a resumed run
    # sends the model exactly the same first message.
    CONTEXT = "context"
    MODEL_CALL = "model_call"
    TOOL_CALL = "tool_call"


class Step(BaseModel):
    run_id: str
    index: int
    kind: StepKind
    data: dict[str, Any] = Field(default_factory=dict)


class OutboxStatus(StrEnum):
    PENDING = "pending"
    SENT = "sent"
    FAILED = "failed"


class OutboxItem(BaseModel):
    """A message the agent decided to send, waiting for the dispatcher to deliver it."""

    id: str = Field(default_factory=_new_id)
    agent_id: str
    channel: str
    body: str
    # When the next delivery attempt is due. Starts at the requested delivery time.
    next_attempt_at: AwareDatetime = Field(default_factory=_now)
    status: OutboxStatus = OutboxStatus.PENDING
    attempts: int = 0
    last_error: str | None = None
    created_at: AwareDatetime = Field(default_factory=_now)


class ApprovalStatus(StrEnum):
    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"


class Approval(BaseModel):
    """A request for a human to allow or refuse one tool call."""

    id: str
    agent_id: str
    run_id: str
    tool_name: str
    input: dict[str, Any] = Field(default_factory=dict)
    status: ApprovalStatus = ApprovalStatus.PENDING
    reason: str | None = None
    created_at: AwareDatetime = Field(default_factory=_now)


class TaskStatus(StrEnum):
    OPEN = "open"
    DONE = "done"
    CANCELLED = "cancelled"


class ItemStatus(StrEnum):
    TODO = "todo"
    WAITING = "waiting"
    DONE = "done"


class Task(BaseModel):
    """A goal that takes several runs, such as collecting things from several people."""

    id: str
    agent_id: str
    title: str
    goal: str = ""
    status: TaskStatus = TaskStatus.OPEN
    notes: str = ""
    created_at: AwareDatetime = Field(default_factory=_now)
    updated_at: AwareDatetime = Field(default_factory=_now)


class TaskItem(BaseModel):
    """One part of a task, such as one document from one person."""

    key: str
    description: str
    party: str | None = None
    status: ItemStatus = ItemStatus.TODO
    note: str | None = None


class TaskLogEntry(BaseModel):
    at: AwareDatetime = Field(default_factory=_now)
    text: str


class TaskView(BaseModel):
    """A task with its items and log, as the agent and the CLI see it."""

    task: Task
    items: list[TaskItem]
    log: list[TaskLogEntry]


class ItemUpdate(BaseModel):
    key: str
    status: ItemStatus | None = None
    note: str | None = None


class TaskUpdate(BaseModel):
    """Changes to a task, applied together."""

    items: list[ItemUpdate] = Field(default_factory=lambda: list[ItemUpdate]())
    new_items: list[TaskItem] = Field(default_factory=lambda: list[TaskItem]())
    notes: str | None = None
    log: str | None = None
    status: TaskStatus | None = None
