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
    status: TimerStatus = TimerStatus.ACTIVE


class StepKind(StrEnum):
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


class OutboxItem(BaseModel):
    id: str = Field(default_factory=_new_id)
    agent_id: str
    channel: str
    target: str
    body: str
    dedupe_key: str
    status: OutboxStatus = OutboxStatus.PENDING
