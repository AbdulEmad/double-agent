import asyncio
import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiosqlite

from double_agent.types import (
    Approval,
    ApprovalStatus,
    Event,
    EventStatus,
    OutboxItem,
    OutboxStatus,
    Run,
    RunStatus,
    Step,
    Timer,
    TimerStatus,
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    subject TEXT,
    source TEXT NOT NULL,
    type TEXT NOT NULL,
    payload TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS events_pending ON events (agent_id, status, created_at);

CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    event_id TEXT NOT NULL REFERENCES events (id),
    status TEXT NOT NULL,
    started_at TEXT NOT NULL
);
-- At most one unfinished run per agent, enforced by the database.
CREATE UNIQUE INDEX IF NOT EXISTS runs_one_running
    ON runs (agent_id) WHERE status = 'running';

CREATE TABLE IF NOT EXISTS steps (
    run_id TEXT NOT NULL REFERENCES runs (id),
    idx INTEGER NOT NULL,
    kind TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (run_id, idx)
);

CREATE TABLE IF NOT EXISTS timers (
    id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    due_at TEXT NOT NULL,
    cron TEXT,
    tz TEXT NOT NULL,
    note TEXT NOT NULL,
    status TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS timers_due ON timers (status, due_at);

CREATE TABLE IF NOT EXISTS outbox (
    id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    body TEXT NOT NULL,
    next_attempt_at TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    last_error TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS outbox_due ON outbox (status, next_attempt_at);

CREATE TABLE IF NOT EXISTS approvals (
    id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    run_id TEXT NOT NULL REFERENCES runs (id),
    tool_name TEXT NOT NULL,
    input TEXT NOT NULL,
    status TEXT NOT NULL,
    reason TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS approvals_status ON approvals (status, created_at);
"""


def _ts(value: datetime) -> str:
    # Fixed-width UTC timestamps so text order matches time order.
    return value.astimezone(UTC).isoformat(timespec="microseconds")


def _event(row: aiosqlite.Row) -> Event:
    fields: dict[str, Any] = dict(row)
    fields["payload"] = json.loads(fields["payload"])
    return Event.model_validate(fields)


def _approval(row: aiosqlite.Row) -> Approval:
    fields: dict[str, Any] = dict(row)
    fields["input"] = json.loads(fields["input"])
    return Approval.model_validate(fields)


class SqliteStorage:
    """Storage backed by a SQLite file in WAL mode.

    One connection is shared by the API and every worker, so each method holds a lock. Without
    it, one caller's statement could run inside another caller's open transaction.
    """

    def __init__(self, db: aiosqlite.Connection) -> None:
        self._db = db
        self._lock = asyncio.Lock()

    @classmethod
    async def open(cls, path: str | Path) -> "SqliteStorage":
        # Autocommit mode: multi-statement writes use explicit transactions below.
        db = await aiosqlite.connect(path, isolation_level=None)
        db.row_factory = aiosqlite.Row
        await db.execute("PRAGMA journal_mode = WAL")
        await db.execute("PRAGMA foreign_keys = ON")
        await db.executescript(_SCHEMA)
        return cls(db)

    async def close(self) -> None:
        await self._db.close()

    @asynccontextmanager
    async def _transaction(self) -> AsyncGenerator[None]:
        async with self._lock:
            await self._db.execute("BEGIN IMMEDIATE")
            try:
                yield
            except BaseException:
                await self._db.execute("ROLLBACK")
                raise
            await self._db.execute("COMMIT")

    async def add_event(self, event: Event) -> bool:
        async with self._lock:
            return await self._insert_event(event)

    async def _insert_event(self, event: Event) -> bool:
        cursor = await self._db.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
            " ON CONFLICT (idempotency_key) DO NOTHING",
            (
                event.id,
                event.agent_id,
                event.subject,
                event.source,
                event.type,
                json.dumps(event.payload),
                event.idempotency_key,
                event.status,
                _ts(event.created_at),
            ),
        )
        return cursor.rowcount == 1

    async def get_event(self, event_id: str) -> Event | None:
        async with (
            self._lock,
            self._db.execute("SELECT * FROM events WHERE id = ?", (event_id,)) as cursor,
        ):
            row = await cursor.fetchone()
        return _event(row) if row else None

    async def start_next_run(self, agent_id: str) -> Run | None:
        async with self._transaction():
            if await self._unfinished_run(agent_id):
                return None
            async with self._db.execute(
                "SELECT id FROM events WHERE agent_id = ? AND status = ?"
                " ORDER BY created_at, rowid LIMIT 1",
                (agent_id, EventStatus.PENDING),
            ) as cursor:
                row = await cursor.fetchone()
            if row is None:
                return None
            run = Run(agent_id=agent_id, event_id=row["id"])
            await self._db.execute(
                "UPDATE events SET status = ? WHERE id = ?", (EventStatus.CLAIMED, run.event_id)
            )
            await self._db.execute(
                "INSERT INTO runs VALUES (?, ?, ?, ?, ?)",
                (run.id, run.agent_id, run.event_id, run.status, _ts(run.started_at)),
            )
        return run

    async def unfinished_run(self, agent_id: str) -> Run | None:
        async with self._lock:
            return await self._unfinished_run(agent_id)

    async def _unfinished_run(self, agent_id: str) -> Run | None:
        async with self._db.execute(
            "SELECT * FROM runs WHERE agent_id = ? AND status = ?",
            (agent_id, RunStatus.RUNNING),
        ) as cursor:
            row = await cursor.fetchone()
        return Run.model_validate(dict(row)) if row else None

    async def pause_run(self, run_id: str) -> None:
        async with self._lock:
            await self._db.execute(
                "UPDATE runs SET status = ? WHERE id = ?", (RunStatus.AWAITING_APPROVAL, run_id)
            )

    async def resume_answered_run(self, agent_id: str) -> Run | None:
        async with self._transaction():
            if await self._unfinished_run(agent_id):
                return None
            async with self._db.execute(
                "SELECT * FROM runs WHERE agent_id = ? AND status = ? AND id IN"
                " (SELECT run_id FROM approvals WHERE status != ?) ORDER BY started_at LIMIT 1",
                (agent_id, RunStatus.AWAITING_APPROVAL, ApprovalStatus.PENDING),
            ) as cursor:
                row = await cursor.fetchone()
            if row is None:
                return None
            await self._db.execute(
                "UPDATE runs SET status = ? WHERE id = ?", (RunStatus.RUNNING, row["id"])
            )
        return Run.model_validate({**dict(row), "status": RunStatus.RUNNING})

    async def finish_run(self, run_id: str) -> None:
        async with self._transaction():
            await self._db.execute(
                "UPDATE runs SET status = ? WHERE id = ?", (RunStatus.FINISHED, run_id)
            )
            await self._db.execute(
                "UPDATE events SET status = ? WHERE id = (SELECT event_id FROM runs WHERE id = ?)",
                (EventStatus.DONE, run_id),
            )

    async def save_step(self, step: Step) -> None:
        async with self._lock:
            await self._db.execute(
                "INSERT INTO steps VALUES (?, ?, ?, ?)",
                (step.run_id, step.index, step.kind, json.dumps(step.data)),
            )

    async def load_steps(self, run_id: str) -> list[Step]:
        async with (
            self._lock,
            self._db.execute(
                "SELECT * FROM steps WHERE run_id = ? ORDER BY idx", (run_id,)
            ) as cursor,
        ):
            rows = await cursor.fetchall()
        return [
            Step(run_id=r["run_id"], index=r["idx"], kind=r["kind"], data=json.loads(r["data"]))
            for r in rows
        ]

    async def add_timer(self, timer: Timer) -> bool:
        async with self._lock:
            cursor = await self._db.execute(
                "INSERT INTO timers VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO NOTHING",
                (
                    timer.id,
                    timer.agent_id,
                    _ts(timer.due_at),
                    timer.cron,
                    timer.tz,
                    timer.note,
                    timer.status,
                ),
            )
            return cursor.rowcount == 1

    async def due_timers(self, now: datetime) -> list[Timer]:
        async with (
            self._lock,
            self._db.execute(
                "SELECT * FROM timers WHERE status = ? AND due_at <= ? ORDER BY due_at",
                (TimerStatus.ACTIVE, _ts(now)),
            ) as cursor,
        ):
            rows = await cursor.fetchall()
        return [Timer.model_validate(dict(r)) for r in rows]

    async def next_due_at(self) -> datetime | None:
        async with (
            self._lock,
            self._db.execute(
                "SELECT MIN(due_at) FROM timers WHERE status = ?", (TimerStatus.ACTIVE,)
            ) as cursor,
        ):
            row = await cursor.fetchone()
        return datetime.fromisoformat(row[0]) if row and row[0] else None

    async def list_timers(self, agent_id: str) -> list[Timer]:
        async with (
            self._lock,
            self._db.execute(
                "SELECT * FROM timers WHERE agent_id = ? AND status = ? ORDER BY due_at",
                (agent_id, TimerStatus.ACTIVE),
            ) as cursor,
        ):
            rows = await cursor.fetchall()
        return [Timer.model_validate(dict(r)) for r in rows]

    async def cancel_timer(self, agent_id: str, timer_id: str) -> bool:
        async with self._lock:
            cursor = await self._db.execute(
                "UPDATE timers SET status = ? WHERE id = ? AND agent_id = ? AND status = ?",
                (TimerStatus.CANCELLED, timer_id, agent_id, TimerStatus.ACTIVE),
            )
            return cursor.rowcount == 1

    async def fire_timer(self, timer: Timer, event: Event, next_due_at: datetime | None) -> bool:
        async with self._transaction():
            async with self._db.execute(
                "SELECT 1 FROM timers WHERE id = ? AND status = ? AND due_at = ?",
                (timer.id, TimerStatus.ACTIVE, _ts(timer.due_at)),
            ) as cursor:
                if await cursor.fetchone() is None:
                    return False
            await self._insert_event(event)
            if next_due_at is None:
                await self._db.execute(
                    "UPDATE timers SET status = ? WHERE id = ?", (TimerStatus.DONE, timer.id)
                )
            else:
                await self._db.execute(
                    "UPDATE timers SET due_at = ? WHERE id = ?", (_ts(next_due_at), timer.id)
                )
        return True

    async def add_outbox_item(self, item: OutboxItem) -> bool:
        async with self._lock:
            cursor = await self._db.execute(
                "INSERT INTO outbox VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO NOTHING",
                (
                    item.id,
                    item.agent_id,
                    item.channel,
                    item.body,
                    _ts(item.next_attempt_at),
                    item.status,
                    item.attempts,
                    item.last_error,
                    _ts(item.created_at),
                ),
            )
            return cursor.rowcount == 1

    async def due_outbox_items(self, now: datetime) -> list[OutboxItem]:
        async with (
            self._lock,
            self._db.execute(
                "SELECT * FROM outbox WHERE status = ? AND next_attempt_at <= ?"
                " ORDER BY next_attempt_at, created_at",
                (OutboxStatus.PENDING, _ts(now)),
            ) as cursor,
        ):
            rows = await cursor.fetchall()
        return [OutboxItem.model_validate(dict(r)) for r in rows]

    async def next_outbox_at(self) -> datetime | None:
        async with (
            self._lock,
            self._db.execute(
                "SELECT MIN(next_attempt_at) FROM outbox WHERE status = ?", (OutboxStatus.PENDING,)
            ) as cursor,
        ):
            row = await cursor.fetchone()
        return datetime.fromisoformat(row[0]) if row and row[0] else None

    async def record_delivery(self, item_id: str) -> None:
        async with self._lock:
            await self._db.execute(
                "UPDATE outbox SET status = ? WHERE id = ?", (OutboxStatus.SENT, item_id)
            )

    async def record_failure(self, item_id: str, error: str, retry_at: datetime | None) -> None:
        async with self._lock:
            if retry_at is None:
                await self._db.execute(
                    "UPDATE outbox SET attempts = attempts + 1, last_error = ?, status = ?"
                    " WHERE id = ?",
                    (error, OutboxStatus.FAILED, item_id),
                )
            else:
                await self._db.execute(
                    "UPDATE outbox SET attempts = attempts + 1, last_error = ?, next_attempt_at = ?"
                    " WHERE id = ?",
                    (error, _ts(retry_at), item_id),
                )

    async def add_approval(self, approval: Approval) -> bool:
        async with self._lock:
            cursor = await self._db.execute(
                "INSERT INTO approvals VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO NOTHING",
                (
                    approval.id,
                    approval.agent_id,
                    approval.run_id,
                    approval.tool_name,
                    json.dumps(approval.input),
                    approval.status,
                    approval.reason,
                    _ts(approval.created_at),
                ),
            )
            return cursor.rowcount == 1

    async def get_approval(self, approval_id: str) -> Approval | None:
        async with (
            self._lock,
            self._db.execute("SELECT * FROM approvals WHERE id = ?", (approval_id,)) as cursor,
        ):
            row = await cursor.fetchone()
        return _approval(row) if row else None

    async def pending_approvals(self) -> list[Approval]:
        async with (
            self._lock,
            self._db.execute(
                "SELECT * FROM approvals WHERE status = ? ORDER BY created_at",
                (ApprovalStatus.PENDING,),
            ) as cursor,
        ):
            rows = await cursor.fetchall()
        return [_approval(r) for r in rows]

    async def answer_approval(
        self, approval_id: str, approved: bool, reason: str | None
    ) -> Approval | None:
        status = ApprovalStatus.APPROVED if approved else ApprovalStatus.DENIED
        async with self._lock:
            cursor = await self._db.execute(
                "UPDATE approvals SET status = ?, reason = ? WHERE id = ? AND status = ?"
                " RETURNING *",
                (status, reason, approval_id, ApprovalStatus.PENDING),
            )
            row = await cursor.fetchone()
            await cursor.close()
        return _approval(row) if row else None
