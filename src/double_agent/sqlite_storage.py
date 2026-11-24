import asyncio
import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiosqlite

from double_agent.storage import apply_task_update, task_subject
from double_agent.types import (
    Approval,
    ApprovalStatus,
    Event,
    EventStatus,
    MemoryFile,
    MemoryVersion,
    OutboxItem,
    OutboxStatus,
    Run,
    RunStatus,
    Step,
    Task,
    TaskItem,
    TaskLogEntry,
    TaskStatus,
    TaskUpdate,
    TaskView,
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
    status TEXT NOT NULL,
    subject TEXT
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

CREATE TABLE IF NOT EXISTS tasks (
    agent_id TEXT NOT NULL,
    id TEXT NOT NULL,
    title TEXT NOT NULL,
    goal TEXT NOT NULL,
    status TEXT NOT NULL,
    notes TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (agent_id, id)
);

CREATE TABLE IF NOT EXISTS task_items (
    agent_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    position INTEGER NOT NULL,
    key TEXT NOT NULL,
    description TEXT NOT NULL,
    party TEXT,
    status TEXT NOT NULL,
    note TEXT,
    PRIMARY KEY (agent_id, task_id, key)
);

CREATE TABLE IF NOT EXISTS task_log (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    at TEXT NOT NULL,
    text TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS task_log_task ON task_log (agent_id, task_id, seq);

-- Tool calls that created or updated a task, so a resumed run doesn't apply one twice.
CREATE TABLE IF NOT EXISTS task_ops (
    op_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    task_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS memory_versions (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id TEXT NOT NULL,
    path TEXT NOT NULL,
    content TEXT,
    op_id TEXT NOT NULL,
    source TEXT NOT NULL,
    run_id TEXT,
    at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS memory_versions_path ON memory_versions (agent_id, path, seq);

-- Memory writes already applied, with the result the tool returned, so a resumed run gets the
-- same answer without applying the write twice.
CREATE TABLE IF NOT EXISTS memory_ops (
    op_id TEXT PRIMARY KEY,
    result TEXT NOT NULL
);
"""

# Columns added after a table was first released, as (table, column, definition).
_ADDED_COLUMNS = [("timers", "subject", "TEXT")]


def _ts(value: datetime) -> str:
    # Fixed-width UTC timestamps so text order matches time order.
    return value.astimezone(UTC).isoformat(timespec="microseconds")


def _event(row: aiosqlite.Row) -> Event:
    fields: dict[str, Any] = dict(row)
    fields["payload"] = json.loads(fields["payload"])
    return Event.model_validate(fields)


# The latest version of each path, leaving out deleted files.
_CURRENT_MEMORY = """
SELECT path, content, at FROM memory_versions AS v
WHERE agent_id = ? AND content IS NOT NULL AND seq = (
    SELECT MAX(seq) FROM memory_versions WHERE agent_id = v.agent_id AND path = v.path
)
ORDER BY path
"""


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
        for table, column, definition in _ADDED_COLUMNS:
            existing = {row[1] for row in await db.execute_fetchall(f"PRAGMA table_info({table})")}
            if column not in existing:
                await db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
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
                "INSERT INTO timers (id, agent_id, due_at, cron, tz, note, status, subject)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO NOTHING",
                (
                    timer.id,
                    timer.agent_id,
                    _ts(timer.due_at),
                    timer.cron,
                    timer.tz,
                    timer.note,
                    timer.status,
                    timer.subject,
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

    async def create_task(self, task: Task, items: list[TaskItem], op_id: str) -> TaskView:
        async with self._transaction():
            if existing := await self._task_for_op(op_id):
                return existing
            if await self._load_task(task.agent_id, task.id):
                raise ValueError(f"Task id {task.id!r} is already used")
            view = TaskView(task=task, items=items, log=[])
            await self._db.execute(
                "INSERT INTO tasks VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task.agent_id,
                    task.id,
                    task.title,
                    task.goal,
                    task.status,
                    task.notes,
                    _ts(task.created_at),
                    _ts(task.updated_at),
                ),
            )
            await self._write_items(view)
            await self._record_task_op(op_id, task.agent_id, task.id)
        return view

    async def get_task(self, agent_id: str, task_id: str) -> TaskView | None:
        async with self._lock:
            return await self._load_task(agent_id, task_id)

    async def list_tasks(self, agent_id: str | None, status: TaskStatus | None) -> list[Task]:
        query, args = "SELECT * FROM tasks WHERE 1 = 1", list[str]()
        if agent_id is not None:
            query, args = query + " AND agent_id = ?", [*args, agent_id]
        if status is not None:
            query, args = query + " AND status = ?", [*args, status]
        async with self._lock, self._db.execute(query + " ORDER BY created_at DESC", args) as cur:
            rows = await cur.fetchall()
        return [Task.model_validate(dict(r)) for r in rows]

    async def update_task(
        self, agent_id: str, task_id: str, update: TaskUpdate, op_id: str
    ) -> TaskView:
        async with self._transaction():
            view = await self._load_task(agent_id, task_id)
            if view is None:
                raise ValueError(f"No task {task_id!r}")
            if await self._task_for_op(op_id):
                return view
            apply_task_update(view, update)
            await self._db.execute(
                "UPDATE tasks SET status = ?, notes = ?, updated_at = ?"
                " WHERE agent_id = ? AND id = ?",
                (view.task.status, view.task.notes, _ts(view.task.updated_at), agent_id, task_id),
            )
            await self._write_items(view)
            if update.log:
                entry = view.log[-1]
                await self._db.execute(
                    "INSERT INTO task_log (agent_id, task_id, at, text) VALUES (?, ?, ?, ?)",
                    (agent_id, task_id, _ts(entry.at), entry.text),
                )
            if view.task.status != TaskStatus.OPEN:
                await self._db.execute(
                    "UPDATE timers SET status = ?"
                    " WHERE agent_id = ? AND subject = ? AND status = ?",
                    (TimerStatus.CANCELLED, agent_id, task_subject(task_id), TimerStatus.ACTIVE),
                )
            await self._record_task_op(op_id, agent_id, task_id)
        return view

    async def _load_task(self, agent_id: str, task_id: str) -> TaskView | None:
        key = (agent_id, task_id)
        async with self._db.execute(
            "SELECT * FROM tasks WHERE agent_id = ? AND id = ?", key
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            return None
        async with self._db.execute(
            "SELECT key, description, party, status, note FROM task_items"
            " WHERE agent_id = ? AND task_id = ? ORDER BY position",
            key,
        ) as cursor:
            items = [TaskItem.model_validate(dict(r)) for r in await cursor.fetchall()]
        async with self._db.execute(
            "SELECT at, text FROM task_log WHERE agent_id = ? AND task_id = ? ORDER BY seq", key
        ) as cursor:
            log = [TaskLogEntry.model_validate(dict(r)) for r in await cursor.fetchall()]
        return TaskView(task=Task.model_validate(dict(row)), items=items, log=log)

    async def _write_items(self, view: TaskView) -> None:
        key = (view.task.agent_id, view.task.id)
        await self._db.execute("DELETE FROM task_items WHERE agent_id = ? AND task_id = ?", key)
        await self._db.executemany(
            "INSERT INTO task_items VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (*key, position, i.key, i.description, i.party, i.status, i.note)
                for position, i in enumerate(view.items)
            ],
        )

    async def _task_for_op(self, op_id: str) -> TaskView | None:
        async with self._db.execute(
            "SELECT agent_id, task_id FROM task_ops WHERE op_id = ?", (op_id,)
        ) as cursor:
            row = await cursor.fetchone()
        return await self._load_task(row["agent_id"], row["task_id"]) if row else None

    async def _record_task_op(self, op_id: str, agent_id: str, task_id: str) -> None:
        await self._db.execute("INSERT INTO task_ops VALUES (?, ?, ?)", (op_id, agent_id, task_id))

    async def list_memory(self, agent_id: str) -> list[MemoryFile]:
        async with self._lock, self._db.execute(_CURRENT_MEMORY, (agent_id,)) as cursor:
            rows = await cursor.fetchall()
        return [
            MemoryFile(path=r["path"], size=len(r["content"]), updated_at=r["at"]) for r in rows
        ]

    async def read_memory(self, agent_id: str, path: str) -> str | None:
        async with (
            self._lock,
            self._db.execute(
                "SELECT content FROM memory_versions WHERE agent_id = ? AND path = ?"
                " ORDER BY seq DESC LIMIT 1",
                (agent_id, path),
            ) as cursor,
        ):
            row = await cursor.fetchone()
        return row["content"] if row else None

    async def memory_op_result(self, op_id: str) -> str | None:
        async with (
            self._lock,
            self._db.execute("SELECT result FROM memory_ops WHERE op_id = ?", (op_id,)) as cursor,
        ):
            row = await cursor.fetchone()
        return row["result"] if row else None

    async def write_memory(
        self,
        agent_id: str,
        changes: dict[str, str | None],
        op_id: str,
        result: str,
        source: str,
        run_id: str | None,
    ) -> None:
        async with self._transaction():
            cursor = await self._db.execute(
                "INSERT INTO memory_ops VALUES (?, ?) ON CONFLICT (op_id) DO NOTHING",
                (op_id, result),
            )
            if cursor.rowcount == 0:
                return
            at = _ts(datetime.now(UTC))
            await self._db.executemany(
                "INSERT INTO memory_versions (agent_id, path, content, op_id, source, run_id, at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                [(agent_id, p, c, op_id, source, run_id, at) for p, c in changes.items()],
            )

    async def memory_history(self, agent_id: str, path: str) -> list[MemoryVersion]:
        async with (
            self._lock,
            self._db.execute(
                "SELECT * FROM memory_versions WHERE agent_id = ? AND path = ? ORDER BY seq",
                (agent_id, path),
            ) as cursor,
        ):
            rows = await cursor.fetchall()
        return [
            MemoryVersion.model_validate({k: r[k] for k in r.keys() if k != "seq"}) for r in rows
        ]
