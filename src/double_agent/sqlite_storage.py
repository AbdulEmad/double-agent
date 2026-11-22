import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

import aiosqlite

from double_agent.types import Event, EventStatus, Run, RunStatus, Step

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
"""


def _ts(value: datetime) -> str:
    # Fixed-width timestamps so text order matches time order.
    return value.isoformat(timespec="microseconds")


def _event(row: aiosqlite.Row) -> Event:
    fields: dict[str, Any] = dict(row)
    fields["payload"] = json.loads(fields["payload"])
    return Event.model_validate(fields)


class SqliteStorage:
    """Storage backed by a SQLite file in WAL mode."""

    def __init__(self, db: aiosqlite.Connection) -> None:
        self._db = db

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
        await self._db.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            await self._db.execute("ROLLBACK")
            raise
        await self._db.execute("COMMIT")

    async def add_event(self, event: Event) -> bool:
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
        async with self._db.execute("SELECT * FROM events WHERE id = ?", (event_id,)) as cursor:
            row = await cursor.fetchone()
        return _event(row) if row else None

    async def start_next_run(self, agent_id: str) -> Run | None:
        async with self._transaction():
            if await self.unfinished_run(agent_id):
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
        async with self._db.execute(
            "SELECT * FROM runs WHERE agent_id = ? AND status = ?",
            (agent_id, RunStatus.RUNNING),
        ) as cursor:
            row = await cursor.fetchone()
        return Run.model_validate(dict(row)) if row else None

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
        await self._db.execute(
            "INSERT INTO steps VALUES (?, ?, ?, ?)",
            (step.run_id, step.index, step.kind, json.dumps(step.data)),
        )

    async def load_steps(self, run_id: str) -> list[Step]:
        async with self._db.execute(
            "SELECT * FROM steps WHERE run_id = ? ORDER BY idx", (run_id,)
        ) as cursor:
            rows = await cursor.fetchall()
        return [
            Step(run_id=r["run_id"], index=r["idx"], kind=r["kind"], data=json.loads(r["data"]))
            for r in rows
        ]
