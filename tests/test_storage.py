import sqlite3
from collections.abc import AsyncIterator
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from double_agent.sqlite_storage import SqliteStorage
from double_agent.storage import InMemoryStorage, Storage
from double_agent.types import Event, EventStatus, RunStatus, Step, StepKind

T0 = datetime(2026, 1, 1, tzinfo=UTC)


def _event(key: str, agent_id: str = "a1", at: datetime = T0) -> Event:
    return Event(
        agent_id=agent_id, source="webhook", type="ping", idempotency_key=key, created_at=at
    )


@pytest.fixture(params=["memory", "sqlite"])
async def storage(request: pytest.FixtureRequest, tmp_path: Path) -> AsyncIterator[Storage]:
    if request.param == "memory":
        yield InMemoryStorage()
        return
    sqlite = await SqliteStorage.open(tmp_path / "db.sqlite")
    yield sqlite
    await sqlite.close()


async def test_duplicate_event_is_stored_once(storage: Storage) -> None:
    assert await storage.add_event(_event("k1"))
    assert not await storage.add_event(_event("k1"))
    run = await storage.start_next_run("a1")
    assert run is not None
    await storage.finish_run(run.id)
    assert await storage.start_next_run("a1") is None


async def test_runs_take_oldest_event_for_agent(storage: Storage) -> None:
    await storage.add_event(_event("other", agent_id="a2"))
    await storage.add_event(_event("later", at=T0 + timedelta(seconds=1)))
    await storage.add_event(_event("earlier"))

    keys: list[str] = []
    while run := await storage.start_next_run("a1"):
        event = await storage.get_event(run.event_id)
        assert event is not None
        keys.append(event.idempotency_key)
        await storage.finish_run(run.id)
    assert keys == ["earlier", "later"]


async def test_one_unfinished_run_per_agent(storage: Storage) -> None:
    await storage.add_event(_event("k1"))
    await storage.add_event(_event("k2"))
    await storage.add_event(_event("k3", agent_id="a2"))

    run = await storage.start_next_run("a1")
    assert run is not None
    assert await storage.start_next_run("a1") is None
    assert await storage.start_next_run("a2") is not None

    assert await storage.unfinished_run("a1") == run
    await storage.finish_run(run.id)
    assert await storage.unfinished_run("a1") is None
    assert await storage.start_next_run("a1") is not None


async def test_event_status_follows_run(storage: Storage) -> None:
    event = _event("k1")
    await storage.add_event(event)
    run = await storage.start_next_run("a1")
    assert run is not None and run.status == RunStatus.RUNNING

    async def status() -> EventStatus:
        stored = await storage.get_event(event.id)
        assert stored is not None
        return stored.status

    assert await status() == EventStatus.CLAIMED
    await storage.finish_run(run.id)
    assert await status() == EventStatus.DONE


async def test_steps_load_in_order(storage: Storage) -> None:
    await storage.add_event(_event("k1"))
    run = await storage.start_next_run("a1")
    assert run is not None
    await storage.save_step(Step(run_id=run.id, index=1, kind=StepKind.TOOL_CALL, data={"n": 1}))
    await storage.save_step(Step(run_id=run.id, index=0, kind=StepKind.MODEL_CALL))
    steps = await storage.load_steps(run.id)
    assert [(s.index, s.data) for s in steps] == [(0, {}), (1, {"n": 1})]
    assert await storage.load_steps("missing") == []


async def test_sqlite_state_survives_reopen(tmp_path: Path) -> None:
    path = tmp_path / "db.sqlite"
    first = await SqliteStorage.open(path)
    await first.add_event(_event("k1"))
    run = await first.start_next_run("a1")
    assert run is not None
    await first.save_step(Step(run_id=run.id, index=0, kind=StepKind.MODEL_CALL))
    await first.close()

    second = await SqliteStorage.open(path)
    assert await second.unfinished_run("a1") == run
    assert len(await second.load_steps(run.id)) == 1
    assert not await second.add_event(_event("k1"))
    await second.close()


async def test_sqlite_uses_wal_mode(tmp_path: Path) -> None:
    path = tmp_path / "db.sqlite"
    await (await SqliteStorage.open(path)).close()
    with closing(sqlite3.connect(path)) as db:
        assert db.execute("PRAGMA journal_mode").fetchone() == ("wal",)
