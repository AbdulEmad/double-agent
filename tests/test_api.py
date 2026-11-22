import asyncio
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest

from double_agent.agent import Agent
from double_agent.api import create_app
from double_agent.model import FakeModel, ModelResponse
from double_agent.sqlite_storage import SqliteStorage
from double_agent.worker import Worker

DONE = ModelResponse(content=[{"type": "text", "text": "done"}], stop_reason="end_turn")
WEBHOOK = {"agent_id": "a1", "type": "push", "idempotency_key": "delivery-1", "payload": {"x": 1}}


@pytest.fixture
async def storage(tmp_path: Path) -> AsyncIterator[SqliteStorage]:
    storage = await SqliteStorage.open(tmp_path / "db.sqlite")
    yield storage
    await storage.close()


def _client(worker: Worker) -> httpx.AsyncClient:
    app = create_app(worker.storage, {worker.agent.id: worker})
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


async def test_duplicate_webhook_creates_one_event_and_one_run(storage: SqliteStorage) -> None:
    model = FakeModel([DONE, DONE])
    worker = Worker(Agent(id="a1", model=model), storage)
    async with _client(worker) as client:
        first = await client.post("/events", json=WEBHOOK)
        second = await client.post("/events", json=WEBHOOK)

    assert (first.status_code, first.json()) == (202, {"duplicate": False})
    assert (second.status_code, second.json()) == (200, {"duplicate": True})
    assert await worker.run_pending() == 1
    assert len(model.requests) == 1
    assert '"payload": {"x": 1}' in model.requests[0][0].content[0]["text"]


async def test_unknown_agent_and_missing_key_are_rejected(storage: SqliteStorage) -> None:
    worker = Worker(Agent(id="a1", model=FakeModel([])), storage)
    async with _client(worker) as client:
        unknown = await client.post("/events", json={**WEBHOOK, "agent_id": "nope"})
        no_key = await client.post("/events", json={"agent_id": "a1", "type": "push"})
    assert unknown.status_code == 404
    assert no_key.status_code == 422


async def test_new_event_wakes_the_worker(storage: SqliteStorage) -> None:
    model = FakeModel([DONE])
    worker = Worker(Agent(id="a1", model=model), storage)
    # Workers don't poll, so only the wake-up can explain the run.
    task = asyncio.create_task(worker.run_forever())
    await asyncio.sleep(0.05)
    async with _client(worker) as client:
        await client.post("/events", json=WEBHOOK)
    async with asyncio.timeout(2):
        while await storage.unfinished_run("a1") or not model.requests:
            await asyncio.sleep(0.01)
    task.cancel()
    assert len(model.requests) == 1
