import asyncio
import hashlib
import hmac
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest

from double_agent.agent import Agent, Tool, ToolCall, current_tool_call
from double_agent.dispatcher import Dispatcher, retry_delay
from double_agent.message_tool import send_message_tool
from double_agent.model import FakeModel, ModelResponse
from double_agent.outbox import FileChannel, WebhookChannel, signature
from double_agent.sqlite_storage import SqliteStorage
from double_agent.storage import Storage
from double_agent.types import Event, OutboxItem
from double_agent.worker import Worker

T0 = datetime(2026, 3, 2, 8, 0, tzinfo=UTC)
DONE = ModelResponse(content=[{"type": "text", "text": "done"}], stop_reason="end_turn")


class Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


class FakeChannel:
    """Records what it delivers. Fails the first `failures` sends."""

    def __init__(self, failures: int = 0) -> None:
        self.sent: list[OutboxItem] = []
        self.failures = failures

    async def send(self, item: OutboxItem) -> None:
        if self.failures:
            self.failures -= 1
            raise ConnectionError("receiver down")
        self.sent.append(item)


def _item(id: str, at: datetime = T0, channel: str = "ops") -> OutboxItem:
    return OutboxItem(id=id, agent_id="a1", channel=channel, body=f"body {id}", next_attempt_at=at)


async def _status(storage: Storage, item_id: str) -> OutboxItem:
    # Pending items are all due within a year in these tests.
    for item in await storage.due_outbox_items(T0 + timedelta(days=365)):
        if item.id == item_id:
            return item
    raise AssertionError(f"{item_id} is not pending")


async def _call(tool: Tool, input: dict[str, Any], step: int = 1) -> str:
    token = current_tool_call.set(ToolCall("a1", "r1", step))
    try:
        return await tool.fn(input)
    finally:
        current_tool_call.reset(token)


# Storage


async def test_outbox_storage(storage: Storage) -> None:
    assert await storage.next_outbox_at() is None
    assert await storage.add_outbox_item(_item("later", T0 + timedelta(hours=1)))
    assert await storage.add_outbox_item(_item("now"))
    assert not await storage.add_outbox_item(_item("now"))

    assert await storage.next_outbox_at() == T0
    assert [i.id for i in await storage.due_outbox_items(T0)] == ["now"]

    await storage.record_failure("now", "boom", T0 + timedelta(minutes=5))
    retried = await _status(storage, "now")
    assert (retried.attempts, retried.last_error) == (1, "boom")
    assert await storage.next_outbox_at() == T0 + timedelta(minutes=5)

    await storage.record_delivery("now")
    await storage.record_failure("later", "gone", None)
    assert await storage.next_outbox_at() is None
    assert await storage.due_outbox_items(T0 + timedelta(days=365)) == []


# The three run outcomes


async def test_send_delivers_once(storage: Storage) -> None:
    channel = FakeChannel()
    dispatcher = Dispatcher(storage, {"a1": {"ops": channel}}, clock=Clock(T0))
    await storage.add_outbox_item(_item("m1"))

    assert await dispatcher.run_due() == 1
    assert await dispatcher.run_due() == 0
    assert [i.body for i in channel.sent] == ["body m1"]


async def test_staying_silent_delivers_nothing(storage: Storage) -> None:
    channel = FakeChannel()
    tool = send_message_tool(["ops"], storage)
    worker = Worker(
        Agent(id="a1", model=FakeModel([DONE]), channels={"ops": channel}), storage, [tool]
    )
    await storage.add_event(Event(agent_id="a1", source="test", type="ping", idempotency_key="e1"))

    assert await worker.run_pending() == 1
    assert await Dispatcher(storage, {"a1": {"ops": channel}}).run_due() == 0
    assert channel.sent == []


async def test_deferred_message_is_delivered_later(storage: Storage) -> None:
    clock, channel = Clock(T0), FakeChannel()
    dispatcher = Dispatcher(storage, {"a1": {"ops": channel}}, clock=clock)
    tool = send_message_tool(["ops"], storage, clock=clock)
    result = await _call(tool, {"channel": "ops", "body": "tomorrow", "deliver_after_minutes": 60})
    assert "to be delivered at 2026-03-02T09:00:00+00:00" in result

    assert await dispatcher.run_due() == 0
    clock.now += timedelta(minutes=60)
    assert await dispatcher.run_due() == 1
    assert [i.body for i in channel.sent] == ["tomorrow"]


# Failures


async def test_failed_delivery_is_retried_with_backoff(storage: Storage) -> None:
    clock, channel = Clock(T0), FakeChannel(failures=2)
    dispatcher = Dispatcher(storage, {"a1": {"ops": channel}}, clock=clock)
    await storage.add_outbox_item(_item("m1"))

    assert await dispatcher.run_due() == 0
    assert await storage.next_outbox_at() == T0 + timedelta(seconds=10)
    clock.now += timedelta(seconds=10)
    assert await dispatcher.run_due() == 0
    assert await storage.next_outbox_at() == clock.now + timedelta(seconds=20)
    clock.now += timedelta(seconds=20)
    assert await dispatcher.run_due() == 1
    assert len(channel.sent) == 1


async def test_delivery_gives_up_after_max_attempts(storage: Storage) -> None:
    clock = Clock(T0)
    dispatcher = Dispatcher(storage, {"a1": {"ops": FakeChannel(failures=99)}}, clock=clock)
    await storage.add_outbox_item(_item("m1"))
    await storage.add_outbox_item(_item("m2", channel="nowhere"))

    for _ in range(5):
        await dispatcher.run_due()
        clock.now += timedelta(hours=1)
    assert await storage.next_outbox_at() is None  # Both marked failed.


def test_retry_delay_doubles_up_to_an_hour() -> None:
    assert [retry_delay(n).total_seconds() for n in (1, 2, 3, 10, 20)] == [10, 20, 40, 3600, 3600]


async def test_crash_during_dispatch_sends_with_the_same_key_again(tmp_path: Path) -> None:
    """A crash after sending but before recording it resends; the key lets receivers drop it."""
    path, seen_keys, delivered = tmp_path / "db.sqlite", set[str](), list[str]()

    class DedupingReceiver:
        async def send(self, item: OutboxItem) -> None:
            if item.id not in seen_keys:
                seen_keys.add(item.id)
                delivered.append(item.body)

    storage = await SqliteStorage.open(path)
    await storage.add_outbox_item(_item("m1"))

    async def crash(item_id: str) -> None:
        raise SystemExit("crashed before recording the delivery")

    storage.record_delivery = crash  # type: ignore[method-assign]
    with pytest.raises(SystemExit):
        await Dispatcher(storage, {"a1": {"ops": DedupingReceiver()}}, clock=Clock(T0)).run_due()
    await storage.close()

    storage = await SqliteStorage.open(path)
    dispatcher = Dispatcher(storage, {"a1": {"ops": DedupingReceiver()}}, clock=Clock(T0))
    assert await dispatcher.run_due() == 1
    assert await dispatcher.run_due() == 0
    assert delivered == ["body m1"]
    await storage.close()


# The send_message tool


async def test_send_message_tool(storage: Storage) -> None:
    queued: list[str] = []
    tool = send_message_tool(["ops", "team"], storage, on_queue=lambda: queued.append("x"))

    result = await _call(tool, {"channel": "team", "body": "hello"})
    assert result.startswith("Message r1:1 queued on team")
    assert queued == ["x"]
    # A resumed run repeating the call queues nothing new.
    await _call(tool, {"channel": "team", "body": "hello"})
    assert [i.id for i in await storage.due_outbox_items(T0 + timedelta(days=365))] == ["r1:1"]

    for bad in (
        {"channel": "https://evil.example", "body": "x"},
        {"channel": "ops", "body": "x", "deliver_at": "2026-03-05T12:00:00"},
        {"channel": "ops", "body": "x", "deliver_at": T0.isoformat(), "deliver_after_minutes": 5},
    ):
        with pytest.raises(ValueError):
            await _call(tool, bad, step=2)


async def test_agent_message_reaches_its_channel(storage: Storage) -> None:
    channel = FakeChannel()
    use = {
        "type": "tool_use",
        "id": "t1",
        "name": "send_message",
        "input": {"channel": "ops", "body": "PR #42 is ready"},
    }
    model = FakeModel([ModelResponse(content=[use], stop_reason="tool_use"), DONE])
    agent = Agent(id="a1", model=model, channels={"ops": channel})
    dispatcher = Dispatcher(storage, {"a1": agent.channels})
    worker = Worker(agent, storage, [send_message_tool(["ops"], storage, dispatcher.wake)])
    await storage.add_event(Event(agent_id="a1", source="test", type="pr", idempotency_key="e1"))

    await worker.run_pending()
    assert channel.sent == []  # Queued, not sent by the run itself.
    assert await dispatcher.run_due() == 1
    assert [i.body for i in channel.sent] == ["PR #42 is ready"]


async def test_dispatcher_wakes_when_a_message_is_queued(storage: Storage) -> None:
    channel = FakeChannel()
    dispatcher = Dispatcher(storage, {"a1": {"ops": channel}})
    task = asyncio.create_task(dispatcher.run_forever())
    await asyncio.sleep(0.05)
    await storage.add_outbox_item(_item("m1", at=datetime.now(UTC)))
    dispatcher.wake()
    async with asyncio.timeout(2):
        while not channel.sent:
            await asyncio.sleep(0.01)
    task.cancel()


# Channels


async def test_webhook_channel_signs_and_keys_requests() -> None:
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    await WebhookChannel("https://hooks.test/in", secret="s3cret", client=client).send(_item("m1"))

    (request,) = requests
    assert request.headers["Idempotency-Key"] == "m1"
    expected = hmac.new(b"s3cret", request.content, hashlib.sha256).hexdigest()
    assert request.headers["X-Double-Agent-Signature"] == f"sha256={expected}"
    assert signature("s3cret", request.content) == f"sha256={expected}"
    assert json.loads(request.content)["body"] == "body m1"


async def test_webhook_channel_raises_on_error_status() -> None:
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(503)))
    with pytest.raises(httpx.HTTPStatusError):
        await WebhookChannel("https://hooks.test/in", client=client).send(_item("m1"))


async def test_file_channel_appends_json_lines(tmp_path: Path) -> None:
    channel = FileChannel(tmp_path / "out.jsonl")
    await channel.send(_item("m1"))
    await channel.send(_item("m2"))
    lines = (tmp_path / "out.jsonl").read_text().splitlines()
    assert [json.loads(line)["id"] for line in lines] == ["m1", "m2"]
