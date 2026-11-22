from double_agent.model import FakeModel, Message, Model, ModelResponse
from double_agent.outbox import InMemoryOutbox, Outbox
from double_agent.storage import InMemoryStorage, Storage
from double_agent.types import Event, OutboxItem, Step, StepKind


def _event(key: str, agent_id: str = "a1") -> Event:
    return Event(agent_id=agent_id, source="webhook", type="ping", idempotency_key=key)


def _item(key: str) -> OutboxItem:
    return OutboxItem(
        agent_id="a1", channel="webhook", target="https://x.test", body="hi", dedupe_key=key
    )


async def test_storage_ignores_duplicate_events() -> None:
    storage: Storage = InMemoryStorage()
    assert await storage.add_event(_event("k1"))
    assert not await storage.add_event(_event("k1"))
    assert await storage.claim_next_event("a1") is not None
    assert await storage.claim_next_event("a1") is None


async def test_storage_claims_oldest_event_for_agent() -> None:
    storage: Storage = InMemoryStorage()
    first, second = _event("k1"), _event("k2")
    await storage.add_event(_event("other", agent_id="a2"))
    await storage.add_event(second)
    await storage.add_event(first)
    second.created_at = first.created_at.replace(year=first.created_at.year + 1)

    claimed = [await storage.claim_next_event("a1"), await storage.claim_next_event("a1")]
    assert [e.idempotency_key for e in claimed if e] == ["k1", "k2"]
    assert await storage.claim_next_event("a1") is None


async def test_storage_loads_steps_in_order() -> None:
    storage: Storage = InMemoryStorage()
    await storage.save_step(Step(run_id="r1", index=1, kind=StepKind.TOOL_CALL))
    await storage.save_step(Step(run_id="r1", index=0, kind=StepKind.MODEL_CALL))
    assert [s.index for s in await storage.load_steps("r1")] == [0, 1]
    assert await storage.load_steps("r2") == []


async def test_fake_model_returns_scripted_responses() -> None:
    response = ModelResponse(content=[{"type": "text", "text": "hi"}], stop_reason="end_turn")
    fake = FakeModel([response])
    model: Model = fake
    messages = [Message(role="user", content=[{"type": "text", "text": "hello"}])]
    assert await model.complete(messages, []) == response
    assert fake.requests == [messages]


async def test_outbox_dedupes_and_sends_each_item_once() -> None:
    outbox: Outbox = InMemoryOutbox()
    sent: list[str] = []

    async def send(item: OutboxItem) -> None:
        sent.append(item.dedupe_key)

    assert await outbox.enqueue(_item("m1"))
    assert not await outbox.enqueue(_item("m1"))
    assert await outbox.dispatch(send) == 1
    assert await outbox.dispatch(send) == 0
    assert sent == ["m1"]
