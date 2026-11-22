from double_agent.model import FakeModel, Message, Model, ModelResponse
from double_agent.outbox import InMemoryOutbox, Outbox
from double_agent.types import OutboxItem


def _item(key: str) -> OutboxItem:
    return OutboxItem(
        agent_id="a1", channel="webhook", target="https://x.test", body="hi", dedupe_key=key
    )


async def test_fake_model_returns_scripted_responses() -> None:
    response = ModelResponse(content=[{"type": "text", "text": "hi"}], stop_reason="end_turn")
    fake = FakeModel([response])
    model: Model = fake
    messages = [Message(role="user", content=[{"type": "text", "text": "hello"}])]
    assert await model.complete("", messages, []) == response
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
