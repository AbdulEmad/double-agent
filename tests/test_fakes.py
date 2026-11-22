from double_agent.model import FakeModel, Message, Model, ModelResponse


async def test_fake_model_returns_scripted_responses() -> None:
    response = ModelResponse(content=[{"type": "text", "text": "hi"}], stop_reason="end_turn")
    fake = FakeModel([response])
    model: Model = fake
    messages = [Message(role="user", content=[{"type": "text", "text": "hello"}])]
    assert await model.complete("", messages, []) == response
    assert fake.requests == [messages]
