from pathlib import Path
from typing import Any

import httpx

from double_agent.agent import Agent, Tool
from double_agent.api import create_app
from double_agent.dispatcher import Dispatcher
from double_agent.model import FakeModel, ModelResponse
from double_agent.outbox import Channel
from double_agent.sqlite_storage import SqliteStorage
from double_agent.storage import Storage
from double_agent.types import ApprovalStatus, Event, OutboxItem
from double_agent.worker import Worker

DONE = ModelResponse(content=[{"type": "text", "text": "done"}], stop_reason="end_turn")


def _deploy_call(tool_id: str = "t1", env: str = "prod") -> ModelResponse:
    block = {"type": "tool_use", "id": tool_id, "name": "deploy", "input": {"env": env}}
    return ModelResponse(content=[block], stop_reason="tool_use")


def _event(key: str, agent_id: str = "a1") -> Event:
    return Event(agent_id=agent_id, source="test", type="ping", idempotency_key=key)


class Deploys:
    """A tool that needs approval, recording each time it actually runs."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def fn(self, input: dict[str, Any]) -> str:
        self.calls.append(input)
        return f"deployed to {input['env']}"

    def tool(self) -> Tool:
        return Tool(
            name="deploy", description="Deploy.", input_schema={}, fn=self.fn, needs_approval=True
        )


class Recorder:
    def __init__(self) -> None:
        self.sent: list[OutboxItem] = []

    async def send(self, item: OutboxItem) -> None:
        self.sent.append(item)


def _agent(model: FakeModel, deploys: Deploys, notify: Recorder | None = None) -> Agent:
    channels: dict[str, Channel] = {"ops": notify} if notify else {}
    return Agent(
        id="a1",
        model=model,
        tools=[deploys.tool()],
        channels=channels,
        approval_channel="ops" if notify else None,
    )


async def test_tool_needing_approval_pauses_the_run(storage: Storage) -> None:
    deploys, notify = Deploys(), Recorder()
    model = FakeModel([_deploy_call(), DONE])
    worker = Worker(_agent(model, deploys, notify), storage)
    await storage.add_event(_event("e1"))

    assert await worker.run_pending() == 1

    assert deploys.calls == []
    (approval,) = await storage.pending_approvals()
    assert (approval.tool_name, approval.input) == ("deploy", {"env": "prod"})
    assert await storage.unfinished_run("a1") is None  # Paused, not running.
    assert len(model.requests) == 1
    # The request was announced on the approval channel.
    assert await Dispatcher(storage, {"a1": {"ops": notify}}).run_due() == 1
    assert approval.id in notify.sent[0].body and "deploy" in notify.sent[0].body


async def test_approving_resumes_the_run_and_runs_the_tool(storage: Storage) -> None:
    deploys = Deploys()
    model = FakeModel([_deploy_call(), DONE])
    worker = Worker(_agent(model, deploys), storage)
    await storage.add_event(_event("e1"))
    await worker.run_pending()
    (approval,) = await storage.pending_approvals()

    answered = await storage.answer_approval(approval.id, True, None)
    assert answered is not None and answered.status == ApprovalStatus.APPROVED
    assert await worker.run_pending() == 1

    assert deploys.calls == [{"env": "prod"}]
    assert len(model.requests) == 2  # The first model call was replayed, not repeated.
    assert model.requests[1][-1].content[0]["content"] == "deployed to prod"
    assert await storage.unfinished_run("a1") is None
    assert await storage.start_next_run("a1") is None
    assert await storage.pending_approvals() == []


async def test_denying_returns_a_denial_to_the_agent(storage: Storage) -> None:
    deploys = Deploys()
    model = FakeModel([_deploy_call(), DONE])
    worker = Worker(_agent(model, deploys), storage)
    await storage.add_event(_event("e1"))
    await worker.run_pending()
    (approval,) = await storage.pending_approvals()

    await storage.answer_approval(approval.id, False, "not on a Friday")
    await worker.run_pending()

    assert deploys.calls == []
    result = model.requests[1][-1].content[0]
    assert (result["content"], result["is_error"]) == ("Denied by the user: not on a Friday", True)


async def test_other_events_run_while_an_approval_is_pending(storage: Storage) -> None:
    deploys = Deploys()
    model = FakeModel([_deploy_call(), DONE, DONE])
    worker = Worker(_agent(model, deploys), storage)
    await storage.add_event(_event("e1"))
    await storage.add_event(_event("e2"))

    assert await worker.run_pending() == 2  # e1 pauses, e2 runs to the end.
    assert len(await storage.pending_approvals()) == 1
    assert len(model.requests) == 2


async def test_an_approval_can_only_be_answered_once(storage: Storage) -> None:
    deploys = Deploys()
    worker = Worker(_agent(FakeModel([_deploy_call(), DONE]), deploys), storage)
    await storage.add_event(_event("e1"))
    await worker.run_pending()
    (approval,) = await storage.pending_approvals()

    assert await storage.answer_approval(approval.id, True, None) is not None
    assert await storage.answer_approval(approval.id, False, None) is None
    assert await storage.answer_approval("missing", True, None) is None


async def test_pending_approval_survives_a_restart(tmp_path: Path) -> None:
    path, deploys = tmp_path / "db.sqlite", Deploys()
    storage = await SqliteStorage.open(path)
    await storage.add_event(_event("e1"))
    await Worker(_agent(FakeModel([_deploy_call(), DONE]), deploys), storage).run_pending()
    await storage.close()

    storage = await SqliteStorage.open(path)
    (approval,) = await storage.pending_approvals()
    assert approval.tool_name == "deploy"
    await storage.answer_approval(approval.id, True, None)
    model = FakeModel([DONE])  # Only the model call after the tool is new.
    assert await Worker(_agent(model, deploys), storage).run_pending() == 1
    assert deploys.calls == [{"env": "prod"}]
    assert len(model.requests) == 1
    assert await storage.pending_approvals() == []
    await storage.close()


async def test_crash_before_pausing_creates_one_request(storage: Storage) -> None:
    """A crash after the request is stored but before the run is paused re-runs the step."""
    deploys, notify = Deploys(), Recorder()
    await storage.add_event(_event("e1"))
    run = await storage.start_next_run("a1")
    assert run is not None

    real_pause = storage.pause_run

    async def crash(run_id: str) -> None:
        raise SystemExit("crashed before pausing")

    storage.pause_run = crash  # type: ignore[method-assign]
    worker = Worker(_agent(FakeModel([_deploy_call(), DONE]), deploys, notify), storage)
    try:
        await worker.execute(run)
    except SystemExit:
        pass
    storage.pause_run = real_pause  # type: ignore[method-assign]

    assert await storage.unfinished_run("a1") is not None  # Still running after the crash.
    await Worker(_agent(FakeModel([]), deploys, notify), storage).run_pending()
    assert len(await storage.pending_approvals()) == 1
    assert await storage.unfinished_run("a1") is None
    assert await Dispatcher(storage, {"a1": {"ops": notify}}).run_due() == 1


# API


def _client(storage: Storage, worker: Worker) -> httpx.AsyncClient:
    app = create_app(storage, {worker.agent.id: worker})
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


async def test_api_lists_and_answers_approvals(storage: Storage) -> None:
    deploys = Deploys()
    worker = Worker(_agent(FakeModel([_deploy_call(), DONE]), deploys), storage)
    await storage.add_event(_event("e1"))
    await worker.run_pending()

    async with _client(storage, worker) as client:
        listed = (await client.get("/approvals")).json()
        assert [a["tool_name"] for a in listed] == ["deploy"]
        approval_id = listed[0]["id"]

        denied = await client.post(f"/approvals/{approval_id}", json={"approve": False})
        assert (denied.status_code, denied.json()["status"]) == (200, "denied")
        again = await client.post(f"/approvals/{approval_id}", json={"approve": True})
        assert again.status_code == 409
        missing = await client.post("/approvals/nope", json={"approve": True})
        assert missing.status_code == 404
        assert (await client.get("/approvals")).json() == []
