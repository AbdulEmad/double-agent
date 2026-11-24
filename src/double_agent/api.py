from typing import Any
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Response, status
from pydantic import BaseModel, Field

from double_agent.memory import INSTRUCTIONS, MAX_FILE_CHARS, memory_path
from double_agent.safety import check_text
from double_agent.storage import Storage
from double_agent.tasks import render_task
from double_agent.types import Approval, Event, MemoryFile, MemoryVersion, Task, TaskStatus
from double_agent.worker import Worker


class EventIn(BaseModel):
    agent_id: str
    type: str
    idempotency_key: str
    source: str = "webhook"
    subject: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


class EventAccepted(BaseModel):
    duplicate: bool


class MemoryWrite(BaseModel):
    # None deletes the file.
    content: str | None


def _operator_path(path: str) -> str:
    """A path the operator may read or write: the standing instructions or a memory file."""
    full = "/" + path.lstrip("/")
    if full == INSTRUCTIONS:
        return full
    try:
        return memory_path(full)
    except ValueError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from None


class ApprovalAnswer(BaseModel):
    approve: bool
    reason: str | None = None


def create_app(storage: Storage, workers: dict[str, Worker]) -> FastAPI:
    app = FastAPI(title="double-agent")

    @app.post("/events", status_code=status.HTTP_202_ACCEPTED)
    async def post_event(body: EventIn, response: Response) -> EventAccepted:
        """Add an event to an agent's inbox. Repeat deliveries with the same key are ignored."""
        worker = workers.get(body.agent_id)
        if worker is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, f"Unknown agent: {body.agent_id}")
        if not await storage.add_event(Event(**body.model_dump())):
            response.status_code = status.HTTP_200_OK
            return EventAccepted(duplicate=True)
        worker.wake()
        return EventAccepted(duplicate=False)

    @app.get("/approvals")
    async def list_approvals() -> list[Approval]:
        """Every agent's approval requests that are waiting for an answer."""
        return await storage.pending_approvals()

    @app.post("/approvals/{approval_id}")
    async def answer_approval(approval_id: str, body: ApprovalAnswer) -> Approval:
        """Approve or deny a pending request. The paused run resumes straight away."""
        answered = await storage.answer_approval(approval_id, body.approve, body.reason)
        if answered is None:
            if await storage.get_approval(approval_id) is None:
                raise HTTPException(status.HTTP_404_NOT_FOUND, f"No approval {approval_id}")
            raise HTTPException(status.HTTP_409_CONFLICT, f"Approval {approval_id} is answered")
        if worker := workers.get(answered.agent_id):
            worker.wake()
        return answered

    @app.get("/agents/{agent_id}/memory")
    async def list_memory(agent_id: str) -> list[MemoryFile]:
        """The agent's memory files and standing instructions."""
        return await storage.list_memory(agent_id)

    @app.get("/agents/{agent_id}/memory/{path:path}")
    async def read_memory(agent_id: str, path: str) -> dict[str, str]:
        full = _operator_path(path)
        content = await storage.read_memory(agent_id, full)
        if content is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, f"No file {full} for {agent_id}")
        return {"path": full, "content": content}

    @app.put("/agents/{agent_id}/memory/{path:path}")
    async def write_memory(agent_id: str, path: str, body: MemoryWrite) -> dict[str, str]:
        """Write or delete a file as the operator. The only way to set standing instructions."""
        full = _operator_path(path)
        try:
            check_text(full, body.content, MAX_FILE_CHARS)
        except ValueError as exc:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from None
        result = f"Deleted {full}" if body.content is None else f"Saved {full}"
        await storage.write_memory(
            agent_id, {full: body.content}, f"operator:{uuid4().hex}", result, "operator", None
        )
        return {"path": full, "result": result}

    @app.get("/agents/{agent_id}/memory-history/{path:path}")
    async def memory_history(agent_id: str, path: str) -> list[MemoryVersion]:
        """Every write to a file: what it said, and which run and source wrote it."""
        return await storage.memory_history(agent_id, _operator_path(path))

    @app.get("/tasks")
    async def list_tasks(
        agent_id: str | None = None, status: TaskStatus | None = TaskStatus.OPEN
    ) -> list[Task]:
        """Tasks, newest first. Open tasks unless another status is asked for."""
        return await storage.list_tasks(agent_id, status)

    @app.get("/tasks/{agent_id}/{task_id}")
    async def get_task(agent_id: str, task_id: str) -> dict[str, str]:
        """One task, as the markdown the agent sees."""
        view = await storage.get_task(agent_id, task_id)
        if view is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, f"No task {task_id} for {agent_id}")
        return {"markdown": render_task(view)}

    return app
