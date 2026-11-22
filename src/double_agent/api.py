from typing import Any

from fastapi import FastAPI, HTTPException, Response, status
from pydantic import BaseModel, Field

from double_agent.storage import Storage
from double_agent.types import Event
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

    return app
