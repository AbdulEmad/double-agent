from typing import Protocol

from double_agent.types import Event, EventStatus, Step


class Storage(Protocol):
    async def add_event(self, event: Event) -> bool:
        """Store an event. Returns False if its idempotency key was already stored."""
        ...

    async def claim_next_event(self, agent_id: str) -> Event | None:
        """Mark the oldest pending event for an agent as claimed and return it."""
        ...

    async def save_step(self, step: Step) -> None: ...

    async def load_steps(self, run_id: str) -> list[Step]:
        """Return a run's saved steps in order."""
        ...


class InMemoryStorage:
    def __init__(self) -> None:
        self._events: dict[str, Event] = {}
        self._steps: dict[str, list[Step]] = {}

    async def add_event(self, event: Event) -> bool:
        if event.idempotency_key in self._events:
            return False
        self._events[event.idempotency_key] = event
        return True

    async def claim_next_event(self, agent_id: str) -> Event | None:
        pending = [
            e
            for e in self._events.values()
            if e.agent_id == agent_id and e.status == EventStatus.PENDING
        ]
        if not pending:
            return None
        event = min(pending, key=lambda e: e.created_at)
        event.status = EventStatus.CLAIMED
        return event

    async def save_step(self, step: Step) -> None:
        self._steps.setdefault(step.run_id, []).append(step)

    async def load_steps(self, run_id: str) -> list[Step]:
        return sorted(self._steps.get(run_id, []), key=lambda s: s.index)
