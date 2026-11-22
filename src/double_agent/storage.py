from typing import Protocol

from double_agent.types import Event, EventStatus, Run, RunStatus, Step


class Storage(Protocol):
    async def add_event(self, event: Event) -> bool:
        """Store an event. Returns False if its idempotency key was already stored."""
        ...

    async def get_event(self, event_id: str) -> Event | None: ...

    async def start_next_run(self, agent_id: str) -> Run | None:
        """Claim the agent's oldest pending event and start a run for it, in one step.

        Returns None if the agent has no pending events or already has an unfinished run.
        """
        ...

    async def unfinished_run(self, agent_id: str) -> Run | None:
        """Return the agent's run that started but did not finish, if any."""
        ...

    async def finish_run(self, run_id: str) -> None:
        """Mark a run finished and its event done."""
        ...

    async def save_step(self, step: Step) -> None: ...

    async def load_steps(self, run_id: str) -> list[Step]:
        """Return a run's saved steps in order."""
        ...


class InMemoryStorage:
    def __init__(self) -> None:
        self._events: dict[str, Event] = {}
        self._runs: dict[str, Run] = {}
        self._steps: dict[str, list[Step]] = {}

    async def add_event(self, event: Event) -> bool:
        if event.idempotency_key in self._events:
            return False
        self._events[event.idempotency_key] = event.model_copy()
        return True

    async def get_event(self, event_id: str) -> Event | None:
        for event in self._events.values():
            if event.id == event_id:
                return event.model_copy()
        return None

    async def start_next_run(self, agent_id: str) -> Run | None:
        if await self.unfinished_run(agent_id):
            return None
        pending = [
            e
            for e in self._events.values()
            if e.agent_id == agent_id and e.status == EventStatus.PENDING
        ]
        if not pending:
            return None
        event = min(pending, key=lambda e: e.created_at)
        event.status = EventStatus.CLAIMED
        run = Run(agent_id=agent_id, event_id=event.id)
        self._runs[run.id] = run
        return run.model_copy()

    async def unfinished_run(self, agent_id: str) -> Run | None:
        for run in self._runs.values():
            if run.agent_id == agent_id and run.status == RunStatus.RUNNING:
                return run.model_copy()
        return None

    async def finish_run(self, run_id: str) -> None:
        run = self._runs[run_id]
        run.status = RunStatus.FINISHED
        for event in self._events.values():
            if event.id == run.event_id:
                event.status = EventStatus.DONE

    async def save_step(self, step: Step) -> None:
        self._steps.setdefault(step.run_id, []).append(step.model_copy())

    async def load_steps(self, run_id: str) -> list[Step]:
        steps = sorted(self._steps.get(run_id, []), key=lambda s: s.index)
        return [s.model_copy() for s in steps]
