from datetime import datetime
from typing import Protocol

from double_agent.types import Event, EventStatus, Run, RunStatus, Step, Timer, TimerStatus


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

    async def add_timer(self, timer: Timer) -> bool:
        """Store a timer. Returns False if a timer with its id already exists."""
        ...

    async def due_timers(self, now: datetime) -> list[Timer]:
        """Return active timers due at or before `now`, earliest first."""
        ...

    async def next_due_at(self) -> datetime | None:
        """Return when the earliest active timer is due, if there is one."""
        ...

    async def fire_timer(self, timer: Timer, event: Event, next_due_at: datetime | None) -> bool:
        """Add a timer's event and move the timer on, in one step.

        The timer moves to `next_due_at`, or is marked done when that is None. Does nothing and
        returns False if the timer has already fired for `timer.due_at`, so two schedulers can
        never fire the same tick twice.
        """
        ...


class InMemoryStorage:
    def __init__(self) -> None:
        self._events: dict[str, Event] = {}
        self._runs: dict[str, Run] = {}
        self._steps: dict[str, list[Step]] = {}
        self._timers: dict[str, Timer] = {}

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

    async def add_timer(self, timer: Timer) -> bool:
        if timer.id in self._timers:
            return False
        self._timers[timer.id] = timer.model_copy()
        return True

    async def due_timers(self, now: datetime) -> list[Timer]:
        due = [t for t in self._active_timers() if t.due_at <= now]
        return [t.model_copy() for t in sorted(due, key=lambda t: t.due_at)]

    async def next_due_at(self) -> datetime | None:
        return min((t.due_at for t in self._active_timers()), default=None)

    async def fire_timer(self, timer: Timer, event: Event, next_due_at: datetime | None) -> bool:
        stored = self._timers.get(timer.id)
        if stored is None or stored.status != TimerStatus.ACTIVE or stored.due_at != timer.due_at:
            return False
        await self.add_event(event)
        if next_due_at is None:
            stored.status = TimerStatus.DONE
        else:
            stored.due_at = next_due_at
        return True

    def _active_timers(self) -> list[Timer]:
        return [t for t in self._timers.values() if t.status == TimerStatus.ACTIVE]
