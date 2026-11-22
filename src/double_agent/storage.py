from datetime import datetime
from typing import Protocol

from double_agent.types import (
    Approval,
    ApprovalStatus,
    Event,
    EventStatus,
    OutboxItem,
    OutboxStatus,
    Run,
    RunStatus,
    Step,
    Timer,
    TimerStatus,
)


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
        """Return the agent's running run, if any. Runs awaiting approval are not included."""
        ...

    async def pause_run(self, run_id: str) -> None:
        """Mark a run as awaiting approval, so the agent can run other events meanwhile."""
        ...

    async def resume_answered_run(self, agent_id: str) -> Run | None:
        """Set one of the agent's paused runs running again, if one has an answered approval.

        Does nothing and returns None while the agent has another run in progress.
        """
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

    async def list_timers(self, agent_id: str) -> list[Timer]:
        """Return the agent's active timers, earliest first."""
        ...

    async def cancel_timer(self, agent_id: str, timer_id: str) -> bool:
        """Cancel one of the agent's active timers. Returns False if there was no such timer."""
        ...

    async def fire_timer(self, timer: Timer, event: Event, next_due_at: datetime | None) -> bool:
        """Add a timer's event and move the timer on, in one step.

        The timer moves to `next_due_at`, or is marked done when that is None. Does nothing and
        returns False if the timer has already fired for `timer.due_at`, so two schedulers can
        never fire the same tick twice.
        """
        ...

    async def add_approval(self, approval: Approval) -> bool:
        """Store an approval request. Returns False if one with its id already exists."""
        ...

    async def get_approval(self, approval_id: str) -> Approval | None: ...

    async def pending_approvals(self) -> list[Approval]:
        """Return every agent's unanswered approval requests, oldest first."""
        ...

    async def answer_approval(
        self, approval_id: str, approved: bool, reason: str | None
    ) -> Approval | None:
        """Record a decision on a pending request. Returns None if it is unknown or answered."""
        ...

    async def add_outbox_item(self, item: OutboxItem) -> bool:
        """Queue a message. Returns False if an item with its id is already queued."""
        ...

    async def due_outbox_items(self, now: datetime) -> list[OutboxItem]:
        """Return pending items whose next attempt is due at or before `now`, oldest first."""
        ...

    async def next_outbox_at(self) -> datetime | None:
        """Return when the earliest pending item's next attempt is due, if there is one."""
        ...

    async def record_delivery(self, item_id: str) -> None:
        """Mark an item sent."""
        ...

    async def record_failure(self, item_id: str, error: str, retry_at: datetime | None) -> None:
        """Count a failed attempt. Retry at `retry_at`, or mark the item failed when it is None."""
        ...


class InMemoryStorage:
    def __init__(self) -> None:
        self._events: dict[str, Event] = {}
        self._runs: dict[str, Run] = {}
        self._steps: dict[str, list[Step]] = {}
        self._timers: dict[str, Timer] = {}
        self._outbox: dict[str, OutboxItem] = {}
        self._approvals: dict[str, Approval] = {}

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

    async def pause_run(self, run_id: str) -> None:
        self._runs[run_id].status = RunStatus.AWAITING_APPROVAL

    async def resume_answered_run(self, agent_id: str) -> Run | None:
        if await self.unfinished_run(agent_id):
            return None
        answered = {
            a.run_id for a in self._approvals.values() if a.status != ApprovalStatus.PENDING
        }
        for run in self._runs.values():
            if (
                run.agent_id == agent_id
                and run.status == RunStatus.AWAITING_APPROVAL
                and run.id in answered
            ):
                run.status = RunStatus.RUNNING
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

    async def list_timers(self, agent_id: str) -> list[Timer]:
        mine = [t for t in self._active_timers() if t.agent_id == agent_id]
        return [t.model_copy() for t in sorted(mine, key=lambda t: t.due_at)]

    async def cancel_timer(self, agent_id: str, timer_id: str) -> bool:
        timer = self._timers.get(timer_id)
        if timer is None or timer.agent_id != agent_id or timer.status != TimerStatus.ACTIVE:
            return False
        timer.status = TimerStatus.CANCELLED
        return True

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

    async def add_outbox_item(self, item: OutboxItem) -> bool:
        if item.id in self._outbox:
            return False
        self._outbox[item.id] = item.model_copy()
        return True

    async def due_outbox_items(self, now: datetime) -> list[OutboxItem]:
        due = [i for i in self._pending_outbox() if i.next_attempt_at <= now]
        due.sort(key=lambda i: (i.next_attempt_at, i.created_at))
        return [i.model_copy() for i in due]

    async def next_outbox_at(self) -> datetime | None:
        return min((i.next_attempt_at for i in self._pending_outbox()), default=None)

    async def record_delivery(self, item_id: str) -> None:
        self._outbox[item_id].status = OutboxStatus.SENT

    async def record_failure(self, item_id: str, error: str, retry_at: datetime | None) -> None:
        item = self._outbox[item_id]
        item.attempts += 1
        item.last_error = error
        if retry_at is None:
            item.status = OutboxStatus.FAILED
        else:
            item.next_attempt_at = retry_at

    def _pending_outbox(self) -> list[OutboxItem]:
        return [i for i in self._outbox.values() if i.status == OutboxStatus.PENDING]

    async def add_approval(self, approval: Approval) -> bool:
        if approval.id in self._approvals:
            return False
        self._approvals[approval.id] = approval.model_copy()
        return True

    async def get_approval(self, approval_id: str) -> Approval | None:
        approval = self._approvals.get(approval_id)
        return approval.model_copy() if approval else None

    async def pending_approvals(self) -> list[Approval]:
        pending = [a for a in self._approvals.values() if a.status == ApprovalStatus.PENDING]
        return [a.model_copy() for a in sorted(pending, key=lambda a: a.created_at)]

    async def answer_approval(
        self, approval_id: str, approved: bool, reason: str | None
    ) -> Approval | None:
        approval = self._approvals.get(approval_id)
        if approval is None or approval.status != ApprovalStatus.PENDING:
            return None
        approval.status = ApprovalStatus.APPROVED if approved else ApprovalStatus.DENIED
        approval.reason = reason
        return approval.model_copy()
