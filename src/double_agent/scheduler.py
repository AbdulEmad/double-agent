import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import croniter

from double_agent.storage import Storage
from double_agent.types import Event, Timer

# Upper bound on one sleep, so a wall-clock change or a laptop waking from sleep is noticed.
MAX_SLEEP_SECONDS = 60.0


def _now() -> datetime:
    return datetime.now(UTC)


def next_cron_time(cron: str, tz: str, after: datetime) -> datetime:
    """Return the first time after `after` that matches `cron`, read in time zone `tz`."""
    local = after.astimezone(ZoneInfo(tz))
    return croniter(cron, local).get_next(datetime).astimezone(UTC)


def once(agent_id: str, at: datetime, note: str = "") -> Timer:
    """A timer that fires once at `at`."""
    return Timer(agent_id=agent_id, due_at=at, note=note)


def recurring(
    agent_id: str, cron: str, tz: str = "UTC", note: str = "", now: datetime | None = None
) -> Timer:
    """A timer that fires on every tick of `cron` in time zone `tz`."""
    if not croniter.is_valid(cron):
        raise ValueError(f"Invalid cron expression: {cron!r}")
    try:
        ZoneInfo(tz)
    except ZoneInfoNotFoundError:
        raise ValueError(f"Unknown time zone: {tz!r}") from None
    first = next_cron_time(cron, tz, now or _now())
    return Timer(agent_id=agent_id, due_at=first, cron=cron, tz=tz, note=note)


def timer_event(timer: Timer) -> Event:
    """The inbox event for one tick of a timer. Its key makes each tick fire at most once."""
    return Event(
        agent_id=timer.agent_id,
        source="timer",
        type="timer",
        payload={"timer_id": timer.id, "note": timer.note, "due_at": timer.due_at.isoformat()},
        idempotency_key=f"timer:{timer.id}:{timer.due_at.isoformat()}",
    )


class Scheduler:
    """Turns due timers into inbox events and sleeps until the next one is due."""

    def __init__(
        self,
        storage: Storage,
        on_event: Callable[[str], None] = lambda agent_id: None,
        clock: Callable[[], datetime] = _now,
    ) -> None:
        self.storage = storage
        self.on_event = on_event
        self.clock = clock
        self._wake = asyncio.Event()

    def wake(self) -> None:
        """Recheck timers now, for example after a new timer is added."""
        self._wake.set()

    async def run_due(self) -> int:
        """Fire every due timer once. Returns how many ticks this call fired.

        A recurring timer moves to its first tick after now, so ticks missed while the
        process was down fire once on restart rather than once each.
        """
        now = self.clock()
        fired = 0
        for timer in await self.storage.due_timers(now):
            next_due = next_cron_time(timer.cron, timer.tz, now) if timer.cron else None
            if await self.storage.fire_timer(timer, timer_event(timer), next_due):
                self.on_event(timer.agent_id)
                fired += 1
        return fired

    async def run_forever(self) -> None:
        while True:
            self._wake.clear()
            await self.run_due()
            next_due = await self.storage.next_due_at()
            timeout = MAX_SLEEP_SECONDS
            if next_due is not None:
                timeout = min(timeout, max(0.0, (next_due - self.clock()).total_seconds()))
            try:
                await asyncio.wait_for(self._wake.wait(), timeout)
            except TimeoutError:
                pass
