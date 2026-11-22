import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from double_agent.scheduler import Scheduler, next_cron_time, once, recurring, timer_event
from double_agent.sqlite_storage import SqliteStorage
from double_agent.storage import Storage
from double_agent.types import EventStatus

T0 = datetime(2026, 3, 2, 8, 0, tzinfo=UTC)  # A Monday.


class Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


async def _events(storage: Storage, agent_id: str = "a1") -> list[str]:
    """Drain the agent's inbox and return each event's note."""
    notes: list[str] = []
    while run := await storage.start_next_run(agent_id):
        event = await storage.get_event(run.event_id)
        assert event is not None and event.status == EventStatus.CLAIMED
        notes.append(event.payload["note"])
        await storage.finish_run(run.id)
    return notes


# Storage


async def test_add_timer_ignores_a_repeated_id(storage: Storage) -> None:
    timer = once("a1", T0, note="x")
    assert await storage.add_timer(timer)
    assert not await storage.add_timer(timer)


async def test_due_timers_and_next_due_at(storage: Storage) -> None:
    assert await storage.next_due_at() is None
    later, sooner = once("a1", T0 + timedelta(hours=1)), once("a1", T0)
    await storage.add_timer(later)
    await storage.add_timer(sooner)

    assert await storage.next_due_at() == T0
    assert await storage.due_timers(T0 - timedelta(seconds=1)) == []
    assert [t.id for t in await storage.due_timers(T0 + timedelta(hours=2))] == [
        sooner.id,
        later.id,
    ]


async def test_one_off_timer_fires_once(storage: Storage) -> None:
    timer = once("a1", T0, note="ping")
    await storage.add_timer(timer)

    assert await storage.fire_timer(timer, timer_event(timer), None)
    assert not await storage.fire_timer(timer, timer_event(timer), None)
    assert await storage.due_timers(T0 + timedelta(days=1)) == []
    assert await storage.next_due_at() is None
    assert await _events(storage) == ["ping"]


async def test_stale_fire_is_ignored(storage: Storage) -> None:
    timer = recurring("a1", "0 9 * * *", now=T0)
    await storage.add_timer(timer)
    next_day = timer.due_at + timedelta(days=1)

    assert await storage.fire_timer(timer, timer_event(timer), next_day)
    # A second scheduler still holding the old tick must not fire it again.
    assert not await storage.fire_timer(timer, timer_event(timer), next_day)
    assert await storage.next_due_at() == next_day
    assert len(await _events(storage)) == 1


# Scheduler


async def test_scheduler_fires_due_timers_and_wakes_their_agents(storage: Storage) -> None:
    clock, woken = Clock(T0), list[str]()
    scheduler = Scheduler(storage, on_event=woken.append, clock=clock)
    await storage.add_timer(once("a1", T0 + timedelta(minutes=5), note="a1 later"))
    await storage.add_timer(once("a2", T0, note="a2 now"))

    assert await scheduler.run_due() == 1
    assert woken == ["a2"]
    clock.now += timedelta(minutes=5)
    assert await scheduler.run_due() == 1
    assert await scheduler.run_due() == 0
    assert woken == ["a2", "a1"]
    assert await _events(storage, "a1") == ["a1 later"]
    assert await _events(storage, "a2") == ["a2 now"]


async def test_recurring_timer_fires_once_per_tick(storage: Storage) -> None:
    clock = Clock(T0)
    scheduler = Scheduler(storage, clock=clock)
    await storage.add_timer(recurring("a1", "0 9 * * *", note="daily", now=T0))

    for _ in range(3):
        clock.now += timedelta(days=1)
        assert await scheduler.run_due() == 1
        assert await scheduler.run_due() == 0
    assert await _events(storage) == ["daily"] * 3


async def test_missed_ticks_fire_once_after_downtime(storage: Storage) -> None:
    clock = Clock(T0)
    scheduler = Scheduler(storage, clock=clock)
    await storage.add_timer(recurring("a1", "0 * * * *", note="hourly", now=T0))

    clock.now = T0 + timedelta(hours=5, minutes=30)  # Five ticks missed.
    assert await scheduler.run_due() == 1
    assert await storage.next_due_at() == T0 + timedelta(hours=6)
    assert await _events(storage) == ["hourly"]


async def test_two_schedulers_fire_each_tick_once(storage: Storage) -> None:
    clock = Clock(T0)
    timers = [once("a1", T0, note=f"t{i}") for i in range(10)]
    for timer in timers:
        await storage.add_timer(timer)
    first, second = Scheduler(storage, clock=clock), Scheduler(storage, clock=clock)

    fired = await asyncio.gather(first.run_due(), second.run_due())
    assert sum(fired) == 10
    assert sorted(await _events(storage)) == sorted(t.note for t in timers)


async def test_timer_survives_restart_and_fires_once(tmp_path: Path) -> None:
    path = tmp_path / "db.sqlite"
    first = await SqliteStorage.open(path)
    await first.add_timer(once("a1", T0 + timedelta(hours=1), note="after restart"))
    assert await Scheduler(first, clock=Clock(T0)).run_due() == 0
    await first.close()

    second = await SqliteStorage.open(path)
    scheduler = Scheduler(second, clock=Clock(T0 + timedelta(hours=2)))
    assert await scheduler.run_due() == 1
    assert await scheduler.run_due() == 0
    assert await _events(second) == ["after restart"]
    await second.close()


async def test_run_forever_sleeps_until_the_timer_is_due(storage: Storage) -> None:
    woken: list[str] = []
    scheduler = Scheduler(storage, on_event=woken.append)
    task = asyncio.create_task(scheduler.run_forever())
    await asyncio.sleep(0.05)  # Nothing scheduled: the scheduler is asleep.

    await storage.add_timer(once("a1", datetime.now(UTC) + timedelta(seconds=0.2)))
    scheduler.wake()
    async with asyncio.timeout(3):
        while not woken:
            await asyncio.sleep(0.01)
    task.cancel()
    assert woken == ["a1"]


# Cron and time zones


def test_cron_is_read_in_the_timer_time_zone() -> None:
    # 9:00 in London is 9:00 UTC in winter and 8:00 UTC in summer.
    assert next_cron_time("0 9 * * *", "Europe/London", T0) == datetime(2026, 3, 2, 9, tzinfo=UTC)
    summer = datetime(2026, 7, 1, tzinfo=UTC)
    assert next_cron_time("0 9 * * *", "Europe/London", summer) == datetime(
        2026, 7, 1, 8, tzinfo=UTC
    )


def test_invalid_timers_are_rejected() -> None:
    with pytest.raises(ValueError):
        recurring("a1", "not a cron")
    with pytest.raises(ValueError):
        recurring("a1", "0 9 * * *", tz="Mars/Olympus")
    with pytest.raises(ValueError):
        once("a1", datetime(2026, 1, 1))  # No time zone.
