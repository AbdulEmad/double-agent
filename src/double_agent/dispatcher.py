import asyncio
import logging
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta

from double_agent.outbox import Channel
from double_agent.storage import Storage

log = logging.getLogger(__name__)

MAX_ATTEMPTS = 5
# Upper bound on one sleep, so a wall-clock change or a laptop waking from sleep is noticed.
MAX_SLEEP_SECONDS = 60.0


def _now() -> datetime:
    return datetime.now(UTC)


def retry_delay(attempts: int) -> timedelta:
    """Wait before the next attempt, after `attempts` failures: 10s, 20s, 40s, ... up to 1 hour."""
    return timedelta(seconds=min(10 * 2 ** (attempts - 1), 3600))


class Dispatcher:
    """Delivers queued messages through each agent's channels, retrying failures with backoff.

    A message is marked sent only after its channel accepts it, so a crash in between means it
    is sent again on restart. Channels pass the message id along so receivers can drop repeats.
    """

    def __init__(
        self,
        storage: Storage,
        channels: Mapping[str, Mapping[str, Channel]],
        clock: Callable[[], datetime] = _now,
        max_attempts: int = MAX_ATTEMPTS,
    ) -> None:
        """`channels` maps each agent id to that agent's channels by name."""
        self.storage = storage
        self.channels = channels
        self.clock = clock
        self.max_attempts = max_attempts
        self._wake = asyncio.Event()

    def wake(self) -> None:
        """Recheck the outbox now, for example after a message is queued."""
        self._wake.set()

    async def run_due(self) -> int:
        """Try every due message once. Returns how many were delivered."""
        delivered = 0
        for item in await self.storage.due_outbox_items(self.clock()):
            channel = self.channels.get(item.agent_id, {}).get(item.channel)
            if channel is None:
                await self.storage.record_failure(item.id, f"Unknown channel: {item.channel}", None)
                continue
            try:
                await channel.send(item)
            except Exception as exc:
                attempts = item.attempts + 1
                retry_at = None
                if attempts < self.max_attempts:
                    retry_at = self.clock() + retry_delay(attempts)
                log.warning("Delivery of %s failed (attempt %s): %s", item.id, attempts, exc)
                await self.storage.record_failure(item.id, f"{type(exc).__name__}: {exc}", retry_at)
                continue
            await self.storage.record_delivery(item.id)
            delivered += 1
        return delivered

    async def run_forever(self) -> None:
        while True:
            self._wake.clear()
            await self.run_due()
            next_at = await self.storage.next_outbox_at()
            timeout = MAX_SLEEP_SECONDS
            if next_at is not None:
                timeout = min(timeout, max(0.0, (next_at - self.clock()).total_seconds()))
            try:
                await asyncio.wait_for(self._wake.wait(), timeout)
            except TimeoutError:
                pass
