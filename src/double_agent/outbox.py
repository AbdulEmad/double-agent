from collections.abc import Awaitable, Callable
from typing import Protocol

from double_agent.types import OutboxItem, OutboxStatus

Sender = Callable[[OutboxItem], Awaitable[None]]


class Outbox(Protocol):
    async def enqueue(self, item: OutboxItem) -> bool:
        """Queue an item. Returns False if its dedupe key was already queued."""
        ...

    async def dispatch(self, send: Sender) -> int:
        """Send every pending item and mark it sent. Returns how many were sent."""
        ...


class InMemoryOutbox:
    def __init__(self) -> None:
        self._items: dict[str, OutboxItem] = {}

    async def enqueue(self, item: OutboxItem) -> bool:
        if item.dedupe_key in self._items:
            return False
        self._items[item.dedupe_key] = item
        return True

    async def dispatch(self, send: Sender) -> int:
        sent = 0
        for item in self._items.values():
            if item.status == OutboxStatus.PENDING:
                await send(item)
                item.status = OutboxStatus.SENT
                sent += 1
        return sent
