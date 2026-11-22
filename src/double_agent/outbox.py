import hashlib
import hmac
import json
from pathlib import Path
from typing import Protocol

import httpx

from double_agent.types import OutboxItem


class Channel(Protocol):
    """Somewhere an agent can send messages, such as a webhook or a file."""

    async def send(self, item: OutboxItem) -> None:
        """Deliver one message. Raise to have the dispatcher retry later."""
        ...


def _payload(item: OutboxItem) -> dict[str, str]:
    return {
        "id": item.id,
        "agent_id": item.agent_id,
        "channel": item.channel,
        "body": item.body,
        "created_at": item.created_at.isoformat(),
    }


def signature(secret: str, body: bytes) -> str:
    """The `X-Double-Agent-Signature` value for a request body: `sha256=<hex HMAC>`."""
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


class WebhookChannel:
    """POSTs each message as JSON.

    Every request carries an `Idempotency-Key` header with the message id. A message can be
    sent again after a crash, so receivers should ignore keys they have already seen. With a
    `secret`, requests are also signed so the receiver can check where they came from.
    """

    def __init__(
        self,
        url: str,
        secret: str | None = None,
        timeout: float = 10.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.url = url
        self.secret = secret
        self._client = client or httpx.AsyncClient(timeout=timeout)

    async def send(self, item: OutboxItem) -> None:
        body = json.dumps(_payload(item)).encode()
        headers = {"Content-Type": "application/json", "Idempotency-Key": item.id}
        if self.secret:
            headers["X-Double-Agent-Signature"] = signature(self.secret, body)
        response = await self._client.post(self.url, content=body, headers=headers)
        response.raise_for_status()


class FileChannel:
    """Appends each message to a file as one JSON line. Useful for demos and local testing."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    async def send(self, item: OutboxItem) -> None:
        with self.path.open("a") as f:
            f.write(json.dumps(_payload(item)) + "\n")
