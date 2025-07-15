"""Exchange, Queue, and Binding models for vortex-mq."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

from vortex.message import Message


@dataclass
class Exchange:
    """An exchange that routes messages to queues."""

    name: str
    exchange_type: str = "direct"  # direct, topic, fanout, headers
    durable: bool = False
    auto_delete: bool = False
    arguments: dict[str, Any] = field(default_factory=dict)


@dataclass
class Queue:
    """A queue that stores messages for consumers."""

    name: str
    durable: bool = False
    exclusive: bool = False
    auto_delete: bool = False
    arguments: dict[str, Any] = field(default_factory=dict)
    _messages: list[Message] = field(default_factory=list, repr=False)
    _consumers: list[asyncio.Queue[Message]] = field(default_factory=list, repr=False)

    async def put(self, message: Message) -> None:
        """Enqueue a message for consumers."""
        self._messages.append(message)
        for consumer_q in self._consumers:
            await consumer_q.put(message)

    async def get(self) -> Message | None:
        """Dequeue a message (non-blocking if no consumers)."""
        if not self._messages:
            return None
        return self._messages.pop(0)

    def consume(self) -> asyncio.Queue[Message]:
        """Register a consumer and return its queue."""
        q: asyncio.Queue[Message] = asyncio.Queue()
        self._consumers.append(q)
        return q

    def ack(self, message_id: str) -> bool:
        """Acknowledge a message (placeholder)."""
        return True

    def nack(self, message_id: str, requeue: bool = False) -> bool:
        """Negative acknowledgement (placeholder)."""
        return True

    @property
    def message_count(self) -> int:
        return len(self._messages)

    @property
    def consumer_count(self) -> int:
        return len(self._consumers)


@dataclass
class Binding:
    """A binding between a queue and an exchange."""

    queue_name: str
    exchange_name: str
    routing_key: str = ""
    arguments: dict[str, Any] = field(default_factory=dict)
