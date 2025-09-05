"""Consumer groups for vortex-mq."""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field
from typing import Callable, Coroutine

import structlog

from vortex.message import Message

logger = structlog.get_logger(__name__)

ConsumerHandler = Callable[[Message], Coroutine[None, None, None]]


@dataclass
class Consumer:
    """Represents a single consumer in a consumer group."""

    consumer_tag: str
    handler: ConsumerHandler
    queue_name: str
    active: bool = True
    prefetch_count: int = 1


class ConsumerGroup:
    """Manages a group of consumers sharing a queue."""

    def __init__(self, queue_name: str, group_id: str | None = None) -> None:
        self.queue_name = queue_name
        self.group_id = group_id or str(uuid.uuid4())
        self._consumers: dict[str, Consumer] = {}
        self._lock = asyncio.Lock()

    @property
    def consumer_tags(self) -> list[str]:
        return list(self._consumers.keys())

    @property
    def count(self) -> int:
        return len(self._consumers)

    def add_consumer(self, handler: ConsumerHandler, prefetch: int = 1) -> str:
        """Add a consumer to the group. Returns consumer tag."""
        tag = str(uuid.uuid4())
        self._consumers[tag] = Consumer(
            consumer_tag=tag,
            handler=handler,
            queue_name=self.queue_name,
            prefetch_count=prefetch,
        )
        logger.debug(
            "consumer.added",
            consumer_tag=tag,
            queue=self.queue_name,
            group=self.group_id,
        )
        return tag

    def remove_consumer(self, consumer_tag: str) -> bool:
        """Remove a consumer from the group."""
        consumer = self._consumers.pop(consumer_tag, None)
        if consumer:
            logger.debug(
                "consumer.removed",
                consumer_tag=consumer_tag,
                queue=self.queue_name,
            )
            return True
        return False

    def get_consumer(self, consumer_tag: str) -> Consumer | None:
        """Get a specific consumer."""
        return self._consumers.get(consumer_tag)

    def get_active_consumers(self) -> list[Consumer]:
        """Get all active (non-paused) consumers."""
        return [c for c in self._consumers.values() if c.active]

    def pause_consumer(self, consumer_tag: str) -> bool:
        """Pause a specific consumer."""
        consumer = self._consumers.get(consumer_tag)
        if consumer:
            consumer.active = False
            return True
        return False

    def resume_consumer(self, consumer_tag: str) -> bool:
        """Resume a paused consumer."""
        consumer = self._consumers.get(consumer_tag)
        if consumer:
            consumer.active = True
            return True
        return False
