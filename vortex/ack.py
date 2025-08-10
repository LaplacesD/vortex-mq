"""Acknowledgment protocol for vortex-mq."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum

import structlog

from vortex.message import Message

logger = structlog.get_logger(__name__)


class AckStatus(Enum):
    """Acknowledgment status."""

    UNACKED = "unacked"
    ACKED = "acked"
    NACKED = "nacked"
    REJECTED = "rejected"
    DEAD_LETTERED = "dead-lettered"


@dataclass
class AckEntry:
    """Track a message's acknowledgment state."""

    message_id: str
    status: AckStatus = AckStatus.UNACKED
    timestamp: float = field(default_factory=time.time)
    delivery_tag: int = 0
    consumer_tag: str = ""
    requeue_count: int = 0


class AckManager:
    """Manages message acknowledgments across queues."""

    def __init__(self) -> None:
        self._entries: dict[str, AckEntry] = {}
        self._delivery_counter = 0
        self._lock = asyncio.Lock()

    async def register(self, message: Message) -> int:
        """Register a message for tracking. Returns delivery tag."""
        async with self._lock:
            self._delivery_counter += 1
            tag = self._delivery_counter
            self._entries[message.message_id] = AckEntry(
                message_id=message.message_id,
                delivery_tag=tag,
            )
            return tag

    async def ack(self, delivery_tag: int) -> bool:
        """Mark a message as acknowledged."""
        async with self._lock:
            for entry in self._entries.values():
                if entry.delivery_tag == delivery_tag:
                    entry.status = AckStatus.ACKED
                    logger.debug("ack.confirmed", delivery_tag=delivery_tag)
                    return True
            logger.warning("ack.not_found", delivery_tag=delivery_tag)
            return False

    async def nack(self, delivery_tag: int, requeue: bool = False) -> bool:
        """Negative acknowledgment."""
        async with self._lock:
            for entry in self._entries.values():
                if entry.delivery_tag == delivery_tag:
                    entry.status = AckStatus.NACKED
                    entry.requeue_count += 1
                    logger.debug(
                        "ack.nacked",
                        delivery_tag=delivery_tag,
                        requeue=requeue,
                    )
                    return True
            return False

    async def reject(self, delivery_tag: int) -> bool:
        """Reject a message (dead-letter)."""
        async with self._lock:
            for entry in self._entries.values():
                if entry.delivery_tag == delivery_tag:
                    entry.status = AckStatus.REJECTED
                    logger.debug("ack.rejected", delivery_tag=delivery_tag)
                    return True
            return False

    async def is_acked(self, message_id: str) -> bool:
        """Check if a message was already acknowledged (for dedup)."""
        entry = self._entries.get(message_id)
        if entry is None:
            return False
        return entry.status == AckStatus.ACKED

    async def unacked_count(self) -> int:
        """Count unacknowledged messages."""
        return sum(
            1 for e in self._entries.values() if e.status == AckStatus.UNACKED
        )
