"""Retry with backoff and dead-letter support for vortex-mq."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Callable

import structlog

from vortex.message import Message

logger = structlog.get_logger(__name__)


@dataclass
class RetryPolicy:
    """Configuration for retry behavior."""

    max_retries: int = 3
    initial_delay: float = 1.0
    backoff_factor: float = 2.0
    max_delay: float = 60.0
    jitter: bool = True


@dataclass
class DeadLetterConfig:
    """Configuration for dead-letter handling."""

    exchange: str = "dlx"
    routing_key: str = "dead-letter"
    max_retries_before_dead_letter: int = 3


DeadLetterHandler = Callable[[Message], None]


class RetryManager:
    """Manages retry logic with exponential backoff and dead-letter."""

    def __init__(
        self,
        policy: RetryPolicy | None = None,
        dead_letter_config: DeadLetterConfig | None = None,
        dead_letter_handler: DeadLetterHandler | None = None,
    ) -> None:
        self.policy = policy or RetryPolicy()
        self.dead_letter_config = dead_letter_config or DeadLetterConfig()
        self.dead_letter_handler = dead_letter_handler
        self._retry_counts: dict[str, int] = {}
        self._lock = asyncio.Lock()

    async def should_retry(self, message: Message) -> bool:
        """Determine if a message should be retried."""
        async with self._lock:
            count = self._retry_counts.get(message.message_id, 0)
            if count < self.policy.max_retries:
                return True
            if count < self.dead_letter_config.max_retries_before_dead_letter:
                return True
            return False

    async def record_attempt(self, message: Message) -> float:
        """Record a retry attempt and return the delay before next attempt."""
        async with self._lock:
            count = self._retry_counts.get(message.message_id, 0) + 1
            self._retry_counts[message.message_id] = count

            delay = min(
                self.policy.initial_delay * (self.policy.backoff_factor ** (count - 1)),
                self.policy.max_delay,
            )
            if self.policy.jitter:
                import random as _random

                delay *= 0.5 + _random.random() * 0.5

            logger.debug(
                "retry.recorded",
                message_id=message.message_id,
                attempt=count,
                next_delay=round(delay, 3),
            )
            return delay

    async def dead_letter(self, message: Message) -> None:
        """Send a message to the dead-letter exchange."""
        message.exchange = self.dead_letter_config.exchange
        message.routing_key = self.dead_letter_config.routing_key
        message.redelivered = True

        if self.dead_letter_handler:
            self.dead_letter_handler(message)

        logger.warning(
            "message.dead_lettered",
            message_id=message.message_id,
            exchange=message.exchange,
        )

    async def retry_count(self, message_id: str) -> int:
        """Get the current retry count for a message."""
        return self._retry_counts.get(message_id, 0)
