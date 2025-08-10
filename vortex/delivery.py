"""Delivery guarantee modes for vortex-mq."""

from __future__ import annotations

import asyncio
from enum import Enum
from typing import Callable, Coroutine

import structlog

from vortex.message import Message

logger = structlog.get_logger(__name__)


class DeliveryMode(Enum):
    """Delivery guarantee levels."""

    AT_MOST_ONCE = "at-most-once"
    AT_LEAST_ONCE = "at-least-once"
    EXACTLY_ONCE = "exactly-once"


DeliveryHandler = Callable[[Message], Coroutine[None, None, None]]


async def deliver_at_most_once(
    message: Message,
    handler: DeliveryHandler,
) -> None:
    """Fire-and-forget delivery. No retries, no ack needed."""
    try:
        await handler(message)
        logger.debug("delivered.at_most_once", message_id=message.message_id)
    except Exception:
        logger.warning(
            "delivery.failed.at_most_once",
            message_id=message.message_id,
            exc_info=True,
        )


async def deliver_at_least_once(
    message: Message,
    handler: DeliveryHandler,
    max_retries: int = 3,
    retry_delay: float = 1.0,
) -> None:
    """Keep retrying until the handler succeeds or max_retries exhausted."""
    for attempt in range(max_retries):
        try:
            await handler(message)
            logger.debug(
                "delivered.at_least_once",
                message_id=message.message_id,
                attempt=attempt + 1,
            )
            return
        except Exception:
            if attempt < max_retries - 1:
                wait = retry_delay * (2**attempt)
                logger.warning(
                    "delivery.retry",
                    message_id=message.message_id,
                    attempt=attempt + 1,
                    next_retry=wait,
                )
                await asyncio.sleep(wait)
            else:
                logger.error(
                    "delivery.failed.at_least_once",
                    message_id=message.message_id,
                    max_retries=max_retries,
                    exc_info=True,
                )
                raise


async def deliver_exactly_once(
    message: Message,
    handler: DeliveryHandler,
    ack_fn: Callable[[str], Coroutine[None, None, bool]] | None = None,
    max_retries: int = 5,
    retry_delay: float = 0.5,
) -> None:
    """Transactional delivery with dedup via message_id."""
    if ack_fn is not None:
        acknowledged = await ack_fn(message.message_id)
        if acknowledged:
            logger.debug(
                "delivery.exactly_once.already_acked",
                message_id=message.message_id,
            )
            return

    for attempt in range(max_retries):
        try:
            await handler(message)
            if ack_fn is not None:
                await ack_fn(message.message_id)
            logger.debug(
                "delivered.exactly_once",
                message_id=message.message_id,
                attempt=attempt + 1,
            )
            return
        except Exception:
            if attempt < max_retries - 1:
                wait = retry_delay * (2**attempt)
                logger.warning(
                    "delivery.exactly_once.retry",
                    message_id=message.message_id,
                    attempt=attempt + 1,
                    next_retry=wait,
                )
                await asyncio.sleep(wait)
            else:
                logger.error(
                    "delivery.failed.exactly_once",
                    message_id=message.message_id,
                    exc_info=True,
                )
                raise
