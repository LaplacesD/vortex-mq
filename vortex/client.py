"""Async client SDK for vortex-mq."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable

import structlog

from vortex.broker import Broker
from vortex.message import Headers, Message

logger = structlog.get_logger(__name__)

MessageHandler = Callable[[Message], Any]


@dataclass
class ClientConfig:
    """Configuration for the async client."""

    host: str = "127.0.0.1"
    port: int = 5672
    node_id: str = "client"
    username: str | None = None
    password: str | None = None
    virtual_host: str = "/"
    heartbeat: float = 30.0


class VortexClient:
    """Async client SDK for vortex-mq."""

    def __init__(self, config: ClientConfig | None = None) -> None:
        self.config = config or ClientConfig()
        self._broker: Broker | None = None
        self._connected = False
        self._handlers: dict[str, list[MessageHandler]] = {}

    async def connect(self) -> None:
        """Connect to the broker."""
        self._broker = Broker(node_id=self.config.node_id)
        await self._broker.start()
        self._connected = True
        logger.info("client.connected", node_id=self.config.node_id)

    async def disconnect(self) -> None:
        """Disconnect from the broker."""
        if self._broker:
            await self._broker.stop()
        self._connected = False
        logger.info("client.disconnected")

    async def publish(
        self,
        exchange: str,
        routing_key: str,
        body: bytes,
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> str:
        """Publish a message. Returns message_id."""
        if not self._broker or not self._connected:
            raise RuntimeError("Client not connected")

        msg = Message(
            body=body,
            routing_key=routing_key,
            headers=Headers(headers=headers or {}),
            **kwargs,
        )
        await self._broker.publish(exchange, msg)
        return msg.message_id

    async def subscribe(
        self,
        queue: str,
        handler: MessageHandler,
        exchange: str | None = None,
        routing_key: str | None = None,
    ) -> None:
        """Subscribe to a queue with an optional binding."""
        if not self._broker:
            raise RuntimeError("Client not connected")

        # Ensure queue exists
        try:
            q = self._broker.declare_queue(queue)
        except ValueError:
            q = self._broker._queues[queue]

        # Bind if exchange and routing key provided
        if exchange and routing_key is not None:
            try:
                self._broker.declare_exchange(exchange)
            except ValueError:
                pass
            self._broker.bind(queue, exchange, routing_key)

        consumer_q = q.consume()
        self._handlers.setdefault(queue, []).append(handler)

        async def _consume_loop():
            while self._connected:
                msg = await consumer_q.get()
                for h in self._handlers.get(queue, []):
                    try:
                        result = h(msg)
                        if asyncio.iscoroutine(result):
                            await result
                    except Exception:
                        logger.exception("handler.error", queue=queue)

        asyncio.create_task(_consume_loop())
        logger.info("client.subscribed", queue=queue)

    @property
    def is_connected(self) -> bool:
        return self._connected
