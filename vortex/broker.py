"""Main broker class for vortex-mq."""

from __future__ import annotations

import asyncio
from typing import Any

import structlog

from vortex.message import Message
from vortex.topology import Binding, Exchange, Queue

logger = structlog.get_logger(__name__)


class Broker:
    """In-memory message broker supporting exchanges, queues, and bindings."""

    def __init__(self, node_id: str | None = None) -> None:
        self.node_id = node_id or "default"
        self._exchanges: dict[str, Exchange] = {}
        self._queues: dict[str, Queue] = {}
        self._bindings: list[Binding] = []
        self._running = False
        self._loop: asyncio.AbstractEventLoop | None = None

    async def start(self) -> None:
        """Start the broker event loop."""
        self._running = True
        self._loop = asyncio.get_running_loop()
        logger.info("broker.started", node_id=self.node_id)

    async def stop(self) -> None:
        """Gracefully stop the broker."""
        self._running = False
        logger.info("broker.stopped", node_id=self.node_id)

    def declare_exchange(self, name: str, exchange_type: str = "direct") -> Exchange:
        """Declare a new exchange."""
        if name in self._exchanges:
            raise ValueError(f"Exchange '{name}' already exists")
        exchange = Exchange(name=name, exchange_type=exchange_type)
        self._exchanges[name] = exchange
        logger.debug("exchange.declared", name=name, type=exchange_type)
        return exchange

    def declare_queue(self, name: str) -> Queue:
        """Declare a new queue."""
        if name in self._queues:
            raise ValueError(f"Queue '{name}' already exists")
        queue = Queue(name=name)
        self._queues[name] = queue
        logger.debug("queue.declared", name=name)
        return queue

    def bind(
        self,
        queue_name: str,
        exchange_name: str,
        routing_key: str = "",
    ) -> Binding:
        """Bind a queue to an exchange with a routing key."""
        if queue_name not in self._queues:
            raise ValueError(f"Queue '{queue_name}' not found")
        if exchange_name not in self._exchanges:
            raise ValueError(f"Exchange '{exchange_name}' not found")
        binding = Binding(
            queue_name=queue_name,
            exchange_name=exchange_name,
            routing_key=routing_key,
        )
        self._bindings.append(binding)
        logger.debug(
            "binding.created",
            queue=queue_name,
            exchange=exchange_name,
            key=routing_key,
        )
        return binding

    async def publish(self, exchange_name: str, message: Message) -> None:
        """Publish a message to an exchange."""
        if exchange_name not in self._exchanges:
            raise ValueError(f"Exchange '{exchange_name}' not found")
        exchange = self._exchanges[exchange_name]
        matched_queues = self._route(exchange, message)
        for queue in matched_queues:
            await queue.put(message)
        logger.debug(
            "message.published",
            exchange=exchange_name,
            routing_key=message.routing_key,
            matched_queues=[q.name for q in matched_queues],
        )

    def _route(self, exchange: Exchange, message: Message) -> list[Queue]:
        """Match message to queues via bindings."""
        matched: list[Queue] = []
        for binding in self._bindings:
            if binding.exchange_name != exchange.name:
                continue
            if exchange.exchange_type == "direct":
                if binding.routing_key == message.routing_key:
                    matched.append(self._queues[binding.queue_name])
            elif exchange.exchange_type == "fanout":
                matched.append(self._queues[binding.queue_name])
            elif exchange.exchange_type == "topic":
                if self._topic_match(binding.routing_key, message.routing_key):
                    matched.append(self._queues[binding.queue_name])
            elif exchange.exchange_type == "headers":
                matched.append(self._queues[binding.queue_name])
        return matched

    @staticmethod
    def _topic_match(pattern: str, routing_key: str) -> bool:
        """Simple topic-style wildcard matching."""
        parts = pattern.split(".")
        keys = routing_key.split(".")
        for p, k in zip(parts, keys):
            if p == "#":
                return True
            if p != "*" and p != k:
                return False
        return len(parts) == len(keys)
