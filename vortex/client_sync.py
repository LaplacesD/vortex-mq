"""Synchronous wrapper around the async VortexClient."""

from __future__ import annotations

import asyncio
from typing import Any

from vortex.client import ClientConfig, VortexClient


class VortexClientSync:
    """Synchronous wrapper for VortexClient using asyncio.run()."""

    def __init__(self, config: ClientConfig | None = None) -> None:
        self._client = VortexClient(config)

    def connect(self) -> None:
        asyncio.run(self._client.connect())

    def disconnect(self) -> None:
        asyncio.run(self._client.disconnect())

    def publish(
        self,
        exchange: str,
        routing_key: str,
        body: bytes,
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> str:
        return asyncio.run(
            self._client.publish(exchange, routing_key, body, headers, **kwargs)
        )

    def subscribe(
        self,
        queue: str,
        handler: Any,
        exchange: str | None = None,
        routing_key: str | None = None,
    ) -> None:
        asyncio.run(
            self._client.subscribe(queue, handler, exchange, routing_key)
        )

    @property
    def is_connected(self) -> bool:
        return self._client.is_connected
