"""HTTP Admin API for vortex-mq using aiohttp."""

from __future__ import annotations

from typing import Any

import structlog
from aiohttp import web

from vortex.broker import Broker

logger = structlog.get_logger(__name__)


class AdminAPI:
    """HTTP admin interface for managing the broker."""

    def __init__(self, broker: Broker, host: str = "127.0.0.1", port: int = 8090) -> None:
        self.broker = broker
        self.host = host
        self.port = port
        self._app = web.Application()
        self._runner: web.AppRunner | None = None
        self._setup_routes()

    def _setup_routes(self) -> None:
        self._app.router.add_get("/health", self._handle_health)
        self._app.router.add_get("/api/exchanges", self._handle_list_exchanges)
        self._app.router.add_get("/api/queues", self._handle_list_queues)
        self._app.router.add_get("/api/queues/{name}", self._handle_get_queue)
        self._app.router.add_get("/api/bindings", self._handle_list_bindings)
        self._app.router.add_get("/api/metrics", self._handle_metrics)

    async def start(self) -> None:
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.host, self.port)
        await site.start()
        logger.info("admin.api.started", host=self.host, port=self.port)

    async def stop(self) -> None:
        if self._runner:
            await self._runner.cleanup()
            logger.info("admin.api.stopped")

    async def _handle_health(self, request: web.Request) -> web.Response:
        return web.json_response(
            {
                "status": "ok",
                "node_id": self.broker.node_id,
                "running": self.broker._running,
            }
        )

    async def _handle_list_exchanges(self, request: web.Request) -> web.Response:
        exchanges = []
        for name, ex in self.broker._exchanges.items():
            exchanges.append(
                {
                    "name": name,
                    "type": ex.exchange_type,
                    "durable": ex.durable,
                    "auto_delete": ex.auto_delete,
                }
            )
        return web.json_response({"exchanges": exchanges})

    async def _handle_list_queues(self, request: web.Request) -> web.Response:
        queues = []
        for name, q in self.broker._queues.items():
            queues.append(
                {
                    "name": name,
                    "message_count": q.message_count,
                    "consumer_count": q.consumer_count,
                    "durable": q.durable,
                    "auto_delete": q.auto_delete,
                }
            )
        return web.json_response({"queues": queues})

    async def _handle_get_queue(self, request: web.Request) -> web.Response:
        name = request.match_info["name"]
        q = self.broker._queues.get(name)
        if q is None:
            return web.json_response({"error": "queue not found"}, status=404)
        return web.json_response(
            {
                "name": name,
                "message_count": q.message_count,
                "consumer_count": q.consumer_count,
                "durable": q.durable,
                "auto_delete": q.auto_delete,
            }
        )

    async def _handle_list_bindings(self, request: web.Request) -> web.Response:
        bindings = [
            {
                "queue": b.queue_name,
                "exchange": b.exchange_name,
                "routing_key": b.routing_key,
            }
            for b in self.broker._bindings
        ]
        return web.json_response({"bindings": bindings})

    async def _handle_metrics(self, request: web.Request) -> web.Response:
        return web.json_response(
            {
                "node_id": self.broker.node_id,
                "exchanges": len(self.broker._exchanges),
                "queues": len(self.broker._queues),
                "bindings": len(self.broker._bindings),
            }
        )
