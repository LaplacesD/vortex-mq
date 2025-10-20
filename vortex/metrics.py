"""Prometheus metrics for vortex-mq."""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram, start_http_server

import structlog

logger = structlog.get_logger(__name__)

# --- Counters ---
messages_published = Counter(
    "vortex_messages_published_total",
    "Total messages published",
    ["exchange", "node_id"],
)

messages_delivered = Counter(
    "vortex_messages_delivered_total",
    "Total messages delivered to consumers",
    ["queue", "node_id"],
)

messages_dead_lettered = Counter(
    "vortex_messages_dead_lettered_total",
    "Total messages sent to dead-letter exchange",
    ["node_id"],
)

delivery_errors = Counter(
    "vortex_delivery_errors_total",
    "Total delivery errors",
    ["queue", "node_id"],
)

# --- Gauges ---
queues_count = Gauge(
    "vortex_queues",
    "Number of queues",
    ["node_id"],
)

exchanges_count = Gauge(
    "vortex_exchanges",
    "Number of exchanges",
    ["node_id"],
)

messages_in_flight = Gauge(
    "vortex_messages_in_flight",
    "Messages currently being processed",
    ["node_id"],
)

consumers_count = Gauge(
    "vortex_consumers",
    "Number of active consumers",
    ["node_id", "queue"],
)

# --- Histograms ---
message_size_bytes = Histogram(
    "vortex_message_size_bytes",
    "Message size in bytes",
    buckets=[64, 256, 1024, 4096, 16384, 65536, 262144],
)

delivery_latency_seconds = Histogram(
    "vortex_delivery_latency_seconds",
    "Message delivery latency in seconds",
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
)


class MetricsServer:
    """Prometheus metrics HTTP server."""

    def __init__(self, port: int = 9100) -> None:
        self.port = port
        self._running = False

    async def start(self) -> None:
        start_http_server(self.port)
        self._running = True
        logger.info("metrics.server.started", port=self.port)

    def stop(self) -> None:
        self._running = False
        logger.info("metrics.server.stopped")
