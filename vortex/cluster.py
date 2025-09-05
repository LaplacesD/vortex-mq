"""Cluster node discovery and management for vortex-mq."""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable

import structlog

logger = structlog.get_logger(__name__)


@dataclass
class ClusterNode:
    """Represents a node in the vortex cluster."""

    node_id: str
    host: str
    port: int
    status: str = "active"  # active, inactive, leaving
    last_heartbeat: float = field(default_factory=time.time)
    metadata: dict[str, str] = field(default_factory=dict)


class ClusterDiscovery:
    """Node discovery and cluster membership management."""

    def __init__(
        self,
        node_id: str | None = None,
        heartbeat_interval: float = 5.0,
        heartbeat_timeout: float = 15.0,
    ) -> None:
        self.node_id = node_id or str(uuid.uuid4())
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout
        self._nodes: dict[str, ClusterNode] = {}
        self._self: ClusterNode | None = None
        self._running = False
        self._on_join: list[Callable[[ClusterNode], None]] = []
        self._on_leave: list[Callable[[ClusterNode], None]] = []

    def register_self(self, host: str, port: int) -> ClusterNode:
        """Register this node in the cluster."""
        node = ClusterNode(
            node_id=self.node_id,
            host=host,
            port=port,
        )
        self._nodes[self.node_id] = node
        self._self = node
        logger.info("cluster.node.registered", node_id=self.node_id, host=host, port=port)
        return node

    def add_node(self, node: ClusterNode) -> None:
        """Add or update a cluster node."""
        self._nodes[node.node_id] = node
        self._notify_join(node)
        logger.debug("cluster.node.added", node_id=node.node_id)

    def remove_node(self, node_id: str) -> None:
        """Remove a node from the cluster."""
        node = self._nodes.pop(node_id, None)
        if node:
            self._notify_leave(node)
            logger.info("cluster.node.removed", node_id=node_id)

    def get_node(self, node_id: str) -> ClusterNode | None:
        """Get a specific node."""
        return self._nodes.get(node_id)

    def get_active_nodes(self) -> list[ClusterNode]:
        """Get all nodes considered active."""
        now = time.time()
        return [
            n
            for n in self._nodes.values()
            if n.status == "active"
            and (now - n.last_heartbeat) < self.heartbeat_timeout
        ]

    def get_all_nodes(self) -> list[ClusterNode]:
        """Get all known nodes."""
        return list(self._nodes.values())

    def on_join(self, callback: Callable[[ClusterNode], None]) -> None:
        """Register a callback for node join events."""
        self._on_join.append(callback)

    def on_leave(self, callback: Callable[[ClusterNode], None]) -> None:
        """Register a callback for node leave events."""
        self._on_leave.append(callback)

    async def start_heartbeat(self) -> None:
        """Start the periodic heartbeat loop."""
        self._running = True
        while self._running:
            if self._self:
                self._self.last_heartbeat = time.time()
            await self._check_heartbeats()
            await asyncio.sleep(self.heartbeat_interval)

    async def stop_heartbeat(self) -> None:
        """Stop the heartbeat loop."""
        self._running = False

    async def _check_heartbeats(self) -> None:
        """Remove nodes that have timed out."""
        now = time.time()
        stale = [
            nid
            for nid, n in self._nodes.items()
            if nid != self.node_id
            and (now - n.last_heartbeat) > self.heartbeat_timeout
        ]
        for nid in stale:
            self.remove_node(nid)

    def _notify_join(self, node: ClusterNode) -> None:
        for cb in self._on_join:
            try:
                cb(node)
            except Exception:
                logger.exception("cluster.on_join.error", node_id=node.node_id)

    def _notify_leave(self, node: ClusterNode) -> None:
        for cb in self._on_leave:
            try:
                cb(node)
            except Exception:
                logger.exception("cluster.on_leave.error", node_id=node.node_id)
