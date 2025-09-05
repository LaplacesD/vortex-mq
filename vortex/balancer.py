"""Load balancers for vortex-mq: round-robin and consistent hash."""

from __future__ import annotations

import hashlib
import struct
from abc import ABC, abstractmethod
from typing import Any


class LoadBalancer(ABC):
    """Abstract load balancer."""

    @abstractmethod
    def select(self, items: list[Any], key: str | None = None) -> Any | None: ...

    @abstractmethod
    def reset(self) -> None: ...


class RoundRobinBalancer(LoadBalancer):
    """Round-robin load balancer."""

    def __init__(self) -> None:
        self._index = 0

    def select(self, items: list[Any], key: str | None = None) -> Any | None:
        if not items:
            return None
        item = items[self._index % len(items)]
        self._index = (self._index + 1) % len(items)
        return item

    def reset(self) -> None:
        self._index = 0


class ConsistentHashBalancer(LoadBalancer):
    """Consistent hash load balancer using ring hashing."""

    def __init__(self, virtual_nodes: int = 100) -> None:
        self.virtual_nodes = virtual_nodes
        self._ring: dict[int, Any] = {}
        self._sorted_keys: list[int] = []
        self._items: list[Any] = []

    def select(self, items: list[Any], key: str | None = None) -> Any | None:
        if not items:
            return None

        if items != self._items:
            self._build_ring(items)
            self._items = items

        if key is None:
            return items[0]

        hash_key = self._hash(key)
        if not self._sorted_keys:
            return items[0]

        for ring_key in self._sorted_keys:
            if hash_key <= ring_key:
                return self._ring[ring_key]

        # Wrap around
        return self._ring[self._sorted_keys[0]]

    def _build_ring(self, items: list[Any]) -> None:
        self._ring = {}
        for item in items:
            for i in range(self.virtual_nodes):
                key = self._hash(f"{item}:{i}")
                self._ring[key] = item
        self._sorted_keys = sorted(self._ring.keys())

    @staticmethod
    def _hash(value: str) -> int:
        h = hashlib.sha256(value.encode()).digest()
        return struct.unpack(">Q", h[:8])[0]

    def reset(self) -> None:
        self._ring = {}
        self._sorted_keys = []
        self._items = []
