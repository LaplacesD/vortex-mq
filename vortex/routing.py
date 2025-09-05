"""Routing strategies for vortex-mq: direct, topic, fanout, headers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from vortex.message import Message
from vortex.topology import Binding


class RoutingStrategy(ABC):
    """Abstract base for routing strategies."""

    @abstractmethod
    def matches(self, binding: Binding, message: Message) -> bool: ...


class DirectRouting(RoutingStrategy):
    """Route messages by exact routing key match."""

    def matches(self, binding: Binding, message: Message) -> bool:
        return binding.routing_key == message.routing_key


class TopicRouting(RoutingStrategy):
    """Route messages by topic pattern with wildcards (*, #)."""

    def matches(self, binding: Binding, message: Message) -> bool:
        pattern = binding.routing_key
        key = message.routing_key
        pattern_parts = pattern.split(".")
        key_parts = key.split(".")

        for p, k in zip(pattern_parts, key_parts):
            if p == "#":
                return True
            if p != "*" and p != k:
                return False
        return len(pattern_parts) == len(key_parts)


class FanoutRouting(RoutingStrategy):
    """Route messages to all bound queues (ignores routing key)."""

    def matches(self, binding: Binding, message: Message) -> bool:
        return True


class HeadersRouting(RoutingStrategy):
    """Route messages by matching header key-value pairs."""

    def matches(self, binding: Binding, message: Message) -> bool:
        if not binding.arguments:
            return False
        x_match = binding.arguments.get("x-match", "all")

        matches_required = []
        for key, expected_value in binding.arguments.items():
            if key.startswith("x-"):
                continue
            actual_value = message.headers.headers.get(key)
            matches_required.append(str(actual_value) == str(expected_value))

        if not matches_required:
            return False

        if x_match == "all":
            return all(matches_required)
        else:  # any
            return any(matches_required)


def get_strategy(exchange_type: str) -> RoutingStrategy:
    """Factory to get the appropriate routing strategy."""
    strategies = {
        "direct": DirectRouting(),
        "topic": TopicRouting(),
        "fanout": FanoutRouting(),
        "headers": HeadersRouting(),
    }
    strategy = strategies.get(exchange_type)
    if strategy is None:
        raise ValueError(f"Unknown exchange type: {exchange_type}")
    return strategy
