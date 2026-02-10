"""Tests for routing strategies."""

from __future__ import annotations

import pytest

from vortex.message import Message, Headers
from vortex.routing import (
    DirectRouting,
    FanoutRouting,
    HeadersRouting,
    TopicRouting,
    get_strategy,
)
from vortex.topology import Binding


class TestDirectRouting:
    def test_exact_match(self):
        strategy = DirectRouting()
        binding = Binding(queue_name="q", exchange_name="e", routing_key="my.key")
        message = Message(body=b"", routing_key="my.key")
        assert strategy.matches(binding, message)

    def test_no_match(self):
        strategy = DirectRouting()
        binding = Binding(queue_name="q", exchange_name="e", routing_key="key.a")
        message = Message(body=b"", routing_key="key.b")
        assert not strategy.matches(binding, message)


class TestTopicRouting:
    def test_exact_match(self):
        strategy = TopicRouting()
        b = Binding(queue_name="q", exchange_name="e", routing_key="orders.created")
        m = Message(body=b"", routing_key="orders.created")
        assert strategy.matches(b, m)

    def test_wildcard_star(self):
        strategy = TopicRouting()
        b = Binding(queue_name="q", exchange_name="e", routing_key="orders.*")
        m = Message(body=b"", routing_key="orders.created")
        assert strategy.matches(b, m)

    def test_wildcard_hash(self):
        strategy = TopicRouting()
        b = Binding(queue_name="q", exchange_name="e", routing_key="orders.#")
        m = Message(body=b"", routing_key="orders.created.updated")
        assert strategy.matches(b, m)

    def test_no_match(self):
        strategy = TopicRouting()
        b = Binding(queue_name="q", exchange_name="e", routing_key="orders.*")
        m = Message(body=b"", routing_key="invoices.created")
        assert not strategy.matches(b, m)


class TestFanoutRouting:
    def test_always_matches(self):
        strategy = FanoutRouting()
        b = Binding(queue_name="q", exchange_name="e")
        m = Message(body=b"", routing_key="anything")
        assert strategy.matches(b, m)


class TestHeadersRouting:
    def test_match_all(self):
        strategy = HeadersRouting()
        b = Binding(
            queue_name="q",
            exchange_name="e",
            arguments={"x-match": "all", "source": "web", "format": "json"},
        )
        m = Message(
            body=b"",
            headers=Headers(headers={"source": "web", "format": "json"}),
        )
        assert strategy.matches(b, m)

    def test_match_any(self):
        strategy = HeadersRouting()
        b = Binding(
            queue_name="q",
            exchange_name="e",
            arguments={"x-match": "any", "source": "web", "format": "xml"},
        )
        m = Message(
            body=b"",
            headers=Headers(headers={"source": "web"}),
        )
        assert strategy.matches(b, m)

    def test_no_match(self):
        strategy = HeadersRouting()
        b = Binding(
            queue_name="q",
            exchange_name="e",
            arguments={"x-match": "all", "source": "web"},
        )
        m = Message(
            body=b"",
            headers=Headers(headers={"source": "mobile"}),
        )
        assert not strategy.matches(b, m)


class TestGetStrategy:
    def test_direct(self):
        assert isinstance(get_strategy("direct"), DirectRouting)

    def test_topic(self):
        assert isinstance(get_strategy("topic"), TopicRouting)

    def test_fanout(self):
        assert isinstance(get_strategy("fanout"), FanoutRouting)

    def test_headers(self):
        assert isinstance(get_strategy("headers"), HeadersRouting)

    def test_unknown(self):
        with pytest.raises(ValueError, match="Unknown exchange type"):
            get_strategy("unknown")
