"""Tests for vortex broker."""

from __future__ import annotations

import pytest

from vortex.broker import Broker
from vortex.message import Message, Headers


class TestBroker:
    def test_create_broker(self):
        b = Broker(node_id="node-1")
        assert b.node_id == "node-1"
        assert not b._running

    @pytest.mark.asyncio
    async def test_start_stop(self):
        b = Broker()
        await b.start()
        assert b._running is True
        await b.stop()
        assert b._running is False

    @pytest.mark.asyncio
    async def test_declare_exchange(self, broker):
        ex = broker.declare_exchange("test.ex", "direct")
        assert ex.name == "test.ex"
        assert ex.exchange_type == "direct"

    @pytest.mark.asyncio
    async def test_declare_exchange_duplicate(self, broker):
        broker.declare_exchange("test.ex")
        with pytest.raises(ValueError, match="already exists"):
            broker.declare_exchange("test.ex")

    @pytest.mark.asyncio
    async def test_declare_queue(self, broker):
        q = broker.declare_queue("test.q")
        assert q.name == "test.q"

    @pytest.mark.asyncio
    async def test_bind(self, broker):
        broker.declare_exchange("ex1")
        broker.declare_queue("q1")
        b = broker.bind("q1", "ex1", "key1")
        assert b.queue_name == "q1"
        assert b.exchange_name == "ex1"
        assert b.routing_key == "key1"

    @pytest.mark.asyncio
    async def test_bind_nonexistent_queue(self, broker):
        broker.declare_exchange("ex1")
        with pytest.raises(ValueError, match="not found"):
            broker.bind("no-q", "ex1", "k")

    @pytest.mark.asyncio
    async def test_publish_direct(self, broker):
        broker.declare_exchange("ex1", "direct")
        q = broker.declare_queue("q1")
        broker.bind("q1", "ex1", "my.key")

        msg = Message(body=b"test", routing_key="my.key")
        await broker.publish("ex1", msg)

        fetched = await q.get()
        assert fetched is not None
        assert fetched.body == b"test"

    @pytest.mark.asyncio
    async def test_publish_fanout(self, broker):
        broker.declare_exchange("ex1", "fanout")
        q1 = broker.declare_queue("q1")
        q2 = broker.declare_queue("q2")
        broker.bind("q1", "ex1")
        broker.bind("q2", "ex1")

        msg = Message(body=b"fanout", routing_key="ignored")
        await broker.publish("ex1", msg)

        assert (await q1.get()) is not None
        assert (await q2.get()) is not None
