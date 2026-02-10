"""Tests for persistence backends."""

from __future__ import annotations

import tempfile

import pytest

from vortex.message import Message, Headers
from vortex.persistence import DiskPersistence, InMemoryPersistence


class TestInMemoryPersistence:
    @pytest.mark.asyncio
    async def test_save_and_load(self):
        p = InMemoryPersistence()
        msg = Message(body=b"test", routing_key="k")
        await p.save(msg)
        loaded = await p.load(msg.message_id)
        assert loaded is not None
        assert loaded.body == b"test"
        assert loaded.message_id == msg.message_id

    @pytest.mark.asyncio
    async def test_load_nonexistent(self):
        p = InMemoryPersistence()
        loaded = await p.load("no-such-id")
        assert loaded is None

    @pytest.mark.asyncio
    async def test_delete(self):
        p = InMemoryPersistence()
        msg = Message(body=b"del", routing_key="k")
        await p.save(msg)
        await p.delete(msg.message_id)
        assert await p.load(msg.message_id) is None

    @pytest.mark.asyncio
    async def test_list_all(self):
        p = InMemoryPersistence()
        m1 = Message(body=b"1", routing_key="k1")
        m2 = Message(body=b"2", routing_key="k2")
        await p.save(m1)
        await p.save(m2)
        all_msgs = await p.list_all()
        assert len(all_msgs) == 2


class TestDiskPersistence:
    @pytest.mark.asyncio
    async def test_save_and_load(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            p = DiskPersistence(directory=tmpdir)
            msg = Message(body=b"disk test", routing_key="k")
            await p.save(msg)
            loaded = await p.load(msg.message_id)
            assert loaded is not None
            assert loaded.body == b"disk test"

    @pytest.mark.asyncio
    async def test_persists_across_instances(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            msg = Message(body=b"persist", routing_key="k")
            p1 = DiskPersistence(directory=tmpdir)
            await p1.save(msg)

            # New instance reading same directory
            p2 = DiskPersistence(directory=tmpdir)
            loaded = await p2.load(msg.message_id)
            assert loaded is not None
            assert loaded.body == b"persist"

    @pytest.mark.asyncio
    async def test_delete_from_disk(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            p = DiskPersistence(directory=tmpdir)
            msg = Message(body=b"delete-me", routing_key="k")
            await p.save(msg)
            await p.delete(msg.message_id)
            loaded = await p.load(msg.message_id)
            assert loaded is None
