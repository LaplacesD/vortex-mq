"""Test fixtures and configuration for vortex-mq tests."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio

from vortex.broker import Broker
from vortex.message import Message, Headers


@pytest.fixture
def broker() -> Broker:
    return Broker(node_id="test-node")


@pytest_asyncio.fixture
async def started_broker() -> AsyncGenerator[Broker, None]:
    b = Broker(node_id="test-node")
    await b.start()
    yield b
    await b.stop()


@pytest.fixture
def sample_message() -> Message:
    return Message(
        body=b"hello world",
        routing_key="test.key",
        headers=Headers(content_type="text/plain"),
    )


@pytest.fixture
def sample_headers() -> Headers:
    return Headers(
        content_type="application/json",
        priority=5,
        headers={"source": "test"},
    )
