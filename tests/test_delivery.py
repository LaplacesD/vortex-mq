"""Tests for delivery guarantees."""

from __future__ import annotations

import pytest

from vortex.delivery import (
    deliver_at_most_once,
    deliver_at_least_once,
    deliver_exactly_once,
)


class TestDelivery:
    @pytest.mark.asyncio
    async def test_at_most_once_success(self, sample_message):
        calls = []

        async def handler(msg):
            calls.append(msg.message_id)

        await deliver_at_most_once(sample_message, handler)
        assert len(calls) == 1

    @pytest.mark.asyncio
    async def test_at_most_once_failure_silent(self, sample_message):
        async def failing_handler(msg):
            raise RuntimeError("fail")

        # Should not raise
        await deliver_at_most_once(sample_message, failing_handler)

    @pytest.mark.asyncio
    async def test_at_least_once_success(self, sample_message):
        calls = []

        async def handler(msg):
            calls.append(msg.message_id)

        await deliver_at_least_once(sample_message, handler, max_retries=3)
        assert len(calls) == 1

    @pytest.mark.asyncio
    async def test_at_least_once_retry_then_succeed(self, sample_message):
        attempt_count = 0

        async def handler(msg):
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 3:
                raise RuntimeError("not yet")

        await deliver_at_least_once(sample_message, handler, max_retries=5, retry_delay=0.01)
        assert attempt_count == 3

    @pytest.mark.asyncio
    async def test_at_least_once_exhaust_retries(self, sample_message):
        async def always_fail(msg):
            raise RuntimeError("always")

        with pytest.raises(RuntimeError):
            await deliver_at_least_once(sample_message, always_fail, max_retries=2, retry_delay=0.01)

    @pytest.mark.asyncio
    async def test_exactly_once_dedup(self, sample_message):
        acked_ids = set()
        calls = []

        async def ack_fn(mid):
            acked_ids.add(mid)
            return True

        async def handler(msg):
            calls.append(msg.message_id)

        # First delivery
        await deliver_exactly_once(sample_message, handler, ack_fn, max_retries=1)
        assert len(calls) == 1
        assert sample_message.message_id in acked_ids

        # Second delivery — should skip (already acked)
        await deliver_exactly_once(sample_message, handler, ack_fn, max_retries=1)
        assert len(calls) == 1  # not called again
