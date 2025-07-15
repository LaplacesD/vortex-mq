# vortex-mq

**Distributed in-memory message broker** with at-least-once delivery guarantees.

## Features

- In-memory message broker with exchange/queue/binding topology
- Multiple routing strategies: direct, topic, fanout, headers
- Delivery guarantees: at-least-once, at-most-once, exactly-once
- Clustering with node discovery and consistent hashing
- Consumer groups with round-robin distribution
- HTTP admin API and Prometheus metrics
- SASL-like authentication
- Async and sync client SDKs

## Quick Start

```python
import asyncio
from vortex.broker import Broker
from vortex.message import Message, Headers

async def main():
    broker = Broker(node_id="node-1")
    await broker.start()

    exchange = broker.declare_exchange("events", "topic")
    queue = broker.declare_queue("my-queue")
    broker.bind("my-queue", "events", "orders.*")

    msg = Message(
        body=b"hello world",
        routing_key="orders.created",
        headers=Headers(content_type="text/plain"),
    )
    await broker.publish("events", msg)

    await broker.stop()

asyncio.run(main())
```

## License

MIT
