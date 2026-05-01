# vortex-mq

**Distributed in-memory message broker** with at-least-once delivery guarantees.

vortex-mq is a lightweight, high-performance message broker designed for distributed systems. It operates entirely in-memory for low latency while offering optional disk persistence for durability. The broker supports multiple routing strategies, consumer groups, clustering with consistent hashing, and comprehensive monitoring.

## Key Features

- **In-memory first**: Ultra-low latency message passing with optional disk persistence
- **Multiple routing strategies**: Direct, Topic, Fanout, and Headers exchanges
- **Delivery guarantees**: At-most-once, at-least-once, and exactly-once semantics
- **Clustering**: Automatic node discovery, heartbeat-based failure detection, consistent hash partitioning
- **Consumer groups**: Load-balanced message distribution with round-robin
- **Retry & dead-letter**: Exponential backoff with configurable dead-letter exchanges
- **Admin API**: HTTP REST API for broker management and monitoring
- **Prometheus metrics**: Built-in metrics endpoint for observability
- **Authentication**: SASL-like authentication with PBKDF2 password hashing
- **Serialization**: Support for JSON (orjson), MessagePack, and Pickle
- **Client SDKs**: Async and sync Python clients with publish/subscribe API

## Quick Start

```bash
pip install vortex-mq
```

```python
import asyncio
from vortex.broker import Broker
from vortex.message import Message, Headers

async def main():
    broker = Broker(node_id="node-1")
    await broker.start()

    exchange = broker.declare_exchange("events", "topic")
    queue = broker.declare_queue("orders")
    broker.bind("orders", "events", "orders.*")

    msg = Message(
        body=b"Order #1234 created",
        routing_key="orders.created",
        headers=Headers(content_type="text/plain"),
    )
    await broker.publish("events", msg)

    received = await queue.get()
    print(f"Received: {received.body.decode()}")

    await broker.stop()

asyncio.run(main())
```

## Documentation

Full documentation is available in the [docs](./docs/) directory:

- [Quickstart](./docs/quickstart.md)
- [Architecture](./docs/architecture.md)
- [Clustering](./docs/clustering.md)

## Examples

See the [examples](./examples/) directory:

- [Pub/Sub](./examples/pub_sub.py) — publish-subscribe pattern
- [Work Queue](./examples/work_queue.py) — competing consumers
- [RPC](./examples/rpc.py) — request-reply pattern

## License

MIT — see [LICENSE](./LICENSE)
