# vortex-mq Documentation

Welcome to the vortex-mq documentation. vortex-mq is a distributed in-memory message broker designed for high-throughput, low-latency messaging in distributed systems.

## Getting Started

- [Quickstart](./quickstart.md) — Set up and run your first vortex-mq application
- [Architecture](./architecture.md) — Understanding the broker internals
- [Clustering](./clustering.md) — Deploying a multi-node cluster

## Core Concepts

- **Broker**: Central message routing engine
- **Exchange**: Receives messages and routes them to queues
- **Queue**: Stores messages for consumers
- **Binding**: Connects queues to exchanges with routing rules
- **Message**: Unit of data transferred through the system

## Examples

The [examples](../examples/) directory contains runnable scripts:

- `pub_sub.py` — Publish-subscribe messaging pattern
- `work_queue.py` — Work queue with competing consumers
- `rpc.py` — Request-reply (RPC) pattern

## API Reference

### Broker

```python
from vortex.broker import Broker

broker = Broker(node_id="my-node")
await broker.start()
broker.declare_exchange("my-exchange", "direct")
broker.declare_queue("my-queue")
broker.bind("my-queue", "my-exchange", "my-key")
await broker.publish("my-exchange", message)
await broker.stop()
```

### Message

```python
from vortex.message import Message, Headers

msg = Message(
    body=b"hello",
    routing_key="greetings",
    headers=Headers(content_type="text/plain"),
)
```

## License

MIT
