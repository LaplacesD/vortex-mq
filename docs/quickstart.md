# Quickstart Guide

This guide will help you get started with vortex-mq in under 5 minutes.

## Installation

```bash
pip install vortex-mq
```

For optional features:

```bash
pip install vortex-mq[admin]       # HTTP admin API
pip install vortex-mq[monitoring]  # Prometheus metrics
pip install vortex-mq[serialization]  # MessagePack support
pip install vortex-mq[dev]         # Development tools
```

## Minimal Example

Create a file `hello_vortex.py`:

```python
import asyncio
from vortex.broker import Broker
from vortex.message import Message, Headers

async def main():
    # Create and start the broker
    broker = Broker()
    await broker.start()

    # Set up topology
    broker.declare_exchange("greetings", "direct")
    broker.declare_queue("hello-queue")
    broker.bind("hello-queue", "greetings", "hello")

    # Publish a message
    msg = Message(
        body=b"Hello, vortex-mq!",
        routing_key="hello",
        headers=Headers(content_type="text/plain"),
    )
    await broker.publish("greetings", msg)

    # Consume the message
    received = await broker._queues["hello-queue"].get()
    print(f"Received: {received.body.decode()}")

    # Clean up
    await broker.stop()

asyncio.run(main())
```

Run it:

```bash
python hello_vortex.py
# Output: Received: Hello, vortex-mq!
```

## Pub/Sub Example

```python
import asyncio
from vortex.broker import Broker
from vortex.message import Message

async def pub_sub():
    broker = Broker()
    await broker.start()

    # Fanout exchange broadcasts to all queues
    broker.declare_exchange("broadcast", "fanout")
    q1 = broker.declare_queue("subscriber-1")
    q2 = broker.declare_queue("subscriber-2")
    broker.bind("subscriber-1", "broadcast")
    broker.bind("subscriber-2", "broadcast")

    msg = Message(body=b"broadcast message", routing_key="")
    await broker.publish("broadcast", msg)

    print(f"Q1 got: {(await q1.get()).body.decode()}")
    print(f"Q2 got: {(await q2.get()).body.decode()}")

    await broker.stop()

asyncio.run(pub_sub())
```

## Next Steps

- Read the [Architecture](./architecture.md) guide
- Explore [Clustering](./clustering.md) for distributed deployment
- Check the [examples](../examples/) for real-world patterns
