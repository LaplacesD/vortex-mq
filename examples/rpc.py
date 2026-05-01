"""RPC example: request-reply pattern using correlation_id."""

import asyncio

from vortex.broker import Broker
from vortex.message import Headers, Message


async def rpc_server(broker: Broker):
    """Simulate an RPC server that processes requests."""
    rpc_queue = broker._queues["rpc-requests"]

    for _ in range(3):
        request = await rpc_queue.get()
        if request is None:
            await asyncio.sleep(0.1)
            continue

        # Compute response
        payload = request.body.decode()
        response_body = f"Echo: {payload}".encode()

        # Create response message with correlation_id
        response = Message(
            body=response_body,
            routing_key=request.reply_to or "",
            correlation_id=request.correlation_id,
            headers=Headers(content_type="text/plain"),
        )

        # Publish response to the reply exchange
        if request.reply_to:
            await broker.publish("amq.direct", response)


async def main():
    broker = Broker(node_id="rpc-demo")
    await broker.start()

    # Set up topology
    broker.declare_exchange("rpc-exchange", "direct")
    broker.declare_queue("rpc-requests")
    broker.bind("rpc-requests", "rpc-exchange", "rpc")
    broker.declare_exchange("amq.direct", "direct")
    broker.declare_queue("rpc-replies")
    broker.bind("rpc-replies", "amq.direct", "reply")

    # Start RPC server
    asyncio.create_task(rpc_server(broker))

    # Send RPC requests
    for i in range(3):
        request = Message(
            body=f"Request #{i + 1}".encode(),
            routing_key="rpc",
            correlation_id=f"req-{i + 1}",
            reply_to="reply",
        )
        await broker.publish("rpc-exchange", request)
        print(f"Sent: {request.body.decode()}")

    # Collect responses
    await asyncio.sleep(1)
    reply_queue = broker._queues["rpc-replies"]
    for i in range(3):
        response = await reply_queue.get()
        if response:
            print(f"Response: {response.body.decode()} (correlation: {response.correlation_id})")

    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
