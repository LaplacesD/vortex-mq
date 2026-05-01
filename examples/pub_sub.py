"""Pub/Sub example: fanout exchange broadcasts to all subscribers."""

import asyncio

from vortex.broker import Broker
from vortex.message import Message


async def main():
    broker = Broker(node_id="pubsub-demo")
    await broker.start()

    # Create a fanout exchange
    broker.declare_exchange("news", "fanout")

    # Create subscriber queues
    broker.declare_queue("sports-fan")
    broker.declare_queue("tech-enthusiast")
    broker.declare_queue("news-junkie")

    # Bind all queues to the fanout exchange
    broker.bind("sports-fan", "news")
    broker.bind("tech-enthusiast", "news")
    broker.bind("news-junkie", "news")

    # Publish a message
    msg = Message(
        body=b"Breaking News: Python 4.0 released!",
        routing_key="",
        exchange="news",
    )
    print("Publishing:", msg.body.decode())
    await broker.publish("news", msg)

    # All subscribers receive the message
    for q_name in ["sports-fan", "tech-enthusiast", "news-junkie"]:
        received = await broker._queues[q_name].get()
        print(f"{q_name} received: {received.body.decode()}")

    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
