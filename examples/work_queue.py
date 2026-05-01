"""Work Queue example: competing consumers sharing a workload."""

import asyncio

from vortex.broker import Broker
from vortex.message import Message


async def worker(broker: Broker, worker_id: str, queue_name: str):
    """Simulate a worker processing tasks."""
    while True:
        msg = await broker._queues[queue_name].get()
        if msg is None:
            await asyncio.sleep(0.1)
            continue
        print(f"Worker {worker_id} processing: {msg.body.decode()}")
        await asyncio.sleep(0.5)  # simulate work
        print(f"Worker {worker_id} done")


async def main():
    broker = Broker(node_id="workqueue-demo")
    await broker.start()

    # Direct exchange for work queue
    broker.declare_exchange("tasks", "direct")
    broker.declare_queue("work-queue")
    broker.bind("work-queue", "tasks", "task")

    # Start multiple workers
    asyncio.create_task(worker(broker, "A", "work-queue"))
    asyncio.create_task(worker(broker, "B", "work-queue"))
    asyncio.create_task(worker(broker, "C", "work-queue"))

    # Publish tasks
    for i in range(6):
        msg = Message(
            body=f"Task #{i + 1}".encode(),
            routing_key="task",
        )
        await broker.publish("tasks", msg)
        print(f"Published Task #{i + 1}")

    await asyncio.sleep(3)
    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
