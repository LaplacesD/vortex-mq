# Clustering

vortex-mq supports multi-node clustering for high availability and horizontal scaling.

## Architecture

```
Node A ───── Node B
   │            │
   └──── Node C ┘
```

Each node runs a full broker instance. Nodes discover each other through a heartbeat protocol and maintain a shared view of the cluster membership.

## Node Discovery

Nodes are discovered automatically using the `ClusterDiscovery` class:

```python
from vortex.cluster import ClusterDiscovery

discovery = ClusterDiscovery(node_id="node-1")
discovery.register_self("10.0.0.1", 9090)

# Add seed nodes
discovery.add_node(ClusterNode("node-2", "10.0.0.2", 9090))
discovery.add_node(ClusterNode("node-3", "10.0.0.3", 9090))
```

## Heartbeat Protocol

Each node sends heartbeats every 5 seconds. A node is considered dead if no heartbeat is received within 15 seconds.

```python
# Start the heartbeat loop
await discovery.start_heartbeat()

# Register callbacks
discovery.on_join(lambda node: print(f"Node joined: {node.node_id}"))
discovery.on_leave(lambda node: print(f"Node left: {node.node_id}"))
```

## Load Balancing

### Round-Robin
Distributes messages evenly across available consumers/nodes.

```python
from vortex.balancer import RoundRobinBalancer

balancer = RoundRobinBalancer()
node = balancer.select(["node-1", "node-2", "node-3"])
```

### Consistent Hash
Ensures the same routing key maps to the same node, supporting virtual nodes for even distribution.

```python
from vortex.balancer import ConsistentHashBalancer

balancer = ConsistentHashBalancer(virtual_nodes=100)
node = balancer.select(nodes, key="my-routing-key")
```

## Docker Compose

See the [docker-compose.yml](../docker-compose.yml) for a 3-node cluster setup:

```bash
docker-compose up -d
```

This starts three broker nodes on ports 9090, 9091, and 9092 with admin APIs on 8090-8092.
