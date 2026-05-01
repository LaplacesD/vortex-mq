# Architecture

## Overview

vortex-mq is an in-memory message broker following the AMQP 0-9-1 model. Messages flow from publishers through exchanges to queues, where consumers retrieve them.

```
Publisher → Exchange → Binding → Queue → Consumer
```

## Components

### Broker

The central runtime that manages exchanges, queues, and bindings. Each broker instance has a unique `node_id` and maintains its own topology.

### Exchange

An exchange receives messages from publishers and routes them to queues based on rules:

| Type    | Behavior                                      |
| ------- | --------------------------------------------- |
| Direct  | Routes to queues where binding key == routing key |
| Topic   | Routes using wildcard patterns (\*, #)        |
| Fanout  | Routes to all bound queues (ignores key)      |
| Headers | Routes based on header matching (all/any)     |

### Queue

Queues store messages until consumed. Messages are delivered round-robin to registered consumers within a consumer group.

### Binding

A binding connects a queue to an exchange with a routing key. Each binding specifies an exchange type and routing pattern.

## Delivery Guarantees

### At-Most-Once
Fire-and-forget. Messages are delivered once with no retries. Fastest option with potential for message loss.

### At-Least-Once
Messages are retried (with exponential backoff) until the consumer acknowledges or retries are exhausted. Guarantees delivery but may produce duplicates.

### Exactly-Once
Uses message ID deduplication. The broker tracks acknowledged message IDs and skips already-processed messages. Requires an acknowledgment store.

## Persistence

- **InMemoryPersistence**: Default backend with dict-based storage
- **DiskPersistence**: File-based storage using orjson encoding

## Clustering

Nodes discover each other through heartbeat-based membership. Messages can be partitioned using consistent hashing. See [Clustering](./clustering.md) for details.

## Monitoring

- **Admin API**: HTTP endpoints at port 8090 for topology inspection
- **Prometheus Metrics**: /metrics endpoint at port 9100
