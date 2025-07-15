"""Message and Headers models for vortex-mq."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Headers:
    """Message headers with optional metadata."""

    content_type: str = "application/octet-stream"
    content_encoding: str = "identity"
    priority: int = 0
    delivery_mode: int = 1  # 1 = non-persistent, 2 = persistent
    timestamp: float = field(default_factory=time.time)
    user_id: str | None = None
    app_id: str | None = None
    headers: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "content_type": self.content_type,
            "content_encoding": self.content_encoding,
            "priority": self.priority,
            "delivery_mode": self.delivery_mode,
            "timestamp": self.timestamp,
            "user_id": self.user_id,
            "app_id": self.app_id,
            "headers": self.headers,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Headers:
        return cls(
            content_type=data.get("content_type", "application/octet-stream"),
            content_encoding=data.get("content_encoding", "identity"),
            priority=data.get("priority", 0),
            delivery_mode=data.get("delivery_mode", 1),
            timestamp=data.get("timestamp", time.time()),
            user_id=data.get("user_id"),
            app_id=data.get("app_id"),
            headers=data.get("headers", {}),
        )


@dataclass
class Message:
    """Core message type in vortex-mq."""

    body: bytes
    routing_key: str = ""
    headers: Headers = field(default_factory=Headers)
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    correlation_id: str | None = None
    reply_to: str | None = None
    exchange: str | None = None
    redelivered: bool = False
    ttl: float | None = None  # seconds

    @property
    def expired(self) -> bool:
        if self.ttl is None:
            return False
        return (time.time() - self.headers.timestamp) > self.ttl

    def to_dict(self) -> dict[str, Any]:
        return {
            "body": self.body,
            "routing_key": self.routing_key,
            "headers": self.headers.to_dict(),
            "message_id": self.message_id,
            "correlation_id": self.correlation_id,
            "reply_to": self.reply_to,
            "exchange": self.exchange,
            "redelivered": self.redelivered,
            "ttl": self.ttl,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Message:
        return cls(
            body=data["body"],
            routing_key=data.get("routing_key", ""),
            headers=Headers.from_dict(data.get("headers", {})),
            message_id=data.get("message_id", str(uuid.uuid4())),
            correlation_id=data.get("correlation_id"),
            reply_to=data.get("reply_to"),
            exchange=data.get("exchange"),
            redelivered=data.get("redelivered", False),
            ttl=data.get("ttl"),
        )
