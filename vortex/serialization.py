"""Serialization utilities for vortex-mq."""

from __future__ import annotations

import pickle
from abc import ABC, abstractmethod
from typing import Any

import orjson

from vortex.exceptions import SerializationError
from vortex.message import Message


class Serializer(ABC):
    """Abstract serializer for messages."""

    @abstractmethod
    def serialize(self, message: Message) -> bytes: ...

    @abstractmethod
    def deserialize(self, data: bytes) -> Message: ...

    @property
    @abstractmethod
    def content_type(self) -> str: ...


class OrjsonSerializer(Serializer):
    """Serializer using orjson (fast JSON)."""

    @property
    def content_type(self) -> str:
        return "application/json"

    def serialize(self, message: Message) -> bytes:
        try:
            raw = message.to_dict()
            raw["body"] = raw["body"].hex()
            return orjson.dumps(raw)
        except Exception as e:
            raise SerializationError(f"orjson serialize failed: {e}") from e

    def deserialize(self, data: bytes) -> Message:
        try:
            raw = orjson.loads(data)
            if isinstance(raw.get("body"), str):
                raw["body"] = bytes.fromhex(raw["body"])
            return Message.from_dict(raw)
        except Exception as e:
            raise SerializationError(f"orjson deserialize failed: {e}") from e


class MsgpackSerializer(Serializer):
    """Serializer using msgpack."""

    def __init__(self) -> None:
        import msgpack

        self._msgpack = msgpack

    @property
    def content_type(self) -> str:
        return "application/msgpack"

    def serialize(self, message: Message) -> bytes:
        try:
            raw = message.to_dict()
            raw["body"] = raw["body"].hex()
            return self._msgpack.packb(raw)
        except Exception as e:
            raise SerializationError(f"msgpack serialize failed: {e}") from e

    def deserialize(self, data: bytes) -> Message:
        try:
            raw = self._msgpack.unpackb(data)
            if isinstance(raw.get("body"), str):
                raw["body"] = bytes.fromhex(raw["body"])
            return Message.from_dict(raw)
        except Exception as e:
            raise SerializationError(f"msgpack deserialize failed: {e}") from e


class PickleSerializer(Serializer):
    """Serializer using pickle (Python native)."""

    @property
    def content_type(self) -> str:
        return "application/python-pickle"

    def serialize(self, message: Message) -> bytes:
        try:
            return pickle.dumps(message)
        except Exception as e:
            raise SerializationError(f"pickle serialize failed: {e}") from e

    def deserialize(self, data: bytes) -> Message:
        try:
            obj = pickle.loads(data)
            if not isinstance(obj, Message):
                raise SerializationError("Deserialized object is not a Message")
            return obj
        except Exception as e:
            raise SerializationError(f"pickle deserialize failed: {e}") from e


def get_serializer(content_type: str) -> Serializer:
    """Factory to get serializer by content type."""
    serializers = {
        "application/json": OrjsonSerializer(),
        "application/msgpack": MsgpackSerializer(),
        "application/python-pickle": PickleSerializer(),
    }
    serializer = serializers.get(content_type)
    if serializer is None:
        raise SerializationError(f"Unknown content type: {content_type}")
    return serializer
