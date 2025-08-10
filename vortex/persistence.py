"""Persistence layer for vortex-mq — in-memory with optional disk backing."""

from __future__ import annotations

import json
import os
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import orjson
import structlog

from vortex.message import Message

logger = structlog.get_logger(__name__)


class PersistenceBackend(ABC):
    """Abstract persistence backend."""

    @abstractmethod
    async def save(self, message: Message) -> None: ...

    @abstractmethod
    async def load(self, message_id: str) -> Message | None: ...

    @abstractmethod
    async def delete(self, message_id: str) -> None: ...

    @abstractmethod
    async def list_all(self) -> list[Message]: ...


class InMemoryPersistence(PersistenceBackend):
    """In-memory message store."""

    def __init__(self) -> None:
        self._store: dict[str, Message] = {}

    async def save(self, message: Message) -> None:
        self._store[message.message_id] = message

    async def load(self, message_id: str) -> Message | None:
        return self._store.get(message_id)

    async def delete(self, message_id: str) -> None:
        self._store.pop(message_id, None)

    async def list_all(self) -> list[Message]:
        return list(self._store.values())

    @property
    def count(self) -> int:
        return len(self._store)


class DiskPersistence(PersistenceBackend):
    """File-based persistence using orjson."""

    def __init__(self, directory: str | None = None) -> None:
        if directory is None:
            directory = tempfile.mkdtemp(prefix="vortex_")
        self._directory = Path(directory)
        self._directory.mkdir(parents=True, exist_ok=True)
        self._in_memory: dict[str, Message] = {}
        logger.info("persistence.disk.initialized", directory=str(self._directory))

    def _message_path(self, message_id: str) -> Path:
        return self._directory / f"{message_id}.json"

    async def save(self, message: Message) -> None:
        self._in_memory[message.message_id] = message
        path = self._message_path(message.message_id)
        data = message.to_dict()
        data["body"] = data["body"].hex() if isinstance(data["body"], bytes) else data["body"]  # type: ignore
        path.write_bytes(orjson.dumps(data))

    async def load(self, message_id: str) -> Message | None:
        if message_id in self._in_memory:
            return self._in_memory[message_id]
        path = self._message_path(message_id)
        if not path.exists():
            return None
        raw = orjson.loads(path.read_bytes())
        if isinstance(raw.get("body"), str):
            raw["body"] = bytes.fromhex(raw["body"])
        msg = Message.from_dict(raw)
        self._in_memory[message_id] = msg
        return msg

    async def delete(self, message_id: str) -> None:
        self._in_memory.pop(message_id, None)
        path = self._message_path(message_id)
        if path.exists():
            path.unlink()

    async def list_all(self) -> list[Message]:
        return list(self._in_memory.values())

    @property
    def count(self) -> int:
        return len(self._in_memory)
