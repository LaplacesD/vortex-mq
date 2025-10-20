"""SASL-like authentication for vortex-mq."""

from __future__ import annotations

import hashlib
import hmac
import secrets
from dataclasses import dataclass, field
from typing import Any

import structlog

from vortex.exceptions import AuthenticationError

logger = structlog.get_logger(__name__)


@dataclass
class User:
    """A broker user with credentials."""

    username: str
    password_hash: str
    salt: str
    permissions: dict[str, list[str]] = field(default_factory=lambda: {
        "exchange": ["read", "write"],
        "queue": ["read", "write"],
        "admin": [],
    })
    active: bool = True


class Authenticator:
    """SASL-like authentication for vortex-mq."""

    def __init__(self) -> None:
        self._users: dict[str, User] = {}

    def add_user(
        self,
        username: str,
        password: str,
        permissions: dict[str, list[str]] | None = None,
    ) -> User:
        """Register a new user with hashed password."""
        salt = secrets.token_hex(16)
        password_hash = self._hash_password(password, salt)
        user = User(
            username=username,
            password_hash=password_hash,
            salt=salt,
            permissions=permissions or User().permissions,
        )
        self._users[username] = user
        logger.debug("auth.user.added", username=username)
        return user

    def remove_user(self, username: str) -> bool:
        """Remove a user."""
        user = self._users.pop(username, None)
        if user:
            logger.debug("auth.user.removed", username=username)
            return True
        return False

    def authenticate(self, username: str, password: str) -> User:
        """Authenticate a user. Raises AuthenticationError on failure."""
        user = self._users.get(username)
        if user is None:
            raise AuthenticationError(f"Unknown user: {username}")
        if not user.active:
            raise AuthenticationError(f"User inactive: {username}")

        expected_hash = self._hash_password(password, user.salt)
        if not hmac.compare_digest(expected_hash, user.password_hash):
            raise AuthenticationError("Invalid password")

        logger.debug("auth.authenticated", username=username)
        return user

    def check_permission(
        self,
        user: User,
        resource_type: str,
        action: str,
    ) -> bool:
        """Check if a user has permission for a resource/action."""
        allowed = user.permissions.get(resource_type, [])
        if action not in allowed:
            logger.warning(
                "auth.permission.denied",
                username=user.username,
                resource=resource_type,
                action=action,
            )
            return False
        return True

    def user_exists(self, username: str) -> bool:
        """Check if a user exists."""
        return username in self._users

    @staticmethod
    def _hash_password(password: str, salt: str) -> str:
        return hashlib.pbkdf2_hmac(
            "sha256",
            password.encode(),
            salt.encode(),
            100000,
        ).hex()
