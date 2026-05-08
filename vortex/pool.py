"""Connection pooling configuration for vortex-mq broker.

Provides configurable connection limits and backpressure to prevent
resource exhaustion under high load.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PoolConfig:
    """Configuration for connection pool sizing."""

    min_connections: int = 2
    max_connections: int = 50
    max_idle_seconds: int = 300
    acquire_timeout: float = 30.0
    max_lifetime_seconds: int = 3600
    health_check_interval: float = 15.0

    def validate(self) -> None:
        """Validate pool configuration values."""
        if self.min_connections < 0:
            raise ValueError("min_connections must be non-negative")
        if self.max_connections < self.min_connections:
            raise ValueError(
                f"max_connections ({self.max_connections}) must be >= "
                f"min_connections ({self.min_connections})"
            )
        if self.acquire_timeout <= 0:
            raise ValueError("acquire_timeout must be positive")


DEFAULT_POOL_CONFIG = PoolConfig()
