"""Custom exceptions for vortex-mq."""


class VortexError(Exception):
    """Base exception for all vortex-mq errors."""


class BrokerError(VortexError):
    """Raised when a broker operation fails."""


class ExchangeNotFoundError(BrokerError):
    """Raised when an exchange is not found."""


class QueueNotFoundError(BrokerError):
    """Raised when a queue is not found."""


class BindingError(VortexError):
    """Raised when a binding operation fails."""


class DeliveryError(VortexError):
    """Raised when message delivery fails."""


class RetryExhaustedError(DeliveryError):
    """Raised when all retry attempts are exhausted."""


class DeadLetterError(VortexError):
    """Raised when a dead-letter operation fails."""


class ClusterError(VortexError):
    """Raised when a cluster operation fails."""


class AuthenticationError(VortexError):
    """Raised when authentication fails."""


class SerializationError(VortexError):
    """Raised when serialization/deserialization fails."""


class ConnectionError(VortexError):
    """Raised when a connection to a node fails."""
