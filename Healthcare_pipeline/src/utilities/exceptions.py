"""
Reusable exception hierarchy and retry helpers for pipeline resilience.
"""

from __future__ import annotations

import time
from functools import wraps
from typing import Any, Callable, Optional, TypeVar

from config.constants import MAX_RETRIES, RETRY_BACKOFF_SECONDS, RETRY_MAX_BACKOFF_SECONDS

try:
    from tenacity import (
        retry,
        retry_if_exception_type,
        stop_after_attempt,
        wait_exponential,
    )

    _HAS_TENACITY = True
except ImportError:  # Databricks Free Edition may not preinstall tenacity
    _HAS_TENACITY = False


F = TypeVar("F", bound=Callable[..., Any])


class HealthcareLakehouseError(Exception):
    """Base exception for all platform errors."""

    def __init__(self, message: str, *, details: Optional[dict] = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}


class IngestionError(HealthcareLakehouseError):
    """Raised when Auto Loader / file ingestion fails."""


class TransformationError(HealthcareLakehouseError):
    """Raised when silver/gold transforms fail."""


class DataQualityError(HealthcareLakehouseError):
    """Raised when critical data quality rules fail."""


class SchemaEvolutionError(HealthcareLakehouseError):
    """Raised on incompatible schema changes."""


class MergeConflictError(HealthcareLakehouseError):
    """Raised when Delta MERGE encounters an unexpected conflict."""


class ConfigurationError(HealthcareLakehouseError):
    """Raised for invalid configuration."""


class DeadLetterError(HealthcareLakehouseError):
    """Raised when a record is routed to the DLQ."""


# Transient errors that are safe to retry
RETRYABLE_EXCEPTIONS = (
    IngestionError,
    TimeoutError,
    ConnectionError,
    OSError,
)


def with_retry(
    max_attempts: int = MAX_RETRIES,
    min_wait: float = RETRY_BACKOFF_SECONDS,
    max_wait: float = RETRY_MAX_BACKOFF_SECONDS,
    retryable: tuple = RETRYABLE_EXCEPTIONS,
) -> Callable[[F], F]:
    """Decorator applying exponential backoff for retryable failures."""

    def decorator(fn: F) -> F:
        if _HAS_TENACITY:
            wrapped = retry(
                reraise=True,
                stop=stop_after_attempt(max_attempts),
                wait=wait_exponential(multiplier=min_wait, max=max_wait),
                retry=retry_if_exception_type(retryable),
            )(fn)
            return wrapped  # type: ignore[return-value]

        @wraps(fn)
        def fallback(*args: Any, **kwargs: Any) -> Any:
            last_exc: Optional[BaseException] = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except retryable as exc:
                    last_exc = exc
                    if attempt == max_attempts:
                        break
                    time.sleep(min(min_wait * (2 ** (attempt - 1)), max_wait))
            raise last_exc  # type: ignore[misc]

        return fallback  # type: ignore[return-value]

    return decorator


def retry_call(
    fn: Callable[..., Any],
    *args: Any,
    max_attempts: int = MAX_RETRIES,
    backoff: float = RETRY_BACKOFF_SECONDS,
    retryable: tuple = RETRYABLE_EXCEPTIONS,
    **kwargs: Any,
) -> Any:
    """Imperative retry helper when a decorator is inconvenient."""
    last_exc: Optional[BaseException] = None
    for attempt in range(1, max_attempts + 1):
        try:
            return fn(*args, **kwargs)
        except retryable as exc:
            last_exc = exc
            if attempt == max_attempts:
                break
            time.sleep(min(backoff * (2 ** (attempt - 1)), RETRY_MAX_BACKOFF_SECONDS))
    raise last_exc  # type: ignore[misc]
