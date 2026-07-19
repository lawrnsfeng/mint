"""Exceptions for the mint.utils package."""

from dataclasses import dataclass

from mint.exc import TemplatedError


@dataclass
class UtilsError(TemplatedError):
    """Generic mint.utils error."""


@dataclass
class InvalidConcurrencyLimitError(UtilsError):
    """max_concurrent passed to ConcurrencyLimiter is not a positive int."""

    TEMPLATE = "max_concurrent must be a positive integer, got {max_concurrent}"
    max_concurrent: int
