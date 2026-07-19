"""Shared statement type aliases for the mint.db repository layer."""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from sqlalchemy import Select
from sqlalchemy.sql.dml import ReturningDelete, ReturningInsert, ReturningUpdate

type CRUDStatement = (
    Select[Any] | ReturningInsert[Any] | ReturningUpdate[Any] | ReturningDelete[Any]
)
type PrepableStatement = Select[Any] | ReturningUpdate[Any] | ReturningDelete[Any]

PREPABLE_STATEMENT_TYPES: tuple[type, ...] = (Select, ReturningUpdate, ReturningDelete)


@dataclass
class PaginatedResult[T]:
    """One page of rows plus the total matching row count, from one query.

    Returned by ``RepositoryBase.get_many_page()`` in place of the common
    two-query pagination pattern (one query for the page, a separate
    ``count()`` query for the total) — the total comes from a
    ``COUNT(*) OVER()`` window function in the same ``SELECT``.
    """

    items: Sequence[T]
    total: int
