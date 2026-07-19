"""Soft-delete and owner-scoping mixins for async repositories."""

from collections.abc import Sequence
from typing import Any, Protocol, Unpack, runtime_checkable

from sqlmodel import update

from mint.db.exc import NotFoundError
from mint.db.models import Base

from .base import RepositoryBase, RepositoryKwargs
from .database import Database
from .entity import EntityRepository


class SoftDeleteMixin[T: Base, I](EntityRepository[T, I]):
    """Overrides remove/remove_many to soft-delete instead of hard-delete.

    Read-side soft-delete filtering is unconditional on the schema having
    an ``is_deleted`` column (see
    :meth:`mint.db.asynk.base.RepositoryBase._scope_listener`) — this mixin
    only changes what ``remove()`` does, not what reads see.
    """

    async def remove(self, id_: I) -> T:
        """Mark the row matching ``id_`` as deleted, without removing it.

        Args:
            id_: The primary key value to soft-delete.

        Returns:
            The updated row.

        Raises:
            NotFoundError: If no row matches ``id_``.

        """
        stmt = (
            update(self.Schema)
            .where(self._id_column == id_)
            .values(is_deleted=True)
            .returning(self.Schema)
        )
        obj = await self.execute(stmt)
        if obj is None:
            raise NotFoundError(schema=self.Schema.__name__, id_=id_)
        return obj

    async def remove_many(self, ids: Sequence[I]) -> Sequence[T]:
        """Mark every row matching ``ids`` as deleted, without removing them.

        Args:
            ids: Primary key values to soft-delete.

        Returns:
            The updated rows.

        """
        stmt = (
            update(self.Schema)
            .where(self._id_column.in_(ids))
            .values(is_deleted=True)
            .returning(self.Schema)
        )
        return await self.execute_many(stmt)


@runtime_checkable
class IOwner(Protocol):
    """Structural contract for the ``owner`` passed to :class:`ResourceOwnerMixin`."""

    id: Any

    @property
    def is_scoped(self) -> bool:
        """Whether this owner's queries should be scoped to their own rows."""
        ...


class ResourceOwnerMixin[T: Base](RepositoryBase[T]):
    """Adds owner-scoping to a repository.

    Read by :meth:`mint.db.asynk.base.RepositoryBase._scope_listener`
    directly (``self.owner``/``self.is_scoped``) — every query through a
    repository composing this mixin is automatically scoped, including
    hand-written custom queries.
    """

    def __init__(
        self,
        db: Database,
        *,
        owner: IOwner | None = None,
        **kwargs: Unpack[RepositoryKwargs],
    ) -> None:
        """Initialize the repository with an optional scoping owner.

        Args:
            db: The shared :class:`Database` this repository queries.
            owner: The acting owner queries are scoped to. ``None`` means
                unscoped.
            **kwargs: Forwarded to :class:`RepositoryBase`.

        """
        super().__init__(db, **kwargs)
        self.owner = owner

    @property
    def is_scoped(self) -> bool:
        """Whether queries through this repository are owner-scoped."""
        if self.owner is None:
            return False
        return self.owner.is_scoped
