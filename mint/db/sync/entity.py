"""Identifier-based CRUD for tables with a single-column primary key."""

from collections.abc import Sequence
from typing import Any, cast

from sqlalchemy import delete, select, update
from sqlalchemy.orm import InstrumentedAttribute
from sqlmodel import SQLModel

from mint.db.exc import NotFoundError
from mint.db.models import Base

from .base import RepositoryBase


class EntityRepository[T: Base, I](RepositoryBase[T]):
    """Adds get/update/remove/get_many_by_ids to :class:`RepositoryBase`.

    Requires a schema with a single-column ``id``. Schemas without one
    (composite-key join tables, materialized views) use
    :class:`RepositoryBase` directly instead.

    ``T`` is bound only to :class:`Base` — not a closed union of concrete
    ID-typed bases — so a schema's ``id`` can be any type (a snowflake ID,
    a ``NewType``-wrapped primitive, ...), not just UUID/int/str. ``I`` is
    a separately-specified, unconstrained type parameter that must match
    ``T``'s actual ``id`` field type; Python has no mechanism to derive one
    from the other (no higher-kinded types), so this is an honest,
    documented gap rather than a statically-verified one — see
    :attr:`_id_column` and specs/002-db-repository-layer/research.md,
    "Generic ID type".
    """

    @property
    def _id_column(self) -> InstrumentedAttribute[Any]:
        """Return :attr:`Schema`'s ``id`` column for use in query expressions.

        The one ``cast()`` this class needs, in place of an untyped access
        at every call site: ``Base`` doesn't declare ``id`` (only the
        concrete ``BaseWithUUID``/``BaseWithIntID``/``BaseWithStrID``
        bases, and any other convention a consuming app defines, do), and
        Python cannot express "T's id attribute has type I" — see the
        class docstring. Typed ``InstrumentedAttribute[Any]`` rather than
        ``InstrumentedAttribute[I]``: this property is private and only
        used internally to build ``WHERE``/``SET`` clauses, so it doesn't
        need to carry ``I`` — the public methods below (``get``, etc.)
        already type their own ``id_: I`` parameter, which is where a
        caller's type-checking actually matters. Parameterizing on an
        unbound ``I`` instead makes some type checkers resolve
        ``InstrumentedAttribute[I].__eq__``/``.in_()`` incorrectly (falling
        back to ``object.__eq__``, returning ``bool`` instead of
        ``ColumnElement[bool]``, since I has no bound to select the right
        overload).
        """
        schema = cast("type[Any]", self.Schema)
        return cast("InstrumentedAttribute[Any]", schema.id)

    def get(self, id_: I) -> T | None:
        """Return the row matching ``id_``, or ``None``.

        Args:
            id_: The primary key value to look up.

        Returns:
            The matched row, or ``None``.

        """
        stmt = select(self.Schema).where(self._id_column == id_)
        return self.execute(stmt)

    def update(self, id_: I, model: SQLModel) -> T:
        """Update the row matching ``id_`` with only the fields set on ``model``.

        Fields left unset on ``model`` are left unchanged
        (``exclude_unset=True``).

        Args:
            id_: The primary key value to update.
            model: An update-payload model (e.g. ``JobUpdate``).

        Returns:
            The updated row.

        Raises:
            NotFoundError: If no row matches ``id_``.

        """
        update_dict = model.model_dump(exclude_unset=True)
        if not update_dict:
            obj = self.get(id_)
            if obj is None:
                raise NotFoundError(schema=self.Schema.__name__, id_=id_)
            return obj
        stmt = (
            update(self.Schema)
            .where(self._id_column == id_)
            .values(**update_dict)
            .returning(self.Schema)
        )
        obj = self.execute(stmt)
        if obj is None:
            raise NotFoundError(schema=self.Schema.__name__, id_=id_)
        return obj

    def remove(self, id_: I) -> T:
        """Delete the row matching ``id_``.

        Args:
            id_: The primary key value to delete.

        Returns:
            The deleted row.

        Raises:
            NotFoundError: If no row matches ``id_``.

        """
        stmt = delete(self.Schema).where(self._id_column == id_).returning(self.Schema)
        obj = self.execute(stmt)
        if obj is None:
            raise NotFoundError(schema=self.Schema.__name__, id_=id_)
        return obj

    def remove_many(self, ids: Sequence[I]) -> Sequence[T]:
        """Delete every row matching ``ids``.

        Args:
            ids: Primary key values to delete.

        Returns:
            The deleted rows.

        """
        stmt = delete(self.Schema).where(self._id_column.in_(ids)).returning(self.Schema)
        return self.execute_many(stmt)

    def get_many_by_ids(self, ids: Sequence[I]) -> Sequence[T]:
        """Return every row matching ``ids``.

        Args:
            ids: Primary key values to look up.

        Returns:
            The matched rows.

        """
        stmt = select(self.Schema).where(self._id_column.in_(ids))
        return self.execute_many(stmt)
