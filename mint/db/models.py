"""Shared SQLModel base classes for tables defined against mint.db.

Generic table base classes (``BaseWithID[T]``) do not resolve reliably under
SQLModel's pydantic+SQLAlchemy metaclass (see
``specs/002-db-repository-layer/research.md``, "Schema/model collapse and
where genericity lives"). Instead, this module ships concrete, per-ID-type
base classes; genericity lives at the repository layer
(``mint.db.asynk.entity.EntityRepository[T, I]``) instead.
"""

from datetime import UTC, datetime
from typing import Any, ClassVar
from uuid import UUID, uuid4

from sqlalchemy import DateTime
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.sql import func
from sqlmodel import Field, SQLModel


class UTCDateTime(DateTime):
    """``DateTime`` defaulting to ``timezone=True``.

    SQLModel's ``Field(sa_type=...)`` requires a bare class (``type[Any]``),
    not a configured instance — passing ``DateTime(timezone=True)`` directly
    fails both ``ty`` and ``mypy``'s overload resolution, since neither
    stub allows an instance there even though SQLAlchemy's own ``Column()``
    (which ``sa_type`` ultimately reaches) accepts one. Baking the
    ``timezone=True`` default into a subclass lets ``sa_type=UTCDateTime``
    pass a class, satisfying the stub with no cast or suppression needed.
    """

    def __init__(self, *, timezone: bool = True) -> None:
        """Initialize with ``timezone=True`` by default.

        Args:
            timezone: Whether the column stores timezone-aware values.
                Defaults to ``True`` — the whole point of this subclass.

        """
        super().__init__(timezone=timezone)


class Base(AsyncAttrs, SQLModel):
    """Abstract root for every table defined against mint.db.

    Mixing in ``AsyncAttrs`` gives every table ``awaitable_attrs``, usable
    directly by a consuming app's own async code outside the repository
    layer. Relationship eager-loading through the repository layer itself
    goes through ``.options(selectinload(...))``/``joinedload(...)`` on the
    statement passed to ``execute()``/``execute_many()`` — a single extra
    query or JOIN regardless of row count, not a per-row fetch loop. All
    tables share this class's ``metadata``, which is required for
    string-based ``secondary="other_table"`` many-to-many resolution to
    work regardless of which module defines either side.
    """

    type_annotation_map: ClassVar[dict[Any, Any]] = {
        datetime: DateTime(timezone=True),
    }


class BaseWithUUID(Base):
    """Abstract base for tables with a UUID primary key, client-generated."""

    id: UUID = Field(default_factory=uuid4, primary_key=True, index=True)


class BaseWithIntID(Base):
    """Abstract base for tables with a server/auto-generated integer primary key."""

    id: int | None = Field(default=None, primary_key=True, index=True)


class BaseWithStrID(Base):
    """Abstract base for tables with a caller-supplied string primary key."""

    id: str = Field(primary_key=True, index=True)


class AuditMixin(SQLModel):
    """Adds ``created_at``/``updated_at`` timestamp columns.

    Composed alongside an ID base and any other mixin, e.g.
    ``class Job(AuditMixin, BaseWithUUID, table=True): ...``.

    Uses ``sa_type=``/``sa_column_kwargs=`` rather than a pre-built
    ``sa_column=Column(...)`` instance deliberately: a ``Column`` object
    can only be owned by one ``Table`` at a time, and ``sa_column=Column(...)``
    constructs that ``Column`` once, at class-body evaluation time — shared
    by *every* table composing this mixin. A second table composing
    ``AuditMixin`` then fails with ``ArgumentError: Column object
    'created_at' already assigned to Table '<the first table>'`` (confirmed
    via this port's own multi-mixin stress test). ``sa_type=``/
    ``sa_column_kwargs=`` let SQLModel build a fresh ``Column`` per table
    instead. ``sa_type=UTCDateTime`` (a class, not
    ``DateTime(timezone=True)``, an instance) so it also passes both
    ``ty`` and ``mypy``'s ``Field()`` overload resolution — see
    :class:`UTCDateTime`.
    """

    created_at: datetime = Field(
        default_factory=lambda: datetime.now(tz=UTC),
        sa_type=UTCDateTime,
        sa_column_kwargs={"server_default": func.now()},
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(tz=UTC),
        sa_type=UTCDateTime,
        sa_column_kwargs={"server_default": func.now(), "onupdate": func.now()},
    )


class OwnerMixin(SQLModel):
    """Adds a ``created_by_user_id: UUID`` column only — no FK, no relationship.

    Deliberately column-only: a consuming app that wants an actual
    ``relationship()`` to its own user table layers its own concrete mixin
    on top of this one (see research.md's "Scoping" decision for why the FK
    target is intentionally not mint.db's concern). Assumes UUID-keyed owners
    (mint.db's primary offering, ``BaseWithUUID``) — an app whose owner ID is
    an int/str defines its own owner mixin with the matching column type,
    same as it would for the FK relationship.
    """

    created_by_user_id: UUID | None = Field(default=None, index=True)


class IsDeletedMixin(SQLModel):
    """Adds an ``is_deleted`` column, opting a table into soft-delete scoping.

    The repository-layer scoping listener (``mint.db.asynk.base``) checks
    ``issubclass(schema, IsDeletedMixin)`` directly — nominal, not
    structural — to decide whether to filter on this column.
    """

    is_deleted: bool = Field(default=False, index=True)
