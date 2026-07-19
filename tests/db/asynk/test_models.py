"""Confirmation-spike tests for advanced SQLModel patterns on mint.db.models.

Validates the remaining items from the architecture plan's "Complex
real-world patterns" table that concern models.py specifically: subclass
field override, a concrete owner relationship layered on top of OwnerMixin,
``PrivateAttr`` for transient state, and ``hybrid_property``.
(Shared-metadata ``secondary=`` resolution is exercised directly by
test_schema_shapes.py's many-to-many test.)

Note on ``declared_attr``: the architecture plan's research anticipated
using ``sqlalchemy.orm.declared_attr`` for a *reusable* concrete owner
relationship mixin (so multiple tables share one relationship
declaration). In practice, `@declared_attr`-decorated methods raise at
class-construction time under SQLModel's pydantic+SQLAlchemy metaclass
(pydantic's own namespace scanner rejects the un-annotated descriptor,
and even bypassing that via ``model_config.ignored_types`` still hits a
SQLAlchemy-side "typing annotation is required" error) — this is real,
confirmed friction, not a hypothetical one. A **direct** (non-``declared_attr``)
``Relationship()`` on each table works cleanly (verified below), so that's
the recommended pattern for consuming apps: one extra relationship line per
table instead of one shared ``declared_attr`` mixin. Documented in
docs/db-repository-implementation-notes.md.
"""

from typing import ClassVar, cast
from uuid import UUID

import pytest
from pydantic import PrivateAttr
from sqlalchemy import ColumnElement, case, select
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import QueryableAttribute, reconstructor, selectinload
from sqlmodel import Field, Relationship, SQLModel, col
from sqlmodel._compat import SQLModelConfig

from mint.db.asynk.database import Database
from mint.db.asynk.entity import EntityRepository
from mint.db.asynk.mixins import SoftDeleteMixin
from mint.db.models import AuditMixin, BaseWithUUID, IsDeletedMixin, OwnerMixin

# --- Subclass field override + a concrete owner relationship ---


class TRefTarget(BaseWithUUID, table=True):
    """Target table for the owner relationship."""

    name: str


class TOwnerRelExample(OwnerMixin, BaseWithUUID, table=True):
    """Overrides OwnerMixin's column-only field with a real FK + relationship."""

    name: str
    # Subclass field override: OwnerMixin declares created_by_user_id with
    # no FK target; this leaf table adds one.
    created_by_user_id: UUID | None = Field(default=None, foreign_key="treftarget.id")
    # App-owned concrete relationship layered on top of OwnerMixin — a
    # direct Relationship(), not declared_attr (see module docstring).
    owner_ref: TRefTarget | None = Relationship(
        sa_relationship_kwargs={"foreign_keys": "[TOwnerRelExample.created_by_user_id]"},
    )


class TRefTargetCreate(SQLModel):
    """Create-payload for TRefTarget."""

    name: str


class TOwnerRelExampleCreate(SQLModel):
    """Create-payload for TOwnerRelExample."""

    name: str
    created_by_user_id: UUID | None = None


class TRefTargetRepository(EntityRepository[TRefTarget, UUID]):
    """CRUD repository for TRefTarget."""

    Schema = TRefTarget


class TOwnerRelExampleRepository(EntityRepository[TOwnerRelExample, UUID]):
    """CRUD repository for TOwnerRelExample."""

    Schema = TOwnerRelExample


@pytest.mark.asyncio
async def test_subclass_field_override_and_concrete_owner_relationship(
    async_db: Database,
) -> None:
    """A subclass can override an inherited column and add a concrete owner relationship."""
    target = await TRefTargetRepository(async_db).create(TRefTargetCreate(name="target"))

    repo = TOwnerRelExampleRepository(async_db)
    created = await repo.create(
        TOwnerRelExampleCreate(name="x", created_by_user_id=target.id),
    )

    owner_ref_attr = cast("QueryableAttribute[TRefTarget | None]", TOwnerRelExample.owner_ref)
    stmt = (
        select(TOwnerRelExample)
        .where(col(TOwnerRelExample.id) == created.id)
        .options(selectinload(owner_ref_attr))
    )
    fetched = await repo.execute(stmt)
    assert fetched is not None
    assert fetched.owner_ref is not None
    assert fetched.owner_ref.id == target.id


# --- PrivateAttr + @reconstructor for transient non-column state ---


class TWithTransientState(BaseWithUUID, table=True):
    """Transient, non-column instance state via PrivateAttr + @reconstructor."""

    name: str
    _cache: dict[str, int] = PrivateAttr(default_factory=dict)

    @reconstructor
    def init_on_load(self) -> None:
        """Populate transient state after loading from the database."""
        self._cache = {"loaded": 1}


class TWithTransientStateCreate(SQLModel):
    """Create-payload for TWithTransientState."""

    name: str


class TWithTransientStateRepository(EntityRepository[TWithTransientState, UUID]):
    """CRUD repository for TWithTransientState."""

    Schema = TWithTransientState


@pytest.mark.asyncio
async def test_private_attr_and_reconstructor_populate_transient_state(
    async_db: Database,
) -> None:
    """PrivateAttr defaults empty on a never-persisted instance; @reconstructor sets it on load.

    Both create() (INSERT ... RETURNING) and get() (SELECT) hydrate the
    instance from a database row through the same ORM loading path, so
    @reconstructor fires for both — the distinguishing case is an instance
    that never touched the database at all.
    """
    never_persisted = TWithTransientState(name="x")
    assert never_persisted._cache == {}

    repo = TWithTransientStateRepository(async_db)
    created = await repo.create(TWithTransientStateCreate(name="x"))
    assert created._cache == {"loaded": 1}

    fetched = await repo.get(created.id)
    assert fetched is not None
    assert fetched._cache == {"loaded": 1}


# --- hybrid_property with a separate SQL-expression form ---


class TWithHybrid(BaseWithUUID, table=True):
    """A computed property usable both in Python and in SQL ordering/filtering."""

    # pydantic's namespace scanner rejects an un-annotated hybrid_property
    # descriptor otherwise (same friction class as declared_attr, see
    # module docstring) — this is the documented pydantic-suggested fix.
    model_config: ClassVar[SQLModelConfig] = {"ignored_types": (hybrid_property,)}

    first_name: str
    last_name: str | None = None

    @hybrid_property
    def display_name(self) -> str:
        """Python-side computed display name."""
        if self.last_name:
            return f"{self.last_name} {self.first_name}"
        return self.first_name

    @display_name.inplace.expression
    @classmethod
    def _display_name_expression(cls) -> ColumnElement[str]:
        """SQL-side equivalent, usable in order_by/filter."""
        last_name = col(cls.last_name)
        first_name = col(cls.first_name)
        return case(
            (last_name.isnot(None), last_name + " " + first_name),
            else_=first_name,
        )


class TWithHybridCreate(SQLModel):
    """Create-payload for TWithHybrid."""

    first_name: str
    last_name: str | None = None


class TWithHybridRepository(EntityRepository[TWithHybrid, UUID]):
    """CRUD repository for TWithHybrid."""

    Schema = TWithHybrid


@pytest.mark.asyncio
async def test_hybrid_property_python_and_sql_forms(async_db: Database) -> None:
    """hybrid_property works as a Python property and as a SQL expression."""
    repo = TWithHybridRepository(async_db)
    alice = await repo.create(TWithHybridCreate(first_name="Alice", last_name="Smith"))
    bob = await repo.create(TWithHybridCreate(first_name="Bob"))

    assert alice.display_name == "Smith Alice"
    assert bob.display_name == "Bob"

    stmt = select(TWithHybrid).order_by(TWithHybrid.display_name)
    rows = await repo.execute_many(stmt)
    names = [row.display_name for row in rows]
    assert names == sorted(names)


# --- 4+ mixin stack: soft-delete + owner + audit + app-specific mixin ---
#
# Mirrors real production shapes (e.g. a consuming app's own
# User(PermissionMixin, AuditMixin, BaseWithUUID)) more closely than the
# 3-mixin tests/db/schemas.py.TJob spike — stacks IsDeletedMixin, OwnerMixin,
# AuditMixin, and an app-specific-style custom mixin on one table,
# simultaneously with a concrete owner relationship and a hybrid_property
# whose expression spans two different mixins' columns.


class TPermissionMixin(SQLModel):
    """App-specific-style custom mixin (simulates a real consumer app's PermissionMixin)."""

    permission_level: int = Field(default=0)


class TFullStackUser(
    IsDeletedMixin,
    OwnerMixin,
    AuditMixin,
    TPermissionMixin,
    BaseWithUUID,
    table=True,
):
    """4+ mixin stack, plus a concrete owner relationship and a cross-mixin hybrid_property."""

    model_config: ClassVar[SQLModelConfig] = {"ignored_types": (hybrid_property,)}

    username: str
    created_by_user_id: UUID | None = Field(default=None, foreign_key="treftarget.id")
    owner_ref: TRefTarget | None = Relationship(
        sa_relationship_kwargs={"foreign_keys": "[TFullStackUser.created_by_user_id]"},
    )

    @hybrid_property
    def effective_permission_level(self) -> int:
        """Python-side: 0 once soft-deleted, else the stored permission level."""
        if self.is_deleted:
            return 0
        return self.permission_level

    @effective_permission_level.inplace.expression
    @classmethod
    def _effective_permission_level_expression(cls) -> ColumnElement[int]:
        """SQL-side equivalent, usable in order_by/filter — reads both mixins' columns."""
        return case(
            (col(cls.is_deleted).is_(True), 0),
            else_=col(cls.permission_level),
        )


class TFullStackUserCreate(SQLModel):
    """Create-payload for TFullStackUser."""

    username: str
    permission_level: int = 0
    created_by_user_id: UUID | None = None


class TFullStackUserRepository(SoftDeleteMixin[TFullStackUser, UUID]):
    """Soft-deletable repository for TFullStackUser."""

    Schema = TFullStackUser


@pytest.mark.asyncio
async def test_four_plus_mixin_stack_with_owner_relationship_and_hybrid_property(
    async_db: Database,
) -> None:
    """A 4+ mixin stack composes cleanly with a relationship and a cross-mixin hybrid_property."""
    target = await TRefTargetRepository(async_db).create(TRefTargetCreate(name="owner-target"))

    repo = TFullStackUserRepository(async_db)
    created = await repo.create(
        TFullStackUserCreate(username="alice", permission_level=5, created_by_user_id=target.id),
    )

    assert created.is_deleted is False
    assert created.created_at is not None
    assert created.updated_at is not None
    assert created.effective_permission_level == 5

    owner_ref_attr = cast("QueryableAttribute[TRefTarget | None]", TFullStackUser.owner_ref)
    stmt = (
        select(TFullStackUser)
        .where(col(TFullStackUser.id) == created.id)
        .options(selectinload(owner_ref_attr))
    )
    fetched = await repo.execute(stmt)
    assert fetched is not None
    assert fetched.owner_ref is not None
    assert fetched.owner_ref.id == target.id

    removed = await repo.remove(created.id)
    assert removed.is_deleted is True
    assert removed.effective_permission_level == 0

    # SQL-side expression form works too — confirms the hybrid_property's
    # expression half, which reads columns from two different mixins,
    # compiles and executes correctly (not just the Python-side property).
    order_stmt = (
        select(TFullStackUser)
        .execution_options(mint_scope_bypass=frozenset({IsDeletedMixin}))
        .order_by(TFullStackUser.effective_permission_level.desc())
    )
    rows = await repo.execute_many(order_stmt)
    assert len(rows) == 1
    assert rows[0].id == created.id
