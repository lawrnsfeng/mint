"""Real-world schema-shape tests (spec.md User Story 5 / SC-004).

Covers the shapes catalogued from reading two production mini-consumer
codebases: composite-key join tables with no ``id``, read-only
materialized views, self-referential (tree) relationships, and
many-to-many relationships.
"""

from collections.abc import AsyncGenerator
from typing import Optional, cast
from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.orm import QueryableAttribute, selectinload
from sqlmodel import Field, Relationship, SQLModel, col, select

from mint.db.asynk.base import RepositoryBase
from mint.db.asynk.database import Database
from mint.db.asynk.entity import EntityRepository
from mint.db.asynk.mv import MaterializedViewRepository
from mint.db.models import Base, BaseWithUUID
from tests.db.schemas import TItem

# --- Many-to-many + composite-key join table (no id column) ---


class TWidgetLabelLink(Base, table=True):
    """Composite-PK join table: no id column at all."""

    __tablename__ = "twidgetlabellink"

    widget_id: UUID = Field(foreign_key="twidget.id", primary_key=True)
    label_id: UUID = Field(foreign_key="tlabel.id", primary_key=True)


class TWidget(BaseWithUUID, table=True):
    """One side of a many-to-many relationship."""

    name: str
    labels: list["TLabel"] = Relationship(back_populates="widgets", link_model=TWidgetLabelLink)


class TLabel(BaseWithUUID, table=True):
    """Other side of a many-to-many relationship."""

    name: str
    widgets: list["TWidget"] = Relationship(back_populates="labels", link_model=TWidgetLabelLink)


class TWidgetLabelLinkCreate(SQLModel):
    """Create-payload for the join table."""

    widget_id: UUID
    label_id: UUID


class TWidgetCreate(SQLModel):
    """Create-payload for TWidget."""

    name: str


class TLabelCreate(SQLModel):
    """Create-payload for TLabel."""

    name: str


class WidgetLabelLinkRepository(RepositoryBase[TWidgetLabelLink]):
    """No id column → plain RepositoryBase, not EntityRepository (FR-009)."""

    Schema = TWidgetLabelLink


class WidgetRepository(EntityRepository[TWidget, UUID]):
    """CRUD repository for TWidget."""

    Schema = TWidget


class LabelRepository(EntityRepository[TLabel, UUID]):
    """CRUD repository for TLabel."""

    Schema = TLabel


# --- Self-referential (tree) relationship ---


class TFolder(BaseWithUUID, table=True):
    """Self-referential tree: a folder may have a parent folder."""

    name: str
    parent_folder_id: UUID | None = Field(default=None, foreign_key="tfolder.id")
    # SQLAlchemy's relationship-target resolver parses this annotation string
    # to find the related class name; Optional[...] resolves reliably where
    # a PEP 604 "TFolder | None" string does not.
    parent_folder: Optional["TFolder"] = Relationship(
        back_populates="child_folders",
        sa_relationship_kwargs={"remote_side": "TFolder.id"},
    )
    child_folders: list["TFolder"] = Relationship(back_populates="parent_folder")


class TFolderCreate(SQLModel):
    """Create-payload for TFolder."""

    name: str
    parent_folder_id: UUID | None = None


class FolderRepository(EntityRepository[TFolder, UUID]):
    """CRUD repository for TFolder."""

    Schema = TFolder


# --- Read-only materialized view ---


class TMVItemView(Base, table=True):
    """Read-only view mirroring TItem — backed by a real materialized view."""

    __tablename__ = "tmvitemview"

    id: UUID = Field(primary_key=True)
    name: str


class MVItemViewRepository(MaterializedViewRepository[TMVItemView]):
    """Read-only repository for the materialized view."""

    Schema = TMVItemView


@pytest.mark.asyncio
async def test_composite_key_table_uses_plain_repository_base(async_db: Database) -> None:
    """A table with no id column works via RepositoryBase, not EntityRepository (FR-009)."""
    widget = await WidgetRepository(async_db).create(TWidgetCreate(name="w"))
    label = await LabelRepository(async_db).create(TLabelCreate(name="l"))
    link_repo = WidgetLabelLinkRepository(async_db)

    await link_repo.create(TWidgetLabelLinkCreate(widget_id=widget.id, label_id=label.id))

    links = await link_repo.get_many()
    assert len(links) == 1
    assert links[0].widget_id == widget.id
    assert links[0].label_id == label.id


@pytest.mark.asyncio
async def test_composite_key_table_upsert_with_no_extra_columns(async_db: Database) -> None:
    """create(upsert=True) on a table with only conflict-target columns still returns a row.

    TWidgetLabelLink has no columns besides its composite primary key, so
    there's nothing left to put in the ON CONFLICT ... DO UPDATE SET
    clause once the conflict-target columns are excluded — exercises the
    no-op-SET-on-conflict-columns-themselves fallback.
    """
    widget = await WidgetRepository(async_db).create(TWidgetCreate(name="w"))
    label = await LabelRepository(async_db).create(TLabelCreate(name="l"))
    link_repo = WidgetLabelLinkRepository(async_db)
    payload = TWidgetLabelLinkCreate(widget_id=widget.id, label_id=label.id)

    first = await link_repo.create(payload, upsert=True)
    second = await link_repo.create(payload, upsert=True)

    assert first.widget_id == second.widget_id == widget.id
    assert first.label_id == second.label_id == label.id
    links = await link_repo.get_many()
    assert len(links) == 1


@pytest.mark.asyncio
async def test_many_to_many_relationship_resolves_via_selectinload(async_db: Database) -> None:
    """A many-to-many relationship resolves via the shared metadata and secondary= (FR-012)."""
    widget = await WidgetRepository(async_db).create(TWidgetCreate(name="w"))
    label = await LabelRepository(async_db).create(TLabelCreate(name="l"))
    await WidgetLabelLinkRepository(async_db).create(
        TWidgetLabelLinkCreate(widget_id=widget.id, label_id=label.id),
    )

    repo = WidgetRepository(async_db)
    labels_attr = cast("QueryableAttribute[list[TLabel]]", TWidget.labels)
    stmt = select(TWidget).where(col(TWidget.id) == widget.id).options(selectinload(labels_attr))
    fetched = await repo.execute(stmt)

    assert fetched is not None
    assert [w.id for w in fetched.labels] == [label.id]


@pytest.mark.asyncio
async def test_self_referential_relationship_loads_without_runaway_recursion(
    async_db: Database,
) -> None:
    """A tree relationship resolves through selectinload without unbounded recursion (FR-011)."""
    repo = FolderRepository(async_db)
    root = await repo.create(TFolderCreate(name="root"))
    child = await repo.create(TFolderCreate(name="child", parent_folder_id=root.id))

    parent_folder_attr = cast("QueryableAttribute[TFolder | None]", TFolder.parent_folder)
    child_stmt = (
        select(TFolder)
        .where(col(TFolder.id) == child.id)
        .options(selectinload(parent_folder_attr))
    )
    fetched_child = await repo.execute(child_stmt)
    assert fetched_child is not None
    assert fetched_child.parent_folder is not None
    assert fetched_child.parent_folder.id == root.id

    child_folders_attr = cast("QueryableAttribute[list[TFolder]]", TFolder.child_folders)
    root_stmt = (
        select(TFolder).where(col(TFolder.id) == root.id).options(selectinload(child_folders_attr))
    )
    fetched_root = await repo.execute(root_stmt)
    assert fetched_root is not None
    assert [f.id for f in fetched_root.child_folders] == [child.id]


@pytest.fixture
async def materialized_view(async_db: Database) -> AsyncGenerator[None]:
    """Replace the auto-created dummy table with a real materialized view."""
    async with async_db.engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS tmvitemview"))
        await conn.execute(
            text("CREATE MATERIALIZED VIEW tmvitemview AS SELECT id, name FROM titem"),
        )
        await conn.execute(text("CREATE UNIQUE INDEX ON tmvitemview (id)"))
    yield
    async with async_db.engine.begin() as conn:
        await conn.execute(text("DROP MATERIALIZED VIEW IF EXISTS tmvitemview"))


class TItemCreate(SQLModel):
    """Create-payload for TItem, used only to seed the materialized view's source table."""

    name: str


class TItemRepository(EntityRepository[TItem, UUID]):
    """CRUD repository for TItem."""

    Schema = TItem


@pytest.mark.asyncio
async def test_materialized_view_refresh_and_read(
    async_db: Database,
    materialized_view: None,
) -> None:
    """A materialized-view-backed repository refreshes and reads (FR-010)."""
    item_repo = TItemRepository(async_db)
    await item_repo.create(TItemCreate(name="via-item"))

    view_repo = MVItemViewRepository(async_db)
    await view_repo.refresh_materialized_view()
    rows = await view_repo.get_many()

    assert len(rows) == 1
    assert rows[0].name == "via-item"
    # MaterializedViewRepository extends RepositoryBase, not EntityRepository —
    # no identifier-based single-record write methods are exposed.
    assert not hasattr(view_repo, "update")
    assert not hasattr(view_repo, "remove")
