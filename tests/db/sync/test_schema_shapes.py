"""Real-world schema-shape tests (spec.md User Story 5 / SC-004), sync mirror."""

from collections.abc import Iterator
from typing import Optional, cast
from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.orm import QueryableAttribute, selectinload
from sqlmodel import Field, Relationship, SQLModel, col, select

from mint.db.models import Base, BaseWithUUID
from mint.db.sync.base import RepositoryBase
from mint.db.sync.database import Database
from mint.db.sync.entity import EntityRepository
from mint.db.sync.mv import MaterializedViewRepository
from tests.db.schemas import TItem

# --- Many-to-many + composite-key join table (no id column) ---


class SWidgetLabelLink(Base, table=True):
    """Composite-PK join table: no id column at all."""

    __tablename__ = "swidgetlabellink"

    widget_id: UUID = Field(foreign_key="swidget.id", primary_key=True)
    label_id: UUID = Field(foreign_key="slabel.id", primary_key=True)


class SWidget(BaseWithUUID, table=True):
    """One side of a many-to-many relationship."""

    name: str
    labels: list["SLabel"] = Relationship(back_populates="widgets", link_model=SWidgetLabelLink)


class SLabel(BaseWithUUID, table=True):
    """Other side of a many-to-many relationship."""

    name: str
    widgets: list["SWidget"] = Relationship(back_populates="labels", link_model=SWidgetLabelLink)


class SWidgetLabelLinkCreate(SQLModel):
    """Create-payload for the join table."""

    widget_id: UUID
    label_id: UUID


class SWidgetCreate(SQLModel):
    """Create-payload for SWidget."""

    name: str


class SLabelCreate(SQLModel):
    """Create-payload for SLabel."""

    name: str


class WidgetLabelLinkRepository(RepositoryBase[SWidgetLabelLink]):
    """No id column → plain RepositoryBase, not EntityRepository (FR-009)."""

    Schema = SWidgetLabelLink


class WidgetRepository(EntityRepository[SWidget, UUID]):
    """CRUD repository for SWidget."""

    Schema = SWidget


class LabelRepository(EntityRepository[SLabel, UUID]):
    """CRUD repository for SLabel."""

    Schema = SLabel


# --- Self-referential (tree) relationship ---


class SFolder(BaseWithUUID, table=True):
    """Self-referential tree: a folder may have a parent folder."""

    name: str
    parent_folder_id: UUID | None = Field(default=None, foreign_key="sfolder.id")
    # SQLAlchemy's relationship-target resolver parses this annotation string
    # to find the related class name; Optional[...] resolves reliably where
    # a PEP 604 "SFolder | None" string does not.
    parent_folder: Optional["SFolder"] = Relationship(
        back_populates="child_folders",
        sa_relationship_kwargs={"remote_side": "SFolder.id"},
    )
    child_folders: list["SFolder"] = Relationship(back_populates="parent_folder")


class SFolderCreate(SQLModel):
    """Create-payload for SFolder."""

    name: str
    parent_folder_id: UUID | None = None


class FolderRepository(EntityRepository[SFolder, UUID]):
    """CRUD repository for SFolder."""

    Schema = SFolder


# --- Read-only materialized view ---


class SMVItemView(Base, table=True):
    """Read-only view mirroring TItem — backed by a real materialized view."""

    __tablename__ = "smvitemview"

    id: UUID = Field(primary_key=True)
    name: str


class MVItemViewRepository(MaterializedViewRepository[SMVItemView]):
    """Read-only repository for the materialized view."""

    Schema = SMVItemView


def test_composite_key_table_uses_plain_repository_base(sync_db: Database) -> None:
    """A table with no id column works via RepositoryBase, not EntityRepository (FR-009)."""
    widget = WidgetRepository(sync_db).create(SWidgetCreate(name="w"))
    label = LabelRepository(sync_db).create(SLabelCreate(name="l"))
    link_repo = WidgetLabelLinkRepository(sync_db)

    link_repo.create(SWidgetLabelLinkCreate(widget_id=widget.id, label_id=label.id))

    links = link_repo.get_many()
    assert len(links) == 1
    assert links[0].widget_id == widget.id
    assert links[0].label_id == label.id


def test_composite_key_table_upsert_with_no_extra_columns(sync_db: Database) -> None:
    """create(upsert=True) on a table with only conflict-target columns still returns a row.

    SWidgetLabelLink has no columns besides its composite primary key, so
    there's nothing left to put in the ON CONFLICT ... DO UPDATE SET
    clause once the conflict-target columns are excluded — exercises the
    no-op-SET-on-conflict-columns-themselves fallback.
    """
    widget = WidgetRepository(sync_db).create(SWidgetCreate(name="w"))
    label = LabelRepository(sync_db).create(SLabelCreate(name="l"))
    link_repo = WidgetLabelLinkRepository(sync_db)
    payload = SWidgetLabelLinkCreate(widget_id=widget.id, label_id=label.id)

    first = link_repo.create(payload, upsert=True)
    second = link_repo.create(payload, upsert=True)

    assert first.widget_id == second.widget_id == widget.id
    assert first.label_id == second.label_id == label.id
    links = link_repo.get_many()
    assert len(links) == 1


def test_many_to_many_relationship_resolves_via_selectinload(sync_db: Database) -> None:
    """A many-to-many relationship resolves via the shared metadata and secondary= (FR-012)."""
    widget = WidgetRepository(sync_db).create(SWidgetCreate(name="w"))
    label = LabelRepository(sync_db).create(SLabelCreate(name="l"))
    WidgetLabelLinkRepository(sync_db).create(
        SWidgetLabelLinkCreate(widget_id=widget.id, label_id=label.id),
    )

    repo = WidgetRepository(sync_db)
    labels_attr = cast("QueryableAttribute[list[SLabel]]", SWidget.labels)
    stmt = select(SWidget).where(col(SWidget.id) == widget.id).options(selectinload(labels_attr))
    fetched = repo.execute(stmt)

    assert fetched is not None
    assert [w.id for w in fetched.labels] == [label.id]


def test_self_referential_relationship_loads_without_runaway_recursion(
    sync_db: Database,
) -> None:
    """A tree relationship resolves through selectinload without unbounded recursion (FR-011)."""
    repo = FolderRepository(sync_db)
    root = repo.create(SFolderCreate(name="root"))
    child = repo.create(SFolderCreate(name="child", parent_folder_id=root.id))

    parent_folder_attr = cast("QueryableAttribute[SFolder | None]", SFolder.parent_folder)
    child_stmt = (
        select(SFolder)
        .where(col(SFolder.id) == child.id)
        .options(selectinload(parent_folder_attr))
    )
    fetched_child = repo.execute(child_stmt)
    assert fetched_child is not None
    assert fetched_child.parent_folder is not None
    assert fetched_child.parent_folder.id == root.id

    child_folders_attr = cast("QueryableAttribute[list[SFolder]]", SFolder.child_folders)
    root_stmt = (
        select(SFolder).where(col(SFolder.id) == root.id).options(selectinload(child_folders_attr))
    )
    fetched_root = repo.execute(root_stmt)
    assert fetched_root is not None
    assert [f.id for f in fetched_root.child_folders] == [child.id]


@pytest.fixture
def materialized_view(sync_db: Database) -> Iterator[None]:
    """Replace the auto-created dummy table with a real materialized view."""
    with sync_db.engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS smvitemview"))
        conn.execute(text("CREATE MATERIALIZED VIEW smvitemview AS SELECT id, name FROM titem"))
        conn.execute(text("CREATE UNIQUE INDEX ON smvitemview (id)"))
    yield
    with sync_db.engine.begin() as conn:
        conn.execute(text("DROP MATERIALIZED VIEW IF EXISTS smvitemview"))


class TItemCreate(SQLModel):
    """Create-payload for TItem, used only to seed the materialized view's source table."""

    name: str


class TItemRepository(EntityRepository[TItem, UUID]):
    """CRUD repository for TItem."""

    Schema = TItem


def test_materialized_view_refresh_and_read(
    sync_db: Database,
    materialized_view: None,
) -> None:
    """A materialized-view-backed repository refreshes and reads (FR-010)."""
    item_repo = TItemRepository(sync_db)
    item_repo.create(TItemCreate(name="via-item"))

    view_repo = MVItemViewRepository(sync_db)
    view_repo.refresh_materialized_view()
    rows = view_repo.get_many()

    assert len(rows) == 1
    assert rows[0].name == "via-item"
    # MaterializedViewRepository extends RepositoryBase, not EntityRepository —
    # no identifier-based single-record write methods are exposed.
    assert not hasattr(view_repo, "update")
    assert not hasattr(view_repo, "remove")
