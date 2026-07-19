"""Shared test-only SQLModel schemas, reused across tests/db.

Prefixed with ``T`` to make it obvious at every call site that these are
test fixtures, not real mint.db-shipped tables.
"""

from mint.db.models import AuditMixin, BaseWithUUID, IsDeletedMixin, OwnerMixin


class TItem(BaseWithUUID, table=True):
    """Plain table: no owner scoping, no soft delete."""

    name: str


class TJob(OwnerMixin, AuditMixin, BaseWithUUID, table=True):
    """Owner-scoped, audited table."""

    name: str


class TDoc(IsDeletedMixin, BaseWithUUID, table=True):
    """Soft-deletable table."""

    name: str
