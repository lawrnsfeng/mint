"""Read-only repository for Postgres materialized-view-backed schemas."""

from sqlalchemy import text

from mint.db.models import Base

from .base import RepositoryBase


class MaterializedViewRepository[T: Base](RepositoryBase[T]):
    """Read-only data access for a schema backed by a materialized view.

    No create/update/remove — only reads (via the inherited
    :meth:`RepositoryBase.get_many`/:meth:`RepositoryBase.count`) plus
    :meth:`refresh_materialized_view`.
    """

    @property
    def table_name(self) -> str:
        """Return the dbschema-qualified view name."""
        if self._dbschema:
            return f"{self._dbschema}.{self.Schema.__tablename__}"
        return str(self.Schema.__tablename__)

    @RepositoryBase.ensure_session
    async def refresh_materialized_view(self) -> None:
        """Refresh the underlying materialized view without blocking readers."""
        stmt = text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {self.table_name}")
        await self.session.execute(stmt)
        if self.auto_commit:
            await self.session.commit()
