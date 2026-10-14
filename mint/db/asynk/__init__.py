"""Async session lifecycle and repository layer for mint.db.

The lifecycle names — :class:`Database`, :class:`UnitOfWork`,
:func:`scoped` — need SQLAlchemy only and import eagerly. The repository
classes need SQLModel and load on first access, so a service that writes
SQLAlchemy Core and never touches a repository never imports it.
"""

from importlib import import_module
from typing import TYPE_CHECKING, Final

from .database import Database
from .scope import IDatabaseBound, scoped
from .uow import UnitOfWork

# Lazily-loaded export name -> the submodule defining it.
_MP_STR_STR: Final[dict[str, str]] = {
    "EntityRepository": "entity",
    "IOwner": "mixins",
    "MaterializedViewRepository": "mv",
    "RepositoryBase": "base",
    "ResourceOwnerMixin": "mixins",
    "SoftDeleteMixin": "mixins",
}

__all__ = [
    "Database",
    "EntityRepository",
    "IDatabaseBound",
    "IOwner",
    "MaterializedViewRepository",
    "RepositoryBase",
    "ResourceOwnerMixin",
    "SoftDeleteMixin",
    "UnitOfWork",
    "scoped",
]

if TYPE_CHECKING:
    from .base import RepositoryBase
    from .entity import EntityRepository
    from .mixins import IOwner, ResourceOwnerMixin, SoftDeleteMixin
    from .mv import MaterializedViewRepository
else:
    # Runtime only: a module-level __getattr__ visible to a type checker
    # would make every misspelt import from this package type-check.

    def __getattr__(name: str) -> object:
        """Import a repository class the first time it is asked for (PEP 562).

        Args:
            name: The attribute looked up on this package.

        Returns:
            The repository class or protocol of that name.

        Raises:
            AttributeError: If this package exports no such name.

        """
        submodule = _MP_STR_STR.get(name)
        if submodule is None:
            message = f"module {__name__!r} has no attribute {name!r}"
            raise AttributeError(message)
        return getattr(import_module(f"{__name__}.{submodule}"), name)
