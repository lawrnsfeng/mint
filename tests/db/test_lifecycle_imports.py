"""Contract: the session lifecycle of mint.db loads without SQLModel.

A service that writes SQLAlchemy Core uses :class:`Database`,
:class:`UnitOfWork` and ``scoped`` and nothing else. Importing those must
not pull in ``sqlmodel``; the repository classes load on first access.
"""

import multiprocessing
import sys
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from importlib import import_module
from typing import Self

import pytest

_FLAVOURS = ("asynk", "sync")


@dataclass
class ImportProbe:
    """Whether ``sqlmodel`` was loaded at two points of one interpreter's life."""

    after_lifecycle: bool
    after_repository: bool

    @classmethod
    def run(cls, flavour: str) -> Self:
        """Import one flavour of mint.db here and record when ``sqlmodel`` loads.

        Meant to run in a freshly spawned interpreter: in the test process
        ``sqlmodel`` is long since imported by other test modules.

        Args:
            flavour: ``"asynk"`` or ``"sync"``.

        Returns:
            The two observations.

        """
        package = import_module(f"mint.db.{flavour}")
        assert callable(package.Database)
        assert callable(package.UnitOfWork)
        assert callable(package.scoped)
        after_lifecycle = "sqlmodel" in sys.modules
        assert callable(package.RepositoryBase)
        return cls(
            after_lifecycle=after_lifecycle,
            after_repository="sqlmodel" in sys.modules,
        )


@pytest.mark.parametrize("flavour", _FLAVOURS)
def test_lifecycle_names_load_without_sqlmodel(flavour: str) -> None:
    """Database, UnitOfWork and scoped import without importing sqlmodel."""
    context = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(max_workers=1, mp_context=context) as executor:
        probe = executor.submit(ImportProbe.run, flavour).result()

    assert probe.after_lifecycle is False
    assert probe.after_repository is True


@pytest.mark.parametrize("flavour", _FLAVOURS)
def test_every_export_resolves(flavour: str) -> None:
    """Each name in ``__all__`` is reachable, eager or lazy, at its documented path."""
    package = import_module(f"mint.db.{flavour}")

    for name in package.__all__:
        assert getattr(package, name) is not None

    base = import_module(f"mint.db.{flavour}.base")
    assert package.RepositoryBase is base.RepositoryBase


@pytest.mark.parametrize("flavour", _FLAVOURS)
def test_unknown_export_raises_attribute_error(flavour: str) -> None:
    """A name the package does not export fails like any missing attribute."""
    package = import_module(f"mint.db.{flavour}")

    with pytest.raises(AttributeError, match="NotExported"):
        _ = package.NotExported
