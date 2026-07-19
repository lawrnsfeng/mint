"""Shared Postgres testcontainer fixtures for mint.db tests.

Every test in tests/db exercises real behavior against a real PostgreSQL
instance — no sqlite/in-memory substitute — since the bug this feature
fixes (session-isolation under concurrency) only manifests against a real
async driver. See specs/002-db-repository-layer/research.md,
"Verification approach".
"""

from collections.abc import AsyncGenerator, Generator, Iterator
from typing import Final

import pytest
from sqlalchemy import Engine, create_engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from testcontainers.postgres import PostgresContainer

from mint.db.asynk.database import Database as AsyncDatabase
from mint.db.models import Base
from mint.db.sync.database import Database as SyncDatabase

_IMAGE: Final[str] = "postgres:16"


@pytest.fixture(scope="session")
def postgres_container() -> Generator[PostgresContainer]:
    """Provide a single Postgres container for the entire test session.

    Yields:
        PostgresContainer: Running Postgres container instance.

    """
    with PostgresContainer(_IMAGE) as container:
        yield container


@pytest.fixture(scope="session")
def postgres_async_url(postgres_container: PostgresContainer) -> str:
    """Provide the asyncpg connection URL for the running container.

    Args:
        postgres_container: The running Postgres container.

    Returns:
        str: An ``postgresql+asyncpg://`` connection URL.

    """
    return postgres_container.get_connection_url(driver="asyncpg")


@pytest.fixture(scope="session")
def postgres_sync_url(postgres_container: PostgresContainer) -> str:
    """Provide the psycopg2 connection URL for the running container.

    Args:
        postgres_container: The running Postgres container.

    Returns:
        str: A ``postgresql+psycopg2://`` connection URL.

    """
    return postgres_container.get_connection_url(driver="psycopg2")


@pytest.fixture
async def async_engine(postgres_async_url: str) -> AsyncGenerator[AsyncEngine]:
    """Provide a fresh async engine scoped to one test function.

    Function-scoped (not session-scoped): pytest-asyncio gives each test
    its own event loop, and asyncpg connections cannot be reused across
    event loops — a session-scoped engine's pooled connections would break
    on the second test to touch them.

    Args:
        postgres_async_url: The asyncpg connection URL.

    Yields:
        AsyncEngine: A per-test async engine.

    """
    engine = create_async_engine(postgres_async_url)
    yield engine
    await engine.dispose()


@pytest.fixture(scope="session")
def sync_engine(postgres_sync_url: str) -> Generator[Engine]:
    """Provide one pooled sync engine for the whole test session.

    Args:
        postgres_sync_url: The psycopg2 connection URL.

    Yields:
        Engine: The shared sync engine.

    """
    engine = create_engine(postgres_sync_url)
    yield engine
    engine.dispose()


@pytest.fixture
async def clean_schema(async_engine: AsyncEngine) -> AsyncGenerator[None]:
    """Recreate every table registered on ``Base.metadata`` around a test.

    Args:
        async_engine: The shared async engine.

    Yields:
        None.

    """
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest.fixture
def async_db(async_engine: AsyncEngine, clean_schema: None) -> AsyncDatabase:
    """Provide an async ``Database`` bound to a freshly-created schema.

    Args:
        async_engine: The shared async engine.
        clean_schema: Ensures tables exist before the test runs.

    Returns:
        AsyncDatabase: Configured database wrapper.

    """
    return AsyncDatabase(engine=async_engine)


@pytest.fixture
def sync_clean_schema(sync_engine: Engine) -> Iterator[None]:
    """Recreate every table registered on ``Base.metadata`` around a test.

    Args:
        sync_engine: The shared sync engine.

    Yields:
        None.

    """
    Base.metadata.drop_all(sync_engine)
    Base.metadata.create_all(sync_engine)
    yield
    Base.metadata.drop_all(sync_engine)


@pytest.fixture
def sync_db(sync_engine: Engine, sync_clean_schema: None) -> SyncDatabase:
    """Provide a sync ``Database`` bound to a freshly-created schema.

    Args:
        sync_engine: The shared sync engine.
        sync_clean_schema: Ensures tables exist before the test runs.

    Returns:
        SyncDatabase: Configured database wrapper.

    """
    return SyncDatabase(engine=sync_engine)
