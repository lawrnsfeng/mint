"""Fixtures for Azure Blob Storage tests using Azurite testcontainer."""

import os
from collections.abc import AsyncGenerator, Generator
from dataclasses import dataclass
from typing import Final, Self

import pytest
from azure.storage.blob.aio import BlobServiceClient, ContainerClient
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import PortWaitStrategy

from mint.fs.asynk.abs import AzureBlobStorage


@dataclass(frozen=True, slots=True)
class AzuritePorts:
    """Azurite service port mapping."""

    blob: int = 10000
    queue: int = 10001
    table: int = 10002


@dataclass(frozen=True, slots=True)
class AzuriteAccount:
    """Azurite storage account credentials."""

    name: str = "devstoreaccount1"
    key: str = (
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsu"
        "Fq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
    )

    @classmethod
    def from_env(cls) -> Self:
        """Build account credentials from environment overrides."""
        default = cls()
        return cls(
            name=os.environ.get("AZURITE_ACCOUNT_NAME", default.name),
            key=os.environ.get("AZURITE_ACCOUNT_KEY", default.key),
        )


class AzuriteContainer(DockerContainer):
    """Custom Azurite container using new wait strategy API.

    This avoids the deprecated @wait_container_is_ready decorator
    from the upstream testcontainers.azurite module.

    """

    TEST_CONTAINER_NAME: Final[str] = "test-container"
    DEFAULT_PORTS: Final[AzuritePorts] = AzuritePorts()
    DEFAULT_ACCOUNT: Final[AzuriteAccount] = AzuriteAccount()
    AZURITE_ACCOUNT_NAME: Final[str] = DEFAULT_ACCOUNT.name
    AZURITE_ACCOUNT_KEY: Final[str] = DEFAULT_ACCOUNT.key
    AZURITE_BLOB_PORT: Final[int] = DEFAULT_PORTS.blob
    AZURITE_QUEUE_PORT: Final[int] = DEFAULT_PORTS.queue
    AZURITE_TABLE_PORT: Final[int] = DEFAULT_PORTS.table

    def __init__(
        self,
        image: str = "mcr.microsoft.com/azure-storage/azurite:latest",
        *,
        ports: AzuritePorts | None = None,
        account: AzuriteAccount | None = None,
    ) -> None:
        """Initialize AzuriteContainer with structured wait strategy."""
        super().__init__(image=image)
        self.service_ports = ports or self.DEFAULT_PORTS
        self.account = account or AzuriteAccount.from_env()

        self.with_exposed_ports(
            self.service_ports.blob,
            self.service_ports.queue,
            self.service_ports.table,
        )
        self.with_env(
            "AZURITE_ACCOUNTS",
            f"{self.account.name}:{self.account.key}",
        )
        self.waiting_for(PortWaitStrategy(self.service_ports.blob))

    def get_connection_string(self) -> str:
        """Generate connection string for local host access."""
        host_ip = self.get_container_host_ip()
        blob_port = self.get_exposed_port(self.service_ports.blob)
        queue_port = self.get_exposed_port(self.service_ports.queue)
        table_port = self.get_exposed_port(self.service_ports.table)

        return (
            f"DefaultEndpointsProtocol=http;"
            f"AccountName={self.account.name};"
            f"AccountKey={self.account.key};"
            f"BlobEndpoint=http://{host_ip}:{blob_port}/{self.account.name};"
            f"QueueEndpoint=http://{host_ip}:{queue_port}/{self.account.name};"
            f"TableEndpoint=http://{host_ip}:{table_port}/{self.account.name};"
        )

    def start(self) -> Self:
        """Start container without deprecated wait decorator."""
        super().start()
        return self


@pytest.fixture(scope="session")
def azurite_container() -> Generator[AzuriteContainer]:
    """Provide a single Azurite container instance for the entire test session.

    This fixture creates an Azurite container that runs for all tests
    and is cleaned up after the test session completes. Uses custom
    container with PortWaitStrategy to avoid deprecated decorators.

    Yields:
        AzuriteContainer: Running Azurite container instance.

    """
    with AzuriteContainer() as container:
        yield container


@pytest.fixture(scope="session")
def azurite_connection_string(
    azurite_container: AzuriteContainer,
) -> str:
    """Provide the connection string for the Azurite blob service.

    Args:
        azurite_container: The running Azurite container.

    Returns:
        str: Connection string for Azure Blob Storage client.

    """
    return azurite_container.get_connection_string()


@pytest.fixture(scope="session")
def azurite_blob_endpoint(azurite_container: AzuriteContainer) -> str:
    """Provide the blob service endpoint URL for the Azurite container.

    Args:
        azurite_container: The running Azurite container.

    Returns:
        str: Blob service endpoint URL.

    """
    host = azurite_container.get_container_host_ip()
    blob_port = azurite_container.service_ports.blob
    port = azurite_container.get_exposed_port(blob_port)
    return f"http://{host}:{port}/{azurite_container.account.name}"


@pytest.fixture
async def blob_service_client(
    azurite_connection_string: str,
) -> AsyncGenerator[BlobServiceClient]:
    """Provide an async BlobServiceClient connected to Azurite.

    Args:
        azurite_connection_string: Connection string for Azurite.

    Yields:
        BlobServiceClient: Async Azure Blob Service client.

    """
    client = BlobServiceClient.from_connection_string(
        azurite_connection_string,
    )
    try:
        yield client
    finally:
        await client.close()


@pytest.fixture
async def test_container(
    blob_service_client: BlobServiceClient,
) -> AsyncGenerator[ContainerClient]:
    """Provide a clean test container for each test function.

    Creates the container before the test and cleans up all blobs
    after the test completes.

    Args:
        blob_service_client: The blob service client.

    Yields:
        ContainerClient: Container client for the test container.

    """
    container_client = blob_service_client.get_container_client(
        AzuriteContainer.TEST_CONTAINER_NAME,
    )

    # Create container if it doesn't exist
    try:
        await container_client.create_container()
    except Exception:  # noqa: BLE001
        # Container may already exist, clean it up
        async for blob in container_client.list_blobs():
            await container_client.delete_blob(blob.name)

    try:
        yield container_client
    finally:
        # Clean up all blobs after test
        async for blob in container_client.list_blobs():
            await container_client.delete_blob(blob.name)


@pytest.fixture
def test_container_name() -> str:
    """Provide the test container name.

    Returns:
        str: Name of the test container.

    """
    return AzuriteContainer.TEST_CONTAINER_NAME


@pytest.fixture
def azure_storage(
    azurite_connection_string: str,
    test_container_name: str,
) -> AzureBlobStorage:
    """Provide an AzureBlobStorage instance configured for Azurite.

    Args:
        azurite_connection_string: Connection string for Azurite.
        test_container_name: Name of the test container.

    Returns:
        AzureBlobStorage: Storage instance connected to Azurite.

    """
    return AzureBlobStorage(
        container_name=test_container_name,
        storage_account_name=AzuriteContainer.AZURITE_ACCOUNT_NAME,
        connection_string=azurite_connection_string,
    )
