"""Fixtures for Azure Blob Storage tests using Azurite testcontainer."""

from collections.abc import AsyncGenerator, Generator

import pytest
from azure.storage.blob.aio import BlobServiceClient, ContainerClient
from testcontainers.azurite import AzuriteContainer

from mint.fs.asynk.abs import AzureBlobStorage

# Azurite default credentials
AZURITE_ACCOUNT_NAME = "devstoreaccount1"
TEST_CONTAINER_NAME = "test-container"


@pytest.fixture(scope="session")
def azurite_container() -> Generator[AzuriteContainer]:
    """Provide a single Azurite container instance for the entire test session.

    This fixture creates an Azurite container that runs for all tests
    and is cleaned up after the test session completes.

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
    port = azurite_container.get_exposed_port(10000)
    return f"http://{host}:{port}/{AZURITE_ACCOUNT_NAME}"


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
        TEST_CONTAINER_NAME,
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
    return TEST_CONTAINER_NAME


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
        storage_account_name=AZURITE_ACCOUNT_NAME,
        connection_string=azurite_connection_string,
    )
