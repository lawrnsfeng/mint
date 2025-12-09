"""Tests for Azure Blob Storage implementation using Azurite testcontainer."""

from io import BytesIO
from pathlib import Path

import aiofiles.tempfile
import pytest
from azure.storage.blob.aio import BlobServiceClient, ContainerClient

from mint.fs.asynk.abs import AzureBlobStorage
from mint.fs.exc import (
    InvalidArgumentsError,
    ObjectNotFoundError,
)
from mint.fs.structs import (
    CopyResult,
    ListItem,
    MoveResult,
    RemoveResult,
    Stat,
)

# =============================================================================
# Azurite Container Health Check Test
# =============================================================================


@pytest.mark.asyncio
async def test_azurite_is_running_and_operational(
    blob_service_client: BlobServiceClient,
    test_container: ContainerClient,
    test_container_name: str,
) -> None:
    """Verify that Azurite container is running and operational.

    This test performs basic blob operations (create, get, delete) to
    ensure the Azurite testcontainer is properly configured and healthy.

    Args:
        blob_service_client: The blob service client (unused, for fixture).
        test_container: The test container client.
        test_container_name: Name of the test container.

    """
    container_props = await test_container.get_container_properties()
    assert container_props is not None
    assert container_props.name == test_container_name

    blob_name = "health-check-blob.txt"
    blob_content = b"Hello, Azurite!"
    blob_client = test_container.get_blob_client(blob_name)

    await blob_client.upload_blob(blob_content, overwrite=True)

    download_stream = await blob_client.download_blob()
    downloaded_content = await download_stream.readall()
    assert downloaded_content == blob_content

    await blob_client.delete_blob()

    blob_exists = await blob_client.exists()
    assert blob_exists is False


# =============================================================================
# IFileStorage.is_folder Method Tests
# =============================================================================


@pytest.mark.asyncio
async def test_is_folder_returns_true_for_folder_prefix(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that is_folder returns True for a path that is a folder prefix.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    blob_client = test_container.get_blob_client("folder/file.txt")
    await blob_client.upload_blob(b"content", overwrite=True)

    result = await azure_storage.is_folder("folder")
    assert result is True


@pytest.mark.asyncio
async def test_is_folder_returns_true_with_trailing_slash(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that is_folder handles trailing slash correctly.

    The trailing slash is stripped, so "folder/" checks for "folder" prefix.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    blob_client = test_container.get_blob_client("folder/file.txt")
    await blob_client.upload_blob(b"content", overwrite=True)

    result = await azure_storage.is_folder("folder/")
    assert result is True


@pytest.mark.asyncio
async def test_is_folder_returns_false_for_file(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that is_folder returns False for a path that is a file.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    blob_client = test_container.get_blob_client("file.txt")
    await blob_client.upload_blob(b"content", overwrite=True)

    result = await azure_storage.is_folder("file.txt")
    assert result is False


@pytest.mark.asyncio
async def test_is_folder_returns_false_for_nonexistent_path(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that is_folder returns False for nonexistent paths (no exception).

    Unlike other methods, is_folder does NOT raise ObjectNotFoundError.
    This allows safe checking before operations.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    result = await azure_storage.is_folder("nonexistent")
    assert result is False


# =============================================================================
# IFileStorage.get Method Tests
# =============================================================================


@pytest.mark.asyncio
async def test_get_downloads_file_to_local_path(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that get downloads a blob to the specified local path.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    blob_name = "download-test.txt"
    blob_content = b"Content to download"
    blob_client = test_container.get_blob_client(blob_name)
    await blob_client.upload_blob(blob_content, overwrite=True)

    async with aiofiles.tempfile.TemporaryDirectory() as tmpdir:
        save_path = Path(tmpdir) / "downloaded.txt"
        await azure_storage.get(blob_name, str(save_path))

        assert save_path.exists()
        assert save_path.read_bytes() == blob_content


@pytest.mark.asyncio
async def test_get_creates_parent_directories(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that get creates parent directories if they don't exist.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    blob_client = test_container.get_blob_client("nested-get.txt")
    await blob_client.upload_blob(b"content", overwrite=True)

    async with aiofiles.tempfile.TemporaryDirectory() as tmpdir:
        save_path = Path(tmpdir) / "nested" / "deep" / "downloaded.txt"
        await azure_storage.get("nested-get.txt", str(save_path))

        assert save_path.exists()


@pytest.mark.asyncio
async def test_get_empty_file_downloads_correctly(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that get downloads empty files correctly.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    blob_client = test_container.get_blob_client("get-empty.txt")
    await blob_client.upload_blob(b"", overwrite=True)

    async with aiofiles.tempfile.TemporaryDirectory() as tmpdir:
        save_path = Path(tmpdir) / "downloaded-empty.txt"
        await azure_storage.get("get-empty.txt", str(save_path))

        assert save_path.exists()
        assert save_path.read_bytes() == b""


@pytest.mark.asyncio
async def test_get_raises_error_for_nonexistent_file(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that get raises ObjectNotFoundError when the blob doesn't exist.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    async with aiofiles.tempfile.TemporaryDirectory() as tmpdir:
        save_path = Path(tmpdir) / "downloaded.txt"
        with pytest.raises(ObjectNotFoundError):
            await azure_storage.get("nonexistent.txt", str(save_path))


# =============================================================================
# IFileStorage.save Method Tests
# =============================================================================


@pytest.mark.asyncio
async def test_save_uploads_from_string_path(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that save uploads a file from a local string path.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    async with aiofiles.tempfile.TemporaryDirectory() as tmpdir:
        local_path = Path(tmpdir) / "upload.txt"
        content = b"Upload from string path"
        local_path.write_bytes(content)

        result = await azure_storage.save(
            "uploaded-string.txt",
            str(local_path),
        )

        assert result == "uploaded-string.txt"
        blob_client = test_container.get_blob_client("uploaded-string.txt")
        download = await blob_client.download_blob()
        assert await download.readall() == content


@pytest.mark.asyncio
async def test_save_uploads_from_pathlib_path(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that save uploads a file from a pathlib.Path object.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    async with aiofiles.tempfile.TemporaryDirectory() as tmpdir:
        local_path = Path(tmpdir) / "upload.txt"
        content = b"Upload from pathlib path"
        local_path.write_bytes(content)

        result = await azure_storage.save("uploaded-pathlib.txt", local_path)

        assert result == "uploaded-pathlib.txt"
        blob_client = test_container.get_blob_client("uploaded-pathlib.txt")
        download = await blob_client.download_blob()
        assert await download.readall() == content


@pytest.mark.asyncio
async def test_save_uploads_from_io_object(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that save uploads content from an IO object.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    content = b"Upload from BytesIO"
    io_obj = BytesIO(content)

    result = await azure_storage.save("uploaded-io.txt", io_obj)

    assert result == "uploaded-io.txt"
    blob_client = test_container.get_blob_client("uploaded-io.txt")
    download = await blob_client.download_blob()
    assert await download.readall() == content


@pytest.mark.asyncio
async def test_save_uploads_from_bytes(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that save uploads content from bytes directly.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    content = b"Upload from bytes directly"

    result = await azure_storage.save("uploaded-bytes.txt", content)

    assert result == "uploaded-bytes.txt"
    blob_client = test_container.get_blob_client("uploaded-bytes.txt")
    download = await blob_client.download_blob()
    assert await download.readall() == content


@pytest.mark.asyncio
async def test_save_returns_uploaded_path(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that save returns the path of the uploaded blob.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    blob_path = "nested/folder/file.txt"
    result = await azure_storage.save(blob_path, b"content")

    assert result == blob_path


@pytest.mark.asyncio
async def test_save_empty_bytes(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that save handles empty bytes correctly.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    result = await azure_storage.save("empty-bytes.txt", b"")

    assert result == "empty-bytes.txt"
    blob_client = test_container.get_blob_client("empty-bytes.txt")
    download = await blob_client.download_blob()
    assert await download.readall() == b""


@pytest.mark.asyncio
async def test_save_empty_bytesio(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that save handles empty BytesIO correctly.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    empty_io = BytesIO(b"")
    result = await azure_storage.save("empty-io.txt", empty_io)

    assert result == "empty-io.txt"
    blob_client = test_container.get_blob_client("empty-io.txt")
    download = await blob_client.download_blob()
    assert await download.readall() == b""


@pytest.mark.asyncio
async def test_save_empty_file_from_path(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that save handles empty file from path correctly.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    async with aiofiles.tempfile.TemporaryDirectory() as tmpdir:
        empty_file = Path(tmpdir) / "empty.txt"
        empty_file.write_bytes(b"")

        result = await azure_storage.save("empty-path.txt", empty_file)

        assert result == "empty-path.txt"
        blob_client = test_container.get_blob_client("empty-path.txt")
        download = await blob_client.download_blob()
        assert await download.readall() == b""


# =============================================================================
# IFileStorage.stat Method Tests
# =============================================================================


@pytest.mark.asyncio
async def test_stat_returns_file_statistics(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that stat returns Stat object with file information.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    content = b"content for stat test"
    blob_client = test_container.get_blob_client("stat-target.txt")
    await blob_client.upload_blob(content, overwrite=True)

    result = await azure_storage.stat("stat-target.txt")

    assert isinstance(result, Stat)
    assert hasattr(result, "size")
    assert hasattr(result, "last_modified")


@pytest.mark.asyncio
async def test_stat_returns_correct_size(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that stat returns the correct file size.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    content = b"x" * 100
    blob_client = test_container.get_blob_client("size-test.txt")
    await blob_client.upload_blob(content, overwrite=True)

    result = await azure_storage.stat("size-test.txt")

    assert result.size == 100


@pytest.mark.asyncio
async def test_stat_returns_last_modified(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that stat returns a valid last_modified timestamp.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    blob_client = test_container.get_blob_client("modified-test.txt")
    await blob_client.upload_blob(b"content", overwrite=True)

    result = await azure_storage.stat("modified-test.txt")

    assert result.last_modified is not None


@pytest.mark.asyncio
async def test_stat_empty_file_returns_zero_size(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that stat returns size=0 for empty files.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    blob_client = test_container.get_blob_client("stat-empty.txt")
    await blob_client.upload_blob(b"", overwrite=True)

    result = await azure_storage.stat("stat-empty.txt")

    assert isinstance(result, Stat)
    assert result.size == 0
    assert result.last_modified is not None


@pytest.mark.asyncio
async def test_stat_raises_error_for_nonexistent_file(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that stat raises ObjectNotFoundError for nonexistent files.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    with pytest.raises(ObjectNotFoundError):
        await azure_storage.stat("nonexistent.txt")


# =============================================================================
# IFileStorage.list Method Tests
# =============================================================================


@pytest.mark.asyncio
async def test_list_returns_file_names(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that list returns collection of file names in path.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    files = ["list-test/file1.txt", "list-test/file2.txt"]
    for file in files:
        blob_client = test_container.get_blob_client(file)
        await blob_client.upload_blob(b"content", overwrite=True)

    result = await azure_storage.list("list-test/")

    assert len(result) == 2
    result_list = list(result)
    assert "list-test/file1.txt" in result_list
    assert "list-test/file2.txt" in result_list


@pytest.mark.asyncio
async def test_list_with_trailing_slash_lists_folder_contents(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that list with trailing slash lists folder contents.

    The trailing '/' is the convention for folder operations.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    for i in range(2):
        blob = test_container.get_blob_client(f"slash-test/file{i}.txt")
        await blob.upload_blob(b"content", overwrite=True)

    result = await azure_storage.list("slash-test/")

    assert len(result) == 2


@pytest.mark.asyncio
async def test_list_without_trailing_slash_uses_prefix(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that list without trailing slash uses path as prefix.

    Without trailing '/', the path is treated as a prefix, potentially
    matching multiple folders/files with that prefix.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    blob1 = test_container.get_blob_client("prefix-file1.txt")
    await blob1.upload_blob(b"content", overwrite=True)
    blob2 = test_container.get_blob_client("prefix-file2.txt")
    await blob2.upload_blob(b"content", overwrite=True)
    blob3 = test_container.get_blob_client("other-file.txt")
    await blob3.upload_blob(b"content", overwrite=True)

    result = await azure_storage.list("prefix")

    result_list = list(result)
    assert len(result_list) == 2
    assert "prefix-file1.txt" in result_list
    assert "prefix-file2.txt" in result_list


@pytest.mark.asyncio
async def test_list_non_recursive_excludes_nested_files(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that list with recursive=False excludes nested files.

    Non-recursive listing only returns immediate children.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    files = [
        "non-recursive/file1.txt",
        "non-recursive/nested/file2.txt",
    ]
    for file in files:
        blob_client = test_container.get_blob_client(file)
        await blob_client.upload_blob(b"content", overwrite=True)

    result = await azure_storage.list("non-recursive/", recursive=False)

    result_list = list(result)
    assert len(result_list) == 1
    assert "non-recursive/file1.txt" in result_list


@pytest.mark.asyncio
async def test_list_recursive_includes_nested_files(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that list with recursive=True includes nested files.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    files = [
        "recursive-list/file1.txt",
        "recursive-list/sub1/file2.txt",
        "recursive-list/sub1/sub2/file3.txt",
    ]
    for file in files:
        blob_client = test_container.get_blob_client(file)
        await blob_client.upload_blob(b"content", overwrite=True)

    result = await azure_storage.list("recursive-list/", recursive=True)

    result_list = list(result)
    assert len(result_list) == 3
    for file in files:
        assert file in result_list


@pytest.mark.asyncio
async def test_list_returns_empty_for_nonexistent_prefix(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that list returns empty collection for nonexistent prefix.

    Does NOT raise ObjectNotFoundError - just returns empty.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    result = await azure_storage.list("nonexistent-prefix/")

    assert len(result) == 0


@pytest.mark.asyncio
async def test_list_filters_by_prefix(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that list only returns files under the specified path.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    folders = ["folder-a", "folder-b"]
    for folder in folders:
        blob_client = test_container.get_blob_client(f"{folder}/file.txt")
        await blob_client.upload_blob(b"content", overwrite=True)

    result = await azure_storage.list("folder-a/")

    result_list = list(result)
    assert len(result_list) == 1
    assert "folder-a/file.txt" in result_list
    assert "folder-b/file.txt" not in result_list


# =============================================================================
# IFileStorage.list_detailed Method Tests
# =============================================================================


@pytest.mark.asyncio
async def test_list_detailed_returns_list_items(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that list_detailed returns collection of ListItem objects.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    files = ["detailed/file1.txt", "detailed/file2.txt"]
    for file in files:
        blob_client = test_container.get_blob_client(file)
        await blob_client.upload_blob(b"content", overwrite=True)

    result = await azure_storage.list_detailed(
        "detailed/",
        show_stats=True,
        show_info=True,
    )

    result_list = list(result)
    assert len(result_list) == 2
    for item in result_list:
        assert isinstance(item, ListItem)


@pytest.mark.asyncio
async def test_list_detailed_includes_object_name(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that list_detailed returns ListItems with object_name.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    blob_client = test_container.get_blob_client("detailed-name.txt")
    await blob_client.upload_blob(b"content", overwrite=True)

    result = await azure_storage.list_detailed(
        "detailed-name.txt",
        show_stats=True,
        show_info=True,
    )

    result_list = list(result)
    assert len(result_list) == 1
    assert result_list[0].object_name == "detailed-name.txt"


@pytest.mark.asyncio
async def test_list_detailed_with_show_info_includes_size(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that list_detailed with show_info=True includes size info.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    content = b"x" * 50
    blob_client = test_container.get_blob_client("info-size.txt")
    await blob_client.upload_blob(content, overwrite=True)

    result = await azure_storage.list_detailed("info-size.txt", show_info=True)

    result_list = list(result)
    assert len(result_list) == 1
    assert result_list[0].size == 50


@pytest.mark.asyncio
async def test_list_detailed_with_show_info_includes_last_modified(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that list_detailed with show_info=True includes last_modified.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    blob_client = test_container.get_blob_client("info-modified.txt")
    await blob_client.upload_blob(b"content", overwrite=True)

    result = await azure_storage.list_detailed(
        "info-modified.txt",
        show_info=True,
    )

    result_list = list(result)
    assert len(result_list) == 1
    assert result_list[0].last_modified is not None


@pytest.mark.asyncio
async def test_list_detailed_with_show_stats_includes_metadata(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that list_detailed with show_stats=True includes metadata.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    blob_client = test_container.get_blob_client("stats-metadata.txt")
    await blob_client.upload_blob(
        b"content",
        overwrite=True,
        metadata={"key": "value"},
    )

    result = await azure_storage.list_detailed(
        "stats-metadata.txt",
        show_stats=True,
    )

    result_list = list(result)
    assert len(result_list) == 1
    assert result_list[0].metadata is not None


@pytest.mark.asyncio
async def test_list_detailed_with_show_stats_includes_content_type(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that list_detailed with show_stats=True includes content_type.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    blob_client = test_container.get_blob_client("stats-content-type.txt")
    await blob_client.upload_blob(b"content", overwrite=True)

    result = await azure_storage.list_detailed(
        "stats-content-type.txt",
        show_stats=True,
    )

    result_list = list(result)
    assert len(result_list) == 1
    assert result_list[0].content_type is not None


@pytest.mark.asyncio
async def test_list_detailed_recursive_returns_all_nested_files(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test list_detailed with recursive=True returns all nested files.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    files = [
        "detailed-recursive/file1.txt",
        "detailed-recursive/sub1/file2.txt",
        "detailed-recursive/sub1/sub2/file3.txt",
    ]
    for file in files:
        blob_client = test_container.get_blob_client(file)
        await blob_client.upload_blob(b"content", overwrite=True)

    result = await azure_storage.list_detailed(
        "detailed-recursive/",
        recursive=True,
        show_info=True,
    )

    result_list = list(result)
    assert len(result_list) == 3
    names = [item.object_name for item in result_list]
    for file in files:
        assert file in names


@pytest.mark.asyncio
async def test_list_detailed_non_recursive_excludes_nested_files(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test list_detailed with recursive=False excludes nested files.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    files = [
        "detailed-non-recursive/file1.txt",
        "detailed-non-recursive/sub/file2.txt",
    ]
    for file in files:
        blob_client = test_container.get_blob_client(file)
        await blob_client.upload_blob(b"content", overwrite=True)

    result = await azure_storage.list_detailed(
        "detailed-non-recursive/",
        recursive=False,
        show_info=True,
    )

    result_list = list(result)
    assert len(result_list) == 1
    assert result_list[0].object_name == "detailed-non-recursive/file1.txt"


@pytest.mark.asyncio
async def test_list_detailed_returns_empty_for_nonexistent_prefix(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that list_detailed returns empty for nonexistent prefix.

    Does NOT raise ObjectNotFoundError - just returns empty.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    result = await azure_storage.list_detailed(
        "nonexistent-prefix/",
        show_stats=True,
        show_info=True,
    )

    assert len(list(result)) == 0


# =============================================================================
# IFileStorage.copy Method Tests
# =============================================================================


@pytest.mark.asyncio
async def test_copy_single_file_returns_copy_result(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that copy single file returns CopyResult with one success.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    blob_client = test_container.get_blob_client("source.txt")
    await blob_client.upload_blob(b"source content", overwrite=True)

    result = await azure_storage.copy("source.txt", "destination.txt")

    assert isinstance(result, CopyResult)
    assert len(result.success) == 1
    assert "destination.txt" in result.success
    assert len(result.failure) == 0


@pytest.mark.asyncio
async def test_copy_single_file_preserves_content(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that copy preserves the content of the original file.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    content = b"preserve this content exactly"
    blob_client = test_container.get_blob_client("preserve-source.txt")
    await blob_client.upload_blob(content, overwrite=True)

    await azure_storage.copy("preserve-source.txt", "preserve-dest.txt")

    dst_blob = test_container.get_blob_client("preserve-dest.txt")
    download = await dst_blob.download_blob()
    assert await download.readall() == content


@pytest.mark.asyncio
async def test_copy_folder_with_trailing_slash(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test copy with trailing '/' copies folder contents.

    The trailing '/' indicates folder operation.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    for i in range(3):
        blob_client = test_container.get_blob_client(f"src-folder/file{i}.txt")
        await blob_client.upload_blob(f"content {i}".encode(), overwrite=True)

    result = await azure_storage.copy(
        "src-folder/",
        "dst-folder/",
        recursive=True,
    )

    assert isinstance(result, CopyResult)
    assert len(result.success) == 3
    assert len(result.failure) == 0

    for i in range(3):
        dst_blob = test_container.get_blob_client(f"dst-folder/file{i}.txt")
        assert await dst_blob.exists()


@pytest.mark.asyncio
async def test_copy_folder_non_recursive(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test copy folder without recursive only copies immediate children.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    blob1 = test_container.get_blob_client("copy-nr-src/file1.txt")
    await blob1.upload_blob(b"content", overwrite=True)
    blob2 = test_container.get_blob_client("copy-nr-src/sub/file2.txt")
    await blob2.upload_blob(b"content", overwrite=True)

    result = await azure_storage.copy(
        "copy-nr-src/",
        "copy-nr-dst/",
        recursive=False,
    )

    assert isinstance(result, CopyResult)
    assert len(result.success) == 1

    dst1 = test_container.get_blob_client("copy-nr-dst/file1.txt")
    assert await dst1.exists()
    dst2 = test_container.get_blob_client("copy-nr-dst/sub/file2.txt")
    assert not await dst2.exists()


@pytest.mark.asyncio
async def test_copy_empty_folder_returns_empty_result(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test copy of empty/nonexistent folder returns empty CopyResult.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    result = await azure_storage.copy(
        "nonexistent-folder/",
        "dest-folder/",
        recursive=True,
    )

    assert isinstance(result, CopyResult)
    assert len(result.success) == 0
    assert len(result.failure) == 0


@pytest.mark.asyncio
async def test_copy_raises_not_found_for_nonexistent_single_file(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test copy raises ObjectNotFoundError for nonexistent single file.

    Single file copy (no trailing '/') requires source to exist.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    with pytest.raises(ObjectNotFoundError):
        await azure_storage.copy("nonexistent-src.txt", "nonexistent-dst.txt")


@pytest.mark.asyncio
async def test_copy_raises_error_when_src_is_folder_without_slash(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test copy raises InvalidArgumentsError for folder without '/'.

    When src doesn't end with '/' but is actually a folder prefix,
    copy raises InvalidArgumentsError to prevent ambiguity.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    blob_client = test_container.get_blob_client("folder-src/file.txt")
    await blob_client.upload_blob(b"content", overwrite=True)

    with pytest.raises(InvalidArgumentsError):
        await azure_storage.copy("folder-src", "folder-dst.txt")


@pytest.mark.asyncio
async def test_copy_raises_error_when_dst_is_folder(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test copy raises InvalidArgumentsError when dst is a folder.

    When copying single file but dst is a folder prefix (has children),
    copy raises InvalidArgumentsError.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    blob_client = test_container.get_blob_client("single-src.txt")
    await blob_client.upload_blob(b"content", overwrite=True)

    dst_blob = test_container.get_blob_client("dst-folder/file.txt")
    await dst_blob.upload_blob(b"dst content", overwrite=True)

    with pytest.raises(InvalidArgumentsError):
        await azure_storage.copy("single-src.txt", "dst-folder")


# =============================================================================
# IFileStorage.move Method Tests
# =============================================================================


@pytest.mark.asyncio
async def test_move_single_file_returns_move_result(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that move single file returns MoveResult.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    blob_client = test_container.get_blob_client("move-source.txt")
    await blob_client.upload_blob(b"move content", overwrite=True)

    result = await azure_storage.move("move-source.txt", "move-dest.txt")

    assert isinstance(result, MoveResult)
    assert len(result.copy.success) == 1
    assert "move-dest.txt" in result.copy.success
    assert len(result.copy.failure) == 0
    assert len(result.remove.success) == 1
    assert "move-source.txt" in result.remove.success


@pytest.mark.asyncio
async def test_move_single_file_removes_source(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that move removes the source file after moving.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    blob_client = test_container.get_blob_client("move-source.txt")
    await blob_client.upload_blob(b"move content", overwrite=True)

    await azure_storage.move("move-source.txt", "move-dest.txt")

    assert not await blob_client.exists()


@pytest.mark.asyncio
async def test_move_single_file_preserves_content(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that move preserves content at destination.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    blob_client = test_container.get_blob_client("move-source.txt")
    await blob_client.upload_blob(b"move content", overwrite=True)

    await azure_storage.move("move-source.txt", "move-dest.txt")

    dst_blob = test_container.get_blob_client("move-dest.txt")
    download = await dst_blob.download_blob()
    assert await download.readall() == b"move content"


@pytest.mark.asyncio
async def test_move_folder_with_trailing_slash(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test move with trailing '/' moves folder contents.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    for i in range(3):
        blob_client = test_container.get_blob_client(
            f"move-folder/file{i}.txt",
        )
        await blob_client.upload_blob(f"content {i}".encode(), overwrite=True)

    result = await azure_storage.move(
        "move-folder/",
        "moved-folder/",
        recursive=True,
    )

    assert isinstance(result, MoveResult)
    assert len(result.copy.success) == 3
    assert len(result.remove.success) == 3

    for i in range(3):
        dst_blob = test_container.get_blob_client(f"moved-folder/file{i}.txt")
        assert await dst_blob.exists()

    for i in range(3):
        src_blob = test_container.get_blob_client(f"move-folder/file{i}.txt")
        assert not await src_blob.exists()


@pytest.mark.asyncio
async def test_move_empty_folder_returns_empty_result(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test move of empty folder returns MoveResult with empty lists.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    result = await azure_storage.move(
        "empty-move-src/",
        "empty-move-dst/",
        recursive=True,
    )

    assert isinstance(result, MoveResult)
    assert result.copy.success == []
    assert result.copy.failure == []
    assert result.remove.success == []
    assert result.remove.failure == []


@pytest.mark.asyncio
async def test_move_raises_error_for_nonexistent_source(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that move raises ObjectNotFoundError when source doesn't exist.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    with pytest.raises(ObjectNotFoundError):
        await azure_storage.move("nonexistent.txt", "dest.txt")


# =============================================================================
# IFileStorage.remove Method Tests
# =============================================================================


@pytest.mark.asyncio
async def test_remove_single_file_returns_remove_result(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that remove single file returns RemoveResult.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    blob_client = test_container.get_blob_client("remove-target.txt")
    await blob_client.upload_blob(b"to be removed", overwrite=True)

    result = await azure_storage.remove("remove-target.txt")

    assert isinstance(result, RemoveResult)
    assert len(result.success) == 1
    assert "remove-target.txt" in result.success
    assert len(result.failure) == 0


@pytest.mark.asyncio
async def test_remove_single_file_deletes_blob(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that remove actually deletes the blob.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    blob_client = test_container.get_blob_client("remove-target.txt")
    await blob_client.upload_blob(b"to be removed", overwrite=True)

    await azure_storage.remove("remove-target.txt")

    assert not await blob_client.exists()


@pytest.mark.asyncio
async def test_remove_folder_with_trailing_slash(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test remove with trailing '/' removes folder contents.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    for i in range(3):
        blob_client = test_container.get_blob_client(
            f"remove-folder/file{i}.txt",
        )
        await blob_client.upload_blob(f"content {i}".encode(), overwrite=True)

    result = await azure_storage.remove("remove-folder/", recursive=True)

    assert isinstance(result, RemoveResult)
    assert len(result.success) == 3
    assert len(result.failure) == 0

    for i in range(3):
        blob_client = test_container.get_blob_client(
            f"remove-folder/file{i}.txt",
        )
        assert not await blob_client.exists()


@pytest.mark.asyncio
async def test_remove_empty_folder_returns_empty_result(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test remove of empty/nonexistent folder returns empty result.

    Does NOT raise ObjectNotFoundError for folder operations.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    result = await azure_storage.remove("nonexistent-folder/", recursive=True)

    assert isinstance(result, RemoveResult)
    assert len(result.success) == 0
    assert len(result.failure) == 0


@pytest.mark.asyncio
async def test_remove_raises_error_for_nonexistent_file(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test remove raises ObjectNotFoundError for nonexistent single file.

    Single file remove (no trailing '/') requires file to exist.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    with pytest.raises(ObjectNotFoundError):
        await azure_storage.remove("nonexistent.txt")


# =============================================================================
# IFileStorage.remove_many Method Tests
# =============================================================================


@pytest.mark.asyncio
async def test_remove_many_removes_multiple_files(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that remove_many removes multiple files at once.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    paths = ["file1.txt", "file2.txt", "file3.txt"]
    for path in paths:
        blob_client = test_container.get_blob_client(path)
        await blob_client.upload_blob(b"content", overwrite=True)

    result = await azure_storage.remove_many(paths)

    assert isinstance(result, RemoveResult)
    assert len(result.success) == 3
    assert len(result.failure) == 0

    for path in paths:
        blob_client = test_container.get_blob_client(path)
        assert not await blob_client.exists()


@pytest.mark.asyncio
async def test_remove_many_tracks_failures(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that remove_many tracks which files failed to remove.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    blob_client = test_container.get_blob_client("exists.txt")
    await blob_client.upload_blob(b"content", overwrite=True)

    result = await azure_storage.remove_many(["exists.txt", "not-exists.txt"])

    assert isinstance(result, RemoveResult)
    assert "exists.txt" in result.success
    assert len(result.failure) == 1


@pytest.mark.asyncio
async def test_remove_many_recursive_removes_folders(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that remove_many with recursive=True removes folders.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    for folder in ["folder1", "folder2"]:
        for i in range(2):
            blob_client = test_container.get_blob_client(
                f"{folder}/file{i}.txt",
            )
            await blob_client.upload_blob(
                f"content {i}".encode(),
                overwrite=True,
            )

    result = await azure_storage.remove_many(
        ["folder1/", "folder2/"],
        recursive=True,
    )

    assert isinstance(result, RemoveResult)
    assert len(result.success) == 4
    assert len(result.failure) == 0

    for folder in ["folder1", "folder2"]:
        for i in range(2):
            blob_client = test_container.get_blob_client(
                f"{folder}/file{i}.txt",
            )
            assert not await blob_client.exists()


@pytest.mark.asyncio
async def test_remove_many_empty_list_returns_empty_result(
    azure_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test remove_many with empty list returns empty result.

    Args:
        azure_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    result = await azure_storage.remove_many([])

    assert isinstance(result, RemoveResult)
    assert len(result.success) == 0
    assert len(result.failure) == 0
