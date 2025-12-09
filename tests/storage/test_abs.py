"""Tests for Azure Blob Storage implementation using Azurite testcontainer."""

import tempfile
from io import BytesIO
from pathlib import Path

import pytest
from azure.storage.blob.aio import BlobServiceClient, ContainerClient

from mint.fs.asynk.abs import AzureBlobStorage
from mint.fs.exc import ObjectNotFoundError, OperationalError
from mint.fs.structs import CopyManyResult, ListItem, Stat

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
    # Verify container exists
    container_props = await test_container.get_container_properties()
    assert container_props is not None
    assert container_props.name == test_container_name

    # Test creating a blob
    blob_name = "health-check-blob.txt"
    blob_content = b"Hello, Azurite!"
    blob_client = test_container.get_blob_client(blob_name)

    await blob_client.upload_blob(blob_content, overwrite=True)

    # Test getting the blob
    download_stream = await blob_client.download_blob()
    downloaded_content = await download_stream.readall()
    assert downloaded_content == blob_content

    # Test deleting the blob
    await blob_client.delete_blob()

    # Verify blob is deleted
    blob_exists = await blob_client.exists()
    assert blob_exists is False


# =============================================================================
# IFileStorage.is_folder Method Tests
# =============================================================================


@pytest.mark.asyncio
async def test_is_folder_returns_true_for_folder_prefix(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that is_folder returns True for a path that is a folder prefix.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    # Setup: Create a blob with folder prefix
    blob_client = test_container.get_blob_client("folder/file.txt")
    await blob_client.upload_blob(b"content", overwrite=True)

    # Test
    result = await abs_storage.is_folder("folder")
    assert result is True


@pytest.mark.asyncio
async def test_is_folder_returns_false_for_file(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that is_folder returns False for a path that is a file.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    # Setup: Create a blob
    blob_client = test_container.get_blob_client("file.txt")
    await blob_client.upload_blob(b"content", overwrite=True)

    # Test
    result = await abs_storage.is_folder("file.txt")
    assert result is False


@pytest.mark.asyncio
async def test_is_folder_returns_false_for_nonexistent_path(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that is_folder returns False for a path that doesn't exist.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    result = await abs_storage.is_folder("nonexistent")
    assert result is False


# =============================================================================
# IFileStorage.get Method Tests
# =============================================================================


@pytest.mark.asyncio
async def test_get_downloads_file_to_local_path(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that get downloads a blob to the specified local path.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    # Setup: Create a blob with content
    blob_name = "download-test.txt"
    blob_content = b"Content to download"
    blob_client = test_container.get_blob_client(blob_name)
    await blob_client.upload_blob(blob_content, overwrite=True)

    # Test
    with tempfile.TemporaryDirectory() as tmpdir:
        save_path = Path(tmpdir) / "downloaded.txt"
        await abs_storage.get(blob_name, str(save_path))

        assert save_path.exists()
        assert save_path.read_bytes() == blob_content


@pytest.mark.asyncio
async def test_get_raises_error_for_nonexistent_file(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that get raises an error when the blob doesn't exist.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    with tempfile.TemporaryDirectory() as tmpdir:
        save_path = Path(tmpdir) / "downloaded.txt"
        with pytest.raises(ObjectNotFoundError):
            await abs_storage.get("nonexistent.txt", str(save_path))


# =============================================================================
# IFileStorage.save Method Tests
# =============================================================================


@pytest.mark.asyncio
async def test_save_uploads_from_string_path(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that save uploads a file from a local string path.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create a local file
        local_path = Path(tmpdir) / "upload.txt"
        content = b"Upload from string path"
        local_path.write_bytes(content)

        # Upload
        result = await abs_storage.save("uploaded-string.txt", str(local_path))

        # Verify
        assert result == "uploaded-string.txt"
        blob_client = test_container.get_blob_client("uploaded-string.txt")
        download = await blob_client.download_blob()
        assert await download.readall() == content


@pytest.mark.asyncio
async def test_save_uploads_from_pathlib_path(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that save uploads a file from a pathlib.Path object.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create a local file
        local_path = Path(tmpdir) / "upload.txt"
        content = b"Upload from pathlib path"
        local_path.write_bytes(content)

        # Upload
        result = await abs_storage.save("uploaded-pathlib.txt", local_path)

        # Verify
        assert result == "uploaded-pathlib.txt"
        blob_client = test_container.get_blob_client("uploaded-pathlib.txt")
        download = await blob_client.download_blob()
        assert await download.readall() == content


@pytest.mark.asyncio
async def test_save_uploads_from_io_object(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that save uploads content from an IO object.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    content = b"Upload from BytesIO"
    io_obj = BytesIO(content)

    # Upload
    result = await abs_storage.save("uploaded-io.txt", io_obj)

    # Verify
    assert result == "uploaded-io.txt"
    blob_client = test_container.get_blob_client("uploaded-io.txt")
    download = await blob_client.download_blob()
    assert await download.readall() == content


@pytest.mark.asyncio
async def test_save_uploads_from_bytes(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that save uploads content from bytes directly.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    content = b"Upload from bytes directly"

    # Upload
    result = await abs_storage.save("uploaded-bytes.txt", content)

    # Verify
    assert result == "uploaded-bytes.txt"
    blob_client = test_container.get_blob_client("uploaded-bytes.txt")
    download = await blob_client.download_blob()
    assert await download.readall() == content


@pytest.mark.asyncio
async def test_save_returns_uploaded_path(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that save returns the path of the uploaded blob.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    blob_path = "nested/folder/file.txt"
    result = await abs_storage.save(blob_path, b"content")

    assert result == blob_path


# =============================================================================
# IFileStorage.copy Method Tests
# =============================================================================


@pytest.mark.asyncio
async def test_copy_single_file(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that copy copies a single file to destination.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    # Setup: Create source blob
    blob_client = test_container.get_blob_client("source.txt")
    await blob_client.upload_blob(b"source content", overwrite=True)

    # Copy
    result = await abs_storage.copy("source.txt", "destination.txt")

    # Verify
    assert result == "destination.txt"
    dst_blob = test_container.get_blob_client("destination.txt")
    download = await dst_blob.download_blob()
    assert await download.readall() == b"source content"


@pytest.mark.asyncio
async def test_copy_returns_destination_path_for_single_file(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that copy returns the destination path for single file copy.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    # Setup: Create source blob
    blob_client = test_container.get_blob_client("source.txt")
    await blob_client.upload_blob(b"source content", overwrite=True)

    # Copy
    result = await abs_storage.copy("source.txt", "dest-path.txt")

    assert isinstance(result, str)
    assert result == "dest-path.txt"


@pytest.mark.asyncio
async def test_copy_recursive_copies_folder(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that copy with recursive=True copies all files in a folder.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    # Setup: Create multiple blobs in a folder
    for i in range(3):
        blob_client = test_container.get_blob_client(f"src-folder/file{i}.txt")
        await blob_client.upload_blob(f"content {i}".encode(), overwrite=True)

    # Copy folder (path must end with / to indicate folder)
    result = await abs_storage.copy(
        "src-folder/",
        "dst-folder/",
        recursive=True,
    )

    # Verify all files were copied
    assert isinstance(result, CopyManyResult)
    assert len(result.success) == 3
    assert len(result.failure) == 0

    # Verify destination files exist
    for i in range(3):
        dst_blob = test_container.get_blob_client(f"dst-folder/file{i}.txt")
        assert await dst_blob.exists()


@pytest.mark.asyncio
async def test_copy_recursive_returns_copy_many_result(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that copy with recursive=True returns CopyManyResult.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    # Setup: Create multiple blobs in a folder
    for i in range(3):
        blob_client = test_container.get_blob_client(f"src-folder/file{i}.txt")
        await blob_client.upload_blob(f"content {i}".encode(), overwrite=True)

    # Copy folder
    result = await abs_storage.copy(
        "src-folder/",
        "dst-folder/",
        recursive=True,
    )

    assert isinstance(result, CopyManyResult)
    assert hasattr(result, "success")
    assert hasattr(result, "failure")


@pytest.mark.asyncio
async def test_copy_preserves_content(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that copy preserves the content of the original file.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    # Setup: Create source blob with specific content
    content = b"preserve this content exactly"
    blob_client = test_container.get_blob_client("preserve-source.txt")
    await blob_client.upload_blob(content, overwrite=True)

    # Copy
    await abs_storage.copy("preserve-source.txt", "preserve-dest.txt")

    # Verify content is preserved
    dst_blob = test_container.get_blob_client("preserve-dest.txt")
    download = await dst_blob.download_blob()
    assert await download.readall() == content


# =============================================================================
# IFileStorage.move Method Tests
# =============================================================================


@pytest.mark.asyncio
async def test_move_single_file(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that move moves a single file to destination.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    # Setup: Create source blob
    blob_client = test_container.get_blob_client("move-source.txt")
    await blob_client.upload_blob(b"move content", overwrite=True)

    # Move
    await abs_storage.move("move-source.txt", "move-dest.txt")

    # Verify destination exists with correct content
    dst_blob = test_container.get_blob_client("move-dest.txt")
    download = await dst_blob.download_blob()
    assert await download.readall() == b"move content"


@pytest.mark.asyncio
async def test_move_removes_source_file(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that move removes the source file after moving.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    # Setup: Create source blob
    blob_client = test_container.get_blob_client("move-source.txt")
    await blob_client.upload_blob(b"move content", overwrite=True)

    # Move
    await abs_storage.move("move-source.txt", "move-dest.txt")

    # Verify source is removed
    assert not await blob_client.exists()


@pytest.mark.asyncio
async def test_move_returns_none_for_single_file(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that move returns None for single file move.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    # Setup: Create source blob
    blob_client = test_container.get_blob_client("move-source.txt")
    await blob_client.upload_blob(b"move content", overwrite=True)

    # Move
    result = await abs_storage.move("move-source.txt", "move-dest.txt")

    assert result is None


@pytest.mark.asyncio
async def test_move_recursive_moves_folder(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that move with recursive=True moves all files in a folder.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    # Setup: Create multiple blobs in a folder
    for i in range(3):
        blob_client = test_container.get_blob_client(
            f"move-folder/file{i}.txt",
        )
        await blob_client.upload_blob(f"content {i}".encode(), overwrite=True)

    # Move folder
    await abs_storage.move("move-folder/", "moved-folder/", recursive=True)

    # Verify all files were moved to destination
    for i in range(3):
        dst_blob = test_container.get_blob_client(f"moved-folder/file{i}.txt")
        assert await dst_blob.exists()

    # Verify source files are removed
    for i in range(3):
        src_blob = test_container.get_blob_client(f"move-folder/file{i}.txt")
        assert not await src_blob.exists()


@pytest.mark.asyncio
async def test_move_raises_error_for_nonexistent_source(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that move raises error when source doesn't exist.

    The implementation wraps Azure SDK's ResourceNotFoundError as
    OperationalError since copy doesn't pre-check existence.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    with pytest.raises(OperationalError):
        await abs_storage.move("nonexistent.txt", "dest.txt")


# =============================================================================
# IFileStorage.remove Method Tests
# =============================================================================


@pytest.mark.asyncio
async def test_remove_single_file(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that remove deletes a single file.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    # Setup: Create a blob
    blob_client = test_container.get_blob_client("remove-target.txt")
    await blob_client.upload_blob(b"to be removed", overwrite=True)

    # Remove
    await abs_storage.remove("remove-target.txt")

    # Verify blob is removed
    assert not await blob_client.exists()


@pytest.mark.asyncio
async def test_remove_returns_removed_path_for_single_file(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that remove returns the path of the removed file.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    # Setup: Create a blob
    blob_client = test_container.get_blob_client("remove-target.txt")
    await blob_client.upload_blob(b"to be removed", overwrite=True)

    # Remove
    result = await abs_storage.remove("remove-target.txt")

    assert result == "remove-target.txt"


@pytest.mark.asyncio
async def test_remove_recursive_removes_folder(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that remove with recursive=True removes all files in a folder.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    # Setup: Create multiple blobs in a folder
    for i in range(3):
        blob_client = test_container.get_blob_client(
            f"remove-folder/file{i}.txt",
        )
        await blob_client.upload_blob(f"content {i}".encode(), overwrite=True)

    # Remove folder
    await abs_storage.remove("remove-folder/", recursive=True)

    # Verify all blobs are removed
    for i in range(3):
        blob_client = test_container.get_blob_client(
            f"remove-folder/file{i}.txt",
        )
        assert not await blob_client.exists()


@pytest.mark.asyncio
async def test_remove_recursive_returns_tuple_for_folder(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that remove with recursive=True returns tuple of results.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    # Setup: Create multiple blobs in a folder
    for i in range(3):
        blob_client = test_container.get_blob_client(
            f"remove-folder/file{i}.txt",
        )
        await blob_client.upload_blob(f"content {i}".encode(), overwrite=True)

    # Remove folder
    result = await abs_storage.remove("remove-folder/", recursive=True)

    # Should return tuple (success_list, failure_list)
    assert isinstance(result, tuple)
    assert len(result) == 2


# =============================================================================
# IFileStorage.remove_many Method Tests
# =============================================================================


@pytest.mark.asyncio
async def test_remove_many_removes_multiple_files(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that remove_many removes multiple files at once.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    # Setup: Create multiple blobs
    paths = ["file1.txt", "file2.txt", "file3.txt"]
    for path in paths:
        blob_client = test_container.get_blob_client(path)
        await blob_client.upload_blob(b"content", overwrite=True)

    # Remove many
    await abs_storage.remove_many(paths)

    # Verify all blobs are removed
    for path in paths:
        blob_client = test_container.get_blob_client(path)
        assert not await blob_client.exists()


@pytest.mark.asyncio
async def test_remove_many_returns_tuple(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that remove_many returns tuple with success/failure lists.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    # Setup: Create multiple blobs
    paths = ["file1.txt", "file2.txt", "file3.txt"]
    for path in paths:
        blob_client = test_container.get_blob_client(path)
        await blob_client.upload_blob(b"content", overwrite=True)

    # Remove many
    result = await abs_storage.remove_many(paths)

    assert isinstance(result, tuple)
    assert len(result) == 2
    success, failure = result
    assert len(success) == 3
    assert len(failure) == 0


@pytest.mark.asyncio
async def test_remove_many_tracks_failures(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that remove_many tracks which files failed to remove.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    # Setup: Create only some of the files to be removed
    blob_client = test_container.get_blob_client("exists.txt")
    await blob_client.upload_blob(b"content", overwrite=True)

    # Try to remove existing and non-existing files
    result = await abs_storage.remove_many(["exists.txt", "not-exists.txt"])

    success, failure = result
    assert "exists.txt" in success
    assert len(failure) == 1  # not-exists.txt should fail


@pytest.mark.asyncio
async def test_remove_many_recursive_removes_folders(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that remove_many with recursive=True removes folders.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    # Setup: Create blobs in multiple folders
    for folder in ["folder1", "folder2"]:
        for i in range(2):
            blob_client = test_container.get_blob_client(
                f"{folder}/file{i}.txt",
            )
            await blob_client.upload_blob(
                f"content {i}".encode(),
                overwrite=True,
            )

    # Remove folders
    await abs_storage.remove_many(["folder1/", "folder2/"], recursive=True)

    # Verify all blobs are removed
    for folder in ["folder1", "folder2"]:
        for i in range(2):
            blob_client = test_container.get_blob_client(
                f"{folder}/file{i}.txt",
            )
            assert not await blob_client.exists()


# =============================================================================
# IFileStorage.stat Method Tests
# =============================================================================


@pytest.mark.asyncio
async def test_stat_returns_file_statistics(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that stat returns Stat object with file information.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    # Setup: Create a blob with known content
    content = b"content for stat test"
    blob_client = test_container.get_blob_client("stat-target.txt")
    await blob_client.upload_blob(content, overwrite=True)

    # Get stat
    result = await abs_storage.stat("stat-target.txt")

    assert isinstance(result, Stat)
    assert hasattr(result, "size")
    assert hasattr(result, "last_modified")


@pytest.mark.asyncio
async def test_stat_returns_correct_size(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that stat returns the correct file size.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    # Setup: Create a blob with known content size
    content = b"x" * 100
    blob_client = test_container.get_blob_client("size-test.txt")
    await blob_client.upload_blob(content, overwrite=True)

    # Get stat
    result = await abs_storage.stat("size-test.txt")

    assert result.size == 100


@pytest.mark.asyncio
async def test_stat_returns_last_modified(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that stat returns a valid last_modified timestamp.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    # Setup: Create a blob
    blob_client = test_container.get_blob_client("modified-test.txt")
    await blob_client.upload_blob(b"content", overwrite=True)

    # Get stat
    result = await abs_storage.stat("modified-test.txt")

    assert result.last_modified is not None


@pytest.mark.asyncio
async def test_stat_raises_error_for_nonexistent_file(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that stat raises an error for nonexistent files.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    with pytest.raises(ObjectNotFoundError):
        await abs_storage.stat("nonexistent.txt")


# =============================================================================
# IFileStorage.list Method Tests
# =============================================================================


@pytest.mark.asyncio
async def test_list_returns_file_names(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that list returns collection of file names in path.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    # Setup: Create multiple blobs
    files = ["list-test/file1.txt", "list-test/file2.txt"]
    for file in files:
        blob_client = test_container.get_blob_client(file)
        await blob_client.upload_blob(b"content", overwrite=True)

    # List
    result = await abs_storage.list("list-test/")

    assert len(result) == 2
    result_list = list(result)
    assert "list-test/file1.txt" in result_list
    assert "list-test/file2.txt" in result_list


@pytest.mark.asyncio
async def test_list_returns_empty_for_empty_path(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that list returns empty collection for empty path.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    result = await abs_storage.list("nonexistent-prefix/")

    assert len(result) == 0


@pytest.mark.asyncio
async def test_list_filters_by_prefix(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that list only returns files under the specified path.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    # Setup: Create blobs in different folders
    folders = ["folder-a", "folder-b"]
    for folder in folders:
        blob_client = test_container.get_blob_client(f"{folder}/file.txt")
        await blob_client.upload_blob(b"content", overwrite=True)

    # List only folder-a
    result = await abs_storage.list("folder-a/")

    result_list = list(result)
    assert len(result_list) == 1
    assert "folder-a/file.txt" in result_list
    assert "folder-b/file.txt" not in result_list


# =============================================================================
# IFileStorage.list_detailed Method Tests
# =============================================================================


@pytest.mark.asyncio
async def test_list_detailed_returns_list_items(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that list_detailed returns collection of ListItem objects.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    # Setup: Create blobs
    files = ["detailed/file1.txt", "detailed/file2.txt"]
    for file in files:
        blob_client = test_container.get_blob_client(file)
        await blob_client.upload_blob(b"content", overwrite=True)

    # List detailed
    result = await abs_storage.list_detailed(
        "detailed/",
        show_stats=True,
        show_info=True,
    )

    # Check result is collection of ListItems
    result_list = list(result)
    assert len(result_list) == 2
    for item in result_list:
        assert isinstance(item, ListItem)


@pytest.mark.asyncio
async def test_list_detailed_includes_object_name(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that list_detailed returns ListItems with object_name.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    # Setup: Create a blob
    blob_client = test_container.get_blob_client("detailed-name.txt")
    await blob_client.upload_blob(b"content", overwrite=True)

    # List detailed
    result = await abs_storage.list_detailed(
        "detailed-name.txt",
        show_stats=True,
        show_info=True,
    )

    result_list = list(result)
    assert len(result_list) == 1
    assert result_list[0].object_name == "detailed-name.txt"


@pytest.mark.asyncio
async def test_list_detailed_with_show_info_includes_size(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that list_detailed with show_info=True includes size info.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    # Setup: Create a blob with known size
    content = b"x" * 50
    blob_client = test_container.get_blob_client("info-size.txt")
    await blob_client.upload_blob(content, overwrite=True)

    # List detailed
    result = await abs_storage.list_detailed("info-size.txt", show_info=True)

    result_list = list(result)
    assert len(result_list) == 1
    assert result_list[0].size == 50


@pytest.mark.asyncio
async def test_list_detailed_with_show_info_includes_last_modified(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that list_detailed with show_info=True includes last_modified.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    # Setup: Create a blob
    blob_client = test_container.get_blob_client("info-modified.txt")
    await blob_client.upload_blob(b"content", overwrite=True)

    # List detailed
    result = await abs_storage.list_detailed(
        "info-modified.txt",
        show_info=True,
    )

    result_list = list(result)
    assert len(result_list) == 1
    assert result_list[0].last_modified is not None


@pytest.mark.asyncio
async def test_list_detailed_with_show_stats_includes_metadata(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that list_detailed with show_stats=True includes metadata.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    # Setup: Create a blob with metadata
    blob_client = test_container.get_blob_client("stats-metadata.txt")
    await blob_client.upload_blob(
        b"content",
        overwrite=True,
        metadata={"key": "value"},
    )

    # List detailed
    result = await abs_storage.list_detailed(
        "stats-metadata.txt",
        show_stats=True,
    )

    result_list = list(result)
    assert len(result_list) == 1
    assert result_list[0].metadata is not None


@pytest.mark.asyncio
async def test_list_detailed_with_show_stats_includes_content_type(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that list_detailed with show_stats=True includes content_type.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    # Setup: Create a blob
    blob_client = test_container.get_blob_client("stats-content-type.txt")
    await blob_client.upload_blob(b"content", overwrite=True)

    # List detailed
    result = await abs_storage.list_detailed(
        "stats-content-type.txt",
        show_stats=True,
    )

    result_list = list(result)
    assert len(result_list) == 1
    assert result_list[0].content_type is not None


@pytest.mark.asyncio
async def test_list_detailed_returns_empty_for_empty_path(
    abs_storage: AzureBlobStorage,
    test_container: ContainerClient,
) -> None:
    """Test that list_detailed returns empty collection for empty path.

    Args:
        abs_storage: The AzureBlobStorage instance.
        test_container: The test container client.

    """
    result = await abs_storage.list_detailed(
        "nonexistent-prefix/",
        show_stats=True,
        show_info=True,
    )

    assert len(list(result)) == 0
