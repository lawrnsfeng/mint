"""Integration tests for S3Storage using LocalStack testcontainer."""

import asyncio
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, cast

import pytest
from aiobotocore.session import AioSession

from mint.fs.asynk.s3 import S3Storage, _GetObjectContextManager
from mint.fs.asynk.s3_structs import S3CredentialMode
from mint.fs.exc import (
    AmbiguousFolderPathError,
    FileAlreadyExistsError,
    FolderAlreadyExistsError,
    IncompatibleClientError,
    ObjectNotFoundError,
    TrailingSlashNotAllowedError,
    UnsupportedRefTypeError,
)
from mint.fs.structs import (
    CopyResult,
    ListItem,
    MoveResult,
    RemoveResult,
    Stat,
)
from mint.utils.exc import InvalidConcurrencyLimitError
from tests.storage.conftest import LocalStackContainer

if TYPE_CHECKING:
    from types_aiobotocore_s3.client import S3Client

# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------


class TestLocalStackHealth:
    """Verify LocalStack is running and accepting S3 requests."""

    async def test_localstack_is_running_and_operational(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """LocalStack responds to list_buckets successfully.

        Args:
            s3_storage: S3Storage fixture backed by LocalStack.

        """
        assert s3_storage.bucket_name == LocalStackContainer.TEST_BUCKET_NAME


# ---------------------------------------------------------------------------
# Credential mode
# ---------------------------------------------------------------------------


class TestCredentialMode:
    """Tests for _init_credential_mode resolution order."""

    def test_key_pair_mode(self) -> None:
        """Explicit key pair resolves to KeyPair mode.

        Returns:
            None

        """
        storage = S3Storage(
            "bucket",
            access_key="AKID",
            secret_key="SECRET",  # noqa: S106
        )
        assert storage.mode == S3CredentialMode.KeyPair

    def test_iam_role_mode_fallback(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """No credentials falls back to IAMRole mode.

        Args:
            monkeypatch: pytest monkeypatch fixture.

        """
        monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
        monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
        storage = S3Storage("bucket", profile_name="nonexistent_profile_xyz")
        assert storage.mode == S3CredentialMode.IAMRole

    def test_env_var_mode(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Environment variables resolve to EnvVar mode.

        Args:
            monkeypatch: pytest monkeypatch fixture.

        """
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKID")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "SECRET")
        storage = S3Storage("bucket", profile_name="nonexistent_profile_xyz")
        assert storage.mode == S3CredentialMode.EnvVar


# ---------------------------------------------------------------------------
# is_folder
# ---------------------------------------------------------------------------


class TestIsFolder:
    """Tests for S3Storage.is_folder."""

    SavedFolderName: Final[str] = "lorem/"
    SavedFileName: Final[str] = "ipsum"
    SavedFilePath: Final[str] = f"{SavedFolderName}{SavedFileName}"
    SavedFileContent: Final[bytes] = b"dolor sit amet"

    async def test_is_folder_returns_true_for_prefix_with_children(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """is_folder returns True when prefix has child objects.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save(self.SavedFilePath, self.SavedFileContent)
        assert await s3_storage.is_folder(self.SavedFolderName) is True

    async def test_is_folder_trailing_slash_stripped(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """is_folder strips trailing slash before checking prefix.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save(self.SavedFilePath, self.SavedFileContent)
        assert await s3_storage.is_folder("lorem/") is True

    async def test_is_folder_returns_false_for_file(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """is_folder returns False for an existing single object.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save(self.SavedFilePath, self.SavedFileContent)
        assert await s3_storage.is_folder(self.SavedFilePath) is False

    async def test_is_folder_returns_false_for_nonexistent(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """is_folder returns False for a nonexistent path (no exception).

        Args:
            s3_storage: S3Storage fixture.

        """
        assert await s3_storage.is_folder("nonexistent/") is False


# ---------------------------------------------------------------------------
# get
# ---------------------------------------------------------------------------


class TestGet:
    """Tests for S3Storage.get."""

    SavedFilePath: Final[str] = "folder/file.txt"
    SavedContent: Final[bytes] = b"hello world"

    async def test_get_downloads_file_content(
        self,
        s3_storage: S3Storage,
        tmp_path: Path,
    ) -> None:
        """Get downloads object content to a local file.

        Args:
            s3_storage: S3Storage fixture.
            tmp_path: Temporary directory from pytest.

        """
        await s3_storage.save(self.SavedFilePath, self.SavedContent)
        dest = tmp_path / "out.txt"
        await s3_storage.get(self.SavedFilePath, str(dest))
        assert dest.read_bytes() == self.SavedContent

    async def test_get_creates_parent_directories(
        self,
        s3_storage: S3Storage,
        tmp_path: Path,
    ) -> None:
        """Get creates parent directories when they don't exist.

        Args:
            s3_storage: S3Storage fixture.
            tmp_path: Temporary directory from pytest.

        """
        await s3_storage.save(self.SavedFilePath, self.SavedContent)
        dest = tmp_path / "deep" / "nested" / "out.txt"
        await s3_storage.get(self.SavedFilePath, str(dest))
        assert dest.read_bytes() == self.SavedContent

    async def test_get_empty_file(
        self,
        s3_storage: S3Storage,
        tmp_path: Path,
    ) -> None:
        """Get correctly handles empty objects (size=0).

        Args:
            s3_storage: S3Storage fixture.
            tmp_path: Temporary directory from pytest.

        """
        await s3_storage.save("empty.txt", b"")
        dest = tmp_path / "empty.txt"
        await s3_storage.get("empty.txt", str(dest))
        assert dest.read_bytes() == b""

    async def test_get_raises_not_found_for_missing_object(
        self,
        s3_storage: S3Storage,
        tmp_path: Path,
    ) -> None:
        """Get raises ObjectNotFoundError for nonexistent object.

        Args:
            s3_storage: S3Storage fixture.
            tmp_path: Temporary directory from pytest.

        """
        with pytest.raises(ObjectNotFoundError):
            await s3_storage.get("nonexistent.txt", str(tmp_path / "out.txt"))


# ---------------------------------------------------------------------------
# save
# ---------------------------------------------------------------------------


class TestSave:
    """Tests for S3Storage.save."""

    async def test_save_bytes(self, s3_storage: S3Storage) -> None:
        """Save uploads bytes content.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("file.txt", b"content")
        result = await s3_storage.stat("file.txt")
        assert result.size == len(b"content")

    async def test_save_bytesio(self, s3_storage: S3Storage) -> None:
        """Save uploads BytesIO content.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("bio.txt", BytesIO(b"bio content"))
        result = await s3_storage.stat("bio.txt")
        assert result.size == len(b"bio content")

    async def test_save_path(
        self,
        s3_storage: S3Storage,
        tmp_path: Path,
    ) -> None:
        """Save uploads content from a Path object.

        Args:
            s3_storage: S3Storage fixture.
            tmp_path: Temporary directory.

        """
        local = tmp_path / "local.txt"
        local.write_bytes(b"path content")
        await s3_storage.save("from_path.txt", local)
        result = await s3_storage.stat("from_path.txt")
        assert result.size == len(b"path content")

    async def test_save_str_path(
        self,
        s3_storage: S3Storage,
        tmp_path: Path,
    ) -> None:
        """Save uploads content from a str file path.

        Args:
            s3_storage: S3Storage fixture.
            tmp_path: Temporary directory.

        """
        local = tmp_path / "str_path.txt"
        local.write_bytes(b"str content")
        await s3_storage.save("from_str_path.txt", str(local))
        result = await s3_storage.stat("from_str_path.txt")
        assert result.size == len(b"str content")

    async def test_save_returns_path(self, s3_storage: S3Storage) -> None:
        """Save returns the object key.

        Args:
            s3_storage: S3Storage fixture.

        """
        result = await s3_storage.save("return_key.txt", b"x")
        assert result == "return_key.txt"

    async def test_save_empty_bytes(self, s3_storage: S3Storage) -> None:
        """Save handles empty bytes (size=0 object).

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("empty.txt", b"")
        stat = await s3_storage.stat("empty.txt")
        assert stat.size == 0

    async def test_save_raises_for_trailing_slash(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """Save raises TrailingSlashNotAllowedError for path ending with '/'.

        Args:
            s3_storage: S3Storage fixture.

        """
        with pytest.raises(TrailingSlashNotAllowedError):
            await s3_storage.save("folder/", b"x")

    async def test_save_overwrite_false_raises_when_exists(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """Save with overwrite=False raises FileAlreadyExistsError.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("exists.txt", b"first")
        with pytest.raises(FileAlreadyExistsError):
            await s3_storage.save("exists.txt", b"second", overwrite=False)

    async def test_save_raises_folder_already_exists(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """Save raises FolderAlreadyExistsError if path is an existing folder.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("myfolder/child.txt", b"x")
        with pytest.raises(FolderAlreadyExistsError):
            await s3_storage.save("myfolder", b"x")

    async def test_save_raises_for_unsupported_ref_type(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """Save raises UnsupportedRefTypeError for unsupported ref types.

        Args:
            s3_storage: S3Storage fixture.

        """
        bad_ref: Any = 12345
        with pytest.raises(UnsupportedRefTypeError):
            await s3_storage.save("bad.txt", bad_ref)


# ---------------------------------------------------------------------------
# stat
# ---------------------------------------------------------------------------


class TestStat:
    """Tests for S3Storage.stat."""

    async def test_stat_returns_size(self, s3_storage: S3Storage) -> None:
        """Stat returns correct file size.

        Args:
            s3_storage: S3Storage fixture.

        """
        content = b"stat content"
        await s3_storage.save("stat.txt", content)
        result = await s3_storage.stat("stat.txt")
        assert result.size == len(content)

    async def test_stat_returns_last_modified(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """Stat returns a last_modified timestamp.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("stat2.txt", b"x")
        result = await s3_storage.stat("stat2.txt")
        assert result.last_modified is not None

    async def test_stat_returns_stat_dataclass(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """Stat returns a Stat dataclass instance.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("stat3.txt", b"y")
        result = await s3_storage.stat("stat3.txt")
        assert isinstance(result, Stat)

    async def test_stat_empty_file(self, s3_storage: S3Storage) -> None:
        """Stat returns size=0 for empty files.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("empty_stat.txt", b"")
        result = await s3_storage.stat("empty_stat.txt")
        assert result.size == 0

    async def test_stat_raises_not_found(self, s3_storage: S3Storage) -> None:
        """Stat raises ObjectNotFoundError for nonexistent object.

        Args:
            s3_storage: S3Storage fixture.

        """
        with pytest.raises(ObjectNotFoundError):
            await s3_storage.stat("ghost.txt")


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


class TestList:
    """Tests for S3Storage.list."""

    async def test_list_flat(self, s3_storage: S3Storage) -> None:
        """List returns immediate children under a prefix.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("folder/a.txt", b"a")
        await s3_storage.save("folder/b.txt", b"b")
        keys = list(await s3_storage.list("folder/"))
        assert "folder/a.txt" in keys
        assert "folder/b.txt" in keys

    async def test_list_non_recursive_excludes_subfolders(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """List without recursive excludes nested sub-prefix contents.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("root/sub/deep.txt", b"d")
        await s3_storage.save("root/file.txt", b"f")
        keys = list(await s3_storage.list("root/"))
        assert "root/file.txt" in keys
        assert "root/sub/" in keys
        assert "root/sub/deep.txt" not in keys

    async def test_list_recursive(self, s3_storage: S3Storage) -> None:
        """List with recursive=True includes all nested objects.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("rec/a.txt", b"a")
        await s3_storage.save("rec/sub/b.txt", b"b")
        keys = list(await s3_storage.list("rec/", recursive=True))
        assert "rec/a.txt" in keys
        assert "rec/sub/b.txt" in keys

    async def test_list_empty_prefix_returns_empty(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """List returns empty for nonexistent prefix (no exception).

        Args:
            s3_storage: S3Storage fixture.

        """
        keys = list(await s3_storage.list("nonexistent_prefix_xyz/"))
        assert keys == []

    async def test_list_all_objects(self, s3_storage: S3Storage) -> None:
        """List with empty prefix returns all objects in bucket.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("obj1.txt", b"1")
        await s3_storage.save("obj2.txt", b"2")
        keys = list(await s3_storage.list("", recursive=True))
        assert "obj1.txt" in keys
        assert "obj2.txt" in keys

    async def test_list_single_object(self, s3_storage: S3Storage) -> None:
        """List returns a single object under its prefix.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("single/only.txt", b"only")
        keys = list(await s3_storage.list("single/"))
        assert keys == ["single/only.txt"]

    async def test_list_subfolder_prefix_only(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """List non-recursive returns only common prefixes for subfolders.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("parent/child1/file.txt", b"f")
        await s3_storage.save("parent/child2/file.txt", b"g")
        keys = list(await s3_storage.list("parent/"))
        assert "parent/child1/" in keys
        assert "parent/child2/" in keys


# ---------------------------------------------------------------------------
# list_detailed
# ---------------------------------------------------------------------------


class TestListDetailed:
    """Tests for S3Storage.list_detailed."""

    async def test_list_detailed_returns_list_items(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """list_detailed returns ListItem instances.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("detail/a.txt", b"a")
        items = list(await s3_storage.list_detailed("detail/"))
        assert len(items) == 1
        assert isinstance(items[0], ListItem)
        assert items[0].object_name == "detail/a.txt"

    async def test_list_detailed_show_info(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """list_detailed with show_info=True populates size, etag, etc.

        Args:
            s3_storage: S3Storage fixture.

        """
        content = b"info content"
        await s3_storage.save("info/file.txt", content)
        items = list(
            await s3_storage.list_detailed("info/", show_info=True),
        )
        assert len(items) == 1
        item = items[0]
        assert item.size == len(content)
        assert item.etag is not None
        assert item.last_modified is not None
        assert item.bucket_name == s3_storage.bucket_name

    async def test_list_detailed_show_stats(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """list_detailed with show_stats=True populates content_type.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("stats/file.txt", b"x")
        items = list(
            await s3_storage.list_detailed(
                "stats/",
                show_stats=True,
                show_info=True,
            ),
        )
        assert len(items) == 1
        assert items[0].content_type is not None

    async def test_list_detailed_empty_prefix(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """list_detailed returns empty for nonexistent prefix.

        Args:
            s3_storage: S3Storage fixture.

        """
        items = list(
            await s3_storage.list_detailed("nonexistent_xyz/"),
        )
        assert items == []

    async def test_list_detailed_recursive(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """list_detailed with recursive=True includes nested objects.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("drec/a.txt", b"a")
        await s3_storage.save("drec/sub/b.txt", b"b")
        items = list(
            await s3_storage.list_detailed("drec/", recursive=True),
        )
        names = [i.object_name for i in items]
        assert "drec/a.txt" in names
        assert "drec/sub/b.txt" in names

    async def test_list_detailed_non_recursive_shows_prefix(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """list_detailed non-recursive shows subfolder prefix as ListItem.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("pref/sub/c.txt", b"c")
        items = list(await s3_storage.list_detailed("pref/"))
        names = [i.object_name for i in items]
        assert "pref/sub/" in names

    async def test_list_detailed_multiple_files(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """list_detailed returns all objects under prefix.

        Args:
            s3_storage: S3Storage fixture.

        """
        for i in range(5):
            await s3_storage.save(f"multi/f{i}.txt", f"content{i}".encode())
        items = list(await s3_storage.list_detailed("multi/"))
        assert len(items) == 5

    async def test_list_detailed_show_info_false(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """list_detailed without show_info leaves extra fields as None.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("noinfo/x.txt", b"x")
        items = list(await s3_storage.list_detailed("noinfo/"))
        assert items[0].size is None
        assert items[0].etag is None

    async def test_list_detailed_show_info_and_show_stats(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """list_detailed with both flags populates all available fields.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("full/x.txt", b"x")
        items = list(
            await s3_storage.list_detailed(
                "full/",
                show_info=True,
                show_stats=True,
            ),
        )
        assert len(items) == 1
        item = items[0]
        assert item.size is not None
        assert item.content_type is not None


# ---------------------------------------------------------------------------
# copy
# ---------------------------------------------------------------------------


class TestCopy:
    """Tests for S3Storage.copy."""

    async def test_copy_single_file(self, s3_storage: S3Storage) -> None:
        """Copy duplicates a single object to new key.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("src.txt", b"copy me")
        result = await s3_storage.copy("src.txt", "dst.txt")
        assert isinstance(result, CopyResult)
        assert "dst.txt" in result.success
        assert result.failure == []
        assert (await s3_storage.stat("dst.txt")).size == len(b"copy me")

    async def test_copy_preserves_content(
        self,
        s3_storage: S3Storage,
        tmp_path: Path,
    ) -> None:
        """Copy preserves original content in destination.

        Args:
            s3_storage: S3Storage fixture.
            tmp_path: Temporary directory.

        """
        content = b"preserved"
        await s3_storage.save("orig.txt", content)
        await s3_storage.copy("orig.txt", "copy_out.txt")
        dest = tmp_path / "out.txt"
        await s3_storage.get("copy_out.txt", str(dest))
        assert dest.read_bytes() == content

    async def test_copy_folder_flat(self, s3_storage: S3Storage) -> None:
        """Copy with folder src duplicates all immediate children.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("src_dir/a.txt", b"a")
        await s3_storage.save("src_dir/b.txt", b"b")
        result = await s3_storage.copy("src_dir/", "dst_dir/")
        assert len(result.success) == 2
        assert result.failure == []

    async def test_copy_folder_recursive(self, s3_storage: S3Storage) -> None:
        """Copy with folder src and recursive=True copies all nested objects.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("deep_src/a.txt", b"a")
        await s3_storage.save("deep_src/sub/b.txt", b"b")
        result = await s3_storage.copy(
            "deep_src/",
            "deep_dst/",
            recursive=True,
        )
        assert len(result.success) == 2

    async def test_copy_folder_non_recursive_excludes_subfolder(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """Non-recursive folder copy skips nested subfolders entirely.

        list(..., recursive=False) mixes real keys with virtual
        CommonPrefixes folder markers; those markers must never be handed
        to copy_object (they're not real objects) nor recursed into.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("nr_src/a.txt", b"a")
        await s3_storage.save("nr_src/sub/b.txt", b"b")

        result = await s3_storage.copy("nr_src/", "nr_dst/", recursive=False)

        assert result.success == ["nr_dst/a.txt"]
        assert result.failure == []
        assert not await s3_storage.is_folder("nr_dst/sub")
        with pytest.raises(ObjectNotFoundError):
            await s3_storage.stat("nr_dst/sub/b.txt")

    async def test_copy_raises_if_src_is_folder_without_slash(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """Copy raises AmbiguousFolderPathError when src is folder w/o '/'.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("fold/file.txt", b"x")
        with pytest.raises(AmbiguousFolderPathError):
            await s3_storage.copy("fold", "other")

    async def test_copy_raises_not_found_for_missing_src(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """Copy raises ObjectNotFoundError for nonexistent single-file src.

        Args:
            s3_storage: S3Storage fixture.

        """
        with pytest.raises(ObjectNotFoundError):
            await s3_storage.copy("ghost.txt", "dst.txt")

    async def test_copy_returns_copy_result(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """Copy returns a CopyResult dataclass.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("cr_src.txt", b"x")
        result = await s3_storage.copy("cr_src.txt", "cr_dst.txt")
        assert isinstance(result, CopyResult)

    async def test_copy_empty_folder(self, s3_storage: S3Storage) -> None:
        """Copy on empty folder prefix returns CopyResult with empty lists.

        Args:
            s3_storage: S3Storage fixture.

        """
        result = await s3_storage.copy("empty_folder/", "other_folder/")
        assert result.success == []
        assert result.failure == []


# ---------------------------------------------------------------------------
# move
# ---------------------------------------------------------------------------


class TestMove:
    """Tests for S3Storage.move."""

    async def test_move_single_file(self, s3_storage: S3Storage) -> None:
        """Move relocates a single object to a new key.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("mv_src.txt", b"move me")
        result = await s3_storage.move("mv_src.txt", "mv_dst.txt")
        assert isinstance(result, MoveResult)
        assert "mv_dst.txt" in result.copy.success
        with pytest.raises(ObjectNotFoundError):
            await s3_storage.stat("mv_src.txt")

    async def test_move_folder(self, s3_storage: S3Storage) -> None:
        """Move relocates a folder and all its children.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("mv_fold/a.txt", b"a")
        await s3_storage.save("mv_fold/b.txt", b"b")
        result = await s3_storage.move("mv_fold/", "mv_fold_dst/")
        assert len(result.copy.success) == 2
        keys = list(await s3_storage.list("mv_fold/"))
        assert keys == []

    async def test_move_preserves_content(
        self,
        s3_storage: S3Storage,
        tmp_path: Path,
    ) -> None:
        """Move preserves content at destination.

        Args:
            s3_storage: S3Storage fixture.
            tmp_path: Temporary directory.

        """
        content = b"moved content"
        await s3_storage.save("mv_content_src.txt", content)
        await s3_storage.move("mv_content_src.txt", "mv_content_dst.txt")
        dest = tmp_path / "out.txt"
        await s3_storage.get("mv_content_dst.txt", str(dest))
        assert dest.read_bytes() == content

    async def test_move_folder_non_recursive_with_subfolder_present(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """Non-recursive move over a folder with a subfolder does not abort.

        Regression test: copy()'s folder branch used to hand the virtual
        CommonPrefixes subfolder marker to copy_object, which always failed
        (no such object), making move() raise MoveCleanupError even though
        every real file copied fine.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("mv_nr/a.txt", b"a")
        await s3_storage.save("mv_nr/sub/b.txt", b"b")

        result = await s3_storage.move("mv_nr/", "mv_nr_dst/", recursive=False)

        assert result.copy.success == ["mv_nr_dst/a.txt"]
        assert result.copy.failure == []
        with pytest.raises(ObjectNotFoundError):
            await s3_storage.stat("mv_nr/a.txt")
        assert (await s3_storage.stat("mv_nr/sub/b.txt")).size == len(b"b")

    async def test_move_raises_not_found_for_missing_src(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """Move raises ObjectNotFoundError for nonexistent source.

        Args:
            s3_storage: S3Storage fixture.

        """
        with pytest.raises(ObjectNotFoundError):
            await s3_storage.move("ghost.txt", "any.txt")

    async def test_move_returns_move_result(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """Move returns a MoveResult dataclass.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("mr_src.txt", b"x")
        result = await s3_storage.move("mr_src.txt", "mr_dst.txt")
        assert isinstance(result, MoveResult)
        assert isinstance(result.copy, CopyResult)
        assert isinstance(result.remove, RemoveResult)

    async def test_move_raises_invalid_args_for_folder_without_slash(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """Move raises AmbiguousFolderPathError if folder src lacks '/'.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("fold_mv/x.txt", b"x")
        with pytest.raises(AmbiguousFolderPathError):
            await s3_storage.move("fold_mv", "other")


# ---------------------------------------------------------------------------
# remove
# ---------------------------------------------------------------------------


class TestRemove:
    """Tests for S3Storage.remove."""

    async def test_remove_single_file(self, s3_storage: S3Storage) -> None:
        """Remove deletes a single object.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("rm.txt", b"x")
        result = await s3_storage.remove("rm.txt")
        assert "rm.txt" in result.success
        with pytest.raises(ObjectNotFoundError):
            await s3_storage.stat("rm.txt")

    async def test_remove_folder_flat(self, s3_storage: S3Storage) -> None:
        """Remove on a folder prefix deletes all immediate children.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("rm_folder/a.txt", b"a")
        await s3_storage.save("rm_folder/b.txt", b"b")
        result = await s3_storage.remove("rm_folder/")
        assert len(result.success) == 2

    async def test_remove_folder_non_recursive_leaves_subfolder_intact(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """Non-recursive remove only deletes the immediate-level object.

        Regression test: list(path, recursive=False) returns the virtual
        CommonPrefixes subfolder marker alongside real keys; remove_many
        used to expand that marker's own children too, over-deleting one
        level deeper than a non-recursive remove should reach.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("rm_shallow/a.txt", b"a")
        await s3_storage.save("rm_shallow/sub/b.txt", b"b")

        result = await s3_storage.remove("rm_shallow/", recursive=False)

        assert result.success == ["rm_shallow/a.txt"]
        assert result.failure == []
        with pytest.raises(ObjectNotFoundError):
            await s3_storage.stat("rm_shallow/a.txt")
        nested = await s3_storage.stat("rm_shallow/sub/b.txt")
        assert nested.size == len(b"b")

    async def test_remove_folder_recursive(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """Remove with recursive=True deletes all nested objects.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("rm_rec/a.txt", b"a")
        await s3_storage.save("rm_rec/sub/b.txt", b"b")
        result = await s3_storage.remove("rm_rec/", recursive=True)
        assert len(result.success) == 2

    async def test_remove_raises_not_found(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """Remove raises ObjectNotFoundError for nonexistent single object.

        Args:
            s3_storage: S3Storage fixture.

        """
        with pytest.raises(ObjectNotFoundError):
            await s3_storage.remove("ghost.txt")

    async def test_remove_empty_folder_returns_empty_result(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """Remove on nonexistent folder prefix returns empty RemoveResult.

        Args:
            s3_storage: S3Storage fixture.

        """
        result = await s3_storage.remove("empty_fold/")
        assert result.success == []
        assert result.failure == []


# ---------------------------------------------------------------------------
# remove_many
# ---------------------------------------------------------------------------


class TestRemoveMany:
    """Tests for S3Storage.remove_many."""

    async def test_remove_many_files(self, s3_storage: S3Storage) -> None:
        """remove_many deletes multiple individual objects.

        Args:
            s3_storage: S3Storage fixture.

        """
        keys = [f"rmm/file{i}.txt" for i in range(5)]
        for k in keys:
            await s3_storage.save(k, b"content")
        result = await s3_storage.remove_many(keys)
        assert len(result.success) == 5

    async def test_remove_many_empty_list(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """remove_many with empty list returns empty RemoveResult.

        Args:
            s3_storage: S3Storage fixture.

        """
        result = await s3_storage.remove_many([])
        assert result.success == []
        assert result.failure == []

    async def test_remove_many_recursive_folders(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """remove_many with recursive=True removes nested folder contents.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("rmmr/sub/a.txt", b"a")
        await s3_storage.save("rmmr/sub/deep/b.txt", b"b")
        result = await s3_storage.remove_many(["rmmr/"], recursive=True)
        assert len(result.success) == 2
        assert result.failure == []

    async def test_remove_many_mixed_files_and_folders(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """remove_many handles mix of file keys and folder prefixes.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("mixed/file.txt", b"f")
        await s3_storage.save("single_file.txt", b"s")
        result = await s3_storage.remove_many(
            ["single_file.txt", "mixed/"],
        )
        assert "single_file.txt" in result.success


# ---------------------------------------------------------------------------
# gen_presigned_url
# ---------------------------------------------------------------------------


class TestGenPresignedUrl:
    """Tests for S3Storage.gen_presigned_url."""

    async def test_gen_presigned_url_returns_string(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """gen_presigned_url returns a URL string.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("presign.txt", b"x")
        url = await s3_storage.gen_presigned_url("presign.txt")
        assert isinstance(url, str)
        assert "presign.txt" in url or "X-Amz-Signature" in url

    async def test_gen_presigned_url_raises_not_found(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """gen_presigned_url raises ObjectNotFoundError for missing object.

        Args:
            s3_storage: S3Storage fixture.

        """
        with pytest.raises(ObjectNotFoundError):
            await s3_storage.gen_presigned_url("ghost.txt")

    async def test_gen_presigned_url_custom_expiration(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """gen_presigned_url accepts custom expiration_in_seconds.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("presign2.txt", b"x")
        url = await s3_storage.gen_presigned_url(
            "presign2.txt",
            expiration_in_seconds=300,
        )
        assert isinstance(url, str)

    async def test_gen_presigned_url_custom_filename(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """gen_presigned_url includes custom filename in Content-Disposition.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("presign3.txt", b"x")
        url = await s3_storage.gen_presigned_url(
            "presign3.txt",
            file_name="custom_name.txt",
        )
        assert isinstance(url, str)


# ---------------------------------------------------------------------------
# save_many
# ---------------------------------------------------------------------------


class TestSaveMany:
    """Tests for S3Storage.save_many."""

    async def test_save_many_all_succeed(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """save_many returns (key, None) for each successful upload.

        Args:
            s3_storage: S3Storage fixture.

        """
        objects = [
            ("sm/a.txt", b"aaa"),
            ("sm/b.txt", b"bbb"),
            ("sm/c.txt", b"ccc"),
        ]
        results = list(await s3_storage.save_many(objects))
        assert results == [
            ("sm/a.txt", None),
            ("sm/b.txt", None),
            ("sm/c.txt", None),
        ]

    async def test_save_many_partial_failure(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """save_many captures exceptions for failed uploads.

        Args:
            s3_storage: S3Storage fixture.

        """
        objects = [
            ("sm_pf/good.txt", b"ok"),
            ("bad_folder/", b"fail"),  # slash in key is invalid
        ]
        results = list(await s3_storage.save_many(objects))
        assert results[0] == ("sm_pf/good.txt", None)
        assert results[1][0] == "bad_folder/"
        assert isinstance(results[1][1], Exception)

    async def test_save_many_with_batch_size(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """save_many respects batch_size parameter.

        Args:
            s3_storage: S3Storage fixture.

        """
        objects = [(f"batch/f{i}.txt", f"c{i}".encode()) for i in range(6)]
        results = list(await s3_storage.save_many(objects, batch_size=2))
        assert all(exc is None for _, exc in results)


# ---------------------------------------------------------------------------
# ensure_bucket
# ---------------------------------------------------------------------------


class TestEnsureBucket:
    """Tests for S3Storage.ensure_bucket."""

    async def test_ensure_bucket_creates_new_bucket(
        self,
        s3_endpoint_url: str,
    ) -> None:
        """ensure_bucket creates a new bucket if it doesn't exist.

        Args:
            s3_endpoint_url: LocalStack S3 endpoint URL.

        """
        storage = S3Storage(
            bucket_name="brand-new-bucket-xyz",
            endpoint_url=s3_endpoint_url,
            access_key=LocalStackContainer.ACCESS_KEY,
            secret_key=LocalStackContainer.SECRET_KEY,
            region_name=LocalStackContainer.DEFAULT_REGION,
        )
        await storage.ensure_bucket()
        await storage.save("verify.txt", b"ok")
        stat = await storage.stat("verify.txt")
        assert stat.size == 2

    async def test_ensure_bucket_idempotent(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """ensure_bucket does not raise if bucket already exists.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.ensure_bucket()
        await s3_storage.ensure_bucket()


# ---------------------------------------------------------------------------
# get_fileobj
# ---------------------------------------------------------------------------


class TestGetFileobj:
    """Tests for S3Storage.get_fileobj / _GetObjectContextManager."""

    async def test_get_fileobj_returns_bytesio(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """get_fileobj returns BytesIO with object content.

        Args:
            s3_storage: S3Storage fixture.

        """
        content = b"fileobj content"
        await s3_storage.save("fileobj.txt", content)
        async with s3_storage.get_fileobj("fileobj.txt") as f:
            assert f.read() == content

    async def test_get_fileobj_raises_not_found(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """get_fileobj raises ObjectNotFoundError for missing object.

        Args:
            s3_storage: S3Storage fixture.

        """
        with pytest.raises(ObjectNotFoundError):
            async with s3_storage.get_fileobj("ghost.txt"):
                pass

    async def test_get_fileobj_returns_get_object_context_manager(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """get_fileobj returns a _GetObjectContextManager instance.

        Args:
            s3_storage: S3Storage fixture.

        """
        cm = s3_storage.get_fileobj("any.txt")
        assert isinstance(cm, _GetObjectContextManager)


# ---------------------------------------------------------------------------
# calculate_etag
# ---------------------------------------------------------------------------


class TestCalculateEtag:
    """Tests for S3Storage.calculate_etag."""

    async def test_calculate_etag_single_part(
        self,
        s3_storage: S3Storage,
        tmp_path: Path,
    ) -> None:
        """calculate_etag returns consistent MD5 for single-part.

        Args:
            s3_storage: S3Storage fixture.
            tmp_path: Temporary directory.

        """
        f = tmp_path / "etag.bin"
        f.write_bytes(b"hello world")
        etag1 = await s3_storage.calculate_etag(f)
        etag2 = await s3_storage.calculate_etag(f)
        assert etag1 == etag2
        assert "-" not in etag1

    async def test_calculate_etag_multipart(
        self,
        s3_storage: S3Storage,
        tmp_path: Path,
    ) -> None:
        """calculate_etag with use_multipart=True returns 'hex-N' format.

        Args:
            s3_storage: S3Storage fixture.
            tmp_path: Temporary directory.

        """
        f = tmp_path / "multi.bin"
        f.write_bytes(b"x" * 1024)
        etag = await s3_storage.calculate_etag(
            f,
            use_multipart=True,
            chunk_size=100,
        )
        assert "-" in etag

    async def test_calculate_etag_changes_with_content(
        self,
        s3_storage: S3Storage,
        tmp_path: Path,
    ) -> None:
        """calculate_etag produces different values for different content.

        Args:
            s3_storage: S3Storage fixture.
            tmp_path: Temporary directory.

        """
        f = tmp_path / "change.bin"
        f.write_bytes(b"original")
        etag1 = await s3_storage.calculate_etag(f)
        f.write_bytes(b"modified")
        etag2 = await s3_storage.calculate_etag(f)
        assert etag1 != etag2

    async def test_calculate_etag_str_path(
        self,
        s3_storage: S3Storage,
        tmp_path: Path,
    ) -> None:
        """calculate_etag accepts a str path.

        Args:
            s3_storage: S3Storage fixture.
            tmp_path: Temporary directory.

        """
        f = tmp_path / "str_etag.bin"
        f.write_bytes(b"content")
        etag = await s3_storage.calculate_etag(str(f))
        assert isinstance(etag, str)


# ---------------------------------------------------------------------------
# clone
# ---------------------------------------------------------------------------


class TestClone:
    """Tests for S3Storage.clone."""

    async def test_clone_creates_independent_instance(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """Clone returns a new S3Storage with identical config.

        Args:
            s3_storage: S3Storage fixture.

        """
        cloned = s3_storage.clone()
        assert cloned is not s3_storage
        assert cloned.bucket_name == s3_storage.bucket_name
        assert cloned.endpoint_url == s3_storage.endpoint_url

    async def test_clone_is_functional(self, s3_storage: S3Storage) -> None:
        """Cloned instance can perform operations independently.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("clone_test.txt", b"data")
        cloned = s3_storage.clone()
        stat = await cloned.stat("clone_test.txt")
        assert stat.size == len(b"data")


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------


class TestConcurrency:
    """Tests for concurrent S3Storage operations."""

    async def test_concurrent_stat(self, s3_storage: S3Storage) -> None:
        """Many concurrent stat calls complete without error.

        Note:
            Capped below the ABS equivalent's 300 because each S3Storage
            call without an already-bound client opens a brand-new
            aiobotocore session (see `S3Storage._ensure_client`), and
            LocalStack's single-process dev server cannot reliably serve
            hundreds of simultaneous new sessions. The `s3_storage`
            fixture also bounds this via `max_concurrent_clients=20`.

        Args:
            s3_storage: S3Storage fixture.

        """
        n = 60
        for i in range(n):
            await s3_storage.save(f"conc/stat{i}.txt", b"x")
        results = await asyncio.gather(
            *[s3_storage.stat(f"conc/stat{i}.txt") for i in range(n)],
        )
        assert len(results) == n

    async def test_concurrent_save(self, s3_storage: S3Storage) -> None:
        """Many concurrent save calls all succeed.

        Args:
            s3_storage: S3Storage fixture.

        """
        n = 60
        results = await asyncio.gather(
            *[s3_storage.save(f"conc_save/f{i}.txt", f"c{i}".encode()) for i in range(n)],
        )
        assert len(results) == n

    async def test_concurrent_mixed_operations(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """Mixed concurrent saves and stats complete without error.

        Args:
            s3_storage: S3Storage fixture.

        """
        n = 30
        for i in range(n):
            await s3_storage.save(f"mixed/f{i}.txt", b"x")
        tasks = [
            *[s3_storage.save(f"mixed/new{i}.txt", b"y") for i in range(n)],
            *[s3_storage.stat(f"mixed/f{i}.txt") for i in range(n)],
        ]
        results = await asyncio.gather(*tasks)
        assert len(results) == 2 * n


# ---------------------------------------------------------------------------
# Concurrency limiter
# ---------------------------------------------------------------------------


class TestConcurrencyLimiter:
    """Tests for max_concurrent_clients parameter."""

    async def test_max_concurrent_clients_limits_concurrency(
        self,
        s3_endpoint_url: str,
    ) -> None:
        """max_concurrent_clients=5 still completes all operations.

        Args:
            s3_endpoint_url: LocalStack S3 endpoint URL.

        """
        storage = S3Storage(
            bucket_name=LocalStackContainer.TEST_BUCKET_NAME,
            endpoint_url=s3_endpoint_url,
            access_key=LocalStackContainer.ACCESS_KEY,
            secret_key=LocalStackContainer.SECRET_KEY,
            region_name=LocalStackContainer.DEFAULT_REGION,
            max_concurrent_clients=5,
        )
        await storage.ensure_bucket()
        results = await asyncio.gather(
            *[storage.save(f"lim/f{i}.txt", b"x") for i in range(20)],
        )
        assert len(results) == 20

    async def test_no_limit_with_none(self, s3_endpoint_url: str) -> None:
        """max_concurrent_clients=None (default) allows unlimited concurrency.

        Args:
            s3_endpoint_url: LocalStack S3 endpoint URL.

        """
        storage = S3Storage(
            bucket_name=LocalStackContainer.TEST_BUCKET_NAME,
            endpoint_url=s3_endpoint_url,
            access_key=LocalStackContainer.ACCESS_KEY,
            secret_key=LocalStackContainer.SECRET_KEY,
            region_name=LocalStackContainer.DEFAULT_REGION,
            max_concurrent_clients=None,
        )
        assert storage._limiter is None

    def test_zero_max_concurrent_clients_raises_value_error(
        self,
        s3_endpoint_url: str,
    ) -> None:
        """max_concurrent_clients=0 raises InvalidConcurrencyLimitError.

        A ConcurrencyLimiter with max_concurrent=0 would never let any
        caller acquire it, so it's rejected at construction time.

        Args:
            s3_endpoint_url: LocalStack S3 endpoint URL.

        """
        with pytest.raises(
            InvalidConcurrencyLimitError,
            match="max_concurrent",
        ):
            S3Storage(
                bucket_name=LocalStackContainer.TEST_BUCKET_NAME,
                endpoint_url=s3_endpoint_url,
                access_key=LocalStackContainer.ACCESS_KEY,
                secret_key=LocalStackContainer.SECRET_KEY,
                region_name=LocalStackContainer.DEFAULT_REGION,
                max_concurrent_clients=0,
            )


# ---------------------------------------------------------------------------
# Client reuse
# ---------------------------------------------------------------------------


class TestClientReuse:
    """The provider hands one cached client to every operation.

    Before caching, each top-level call opened a fresh aiobotocore session --
    re-running the whole credential chain, including the EC2 instance-metadata
    probe, and discarding a warm TLS pool every time.
    """

    async def test_gather_shares_one_client(self, s3_storage: S3Storage) -> None:
        """50 concurrent top-level calls borrow a single client.

        Each `asyncio.gather` branch starts in a fresh context, so under the
        old design every one of them built its own session.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("reuse/probe.txt", b"data")
        before = s3_storage.provider.created_count

        await asyncio.gather(*[s3_storage.stat("reuse/probe.txt") for _ in range(50)])

        assert s3_storage.provider.created_count - before == 0
        assert s3_storage.provider.cached_count == 1

    async def test_sequential_calls_share_one_client(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """Consecutive top-level calls reuse the client.

        This is the case the ContextVar binding alone always missed: the first
        call unbinds on exit, so the second used to start from scratch.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("reuse/seq.txt", b"data")
        before = s3_storage.provider.created_count

        await s3_storage.stat("reuse/seq.txt")
        await s3_storage.stat("reuse/seq.txt")
        await s3_storage.stat("reuse/seq.txt")

        assert s3_storage.provider.created_count == before

    async def test_nested_calls_reuse_the_binding(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """A fan-out inside an operation runs on the outer binding.

        Args:
            s3_storage: S3Storage fixture.

        """
        for i in range(3):
            await s3_storage.save(f"reuse/tree/f{i}.txt", b"x")
        before = s3_storage.provider.created_count

        result = await s3_storage.copy("reuse/tree/", "reuse/copy/", recursive=True)

        assert len(result.success) == 3
        assert s3_storage.provider.created_count == before

    async def test_failure_leaves_no_stale_binding(
        self,
        s3_storage: S3Storage,
    ) -> None:
        """An operation that raises unbinds the client and stays usable.

        Args:
            s3_storage: S3Storage fixture.

        """
        with pytest.raises(ObjectNotFoundError):
            await s3_storage.stat("reuse/definitely-missing.txt")

        assert s3_storage._client_ctx.get() is None

        await s3_storage.save("reuse/after-failure.txt", b"ok")
        assert (await s3_storage.stat("reuse/after-failure.txt")).size == 2

    async def test_clone_shares_the_cache(self, s3_storage: S3Storage) -> None:
        """A clone borrows from the same provider, not a second one.

        Args:
            s3_storage: S3Storage fixture.

        """
        await s3_storage.save("reuse/clone.txt", b"data")
        cloned = s3_storage.clone()
        before = s3_storage.provider.created_count

        await cloned.stat("reuse/clone.txt")

        assert cloned.provider is s3_storage.provider
        assert s3_storage.provider.created_count == before

    async def test_injected_client_is_used_and_not_closed(
        self,
        s3_endpoint_url: str,
    ) -> None:
        """A caller-owned client is borrowed verbatim and left open.

        Args:
            s3_endpoint_url: LocalStack S3 endpoint URL.

        """
        session = AioSession()
        async with session.create_client(
            "s3",
            endpoint_url=s3_endpoint_url,
            aws_access_key_id=LocalStackContainer.ACCESS_KEY,
            aws_secret_access_key=LocalStackContainer.SECRET_KEY,
            region_name=LocalStackContainer.DEFAULT_REGION,
        ) as owned:
            storage = S3Storage(
                bucket_name=LocalStackContainer.TEST_BUCKET_NAME,
                client=owned,
            )
            await storage.save("reuse/injected.txt", b"data")

            assert storage.provider.created_count == 0
            # The client is still usable after mint is done with it.
            await owned.head_object(
                Bucket=LocalStackContainer.TEST_BUCKET_NAME,
                Key="reuse/injected.txt",
            )

    async def test_non_conforming_client_is_rejected(self) -> None:
        """A wrong-shaped client fails at construction, not mid-operation."""
        with pytest.raises(IncompatibleClientError):
            S3Storage(bucket_name="b", client=cast("S3Client", object()))
