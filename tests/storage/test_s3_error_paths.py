"""Unit tests for S3Storage error branches and edge cases.

These tests mock the underlying aiobotocore client so that native error
codes, pagination continuation, and credential-resolution branches that
are impractical to trigger against a real LocalStack instance can be
exercised deterministically.
"""

import contextlib
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from mint.fs.asynk.s3 import S3Storage
from mint.fs.asynk.s3_structs import S3CredentialMode
from mint.fs.exc import (
    InvalidArgumentsError,
    MoveCleanupError,
    ObjectNotFoundError,
    OperationalError,
)


def _client_error(code: str) -> ClientError:
    """Build a ClientError with the given error code."""
    return ClientError({"Error": {"Code": code}}, "SomeOperation")


@contextlib.asynccontextmanager
async def _client_ctx(client: MagicMock) -> AsyncIterator[MagicMock]:
    """Async context manager yielding the given mock client."""
    yield client


def _storage_with_mock_client(client: MagicMock) -> S3Storage:
    """Return an S3Storage whose _create_client yields the given mock."""
    storage = S3Storage(bucket_name="test-bucket", region_name="us-east-1")
    storage._create_client = MagicMock(  # type: ignore[method-assign]
        return_value=_client_ctx(client),
    )
    return storage


class TestCredentialModeBranches:
    """Tests for _init_credential_mode / _has_aws_profile branches."""

    def test_shared_credentials_mode_when_profile_exists(
        self,
        tmp_path: Path,
    ) -> None:
        """SharedCredentials mode is chosen when the profile file matches."""
        aws_dir = tmp_path / ".aws"
        aws_dir.mkdir()
        (aws_dir / "credentials").write_text("[myprofile]\nkey=1\n")

        with (
            patch("mint.fs.asynk.s3.Path.home", return_value=tmp_path),
            patch.dict("os.environ", {}, clear=True),
        ):
            storage = S3Storage(
                bucket_name="b",
                profile_name="myprofile",
            )
            assert storage.mode == S3CredentialMode.SharedCredentials

    def test_has_aws_profile_returns_false_without_credentials_file(
        self,
        tmp_path: Path,
    ) -> None:
        """IAMRole mode is chosen when no credentials file exists."""
        with (
            patch("mint.fs.asynk.s3.Path.home", return_value=tmp_path),
            patch.dict("os.environ", {}, clear=True),
        ):
            storage = S3Storage(bucket_name="b")
            assert storage.mode == S3CredentialMode.IAMRole


class TestAutoCatchNativeExc:
    """Tests for the _auto_catch_native_exc decorator."""

    async def test_value_error_becomes_invalid_arguments_error(self) -> None:
        """A ValueError raised inside is converted to InvalidArgumentsError."""

        class _Dummy:
            @S3Storage._auto_catch_native_exc
            async def op(self) -> None:
                raise ValueError("bad value")

        with pytest.raises(InvalidArgumentsError):
            await _Dummy().op()

    async def test_generic_exception_becomes_operational_error(self) -> None:
        """An unexpected exception is converted to OperationalError."""

        class _Dummy:
            @S3Storage._auto_catch_native_exc
            async def op(self) -> None:
                raise RuntimeError("boom")

        with pytest.raises(OperationalError):
            await _Dummy().op()


class TestGetErrorPaths:
    """Tests for get() non-404 error propagation."""

    async def test_get_reraises_non_not_found_client_error(self) -> None:
        """A non-404 ClientError from get_object propagates unchanged."""
        client = MagicMock()
        client.get_object = AsyncMock(
            side_effect=_client_error("AccessDenied"),
        )
        storage = _storage_with_mock_client(client)

        with pytest.raises(OperationalError):
            await storage.get("some/key.txt", "/tmp/out.txt")  # noqa: S108


class TestSaveErrorPaths:
    """Tests for save() overwrite=False non-404 error propagation."""

    async def test_save_overwrite_false_reraises_non_not_found_error(
        self,
    ) -> None:
        """A non-404 ClientError from head_object propagates unchanged."""
        client = MagicMock()
        client.list_objects_v2 = AsyncMock(return_value={"Contents": []})
        client.head_object = AsyncMock(
            side_effect=_client_error("AccessDenied"),
        )
        storage = _storage_with_mock_client(client)

        with pytest.raises(OperationalError):
            await storage.save("some/key.txt", b"data", overwrite=False)


class TestCopyErrorPaths:
    """Tests for copy() error branches: 5GB limit, non-404, partial gather."""

    async def test_copy_single_reraises_non_not_found_head_error(
        self,
    ) -> None:
        """A non-404 ClientError from head_object propagates unchanged."""
        client = MagicMock()
        client.list_objects_v2 = AsyncMock(return_value={"Contents": []})
        client.head_object = AsyncMock(
            side_effect=_client_error("AccessDenied"),
        )
        storage = _storage_with_mock_client(client)

        with pytest.raises(OperationalError):
            await storage.copy("src.txt", "dst.txt")

    async def test_copy_single_exceeds_five_gb_limit(self) -> None:
        """Objects larger than 5 GB raise InvalidArgumentsError."""
        client = MagicMock()
        client.list_objects_v2 = AsyncMock(
            return_value={"Contents": []},
        )
        client.head_object = AsyncMock(
            return_value={"ContentLength": 6 * 1024 * 1024 * 1024},
        )
        storage = _storage_with_mock_client(client)

        with pytest.raises(InvalidArgumentsError):
            await storage.copy("huge.bin", "dst.bin")

    async def test_copy_folder_partial_failure_recorded(self) -> None:
        """Failures during folder copy are captured, not raised."""
        client = MagicMock()
        client.list_objects_v2 = AsyncMock(
            return_value={
                "Contents": [{"Key": "src/a.txt"}, {"Key": "src/b.txt"}],
            },
        )

        async def copy_object(**kwargs: object) -> dict[str, Any]:
            if kwargs["Key"] == "dst/b.txt":
                raise RuntimeError("copy failed")
            return {}

        client.copy_object = AsyncMock(side_effect=copy_object)
        storage = _storage_with_mock_client(client)

        result = await storage.copy("src/", "dst/", recursive=True)
        assert result.success == ["dst/a.txt"]
        assert len(result.failure) == 1
        assert "dst/b.txt" in result.failure[0]


class TestMoveErrorPaths:
    """Tests for move() cleanup error branch."""

    async def test_move_raises_cleanup_error_on_copy_failure(self) -> None:
        """MoveCleanupError is raised when the copy step has failures."""
        client = MagicMock()
        client.list_objects_v2 = AsyncMock(
            return_value={"Contents": [{"Key": "src/a.txt"}]},
        )
        client.copy_object = AsyncMock(side_effect=RuntimeError("boom"))
        storage = _storage_with_mock_client(client)

        with pytest.raises(MoveCleanupError):
            await storage.move("src/", "dst/", recursive=True)


class TestRemoveErrorPaths:
    """Tests for remove() non-404 error propagation."""

    async def test_remove_reraises_non_not_found_error(self) -> None:
        """A non-404 ClientError from head_object propagates unchanged."""
        client = MagicMock()
        client.head_object = AsyncMock(
            side_effect=_client_error("AccessDenied"),
        )
        storage = _storage_with_mock_client(client)

        with pytest.raises(OperationalError):
            await storage.remove("some/key.txt")


class TestStatErrorPaths:
    """Tests for stat() non-404 error propagation."""

    async def test_stat_reraises_non_not_found_error(self) -> None:
        """A non-404 ClientError from head_object propagates unchanged."""
        client = MagicMock()
        client.head_object = AsyncMock(
            side_effect=_client_error("AccessDenied"),
        )
        storage = _storage_with_mock_client(client)

        with pytest.raises(OperationalError):
            await storage.stat("some/key.txt")


class TestListPagination:
    """Tests for list()/list_detailed() continuation-token pagination."""

    async def test_list_follows_continuation_token(self) -> None:
        """list() issues a second call when NextContinuationToken is set."""
        client = MagicMock()
        responses = [
            {
                "Contents": [{"Key": "a.txt"}],
                "NextContinuationToken": "page2",
            },
            {"Contents": [{"Key": "b.txt"}]},
        ]
        client.list_objects_v2 = AsyncMock(side_effect=responses)
        storage = _storage_with_mock_client(client)

        keys = await storage.list("", recursive=True)
        assert set(keys) == {"a.txt", "b.txt"}
        assert client.list_objects_v2.call_count == 2
        second_call_kwargs = client.list_objects_v2.call_args_list[1].kwargs
        assert second_call_kwargs["ContinuationToken"] == "page2"

    async def test_list_detailed_follows_continuation_token(self) -> None:
        """list_detailed() issues a second call when a token is present."""
        client = MagicMock()
        responses = [
            {
                "Contents": [{"Key": "a.txt"}],
                "NextContinuationToken": "page2",
            },
            {"Contents": [{"Key": "b.txt"}]},
        ]
        client.list_objects_v2 = AsyncMock(side_effect=responses)
        storage = _storage_with_mock_client(client)

        items = await storage.list_detailed("", recursive=True)
        names = {item.object_name for item in items}
        assert names == {"a.txt", "b.txt"}
        assert client.list_objects_v2.call_count == 2

    async def test_list_detailed_show_stats_handles_head_failure(
        self,
    ) -> None:
        """A failed head_object during show_stats is logged and skipped."""
        client = MagicMock()
        client.list_objects_v2 = AsyncMock(
            return_value={"Contents": [{"Key": "a.txt"}]},
        )
        client.head_object = AsyncMock(side_effect=RuntimeError("boom"))
        storage = _storage_with_mock_client(client)

        items = await storage.list_detailed(
            "",
            show_stats=True,
            recursive=True,
        )
        assert len(items) == 1
        assert items[0].content_type is None


class TestGenPresignedUrlErrorPaths:
    """Tests for gen_presigned_url() non-404 error propagation."""

    async def test_gen_presigned_url_reraises_non_not_found_error(
        self,
    ) -> None:
        """A non-404 ClientError from head_object propagates unchanged."""
        client = MagicMock()
        client.head_object = AsyncMock(
            side_effect=_client_error("AccessDenied"),
        )
        storage = _storage_with_mock_client(client)

        with pytest.raises(OperationalError):
            await storage.gen_presigned_url("some/key.txt")


class TestEnsureBucketBranches:
    """Tests for ensure_bucket() region and error branches."""

    async def test_ensure_bucket_uses_location_constraint_for_region(
        self,
    ) -> None:
        """A non-default region passes a LocationConstraint."""
        client = MagicMock()
        client.create_bucket = AsyncMock(return_value={})
        storage = S3Storage(bucket_name="b", region_name="eu-west-1")
        storage._create_client = MagicMock(  # type: ignore[method-assign]
            return_value=_client_ctx(client),
        )

        await storage.ensure_bucket()

        client.create_bucket.assert_awaited_once_with(
            Bucket="b",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
        )

    async def test_ensure_bucket_reraises_unexpected_client_error(
        self,
    ) -> None:
        """An unrelated ClientError from create_bucket propagates unchanged."""
        client = MagicMock()
        client.create_bucket = AsyncMock(
            side_effect=_client_error("AccessDenied"),
        )
        storage = _storage_with_mock_client(client)

        with pytest.raises(OperationalError):
            await storage.ensure_bucket()


class TestGetFileobjErrorPaths:
    """Tests for get_fileobj() non-404 error propagation."""

    async def test_get_fileobj_reraises_non_not_found_error(self) -> None:
        """A non-404 ClientError from get_object propagates unchanged."""
        client = MagicMock()
        client.get_object = AsyncMock(
            side_effect=_client_error("AccessDenied"),
        )
        storage = _storage_with_mock_client(client)

        with pytest.raises(OperationalError):
            async with storage.get_fileobj("k.txt"):
                pass


class TestGetRaisesNotFound:
    """Test for get() ObjectNotFoundError conversion via mock."""

    async def test_get_raises_object_not_found(self) -> None:
        """A 404 ClientError from get_object raises ObjectNotFoundError."""
        client = MagicMock()
        client.get_object = AsyncMock(
            side_effect=_client_error("NoSuchKey"),
        )
        storage = _storage_with_mock_client(client)

        with pytest.raises(ObjectNotFoundError):
            await storage.get("missing.txt", "/tmp/out.txt")  # noqa: S108
