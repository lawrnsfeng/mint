"""S3-compatible async file storage implementation of IFileStorage."""

import asyncio
import os
from collections.abc import Callable, Collection, Coroutine, Sequence
from contextvars import ContextVar
from functools import wraps
from hashlib import md5
from io import BytesIO
from pathlib import Path
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    Concatenate,
    Final,
    Self,
    cast,
)
from urllib.parse import quote

import aiofiles
from botocore.exceptions import ClientError
from sprout import ChildRef, Executor, FetchResult

from mint.fs.exc import (
    AmbiguousFolderPathError,
    CopySourceTooLargeError,
    FileAlreadyExistsError,
    FileStorageError,
    FolderAlreadyExistsError,
    InvalidArgumentsError,
    MoveCleanupError,
    ObjectNotFoundError,
    OperationalError,
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
from mint.logger import get_logger
from mint.utils.batch import Batch
from mint.utils.limiter import ConcurrencyLimiter

from .interface import IFileStorage
from .s3_structs import S3CredentialMode, S3SessionParams

if TYPE_CHECKING:
    from aiobotocore.session import ClientCreatorContext
    from types_aiobotocore_s3.client import S3Client
    from types_aiobotocore_s3.literals import BucketLocationConstraintType

logger = get_logger(__name__)
type Coro[T] = Coroutine[Any, Any, T]

_ERR_CLIENT_NOT_INITIALIZED: Final[str] = "Client is not initialized"
_S3_COPY_MAX_BYTES: Final[int] = 5 * 1024 * 1024 * 1024
_1MB: Final[int] = 1024 * 1024
_1KB: Final[int] = 1024
_1HOUR: Final[int] = 3600


class S3Storage(IFileStorage["S3Client"]):
    """Async S3-compatible file storage using aiobotocore.

    Implements IFileStorage[S3Client] for any S3-compatible endpoint
    (AWS S3, LocalStack, MinIO, etc.).

    Credential resolution order:
    1. Explicit key pair (access_key + secret_key)
    2. Environment variables AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
    3. ~/.aws/credentials shared profile
    4. IAM role / instance metadata

    Note:
        Each top-level call opens a new aiobotocore client/session
        (see _ensure_client). Pass max_concurrent_clients to bound
        concurrent session creation, especially against single-process
        endpoints like LocalStack.

    """

    AWSAccessKeyID: Final[str] = "AWS_ACCESS_KEY_ID"
    AWSSecretAccessKey: Final[str] = "AWS_SECRET_ACCESS_KEY"
    AWSSessionToken: Final[str] = "AWS_SESSION_TOKEN"
    S3Resource: Final = "s3"

    DefaultPresignedURLExpirationInSeconds: Final[int] = _1HOUR
    DefaultChunkSizeNoMultipartInBytes: Final[int] = 4 * _1KB
    ContentDispositionFormat: Final[str] = "attachment; filename*=UTF-8''{filename_utf8}"

    def __init__(  # noqa: PLR0913, PLR0917
        self,
        bucket_name: str,
        endpoint_url: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        session_token: str | None = None,
        profile_name: str = "default",
        region_name: str | None = None,
        *,
        max_concurrent_clients: int | None = None,
    ) -> None:
        """Initialize S3Storage.

        Args:
            bucket_name: Target S3 bucket name.
            endpoint_url: Custom S3 endpoint (LocalStack, MinIO, etc.).
                Leave None for AWS S3.
            access_key: AWS access key ID.
            secret_key: AWS secret access key.
            session_token: AWS session token (for temporary credentials).
            profile_name: AWS shared credentials profile name.
            region_name: AWS region name.
            max_concurrent_clients: Max concurrent client connections.
                None for unlimited. Each top-level call opens a new
                aiobotocore session (see _ensure_client), so unlimited
                concurrency can overwhelm lightweight/single-process
                endpoints (e.g. LocalStack).

        """
        self.bucket_name = bucket_name
        self.endpoint_url = endpoint_url
        self.aws_access_key_id = access_key
        self.aws_secret_access_key = secret_key
        self.aws_session_token = session_token
        self.profile_name = profile_name
        self.region_name = region_name

        self.mode, self.params = self._init_credential_mode()
        self._client_ctx: ContextVar[S3Client | None] = ContextVar(
            f"_s3_client_{id(self)}",
            default=None,
        )
        self._limiter: ConcurrencyLimiter | None = (
            ConcurrencyLimiter(max_concurrent_clients)
            if max_concurrent_clients is not None
            else None
        )

    @property
    def client(self) -> "S3Client":
        """Get the underlying S3Client.

        Returns:
            The initialized S3Client instance.

        Raises:
            RuntimeError: If client is not initialized.

        """
        client = self._client_ctx.get()
        if client is None:
            raise RuntimeError(_ERR_CLIENT_NOT_INITIALIZED)
        return client

    def clone(self) -> Self:
        """Return a new S3Storage instance with identical configuration.

        Returns:
            New S3Storage with the same parameters.

        """
        return self.__class__(
            self.bucket_name,
            endpoint_url=self.endpoint_url,
            access_key=self.aws_access_key_id,
            secret_key=self.aws_secret_access_key,
            session_token=self.aws_session_token,
            profile_name=self.profile_name,
            region_name=self.region_name,
            max_concurrent_clients=(self._limiter.max_concurrent if self._limiter else None),
        )

    def _init_credential_mode(
        self,
    ) -> tuple[S3CredentialMode, S3SessionParams]:
        """Determine credential mode and build session params.

        Returns:
            Tuple of (credential mode, session parameters dict).

        """
        params: S3SessionParams = {}

        if self.aws_access_key_id and self.aws_secret_access_key:
            params.update(
                {
                    "aws_access_key_id": self.aws_access_key_id,
                    "aws_secret_access_key": self.aws_secret_access_key,
                    "aws_session_token": self.aws_session_token,
                    "region_name": self.region_name,
                },
            )
            return S3CredentialMode.KeyPair, params

        self.aws_access_key_id = os.getenv(self.AWSAccessKeyID)
        self.aws_secret_access_key = os.getenv(self.AWSSecretAccessKey)
        self.aws_session_token = os.getenv(self.AWSSessionToken)
        if self.aws_access_key_id and self.aws_secret_access_key:
            params.update(
                {
                    "aws_access_key_id": self.aws_access_key_id,
                    "aws_secret_access_key": self.aws_secret_access_key,
                    "aws_session_token": self.aws_session_token,
                    "region_name": self.region_name,
                },
            )
            return S3CredentialMode.EnvVar, params

        if self._has_aws_profile(self.profile_name):
            params.update({"profile_name": self.profile_name})
            return S3CredentialMode.SharedCredentials, params

        return S3CredentialMode.IAMRole, params

    @staticmethod
    def _has_aws_profile(profile_name: str) -> bool:
        """Check if the named AWS profile exists in ~/.aws/credentials.

        Args:
            profile_name: AWS shared credentials profile name.

        Returns:
            True if the profile file exists, False otherwise.

        """
        credentials_path = Path.home() / ".aws" / "credentials"
        if not credentials_path.exists():
            return False
        content = credentials_path.read_text(encoding="utf-8")
        header = "[default]" if profile_name == "default" else f"[{profile_name}]"
        return header in content

    def _create_client(self) -> "ClientCreatorContext[S3Client]":
        """Create an aiobotocore async S3 client context manager.

        Returns:
            Async context manager yielding an S3Client.

        """
        from aiobotocore.session import get_session  # noqa: PLC0415

        session = get_session()
        return session.create_client(
            self.S3Resource,
            region_name=self.params.get("region_name"),
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.params.get("aws_access_key_id"),
            aws_secret_access_key=self.params.get("aws_secret_access_key"),
            aws_session_token=self.params.get("aws_session_token"),
        )

    @staticmethod
    def _auto_catch_native_exc[S, **P, R](
        func: Callable[Concatenate[S, P], Coro[R]],
    ) -> Callable[Concatenate[S, P], Coro[R]]:
        """Wrap async function to catch and convert native exceptions.

        Converts ValueError to InvalidArgumentsError and other exceptions
        to OperationalError, while allowing FileStorageError subclasses to
        pass through unchanged.

        Args:
            func: The async function to wrap.

        Returns:
            Wrapped function with exception handling.

        """

        @wraps(func)
        async def wrapper(
            self: S,
            /,
            *args: P.args,
            **kwargs: P.kwargs,
        ) -> R:
            try:
                return await func(self, *args, **kwargs)
            except ValueError as value_error:
                raise InvalidArgumentsError(
                    str(value_error),
                ) from value_error
            except FileStorageError:
                raise
            except Exception as exc:
                logger.exception("Unexpected error occurred")
                raise OperationalError(exc) from exc

        return wrapper

    @staticmethod
    def _ensure_client[S: "S3Storage", **P, RT](
        func: Callable[Concatenate[S, P], Coro[RT]],
    ) -> Callable[Concatenate[S, P], Coro[RT]]:
        """Wrap async function to ensure client is initialized.

        Uses ContextVar for per-coroutine client isolation: nested
        calls (e.g. copy() calling list()) reuse the client already
        bound in the current context, but each top-level call spawned
        via asyncio.gather/create_task starts in a fresh context and
        opens a brand-new aiobotocore session (real connection pool
        setup, not just a lightweight handle like Azure's SDK clients).

        Caution:
            Because each independent top-level call is this expensive,
            fanning out hundreds of concurrent top-level calls against
            a single-process dev server (e.g. LocalStack) can exhaust
            it and cause connection resets/hangs. Set
            max_concurrent_clients to bound how many of these sessions
            may be created at once via ConcurrencyLimiter.

        Args:
            func: The async function requiring a client.

        Returns:
            Wrapped function with client lifecycle management.

        """

        @wraps(func)
        async def wrapper(
            self: S,
            /,
            *args: P.args,
            **kwargs: P.kwargs,
        ) -> RT:
            if self._client_ctx.get() is not None:
                return await func(self, *args, **kwargs)

            async def _execute_with_client() -> RT:
                async with self._create_client() as client:
                    token = self._client_ctx.set(client)
                    try:
                        return await func(self, *args, **kwargs)
                    finally:
                        self._client_ctx.reset(token)

            if self._limiter is not None:
                async with self._limiter:
                    return await _execute_with_client()
            return await _execute_with_client()

        return wrapper

    @staticmethod
    def _is_not_found_error(exc: ClientError) -> bool:
        """Check if a ClientError represents a 404 Not Found.

        Args:
            exc: The ClientError to inspect.

        Returns:
            True if the error is a 404, False otherwise.

        """
        error = exc.response.get("Error", {})
        code = error.get("Code", "")
        return code in ("404", "NoSuchKey", "NotFound")

    @_auto_catch_native_exc
    @_ensure_client
    async def is_folder(self, path: str) -> bool:
        """Check if a path is a folder (has children objects).

        A path is considered a folder if objects exist with names that
        start with the path as a prefix but are not the path itself.

        Note:
            Does NOT raise ObjectNotFoundError for nonexistent paths.
            Returns False for both nonexistent paths and existing files.

        Examples:
            - is_folder("folder") -> True if "folder/file.txt" exists.
            - is_folder("file.txt") -> False if "file.txt" is an object.
            - is_folder("nonexistent") -> False (no exception).
            - is_folder("folder/") -> True (trailing slash stripped).

        Args:
            path: Path to check (trailing '/' is stripped).

        Returns:
            True if path is a folder prefix with children, False otherwise.

        """
        prefix = path.rstrip("/")
        response = await self.client.list_objects_v2(
            Bucket=self.bucket_name,
            Prefix=prefix,
            MaxKeys=2,
        )
        contents = response.get("Contents") or []
        return any(obj["Key"] != path for obj in contents)

    @_auto_catch_native_exc
    @_ensure_client
    async def get(
        self,
        path: str,
        save_to: str,
    ) -> None:
        """Download an S3 object to a local file.

        Creates parent directories if they don't exist.

        Args:
            path: S3 object key.
            save_to: Local file path to save downloaded content.

        Raises:
            ObjectNotFoundError: If the object does not exist.

        """
        try:
            response = await self.client.get_object(
                Bucket=self.bucket_name,
                Key=path,
            )
        except ClientError as exc:
            if self._is_not_found_error(exc):
                raise ObjectNotFoundError(path) from exc
            raise

        savepath = Path(save_to)
        savepath.parent.mkdir(parents=True, exist_ok=True)

        body = response["Body"]
        chunk_size = self.DefaultChunkSizeNoMultipartInBytes
        async with aiofiles.open(savepath, "wb") as f:
            while chunk := await body.read(chunk_size):
                await f.write(chunk)

    @_auto_catch_native_exc
    @_ensure_client
    async def save(
        self,
        path: str,
        ref: str | Path | IO[Any] | bytes,
        *,
        overwrite: bool = True,
    ) -> str:
        """Upload content to S3.

        Args:
            path: Destination object key.
            ref: Content to upload — file path, BytesIO, or bytes.
            overwrite: If False, raises FileAlreadyExistsError when object
                already exists. Note: not atomic (TOCTOU race possible).

        Returns:
            The object key of the uploaded object.

        Raises:
            TrailingSlashNotAllowedError: If path ends with '/'.
            UnsupportedRefTypeError: If ref type is unsupported.
            FolderAlreadyExistsError: If path is an existing folder prefix.
            FileAlreadyExistsError: If object exists and overwrite=False.

        """
        if path.endswith("/"):
            raise TrailingSlashNotAllowedError(path)

        if await self.is_folder(path):
            raise FolderAlreadyExistsError(path)

        if not overwrite:
            try:
                await self.client.head_object(
                    Bucket=self.bucket_name,
                    Key=path,
                )
                raise FileAlreadyExistsError(path)
            except ClientError as exc:
                if not self._is_not_found_error(exc):
                    raise

        match ref:
            case BytesIO() as buf:
                await self.client.put_object(
                    Bucket=self.bucket_name,
                    Key=path,
                    Body=buf.read() if buf.tell() == 0 else buf.getvalue(),
                )
            case bytes():
                await self.client.put_object(
                    Bucket=self.bucket_name,
                    Key=path,
                    Body=ref,
                )
            case Path() | str():
                async with aiofiles.open(ref, "rb") as upload_file:
                    await self.client.put_object(
                        Bucket=self.bucket_name,
                        Key=path,
                        Body=await upload_file.read(),
                    )
            case _:
                raise UnsupportedRefTypeError(path, type(ref).__name__)
        return path

    @_auto_catch_native_exc
    @_ensure_client
    async def copy(
        self,
        src: str,
        dst: str,
        *,
        recursive: bool = False,
    ) -> CopyResult:
        """Copy object(s) from src to dst.

        Path conventions:
            - src without trailing '/': single object copy.
            - src with trailing '/': folder/prefix copy.

        Note:
            Single object copy is limited to 5 GB. Objects larger than
            5 GB will raise CopySourceTooLargeError.

        Args:
            src: Source path. Add trailing '/' for folder operations.
            dst: Destination path.
            recursive: Whether to copy recursively (folder src only).

        Returns:
            CopyResult with success (destination keys) and failure lists.

        Raises:
            ObjectNotFoundError: If single-file src doesn't exist.
            AmbiguousFolderPathError: If src is detected as folder but
                lacks trailing '/'.
            CopySourceTooLargeError: If object exceeds 5 GB.

        """
        if not src.endswith("/"):
            if await self.is_folder(src) or await self.is_folder(dst):
                raise AmbiguousFolderPathError(src, dst)
            dst_key = dst.rstrip("/")
            try:
                head = await self.client.head_object(
                    Bucket=self.bucket_name,
                    Key=src,
                )
            except ClientError as exc:
                if self._is_not_found_error(exc):
                    raise ObjectNotFoundError(src) from exc
                raise
            size: int = head.get("ContentLength", 0)
            if size > _S3_COPY_MAX_BYTES:
                raise CopySourceTooLargeError(src, size, _S3_COPY_MAX_BYTES)
            await self.client.copy_object(
                Bucket=self.bucket_name,
                CopySource={"Bucket": self.bucket_name, "Key": src},
                Key=dst_key,
            )
            return CopyResult(success=[dst_key], failure=[])

        src_prefix = f"{src.rstrip('/')}/"
        dst_prefix = f"{dst.rstrip('/')}/"
        src_keys = [
            key
            for key in await self.list(src_prefix, recursive=recursive)
            if not key.endswith("/")
        ]

        success: list[str] = []
        failure: list[str] = []

        async def _copy_one(src_key: str) -> None:
            rel = src_key[len(src_prefix) :]
            dst_key = f"{dst_prefix}{rel}"
            try:
                await self.client.copy_object(
                    Bucket=self.bucket_name,
                    CopySource={"Bucket": self.bucket_name, "Key": src_key},
                    Key=dst_key,
                )
                success.append(dst_key)
            except Exception as exc:
                logger.exception("Error copying %s to %s", src_key, dst_key)
                failure.append(f"{dst_key}: {exc!s}")

        for batch in Batch.seq(list(src_keys)):
            await asyncio.gather(*[_copy_one(k) for k in batch])

        return CopyResult(success=success, failure=failure)

    @_auto_catch_native_exc
    @_ensure_client
    async def move(
        self,
        src: str,
        dst: str,
        *,
        recursive: bool = False,
    ) -> MoveResult:
        """Move object(s) from src to dst.

        Performs copy then remove. If copy has failures, raises
        MoveCleanupError and does NOT attempt removal.

        Args:
            src: Source path. Add trailing '/' for folder operations.
            dst: Destination path.
            recursive: Whether to move recursively (folder src only).

        Returns:
            MoveResult with copy and remove results.

        Raises:
            ObjectNotFoundError: If single-file src doesn't exist.
            MoveCleanupError: If copy had failures (removal not attempted).
            AmbiguousFolderPathError: If src is folder but lacks trailing
                '/'.

        """
        copy_result = await self.copy(src, dst, recursive=recursive)
        if copy_result.failure:
            raise MoveCleanupError(src=src, failure=copy_result.failure)
        remove_result = await self.remove(src, recursive=recursive)
        return MoveResult(
            copy=CopyResult(
                success=copy_result.success,
                failure=[],
            ),
            remove=remove_result,
        )

    @_auto_catch_native_exc
    @_ensure_client
    async def remove(
        self,
        path: str,
        *,
        recursive: bool = False,
    ) -> RemoveResult:
        """Remove an object or folder.

        Path conventions:
            - path without trailing '/': single object removal.
            - path with trailing '/': folder/prefix removal.

        Args:
            path: Path to remove. Add trailing '/' for folder operations.
            recursive: Whether to remove recursively (folder path only).

        Returns:
            RemoveResult with success and failure lists.

        Raises:
            ObjectNotFoundError: If single object doesn't exist.

        """
        if path.endswith("/"):
            keys = [
                key for key in await self.list(path, recursive=recursive) if not key.endswith("/")
            ]
            if not keys:
                return RemoveResult(success=[], failure=[])
            return await self.remove_many(keys, recursive=recursive)

        try:
            await self.client.head_object(
                Bucket=self.bucket_name,
                Key=path,
            )
        except ClientError as exc:
            if self._is_not_found_error(exc):
                raise ObjectNotFoundError(path) from exc
            raise
        await self.client.delete_object(Bucket=self.bucket_name, Key=path)
        return RemoveResult(success=[path], failure=[])

    @_auto_catch_native_exc
    @_ensure_client
    async def remove_many(
        self,
        paths: Sequence[str],
        *,
        recursive: bool = False,
    ) -> RemoveResult:
        """Remove multiple objects.

        For paths ending with '/' (folder prefixes), if recursive=True uses
        AsyncTreeExecutor to safely traverse and delete nested content.
        S3 delete_objects is limited to 1000 keys per call; batching is
        handled automatically.

        Args:
            paths: Sequence of object keys (or folder prefixes) to remove.
            recursive: Whether to remove folders recursively.

        Returns:
            RemoveResult with success and failure lists.

        """
        if not paths:
            return RemoveResult(success=[], failure=[])

        folders = [p for p in paths if p.endswith("/")]
        files = [p for p in paths if not p.endswith("/")]

        deleted: list[str] = []
        errors: list[str] = []

        if files:
            file_del, file_err = await self._delete_objects_batch(files)
            deleted.extend(file_del)
            errors.extend(file_err)

        if recursive and folders:
            for folder in folders:
                result = await self._remove_folder_tree(folder)
                deleted.extend(result.success)
                errors.extend(result.failure)
        elif folders:
            for folder in folders:
                children = await self.list(folder, recursive=False)
                if children:
                    child_del, child_err = await self._delete_objects_batch(
                        list(children),
                    )
                    deleted.extend(child_del)
                    errors.extend(child_err)

        return RemoveResult(success=deleted, failure=errors)

    async def _delete_objects_batch(
        self,
        keys: list[str],
    ) -> tuple[list[str], list[str]]:
        """Delete objects in batches of 1000 (S3 API limit).

        Args:
            keys: List of object keys to delete.

        Returns:
            Tuple of (deleted keys, error messages).

        """
        deleted: list[str] = []
        errors: list[str] = []

        for batch in Batch.seq(keys, size=1000):
            response = await self.client.delete_objects(
                Bucket=self.bucket_name,
                Delete={
                    "Objects": [{"Key": k} for k in batch],
                    "Quiet": False,
                },
            )
            deleted.extend(obj["Key"] for obj in response.get("Deleted", []))
            errors.extend(
                err.get("Message", err.get("Key", "unknown")) for err in response.get("Errors", [])
            )
        return deleted, errors

    async def _remove_folder_tree(self, folder: str) -> RemoveResult:
        """Remove a folder and all its children using sprout's Executor.

        Uses Executor to safely traverse nested folder structure
        without async recursion. Files at each node are deleted in bulk via
        delete_objects; subfolders become child refs for deeper traversal.

        Args:
            folder: Folder prefix (must end with '/').

        Returns:
            RemoveResult aggregating all deletions.

        """
        prefix = f"{folder.rstrip('/')}/"
        deleted: list[str] = []
        errors: list[str] = []

        async def fetcher(ref: ChildRef, _depth: int) -> FetchResult[str]:
            current_prefix = ref.id
            response = await self.client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=current_prefix,
                Delimiter="/",
            )
            file_keys: list[str] = [
                obj["Key"]
                for obj in (response.get("Contents") or [])
                if obj["Key"] != current_prefix
            ]
            subfolder_prefixes: list[str] = [
                cp["Prefix"] for cp in (response.get("CommonPrefixes") or [])
            ]
            if file_keys:
                del_keys, del_errors = await self._delete_objects_batch(
                    file_keys,
                )
                deleted.extend(del_keys)
                errors.extend(del_errors)
            return FetchResult(
                items=file_keys,
                child_refs=[ChildRef(id=p) for p in subfolder_prefixes],
            )

        executor: Executor[str] = Executor(fetcher)
        await executor.expand_unbounded(ChildRef(id=prefix))
        return RemoveResult(success=deleted, failure=errors)

    @_auto_catch_native_exc
    @_ensure_client
    async def stat(self, path: str) -> Stat:
        """Get statistics for an S3 object.

        Args:
            path: Object key.

        Returns:
            Stat with size and last_modified.

        Raises:
            ObjectNotFoundError: If the object does not exist.

        """
        try:
            head = await self.client.head_object(
                Bucket=self.bucket_name,
                Key=path,
            )
        except ClientError as exc:
            if self._is_not_found_error(exc):
                raise ObjectNotFoundError(path) from exc
            raise
        return Stat(
            last_modified=head["LastModified"],
            size=head["ContentLength"],
        )

    @_auto_catch_native_exc
    @_ensure_client
    async def list(
        self,
        path: str,
        *,
        recursive: bool = False,
    ) -> Collection[str]:
        """List object keys under a path prefix.

        Uses an iterative pagination loop (no async recursion) to handle
        buckets with more than 1000 objects.

        Args:
            path: Prefix to list. Use trailing '/' for folder contents.
            recursive: Whether to list recursively into sub-prefixes.

        Returns:
            Collection of object keys matching the prefix.

        """
        keys: list[str] = []
        token: str | None = None

        while True:
            kwargs: dict[str, Any] = {
                "Bucket": self.bucket_name,
                "Prefix": path,
            }
            if not recursive:
                kwargs["Delimiter"] = "/"
            if token is not None:
                kwargs["ContinuationToken"] = token

            response = await self.client.list_objects_v2(**kwargs)
            keys.extend(obj["Key"] for obj in (response.get("Contents") or []))
            if not recursive:
                keys.extend(cp["Prefix"] for cp in (response.get("CommonPrefixes") or []))
            token = response.get("NextContinuationToken")
            if token is None:
                break

        return keys

    @_auto_catch_native_exc
    @_ensure_client
    async def list_detailed(  # noqa: C901
        self,
        path: str,
        *,
        show_stats: bool = False,
        show_info: bool = False,
        recursive: bool = False,
    ) -> Sequence[ListItem]:
        """List objects with detailed information.

        Note:
            show_stats=True requires an extra head_object call per object
            to retrieve content_type and metadata, which S3 list_objects_v2
            does not return inline. These calls are batched via asyncio.gather.

        Args:
            path: Prefix to list. Use trailing '/' for folder contents.
            show_stats: Include content_type and metadata (extra API calls).
            show_info: Include bucket, last_modified, etag, size,
                storage_class.
            recursive: Whether to list recursively into sub-prefixes.

        Returns:
            Sequence of ListItem objects with requested detail level.

        """
        objs: list[ListItem] = []
        token: str | None = None

        while True:
            kwargs: dict[str, Any] = {
                "Bucket": self.bucket_name,
                "Prefix": path,
                "FetchOwner": show_stats,
            }
            if not recursive:
                kwargs["Delimiter"] = "/"
            if token is not None:
                kwargs["ContinuationToken"] = token

            response = await self.client.list_objects_v2(**kwargs)
            contents = response.get("Contents") or []
            common_prefixes = response.get("CommonPrefixes") or []

            for obj_data in contents:
                item = ListItem(object_name=obj_data["Key"])
                if show_info:
                    item.bucket_name = self.bucket_name
                    item.last_modified = obj_data.get("LastModified")
                    item.etag = obj_data.get("ETag")
                    item.size = obj_data.get("Size")
                    item.storage_class = obj_data.get("StorageClass")
                if show_stats and obj_data.get("Owner"):
                    item.owner_id = obj_data["Owner"].get("ID")
                    item.owner_name = obj_data["Owner"].get("DisplayName")
                objs.append(item)

            for prefix_data in common_prefixes:
                bucket = self.bucket_name if show_info else None
                objs.extend(
                    [
                        ListItem(
                            object_name=prefix_data["Prefix"],
                            bucket_name=bucket,
                        ),
                    ],
                )

            token = response.get("NextContinuationToken")
            if token is None:
                break

        if show_stats and objs:
            file_objs = [o for o in objs if not o.object_name.endswith("/")]
            for batch in Batch.seq(file_objs):
                heads = await asyncio.gather(
                    *[
                        self.client.head_object(
                            Bucket=self.bucket_name,
                            Key=o.object_name,
                        )
                        for o in batch
                    ],
                    return_exceptions=True,
                )
                for item, head in zip(batch, heads, strict=False):
                    if isinstance(head, BaseException):
                        logger.warning(
                            "head_object failed for %s: %s",
                            item.object_name,
                            head,
                        )
                        continue
                    item.content_type = head.get("ContentType")
                    item.metadata = head.get("Metadata") or {}

        return objs

    @_auto_catch_native_exc
    @_ensure_client
    async def gen_presigned_url(
        self,
        path: str,
        *,
        expiration_in_seconds: int | None = None,
        file_name: str | None = None,
    ) -> str:
        """Generate a presigned URL for downloading an S3 object.

        Args:
            path: Object key.
            expiration_in_seconds: URL expiration time in seconds.
                Defaults to DefaultPresignedURLExpirationInSeconds (1 hour).
            file_name: Override filename in Content-Disposition header.

        Returns:
            Presigned URL string.

        Raises:
            ObjectNotFoundError: If the object does not exist.

        """
        try:
            await self.client.head_object(
                Bucket=self.bucket_name,
                Key=path,
            )
        except ClientError as exc:
            if self._is_not_found_error(exc):
                raise ObjectNotFoundError(path) from exc
            raise

        content_disposition = self.ContentDispositionFormat.format(
            filename_utf8=quote(
                file_name or Path(path).name,
                encoding="utf-8",
            ),
        )
        return await self.client.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": self.bucket_name,
                "Key": path,
                "ResponseContentDisposition": content_disposition,
            },
            ExpiresIn=(expiration_in_seconds or self.DefaultPresignedURLExpirationInSeconds),
        )

    @_auto_catch_native_exc
    @_ensure_client
    async def save_many(
        self,
        objects: Sequence[tuple[str, str | Path | IO[Any] | bytes]],
        batch_size: int | None = None,
    ) -> Sequence[tuple[str, BaseException | None]]:
        """Upload multiple objects in batches.

        Args:
            objects: Sequence of (key, content) tuples to upload.
            batch_size: Concurrent uploads per batch.
                Defaults to Batch.DEFAULT_SIZE.

        Returns:
            Sequence of (key, exception or None) for each upload.

        """
        results: list[str | BaseException] = []
        for batch in Batch.seq(
            list(objects),
            size=batch_size or Batch.DEFAULT_SIZE,
        ):
            tasks: list[Coro[str]] = [self.save(path, ref) for path, ref in batch]
            results.extend(
                await asyncio.gather(*tasks, return_exceptions=True),
            )
        return [
            (path, r if isinstance(r, BaseException) else None)
            for (path, _), r in zip(objects, results, strict=False)
        ]

    @_auto_catch_native_exc
    @_ensure_client
    async def ensure_bucket(self) -> None:
        """Create the configured bucket if it does not already exist.

        Raises:
            OperationalError: If bucket creation fails for any reason other
                than the bucket already existing.

        """
        try:
            if self.region_name and self.region_name != "us-east-1":
                await self.client.create_bucket(
                    Bucket=self.bucket_name,
                    CreateBucketConfiguration={
                        "LocationConstraint": cast(
                            "BucketLocationConstraintType",
                            self.region_name,
                        ),
                    },
                )
            else:
                await self.client.create_bucket(Bucket=self.bucket_name)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code not in (
                "BucketAlreadyOwnedByYou",
                "BucketAlreadyExists",
            ):
                raise

    def get_fileobj(
        self,
        path: str,
        chunk_size: int | None = None,
    ) -> "_GetObjectContextManager":
        """Return an async context manager that yields a BytesIO of the object.

        Args:
            path: S3 object key to download.
            chunk_size: Chunk size for reading body in bytes.

        Returns:
            Async context manager yielding BytesIO with object content.

        """
        return _GetObjectContextManager(self, path, chunk_size=chunk_size)

    @_auto_catch_native_exc
    @_ensure_client
    async def _fetch_object_bytes(
        self,
        path: str,
        chunk_size: int,
    ) -> BytesIO:
        """Download an object into an in-memory buffer.

        Args:
            path: S3 object key to download.
            chunk_size: Chunk size for streaming body in bytes.

        Returns:
            BytesIO positioned at the start, containing the full object.

        Raises:
            ObjectNotFoundError: If the object does not exist.

        """
        try:
            response = await self.client.get_object(
                Bucket=self.bucket_name,
                Key=path,
            )
        except ClientError as exc:
            if self._is_not_found_error(exc):
                raise ObjectNotFoundError(path) from exc
            raise
        fileobj = BytesIO()
        body = response["Body"]
        while chunk := await body.read(chunk_size):
            fileobj.write(chunk)
        fileobj.seek(0)
        return fileobj

    async def calculate_etag(
        self,
        filepath: Path | str,
        *,
        use_multipart: bool = False,
        chunk_size: int = DefaultChunkSizeNoMultipartInBytes,
    ) -> str:
        """Calculate the S3 ETag for a local file.

        For single-part uploads, ETag = MD5 hex of file content.
        For multipart uploads, ETag = MD5 of MD5 digests + "-" + part count.

        Args:
            filepath: Path to the local file.
            use_multipart: Whether to compute multipart ETag.
            chunk_size: Chunk size in bytes for multipart calculation.

        Returns:
            ETag string (hex for single, "hex-N" for multipart).

        """
        fpath = Path(filepath)
        if not use_multipart:
            hash_md5 = md5()  # noqa: S324
            async with aiofiles.open(fpath, "rb") as f:
                while True:
                    chunk = await f.read(chunk_size)
                    if not chunk:
                        break
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()

        digests: list[bytes] = []
        async with aiofiles.open(fpath, "rb") as f:
            while True:
                chunk = await f.read(chunk_size)
                if not chunk:
                    break
                digests.append(md5(chunk).digest())  # noqa: S324
        combined = md5(b"".join(digests)).hexdigest()  # noqa: S324
        return f"{combined}-{len(digests)}"


class _GetObjectContextManager:
    """Async context manager that downloads an S3 object into a BytesIO.

    Attributes:
        DefaultChunkSize: Default read chunk size in bytes.

    """

    DefaultChunkSize: Final[int] = 1024

    def __init__(
        self,
        storage: S3Storage,
        key: str,
        chunk_size: int | None = None,
    ) -> None:
        """Initialize the context manager.

        Args:
            storage: The S3Storage instance to use.
            key: S3 object key to download.
            chunk_size: Chunk size for streaming body in bytes.

        """
        self.storage = storage
        self.key = key
        self.fileobj = BytesIO()
        self.chunk_size = chunk_size or self.DefaultChunkSize

    async def __aenter__(self) -> BytesIO:
        """Download the object and return a BytesIO positioned at start.

        Returns:
            BytesIO containing the full object content.

        Raises:
            ObjectNotFoundError: If the object does not exist.

        """
        self.fileobj = await self.storage._fetch_object_bytes(  # noqa: SLF001
            self.key,
            self.chunk_size,
        )
        return self.fileobj

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """Close the BytesIO buffer.

        Args:
            exc_type: Exception type if raised, else None.
            exc_val: Exception instance if raised, else None.
            exc_tb: Traceback if raised, else None.

        """
        self.fileobj.close()
