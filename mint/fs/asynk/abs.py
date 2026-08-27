"""Azure Blob Storage implementation of IFileStorage interface."""

import asyncio
import os
from collections.abc import Callable, Collection, Coroutine, Sequence
from contextvars import ContextVar
from datetime import UTC, datetime, timedelta
from functools import wraps
from io import BytesIO
from pathlib import Path
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    Concatenate,
    Final,
    cast,
)
from urllib.parse import quote

import aiofiles
from azure.core.exceptions import (
    HttpResponseError,
    ResourceNotFoundError,
    ServiceRequestError,
)
from azure.identity.aio import ClientSecretCredential, DefaultAzureCredential
from azure.storage.blob import (
    BlobSasPermissions,
    generate_blob_sas,
)
from azure.storage.blob.aio import (
    BlobServiceClient,
    ContainerClient,
)

from mint.fs.exc import (
    AmbiguousFolderPathError,
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
from .structs import AzureCredentialMode, AzureSessionParams

if TYPE_CHECKING:
    from azure.core.async_paging import AsyncItemPaged
    from azure.storage.blob._models import BlobProperties

logger = get_logger(__name__)
type Coro[T] = Coroutine[Any, Any, T]

_ERR_CLIENT_NOT_INITIALIZED = "Client is not initialized"


class AzureBlobStorage(IFileStorage[BlobServiceClient]):
    """Azure Blob Storage wrapper that ensures client for each operation.

    This class provides async file storage operations for Azure Blob Storage,
    with automatic client lifecycle management and exception handling.

    See more:
    https://learn.microsoft.com/en-us/azure/storage/blobs/
    storage-blob-python-get-started

    """

    AzureStorageAccessKey: Final[str] = "AZURE_STORAGE_ACCESS_KEY"
    AzureStorageConnectionString: Final[str] = "AZURE_STORAGE_CONNECTION_STRING"
    DefaultPresignedURLExpirationInSeconds: Final[int] = 60 * 60
    TmplBlobSAS: Final[str] = (
        "https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
    )
    TmplAccountURL: Final[str] = "https://{storage_account_name}.blob.core.windows.net"
    SessionRetries: Final[int] = 3

    ContentDispositionFormat: Final[str] = "attachment; filename*=UTF-8''{filename_utf8}"

    def __init__(  # noqa: PLR0913, PLR0917
        self,
        container_name: str,
        storage_account_name: str,
        client_secret: str | None = None,
        shared_access_key: str | None = None,
        connection_string: str | None = None,
        sas_token: str | None = None,
        tenant_id: str | None = None,
        client_id: str | None = None,
        *,
        max_concurrent_clients: int | None = None,
    ) -> None:
        """Initialize AzureBlobStorage with credentials.

        Credentials are resolved in the following priority order:
        1. SAS token
        2. Shared access key
        3. Connection string
        4. Client secret (with tenant_id and client_id)
        5. Environment variable AZURE_STORAGE_ACCESS_KEY
        6. Environment variable AZURE_STORAGE_CONNECTION_STRING
        7. DefaultAzureCredential

        Args:
            container_name: Name of the blob container.
            storage_account_name: Azure storage account name.
            client_secret: Client secret for service principal auth.
            shared_access_key: Storage account shared access key.
            connection_string: Full connection string.
            sas_token: Shared access signature token.
            tenant_id: Azure AD tenant ID for service principal.
            client_id: Azure AD client/application ID.
            max_concurrent_clients: Maximum number of concurrent clients.
                Use None for unlimited (default).

        """
        self.container_name = container_name
        self.storage_account_name = storage_account_name
        self.sas_token = sas_token
        self.shared_access_key = shared_access_key
        self.connection_string = connection_string
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret

        self.mode, self.params = self._init_credential_mode()
        self._client_ctx: ContextVar[BlobServiceClient | None] = ContextVar(
            f"_abs_client_{id(self)}",
            default=None,
        )
        self._limiter: ConcurrencyLimiter | None = (
            ConcurrencyLimiter(max_concurrent_clients)
            if max_concurrent_clients is not None
            else None
        )

    @property
    def client(self) -> BlobServiceClient:
        """Get the underlying BlobServiceClient.

        Returns:
            The initialized BlobServiceClient instance.

        Raises:
            RuntimeError: If client is not initialized.

        """
        client = self._client_ctx.get()
        if client is None:
            raise RuntimeError(_ERR_CLIENT_NOT_INITIALIZED)
        return client

    @property
    def container(self) -> ContainerClient:
        """Get the ContainerClient for the configured container.

        Returns:
            ContainerClient for the target container.

        """
        return self.client.get_container_client(self.container_name)

    @staticmethod
    def _auto_catch_native_exc[S, **P, R](
        func: Callable[Concatenate[S, P], Coro[R]],
    ) -> Callable[Concatenate[S, P], Coro[R]]:
        """Wrap async function to catch and convert native exceptions.

        Convert ValueError to InvalidArgumentsError and other exceptions
        to OperationalError, while allowing FileStorageError to pass
        through unchanged.

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
                raise InvalidArgumentsError(str(value_error)) from value_error
            except FileStorageError:
                raise
            except Exception as exc:
                logger.exception("Unexpected error occurred")
                raise OperationalError(exc) from exc

        return wrapper

    def _init_credential_mode(  # noqa: PLR0911
        self,
    ) -> tuple[AzureCredentialMode, AzureSessionParams]:
        """Determine and initialize the credential mode.

        Returns:
            Tuple of (credential mode, session parameters).

        """
        params: AzureSessionParams = {}
        if self.sas_token is not None:
            params.update(
                {
                    "sas_token": self.sas_token,
                },
            )
            return AzureCredentialMode.SharedAccessSignature, params

        if self.shared_access_key is not None:
            params.update(
                {
                    "shared_access_key": self.shared_access_key,
                },
            )

            return AzureCredentialMode.SharedAccessKey, params

        if self.connection_string is not None:
            params.update(
                {
                    "connection_string": self.connection_string,
                },
            )
            return AzureCredentialMode.ConnectionString, params

        if all(
            field is not None
            for field in (
                self.tenant_id,
                self.client_id,
                self.client_secret,
            )
        ):
            params.update(
                {
                    "tenant_id": self.tenant_id,
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
            )
            return AzureCredentialMode.ClientSecret, params

        self.shared_access_key = os.getenv(self.AzureStorageAccessKey)
        if self.shared_access_key is not None:
            params.update(
                {
                    "shared_access_key": self.shared_access_key,
                },
            )

            return AzureCredentialMode.EnvVarSharedAccessKey, params

        self.connection_string = os.getenv(self.AzureStorageConnectionString)
        if self.connection_string is not None:
            params.update(
                {
                    "connection_string": self.connection_string,
                },
            )

            return AzureCredentialMode.EnvVarConnectionString, params

        return AzureCredentialMode.Default, params

    @property
    def account_url(self) -> str:
        """Get the Azure storage account URL.

        Returns:
            The formatted account URL.

        """
        return self.TmplAccountURL.format(
            storage_account_name=self.storage_account_name,
        )

    def _create_client(self) -> BlobServiceClient:
        """Create a BlobServiceClient based on the credential mode.

        Returns:
            Configured BlobServiceClient instance.

        Raises:
            InvalidArgumentsError: If credential mode is unsupported.

        """
        match self.mode:
            case AzureCredentialMode.SharedAccessSignature:
                return BlobServiceClient(
                    self.account_url,
                    credential=self.sas_token,
                )
            case AzureCredentialMode.ClientSecret:
                return BlobServiceClient(
                    self.account_url,
                    credential=ClientSecretCredential(
                        cast("str", self.tenant_id),
                        cast("str", self.client_id),
                        cast("str", self.client_secret),
                    ),
                )
            case AzureCredentialMode.ConnectionString | AzureCredentialMode.EnvVarConnectionString:
                return BlobServiceClient.from_connection_string(
                    cast("str", self.connection_string),
                )
            case AzureCredentialMode.SharedAccessKey | AzureCredentialMode.EnvVarSharedAccessKey:
                return BlobServiceClient(
                    self.account_url,
                    credential=self.shared_access_key,
                )
            case AzureCredentialMode.Default:
                return BlobServiceClient(
                    self.account_url,
                    credential=DefaultAzureCredential(),
                )
            case _:
                raise InvalidArgumentsError(detail=f"mode = {self.mode}")

    @staticmethod
    def _ensure_client[S: "AzureBlobStorage", **P, RT](
        func: Callable[Concatenate[S, P], Coro[RT]],
    ) -> Callable[Concatenate[S, P], Coro[RT]]:
        """Wrap async function to ensure client is initialized.

        Create and manage the client lifecycle, initializing it before
        the operation and cleaning up afterward. Uses ContextVar for
        async-safe, per-coroutine client isolation to avoid race
        conditions when multiple coroutines share the same instance.

        When max_concurrent_clients is set, limits the number of
        concurrent client connections using ConcurrencyLimiter.

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

    @_auto_catch_native_exc
    @_ensure_client
    async def get(
        self,
        path: str,
        save_to: str,
    ) -> None:
        """Download a blob to a local file.

        Creates parent directories if they don't exist.
        Supports downloading empty files (size=0).

        Args:
            path: Path of the blob in the container.
            save_to: Local file path to save the downloaded content.

        Raises:
            ObjectNotFoundError: If the blob does not exist.

        """
        blob = self.container.get_blob_client(blob=path)
        if not (await blob.exists()):
            raise ObjectNotFoundError(path)
        blob_obj = await blob.download_blob()

        savepath = Path(save_to)
        savepath.parent.mkdir(parents=True, exist_ok=True)

        async with aiofiles.open(savepath, "wb") as downloaded_file:
            await downloaded_file.write(await blob_obj.readall())

    @_auto_catch_native_exc
    @_ensure_client
    async def save(
        self,
        path: str,
        ref: str | Path | IO[Any] | bytes,
        *,
        overwrite: bool = True,
    ) -> str:
        """Upload content to a blob.

        Supports uploading empty content (empty bytes, BytesIO, or file).

        Args:
            path: Destination path in the container.
            ref: Content to upload - can be a file path, BytesIO, or bytes.
            overwrite: Whether to overwrite existing blob.

        Returns:
            The path of the uploaded blob.

        Raises:
            FileAlreadyExistsError: If blob exists and overwrite is False.
            TrailingSlashNotAllowedError: If path ends with '/'.
            UnsupportedRefTypeError: If ref type is unsupported.
            FolderAlreadyExistsError: If path is an existing folder prefix.

        """
        if path.endswith("/"):
            raise TrailingSlashNotAllowedError(path)

        if await self.is_folder(path):
            raise FolderAlreadyExistsError(path)

        blob = self.container.get_blob_client(blob=path)

        if not overwrite and (await blob.exists()):
            raise FileAlreadyExistsError(path)

        match ref:
            case BytesIO():
                await blob.upload_blob(ref, overwrite=overwrite)
            case bytes():
                with BytesIO(ref) as ref_bytesio:
                    await blob.upload_blob(ref_bytesio, overwrite=overwrite)
            case Path() | str():
                async with aiofiles.open(ref, "rb") as upload_file:
                    await blob.upload_blob(
                        await upload_file.read(),
                        overwrite=overwrite,
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
        """Copy blob(s) from source to destination.

        Path conventions:
            - src without trailing '/': single file copy.
            - src with trailing '/': folder/prefix copy.

        Examples:
            - copy("file.txt", "backup.txt"): copies single file.
            - copy("folder/", "backup/", recursive=True): copies all files
              under folder/ to backup/.
            - copy("folder/", "backup/"): copies only immediate children
              (non-recursive).

        Edge cases:
            - Single file not found: raises ObjectNotFoundError.
            - Empty folder (no children): returns CopyResult with empty lists.
            - Partial failures: CopyResult.failure lists failed paths with
              error messages.
            - src is folder but missing '/': raises AmbiguousFolderPathError.

        Args:
            src: Source path. Add trailing '/' for folder operations.
            dst: Destination path.
            recursive: Whether to copy recursively (only for folder src).

        Returns:
            CopyResult with success (destination paths) and failure lists.

        Raises:
            ObjectNotFoundError: If single file src doesn't exist.
            AmbiguousFolderPathError: If src or dst is detected as folder
                but src doesn't have trailing '/'.

        """
        if not src.endswith("/"):
            if await self.is_folder(src) or await self.is_folder(dst):
                raise AmbiguousFolderPathError(src, dst)
            dst = dst.rstrip("/")
            src_blob = self.container.get_blob_client(blob=src)
            if not await src_blob.exists():
                raise ObjectNotFoundError(src)
            dst_blob = self.container.get_blob_client(blob=dst)
            await dst_blob.start_copy_from_url(src_blob.url)
            return CopyResult(success=[dst], failure=[])

        src = f"{src.rstrip('/')}/"
        src_blobs = [
            blob for blob in await self.list(src, recursive=recursive) if not blob.endswith("/")
        ]

        paths: list[Path] = []
        tasks: list[asyncio.Task[dict[str, str | datetime]]] = []
        for blob in src_blobs:
            src_blob = self.container.get_blob_client(blob=blob)
            relpath = Path(blob).relative_to(Path(src))
            dst_path = Path(dst) / relpath
            dst_blob = self.container.get_blob_client(str(dst_path))
            paths.extend([dst_path])
            tasks.append(
                asyncio.create_task(
                    dst_blob.start_copy_from_url(src_blob.url),  # type: ignore[arg-type]
                ),
            )

        results = await asyncio.gather(*tasks, return_exceptions=True)

        success: list[str] = []
        failure: list[str] = []

        for path, result_or_exc in zip(paths, results, strict=False):
            if isinstance(result_or_exc, Exception):
                failure.extend([f"{path}: {result_or_exc!s}"])
            else:
                success.extend([str(path)])
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
        """Move blob(s) from source to destination.

        Performs a copy followed by removal of the source.

        Path conventions:
            - src without trailing '/': single file move.
            - src with trailing '/': folder/prefix move.

        Examples:
            - move("file.txt", "backup.txt"): moves single file.
            - move("folder/", "backup/", recursive=True): moves all files
              under folder/ to backup/.

        Edge cases:
            - Single file not found: raises ObjectNotFoundError.
            - Empty folder: returns MoveResult with empty success lists.
            - Partial copy failures: raises MoveCleanupError (no removal
              attempted to prevent data loss).
            - src is folder but missing '/': raises AmbiguousFolderPathError.

        Args:
            src: Source path. Add trailing '/' for folder operations.
            dst: Destination path.
            recursive: Whether to move recursively (only for folder src).

        Returns:
            MoveResult with copy and remove results.

        Raises:
            ObjectNotFoundError: If single file src doesn't exist.
            MoveCleanupError: If copy had failures (removal not attempted).
            AmbiguousFolderPathError: If src or dst is detected as folder
                but src doesn't have trailing '/'.

        """
        copy_result = await self.copy(src, dst, recursive=recursive)
        if len(copy_result.failure) > 0:
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
        """Remove a blob or folder.

        Path conventions:
            - path without trailing '/': single file removal.
            - path with trailing '/': folder/prefix removal.

        Examples:
            - remove("file.txt"): removes single file.
            - remove("folder/", recursive=True): removes all files under
              folder/.
            - remove("folder/"): removes only immediate children
              (non-recursive).

        Edge cases:
            - Single file not found: raises ObjectNotFoundError.
            - Empty folder: returns RemoveResult with empty lists.
            - Partial failures: RemoveResult.failure lists failed paths.

        Args:
            path: Path to remove. Add trailing '/' for folder operations.
            recursive: Whether to remove recursively (only for folder path).

        Returns:
            RemoveResult with success and failure lists.

        Raises:
            ObjectNotFoundError: If single file doesn't exist.

        """
        if path.endswith("/"):
            objs = [
                obj for obj in await self.list(path, recursive=recursive) if not obj.endswith("/")
            ]
            if len(objs) == 0:
                return RemoveResult(success=[], failure=[])
            return await self.remove_many(objs, recursive=recursive)

        blob = self.container.get_blob_client(path)
        if not (await blob.exists()):
            raise ObjectNotFoundError(path)
        await blob.delete_blob()
        return RemoveResult(success=[path], failure=[])

    async def _remove_many_files(
        self,
        filepaths: Sequence[str],
    ) -> list[BaseException | None]:
        """Remove multiple files in batches.

        Args:
            filepaths: List of file paths to remove.

        Returns:
            List of results (None for success, exception for failure).

        """
        results: list[BaseException | None] = []
        for batch in Batch.seq(filepaths):
            results.extend(
                await asyncio.gather(
                    *[
                        asyncio.create_task(
                            self.container.get_blob_client(  # type: ignore[arg-type]
                                filepath,
                            ).delete_blob(),
                        )
                        for filepath in batch
                    ],
                    return_exceptions=True,
                ),
            )
        return results

    @_auto_catch_native_exc
    @_ensure_client
    async def remove_many(  # noqa: PLR0912, C901
        self,
        paths: Collection[str],
        *,
        recursive: bool = False,
    ) -> RemoveResult:
        """Remove multiple blobs.

        Args:
            paths: Collection of paths to remove.
            recursive: Whether to remove folders recursively.

        Returns:
            RemoveResult with success and failure lists.

        """
        if len(paths) == 0:
            return RemoveResult(success=[], failure=[])

        folders: list[str] = [path for path in paths if path.endswith("/")]
        files: list[str] = [path for path in paths if path not in folders]

        existing_results: list[bool] = []
        for batch in Batch.seq(files):
            existing_results.extend(
                await asyncio.gather(
                    *[
                        asyncio.create_task(
                            self.container.get_blob_client(path).exists(),  # type: ignore[arg-type]
                        )
                        for path in batch
                    ],
                ),
            )

        existing_files = []
        notfound: list[str] = []

        for path, is_existing in zip(files, existing_results, strict=True):
            if is_existing:
                existing_files.append(path)
                continue
            notfound.append(f"Blob {path} not found")

        deleted: list[str] = []
        errors: list[str] = []

        results = await self._remove_many_files(existing_files)

        for filepath, result_or_exc in zip(
            existing_files,
            results,
            strict=True,
        ):
            match result_or_exc:
                case ResourceNotFoundError():
                    errors.append(f"Blob {filepath} not found")
                case HttpResponseError():
                    errors.append(
                        f"Delete blob {filepath} failed due to server error",
                    )
                case ServiceRequestError():
                    errors.append(
                        f"Network error when deleting blob {filepath}",
                    )
                case None:
                    deleted.append(filepath)
                case _:
                    errors.append(
                        f"Unexpected error deleting blob {filepath}: {result_or_exc}",
                    )

        if recursive:
            for folderpath in folders:
                children_paths = await self.list(folderpath, recursive=True)
                if len(children_paths) == 0:
                    continue
                child_result = await self.remove_many(
                    children_paths,
                    recursive=True,
                )
                deleted.extend(child_result.success)
                errors.extend(child_result.failure)

        return RemoveResult(success=deleted, failure=[*notfound, *errors])

    @_auto_catch_native_exc
    @_ensure_client
    async def stat(self, path: str) -> Stat:
        """Get statistics for a blob.

        Edge cases:
            - Empty files: returns Stat with size=0 and valid last_modified.

        Args:
            path: Path of the blob.

        Returns:
            Stat object with size and last_modified.

        Raises:
            ObjectNotFoundError: If the blob does not exist.

        """
        blob = self.container.get_blob_client(path)
        if not (await blob.exists()):
            raise ObjectNotFoundError(path)
        props = await blob.get_blob_properties()
        return Stat(
            last_modified=props.last_modified,
            size=props.size,
        )

    @_auto_catch_native_exc
    @_ensure_client
    async def list(
        self,
        path: str,
        *,
        recursive: bool = False,
    ) -> Collection[str]:
        """List blobs under a path prefix.

        The path is used as a prefix filter. Typically use trailing '/'
        to list folder contents.

        Examples:
            - list("folder/"): lists immediate children in folder/.
            - list("folder/", recursive=True): lists all nested blobs.
            - list("prefix"): lists blobs starting with "prefix".

        Edge cases:
            - Nonexistent prefix: returns empty collection (no exception).
            - Non-recursive: excludes blobs in subdirectories.

        Args:
            path: Path prefix to list (use trailing '/' for folders).
            recursive: Whether to list recursively into subfolders.

        Returns:
            Collection of blob names matching the prefix.

        """
        blob_props_list: AsyncItemPaged[BlobProperties] = self.container.list_blobs(
            name_starts_with=path,
        )
        results: list[str] = []
        async for blob_props in blob_props_list:
            if recursive:
                results.append(blob_props.name)
                continue
            suffix = blob_props.name[len(path) :]
            if "/" not in suffix:
                results.append(blob_props.name)
        return results

    @_auto_catch_native_exc
    @_ensure_client
    async def list_detailed(
        self,
        path: str,
        *,
        show_stats: bool = False,
        show_info: bool = False,
        recursive: bool = False,
    ) -> Sequence[ListItem]:
        """List blobs with detailed information.

        The path is used as a prefix filter. Typically use trailing '/'
        to list folder contents.

        Examples:
            - list_detailed("folder/", show_info=True): lists immediate
              children with size/modified info.
            - list_detailed("folder/", recursive=True): lists all nested
              blobs.

        Edge cases:
            - Non-recursive: excludes blobs in subdirectories.
            - Recursive: includes all nested blobs under the prefix.
            - Nonexistent prefix: returns empty collection (no exception).

        Args:
            path: Path prefix to list (use trailing '/' for folders).
            show_stats: Include content_type and metadata.
            show_info: Include bucket, modified, etag, and size.
            recursive: Whether to list recursively into subfolders.

        Returns:
            Sequence of ListItem objects with blob details.

        """
        blob_props_list: AsyncItemPaged[BlobProperties] = self.container.list_blobs(
            name_starts_with=path,
        )

        objs: list[ListItem] = []
        async for blob_props_page in blob_props_list.by_page():
            async for blob_props in blob_props_page:
                if not recursive:
                    suffix = blob_props.name[len(path) :]
                    if "/" in suffix:
                        continue
                obj = ListItem(
                    object_name=blob_props.name,
                )
                if show_info:
                    obj.bucket_name = blob_props.container
                    obj.last_modified = blob_props.last_modified
                    obj.etag = blob_props.etag
                    obj.size = blob_props.size

                if show_stats:
                    obj.content_type = blob_props.blob_type
                    obj.metadata = blob_props.metadata
                objs.append(obj)
        return objs

    @_auto_catch_native_exc
    @_ensure_client
    async def gen_presigned_url(
        self,
        path: str,
        *,
        expiration_in_seconds: int | None = None,
        file_name: str | None = None,
        cache_control: str | None = "no-cache",
    ) -> str:
        """Generate a presigned URL for downloading a blob.

        Args:
            path: Path of the blob.
            expiration_in_seconds: URL expiration time in seconds.
            file_name: Override filename in Content-Disposition header.
            cache_control: Cache-Control header value.

        Returns:
            Presigned URL string.

        Raises:
            ObjectNotFoundError: If the blob does not exist.

        """
        blob = self.container.get_blob_client(path)
        if not (await blob.exists()):
            raise ObjectNotFoundError(path)
        user_delegation_key = None
        account_key = None
        if hasattr(self.client.credential, "get_token"):
            # https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blob-user-delegation-sas-create-python?tabs=container#create-a-user-delegation-sas
            # https://stackoverflow.com/questions/73023464/how-to-create-azure-storage-sas-token-using-defaultazurecredential-class
            user_delegation_key = await self.client.get_user_delegation_key(
                key_start_time=datetime.now(tz=UTC),
                key_expiry_time=datetime.now(tz=UTC)
                + timedelta(
                    seconds=(expiration_in_seconds or self.DefaultPresignedURLExpirationInSeconds),
                ),
            )
        else:
            account_key = self.client.credential.account_key
        blob_name = blob.blob_name
        sas_token = generate_blob_sas(
            account_name=self.storage_account_name,
            container_name=self.container_name,
            blob_name=blob_name,
            user_delegation_key=user_delegation_key,
            account_key=account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.now(tz=UTC)
            + timedelta(
                seconds=(expiration_in_seconds or self.DefaultPresignedURLExpirationInSeconds),
            ),
            content_disposition=self.ContentDispositionFormat.format(
                filename_utf8=quote(
                    file_name or blob_name,
                    encoding="utf-8",
                ),
            ),
            cache_control=cache_control,
        )
        return self.TmplBlobSAS.format(
            account_name=self.client.account_name,
            container_name=self.container_name,
            blob_name=blob_name,
            sas_token=sas_token,
        )

    @_auto_catch_native_exc
    @_ensure_client
    async def save_many(
        self,
        objects: Sequence[tuple[str, str | Path | BytesIO | bytes]],
        batch_size: int | None = None,
    ) -> Sequence[tuple[str, BaseException | None]]:
        """Upload multiple objects in batches.

        Args:
            objects: Sequence of (path, content) tuples to upload.
            batch_size: Number of concurrent uploads per batch.

        Returns:
            Sequence of (path, exception or None) for each upload.

        """
        results: list[str | BaseException] = []
        for batch in Batch.seq(objects, size=batch_size or Batch.DEFAULT_SIZE):
            tasks: list[Coroutine[Any, Any, str]] = [self.save(path, ref) for path, ref in batch]
            results.extend(
                await asyncio.gather(
                    *tasks,
                    return_exceptions=True,
                ),
            )
        batch_results: list[tuple[str, BaseException | None]] = []
        for (path, _), result in zip(objects, results, strict=False):
            maybe_exc = result if isinstance(result, BaseException) else None
            batch_results.append((path, maybe_exc))
        return batch_results

    @_auto_catch_native_exc
    @_ensure_client
    async def is_folder(self, path: str) -> bool:
        """Check if a path is a folder (has children blobs).

        A path is considered a folder if there exist blobs with names that
        start with the path as a prefix but are not the path itself.

        Note:
            Does NOT raise ObjectNotFoundError for nonexistent paths.
            Returns False for both nonexistent paths and existing files.
            Use this to disambiguate between file and folder paths.

        Examples:
            - is_folder("folder") -> True if "folder/file.txt" exists.
            - is_folder("file.txt") -> False if "file.txt" is a blob.
            - is_folder("nonexistent") -> False (no exception).
            - is_folder("folder/") -> True if "folder/file.txt" exists
              (trailing slash is stripped).

        Args:
            path: Path to check (trailing '/' is stripped).

        Returns:
            True if path is a folder prefix with children, False otherwise.

        """
        path_noslash = path.rstrip("/")
        try:
            blob = await anext(
                self.container.list_blobs(name_starts_with=f"{path_noslash}"),
            )
        except StopAsyncIteration:
            return False
        else:
            return blob.name != path_noslash
