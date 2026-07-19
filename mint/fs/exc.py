from dataclasses import dataclass
from pathlib import Path

from mint.exc import TemplatedError


@dataclass
class FileStorageError(TemplatedError):
    """Generic file storage error."""


@dataclass
class UndefinedBucketError(FileStorageError):
    """Bucket or storage name not specified."""

    TEMPLATE = "Bucket or storage name not specified: {name}"
    name: str


@dataclass
class ObjectNotFoundError(FileStorageError):
    """Object not found."""

    TEMPLATE = "Object not found: {path}"
    path: str | Path


@dataclass
class InvalidArgumentsError(FileStorageError):
    """Arguments passed are incorrect or unsupported."""

    TEMPLATE = "Invalid arguments passed, {detail}"
    detail: str


@dataclass
class OperationalError(FileStorageError):
    """Uncaught exception when running logic."""

    TEMPLATE = "Operational uncaught error: {error}"
    error: BaseException


@dataclass
class FolderAlreadyExistsError(FileStorageError):
    """Folder already exists in given path."""

    TEMPLATE = "Folder already exists: {path}"
    path: str | Path


@dataclass
class FileAlreadyExistsError(FileStorageError):
    """File already exists in given path."""

    TEMPLATE = "File already exists: {path}"
    path: str | Path


@dataclass
class MoveCleanupError(FileStorageError):
    """Move file or folder error."""

    TEMPLATE = "Cannot remove src path due to failed objects, src = {src}: {failure}"
    src: str | Path
    failure: list[str]


@dataclass
class TrailingSlashNotAllowedError(FileStorageError):
    """Path must not end with '/' for a single-object operation."""

    TEMPLATE = "path must not end with '/': {path}"
    path: str


@dataclass
class UnsupportedRefTypeError(FileStorageError):
    """Content reference passed to save() is of an unsupported type."""

    TEMPLATE = "unsupported ref type for {path}: {ref_type}"
    path: str
    ref_type: str


@dataclass
class AmbiguousFolderPathError(FileStorageError):
    """src or dst is a folder but src lacks the required trailing '/'."""

    TEMPLATE = "src or dst is a folder; add trailing '/' to src (src={src}, dst={dst})"
    src: str
    dst: str


@dataclass
class CopySourceTooLargeError(FileStorageError):
    """Single-object copy source exceeds the backend's size limit."""

    TEMPLATE = "object {path} is {size} bytes; single copy limited to {max_bytes} bytes (5 GB)"
    path: str
    size: int
    max_bytes: int
