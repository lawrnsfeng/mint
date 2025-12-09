from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar


@dataclass
class TemplatedError(Exception):
    """Base class for dataclass-style exceptions with templated messages.

    Subclasses define fields + TEMPLATE.
    """

    TEMPLATE: ClassVar[str]
    message: str = field(init=False)

    def __post_init__(self) -> None:
        values = {
            name: getattr(self, name)
            for name in self.__dataclass_fields__
            if name != "message"
        }
        self.message = self.TEMPLATE.format(**values)
        super().__init__(self.message)

    def __str__(self) -> str:
        return self.message


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

    TEMPLATE = (
        "Cannot remove src path due to failed objects, src = {src}: {failure}"
    )
    src: str | Path
    failure: list[str]
