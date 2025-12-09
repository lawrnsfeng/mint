"""Data structures for file storage operations."""

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class CopyManyResult:
    """Result of a bulk copy operation.

    Attributes:
        success: List of successfully copied file paths.
        failure: List of failed file paths with error messages.

    """

    success: list[str]
    failure: list[str]


@dataclass
class MoveResult:
    """Result of a move operation.

    Attributes:
        copy: Result of the copy phase of the move.
        remove: List of successfully removed source paths.

    """

    copy: CopyManyResult
    remove: list[str]


@dataclass
class RemoveManyResult:
    """Result of a bulk remove operation.

    Attributes:
        success: List of successfully removed file paths.
        failure: List of failed file paths with error messages.

    """

    success: list[str]
    failure: list[str]


@dataclass
class Stat:
    """File statistics.

    Attributes:
        last_modified: Timestamp of last modification.
        size: File size in bytes.

    """

    last_modified: datetime
    size: int


@dataclass
class ListItem:
    """Detailed information about a listed object.

    Attributes:
        object_name: Name/path of the object.
        bucket_name: Name of the container/bucket.
        last_modified: Timestamp of last modification.
        etag: Entity tag for the object.
        size: Object size in bytes.
        storage_class: Storage tier/class.
        owner_id: Owner identifier.
        owner_name: Owner display name.
        content_type: MIME type of the object.
        metadata: Custom metadata key-value pairs.
        version_id: Version identifier for versioned objects.
        is_latest: Whether this is the latest version.
        is_delete_marker: Whether this is a delete marker.

    """

    object_name: str

    bucket_name: str | None = None
    last_modified: datetime | None = None
    etag: str | None = None
    size: int | None = None
    storage_class: str | None = None

    owner_id: str | None = None
    owner_name: str | None = None

    content_type: str | None = None
    metadata: dict[str, str] = field(default_factory=dict)

    version_id: str | None = None
    is_latest: bool | None = None
    is_delete_marker: bool | None = None
