from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class CopyManyResult:
    success: list[str]
    failure: list[str]


@dataclass
class MoveResult:
    copy: CopyManyResult
    remove: list[str]


@dataclass
class RemoveManyResult:
    success: list[str]
    failure: list[str]


@dataclass
class Stat:
    last_modified: datetime
    size: int


@dataclass
class ListItem:
    object_name: str

    bucket_name: str | None = None
    last_modifited: datetime | None = None
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
