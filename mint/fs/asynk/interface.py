from collections.abc import Collection, Sequence
from pathlib import Path
from typing import IO, Any, Protocol

from mint.fs.structs import (
    CopyResult,
    ListItem,
    MoveResult,
    RemoveResult,
    Stat,
)


class IFileStorage[T](Protocol):
    """Protocol for async file storage operations.

    Path conventions:
        - Paths ending with '/' are treated as folders/prefixes.
        - Paths without trailing '/' are treated as single files.

    """

    @property
    def client(self) -> T: ...
    async def is_folder(self, path: str) -> bool: ...
    async def get(self, path: str, save_to: str) -> None: ...
    async def save(
        self,
        path: str,
        ref: str | Path | IO[Any] | bytes,
    ) -> str: ...
    async def copy(
        self,
        src: str,
        dst: str,
        *,
        recursive: bool = ...,
    ) -> CopyResult: ...
    async def move(
        self,
        src: str,
        dst: str,
        *,
        recursive: bool = ...,
    ) -> MoveResult: ...
    async def remove(
        self,
        path: str,
        *,
        recursive: bool = ...,
    ) -> RemoveResult: ...
    async def remove_many(
        self,
        paths: Sequence[str],
        *,
        recursive: bool = ...,
    ) -> RemoveResult: ...
    async def stat(self, path: str) -> Stat: ...
    async def list(
        self,
        path: str,
        *,
        recursive: bool = ...,
    ) -> Collection[str]: ...
    async def list_detailed(
        self,
        path: str,
        *,
        show_stats: bool = ...,
        show_info: bool = ...,
        recursive: bool = ...,
    ) -> Sequence[ListItem]: ...
