from collections.abc import Collection, Sequence
from pathlib import Path
from typing import IO, Protocol

from mint.fs.structs import (
    CopyManyResult,
    ListItem,
    MoveResult,
    RemoveManyResult,
    Stat,
)


class IFileStorage[T](Protocol):
    @property
    def client(self) -> T: ...
    async def is_folder(self, path: str) -> bool: ...
    async def get(self, path: str, save_to: str) -> None: ...
    async def save(self, path: str, ref: str | Path | IO | bytes) -> str: ...
    async def copy(
        self,
        src: str,
        dst: str,
        *,
        recursive: bool = ...,
    ) -> str | CopyManyResult: ...
    async def move(
        self,
        src: str,
        dst: str,
        *,
        recursive: bool = ...,
    ) -> MoveResult | None: ...
    async def remove(
        self,
        path: str,
        *,
        recursive: bool = ...,
    ) -> str | RemoveManyResult: ...
    async def remove_many(
        self,
        paths: Sequence[str],
        *,
        recursive: bool = ...,
    ) -> RemoveManyResult: ...
    async def stat(self, path: str) -> Stat: ...
    async def list(self, path: str) -> Collection[str]: ...
    async def list_detailed(
        self,
        path: str,
        *,
        show_stats: bool = ...,
        show_info: bool = ...,
    ) -> Collection[ListItem]: ...
