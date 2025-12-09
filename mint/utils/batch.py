from collections.abc import AsyncIterator, Iterator, Sequence
from typing import Final


class Batch:
    """Helper class that split a long sequence into sized batches."""

    DEFAULT_SIZE: Final[int] = 32

    @staticmethod
    def seq[T](
        items: Sequence[T],
        size: int = DEFAULT_SIZE,
    ) -> Sequence[Sequence[T]]:
        """Split a sequence into batches."""
        return [items[idx : idx + size] for idx in range(0, len(items), size)]

    @staticmethod
    def iter[T](
        iterator: Iterator[T],
        size: int = DEFAULT_SIZE,
    ) -> Iterator[list[T]]:
        """Split an iterator into batches."""
        batch: list[T] = []
        for item in iterator:
            batch.append(item)
            if len(batch) == size:
                yield batch
                batch = []
        if len(batch) > 0:
            yield batch

    @staticmethod
    async def aiter[T](
        iterator: AsyncIterator[T],
        size: int = DEFAULT_SIZE,
    ) -> AsyncIterator[list[T]]:
        """Split an iterator into batches asynchronously."""
        batch: list[T] = []
        async for item in iterator:
            batch.append(item)
            if len(batch) == size:
                yield batch
                batch = []
        if len(batch) > 0:
            yield batch
