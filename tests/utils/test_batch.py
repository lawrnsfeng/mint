"""Tests for the Batch helper class."""

from collections.abc import AsyncIterator, Iterator

import pytest

from mint.utils.batch import Batch


def test_seq_splits_evenly() -> None:
    """Split a sequence into equally sized batches."""
    items = list(range(10))
    batches = Batch.seq(items, size=5)
    assert batches == [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]


def test_seq_splits_with_remainder() -> None:
    """Split a sequence with a final partial batch."""
    items = list(range(7))
    batches = Batch.seq(items, size=3)
    assert batches == [[0, 1, 2], [3, 4, 5], [6]]


def test_seq_empty_sequence() -> None:
    """Return no batches for an empty sequence."""
    assert Batch.seq([], size=5) == []


def test_seq_default_size() -> None:
    """Use DEFAULT_SIZE when size is not specified."""
    items = list(range(Batch.DEFAULT_SIZE + 1))
    batches = Batch.seq(items)
    assert len(batches) == 2
    assert len(batches[0]) == Batch.DEFAULT_SIZE
    assert len(batches[1]) == 1


def test_iter_splits_evenly() -> None:
    """Split an iterator into equally sized batches."""

    def gen() -> Iterator[int]:
        yield from range(10)

    batches = list(Batch.iter(gen(), size=5))
    assert batches == [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]


def test_iter_splits_with_remainder() -> None:
    """Split an iterator with a final partial batch."""

    def gen() -> Iterator[int]:
        yield from range(7)

    batches = list(Batch.iter(gen(), size=3))
    assert batches == [[0, 1, 2], [3, 4, 5], [6]]


def test_iter_empty_iterator() -> None:
    """Return no batches for an empty iterator."""

    def gen() -> Iterator[int]:
        return
        yield

    assert list(Batch.iter(gen(), size=5)) == []


def test_iter_default_size() -> None:
    """Use DEFAULT_SIZE when size is not specified."""

    def gen() -> Iterator[int]:
        yield from range(Batch.DEFAULT_SIZE + 1)

    batches = list(Batch.iter(gen()))
    assert len(batches) == 2
    assert len(batches[0]) == Batch.DEFAULT_SIZE
    assert len(batches[1]) == 1


@pytest.mark.asyncio
async def test_aiter_splits_evenly() -> None:
    """Split an async iterator into equally sized batches."""

    async def gen() -> AsyncIterator[int]:
        for i in range(10):
            yield i

    batches = [batch async for batch in Batch.aiter(gen(), size=5)]
    assert batches == [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]


@pytest.mark.asyncio
async def test_aiter_splits_with_remainder() -> None:
    """Split an async iterator with a final partial batch."""

    async def gen() -> AsyncIterator[int]:
        for i in range(7):
            yield i

    batches = [batch async for batch in Batch.aiter(gen(), size=3)]
    assert batches == [[0, 1, 2], [3, 4, 5], [6]]


@pytest.mark.asyncio
async def test_aiter_empty_iterator() -> None:
    """Return no batches for an empty async iterator."""

    async def gen() -> AsyncIterator[int]:
        return
        yield

    batches = [batch async for batch in Batch.aiter(gen(), size=5)]
    assert batches == []


@pytest.mark.asyncio
async def test_aiter_default_size() -> None:
    """Use DEFAULT_SIZE when size is not specified."""

    async def gen() -> AsyncIterator[int]:
        for i in range(Batch.DEFAULT_SIZE + 1):
            yield i

    batches = [batch async for batch in Batch.aiter(gen())]
    assert len(batches) == 2
    assert len(batches[0]) == Batch.DEFAULT_SIZE
    assert len(batches[1]) == 1
