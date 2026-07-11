"""Tests for asynctree retry module."""

import time

import pytest

from mint.asynctree.retry import RetryConfig, _always_retry, build_retrying

_DEFAULT_MAX_ATTEMPTS = 4
_DEFAULT_MIN_WAIT = 0.5
_DEFAULT_MAX_WAIT = 10.0
_DEFAULT_JITTER = 0.2
_CUSTOM_MAX_ATTEMPTS = 2
_RETRY_ATTEMPTS_3 = 3
_JITTER_GAP_MIN = 0.05
_HOOK_WAIT_THRESHOLD = 3


def test_always_retry_predicate() -> None:
    """Test default retry predicate retries everything."""
    assert _always_retry(ValueError("x")) is True
    assert _always_retry(RuntimeError("y")) is True


def test_retry_config_defaults() -> None:
    """Test RetryConfig default values."""
    config = RetryConfig()
    assert config.max_attempts == _DEFAULT_MAX_ATTEMPTS
    assert config.min_wait == _DEFAULT_MIN_WAIT
    assert config.max_wait == _DEFAULT_MAX_WAIT
    assert config.jitter_pct == _DEFAULT_JITTER
    assert config.is_retryable is _always_retry
    assert config.retry_after_hook is None


def test_retry_config_custom_values() -> None:
    """Test RetryConfig with custom values."""

    def custom_pred(exc: BaseException) -> bool:
        return isinstance(exc, ValueError)

    config = RetryConfig(
        max_attempts=_CUSTOM_MAX_ATTEMPTS,
        min_wait=1.0,
        max_wait=5.0,
        jitter_pct=0.1,
        is_retryable=custom_pred,
    )
    assert config.max_attempts == _CUSTOM_MAX_ATTEMPTS
    assert config.is_retryable is custom_pred


def test_build_retrying_creates_instance() -> None:
    """Test build_retrying returns an AsyncRetrying object."""
    config = RetryConfig(max_attempts=_CUSTOM_MAX_ATTEMPTS)
    retrying = build_retrying(config)
    assert retrying is not None


@pytest.mark.asyncio
async def test_retry_succeeds_on_second_attempt() -> None:
    """Test retry succeeds after first failure."""
    config = RetryConfig(
        max_attempts=_RETRY_ATTEMPTS_3,
        min_wait=0.01,
        max_wait=0.02,
    )
    retrying = build_retrying(config)
    call_count = 0

    async def flaky() -> str:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            msg = "transient"
            raise RuntimeError(msg)
        return "ok"

    result = None
    async for attempt in retrying:
        with attempt:
            result = await flaky()

    assert result == "ok"
    assert call_count == _CUSTOM_MAX_ATTEMPTS


@pytest.mark.asyncio
async def test_retry_exhausted_raises() -> None:
    """Test retry raises after max attempts exhausted."""
    config = RetryConfig(
        max_attempts=_CUSTOM_MAX_ATTEMPTS,
        min_wait=0.01,
        max_wait=0.02,
    )
    retrying = build_retrying(config)

    async def always_fails() -> str:
        msg = "always"
        raise RuntimeError(msg)

    async def run() -> None:
        async for attempt in retrying:
            with attempt:
                await always_fails()

    with pytest.raises(RuntimeError, match="always"):
        await run()


@pytest.mark.asyncio
async def test_retry_non_retryable_fails_immediately() -> None:
    """Test non-retryable exception fails on first attempt."""

    def only_runtime(exc: BaseException) -> bool:
        return isinstance(exc, RuntimeError)

    config = RetryConfig(
        max_attempts=_RETRY_ATTEMPTS_3,
        min_wait=0.01,
        is_retryable=only_runtime,
    )
    retrying = build_retrying(config)
    call_count = 0

    async def raises_value_error() -> str:
        nonlocal call_count
        call_count += 1
        msg = "not retryable"
        raise ValueError(msg)

    async def run() -> None:
        async for attempt in retrying:
            with attempt:
                await raises_value_error()

    with pytest.raises(ValueError, match="not retryable"):
        await run()

    assert call_count == 1


@pytest.mark.asyncio
async def test_retry_jitter_bounds() -> None:
    """Test retry wait times respect jitter bounds."""
    config = RetryConfig(
        max_attempts=_DEFAULT_MAX_ATTEMPTS,
        min_wait=0.1,
        max_wait=0.5,
        jitter_pct=_DEFAULT_JITTER,
    )
    retrying = build_retrying(config)
    timestamps: list[float] = []

    async def always_fails() -> None:
        timestamps.append(time.monotonic())
        msg = "fail"
        raise RuntimeError(msg)

    async def run() -> None:
        async for attempt in retrying:
            with attempt:
                await always_fails()

    with pytest.raises(RuntimeError):
        await run()

    for i in range(1, len(timestamps)):
        gap = timestamps[i] - timestamps[i - 1]
        assert gap >= _JITTER_GAP_MIN
        assert gap <= 1.0  # Upper bound (max_wait + jitter)


@pytest.mark.asyncio
async def test_retry_with_retry_after_hook() -> None:
    """Test retry_after_hook is accepted in config."""

    def hook(exc: BaseException) -> float | None:
        if "rate" in str(exc):
            return 0.1
        return None

    config = RetryConfig(
        max_attempts=_RETRY_ATTEMPTS_3,
        min_wait=0.01,
        retry_after_hook=hook,
    )
    retrying = build_retrying(config)
    call_count = 0

    async def flaky() -> str:
        nonlocal call_count
        call_count += 1
        if call_count < _RETRY_ATTEMPTS_3:
            msg = "rate limited"
            raise RuntimeError(msg)
        return "done"

    result = None
    async for attempt in retrying:
        with attempt:
            result = await flaky()

    assert result == "done"
    assert call_count == _RETRY_ATTEMPTS_3
