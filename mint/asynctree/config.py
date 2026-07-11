"""Configuration for async tree executor."""

from dataclasses import dataclass


@dataclass
class AsyncTreeSettings:
    """Plain-dataclass settings for async tree traversal.

    Attributes:
        async_tree_max_at_once: Maximum concurrent fetches.
        async_tree_max_per_second: Maximum fetches per second (0 = unlimited).
        async_tree_level_timeout_seconds: Timeout budget per depth level.
        async_tree_retry_max_attempts: Maximum retry attempts per node.

    """

    async_tree_max_at_once: int = 8
    async_tree_max_per_second: float = 10.0
    async_tree_level_timeout_seconds: float = 30.0
    async_tree_retry_max_attempts: int = 4


@dataclass
class AsyncTreeExecutorConfig:
    """Runtime configuration for AsyncTreeExecutor.

    Attributes:
        retry_max_attempts: Maximum retry attempts per node.
        max_at_once: Maximum concurrent fetches (bounded semaphore).
        max_per_second: Maximum fetches per second (0 = unlimited).
        level_timeout: Timeout budget per depth level in seconds.

    """

    retry_max_attempts: int = 4
    max_at_once: int = 8
    max_per_second: float = 10.0
    level_timeout: float = 30.0

    @classmethod
    def from_settings(
        cls,
        settings: AsyncTreeSettings,
    ) -> "AsyncTreeExecutorConfig":
        """Build config from async tree settings.

        Args:
            settings: Object providing AsyncTreeSettings fields.

        Returns:
            Executor config with values from settings.

        """
        return cls(
            retry_max_attempts=settings.async_tree_retry_max_attempts,
            max_at_once=settings.async_tree_max_at_once,
            max_per_second=settings.async_tree_max_per_second,
            level_timeout=settings.async_tree_level_timeout_seconds,
        )
