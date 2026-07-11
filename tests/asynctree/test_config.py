"""Tests for asynctree config module."""

from mint.asynctree.config import AsyncTreeExecutorConfig, AsyncTreeSettings

_DEFAULT_RETRY_MAX = 4
_DEFAULT_MAX_AT_ONCE = 8
_DEFAULT_MAX_PER_SECOND = 10.0
_DEFAULT_LEVEL_TIMEOUT = 30.0
_CUSTOM_RETRY_MAX = 2
_CUSTOM_MAX_AT_ONCE = 3
_CUSTOM_MAX_PER_SECOND = 5.0
_CUSTOM_LEVEL_TIMEOUT = 10.0
_SETTINGS_MAX_AT_ONCE = 3
_SETTINGS_MAX_PER_SECOND = 5.0
_SETTINGS_LEVEL_TIMEOUT = 15.0
_SETTINGS_RETRY_MAX = 2


def test_executor_config_defaults() -> None:
    """Test AsyncTreeExecutorConfig default values."""
    config = AsyncTreeExecutorConfig()
    assert config.retry_max_attempts == _DEFAULT_RETRY_MAX
    assert config.max_at_once == _DEFAULT_MAX_AT_ONCE
    assert config.max_per_second == _DEFAULT_MAX_PER_SECOND
    assert config.level_timeout == _DEFAULT_LEVEL_TIMEOUT


def test_executor_config_custom() -> None:
    """Test AsyncTreeExecutorConfig with custom values."""
    config = AsyncTreeExecutorConfig(
        retry_max_attempts=_CUSTOM_RETRY_MAX,
        max_at_once=_CUSTOM_MAX_AT_ONCE,
        max_per_second=_CUSTOM_MAX_PER_SECOND,
        level_timeout=_CUSTOM_LEVEL_TIMEOUT,
    )
    assert config.retry_max_attempts == _CUSTOM_RETRY_MAX
    assert config.max_at_once == _CUSTOM_MAX_AT_ONCE
    assert config.max_per_second == _CUSTOM_MAX_PER_SECOND
    assert config.level_timeout == _CUSTOM_LEVEL_TIMEOUT


def test_executor_config_from_settings() -> None:
    """Test AsyncTreeExecutorConfig.from_settings factory."""
    settings = AsyncTreeSettings(
        async_tree_max_at_once=_SETTINGS_MAX_AT_ONCE,
        async_tree_max_per_second=_SETTINGS_MAX_PER_SECOND,
        async_tree_level_timeout_seconds=_SETTINGS_LEVEL_TIMEOUT,
        async_tree_retry_max_attempts=_SETTINGS_RETRY_MAX,
    )
    config = AsyncTreeExecutorConfig.from_settings(settings)
    assert config.max_at_once == _SETTINGS_MAX_AT_ONCE
    assert config.max_per_second == _SETTINGS_MAX_PER_SECOND
    assert config.level_timeout == _SETTINGS_LEVEL_TIMEOUT
    assert config.retry_max_attempts == _SETTINGS_RETRY_MAX


def test_settings_defaults() -> None:
    """Test AsyncTreeSettings default values."""
    settings = AsyncTreeSettings()
    assert settings.async_tree_max_at_once == _DEFAULT_MAX_AT_ONCE
    assert settings.async_tree_max_per_second == _DEFAULT_MAX_PER_SECOND
    assert settings.async_tree_level_timeout_seconds == _DEFAULT_LEVEL_TIMEOUT
    assert settings.async_tree_retry_max_attempts == _DEFAULT_RETRY_MAX
