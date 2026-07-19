"""Engine/pool configuration for mint.db.Database."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    """Tunable engine and connection-pool parameters.

    Values are sourced from the ``SQLALCHEMY_*`` environment variables when
    present, falling back to the documented defaults otherwise.
    """

    model_config = SettingsConfigDict(
        case_sensitive=False,
        env_prefix="SQLALCHEMY_",
    )

    POOL_PRE_PING: bool = True
    ECHO: bool = False
    POOL_SIZE: int = 5
    MAX_OVERFLOW: int = 10
    POOL_RECYCLE: int = 300
