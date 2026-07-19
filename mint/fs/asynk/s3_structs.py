"""S3-specific credential and session structures."""

from enum import StrEnum
from typing import NotRequired, TypedDict


class S3CredentialMode(StrEnum):
    """All credential modes that S3Storage can use."""

    KeyPair = "key_pair"
    EnvVar = "env_var"
    SharedCredentials = "shared_credentials"
    IAMRole = "iam_role"


class S3SessionParams(TypedDict, total=False):
    """Parameters forwarded to aiobotocore session for client creation."""

    aws_access_key_id: NotRequired[str | None]
    aws_secret_access_key: NotRequired[str | None]
    aws_session_token: NotRequired[str | None]
    region_name: NotRequired[str | None]
    profile_name: NotRequired[str | None]
