from enum import StrEnum
from typing import NotRequired, TypedDict


class AzureSessionParams(TypedDict):
    """All supported parameters for constructing credentials."""

    sas_token: NotRequired[str | None]
    tenant_id: NotRequired[str | None]
    client_id: NotRequired[str | None]
    client_secret: NotRequired[str | None]
    shared_access_key: NotRequired[str | None]
    connection_string: NotRequired[str | None]


class AzureCredentialMode(StrEnum):
    """All credential modes that ABS can use."""

    Default = "Default"
    SharedAccessSignature = "SharedAccessSignature"
    ClientSecret = "ClientSecret"
    ConnectionString = "ConnectionString"
    SharedAccessKey = "SharedAccessKey"
    EnvVarSharedAccessKey = "EnvVarSharedAccessKey"
    EnvVarConnectionString = "EnvVarConnectionString"
