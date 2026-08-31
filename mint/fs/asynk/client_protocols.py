"""Structural contracts for the SDK clients the storage backends drive.

These name *only* the members `mint.fs` actually calls, so a caller may inject
an instrumented, retry-wrapped, or otherwise proxied client without subclassing
a vendor type. Both are ``runtime_checkable`` so an injected object can be
narrowed with ``isinstance`` at wiring time rather than failing with an
``AttributeError`` deep inside an operation.
"""

import inspect
from typing import Any, Protocol, cast, runtime_checkable

from mint.fs.exc import IncompatibleClientError


class _RuntimeProtocol(Protocol):
    """The shape `typing` gives every Protocol class, for typed introspection."""

    __protocol_attrs__: frozenset[str]


@runtime_checkable
class IS3Client(Protocol):
    """The S3 surface `S3Storage` drives.

    Method-only, so `issubclass` works against it as well as `isinstance`.
    """

    async def list_objects_v2(self, **kwargs: Any) -> Any: ...
    async def get_object(self, **kwargs: Any) -> Any: ...
    async def put_object(self, **kwargs: Any) -> Any: ...
    async def head_object(self, **kwargs: Any) -> Any: ...
    async def copy_object(self, **kwargs: Any) -> Any: ...
    async def delete_object(self, **kwargs: Any) -> Any: ...
    async def delete_objects(self, **kwargs: Any) -> Any: ...
    async def create_bucket(self, **kwargs: Any) -> Any: ...
    async def generate_presigned_url(self, *args: Any, **kwargs: Any) -> Any: ...


@runtime_checkable
class IBlobServiceClient(Protocol):
    """The Blob service surface `AzureBlobStorage` drives.

    Carries data members (`credential`, `account_name`), so only `isinstance`
    is usable against it — `issubclass` raises `TypeError` on a `Protocol` with
    non-method members.
    """

    credential: Any
    account_name: str | None

    def get_container_client(self, container: Any) -> Any: ...
    async def get_user_delegation_key(self, *args: Any, **kwargs: Any) -> Any: ...
    async def close(self) -> None: ...


def missing_members(client: object, protocol: type) -> list[str]:
    """List the protocol members `client` does not statically provide.

    Uses `inspect.getattr_static`, matching what `isinstance` does against a
    `runtime_checkable` Protocol on Python 3.12+ — it does not fire
    `__getattr__`, so an object that fabricates attributes on demand (a bare
    `MagicMock`, say) is correctly reported as providing none of them.

    Args:
        client: Candidate client object.
        protocol: A `runtime_checkable` Protocol describing the contract.

    Returns:
        Names the client is missing, sorted. Empty if it conforms.

    """
    expected = cast("_RuntimeProtocol", protocol).__protocol_attrs__
    missing: list[str] = []
    for name in sorted(expected):
        try:
            value = inspect.getattr_static(client, name)
        except AttributeError:
            missing.append(name)
            continue
        # `isinstance` against a Protocol also rejects a callable member whose
        # value is None; without this the diagnostic would report nothing
        # missing for a client the check had just refused.
        if value is None and callable(getattr(protocol, name, None)):
            missing.append(name)
    return missing


def ensure_conforms(client: object, protocol: type) -> None:
    """Check `client` against `protocol`, or explain precisely what is missing.

    A check rather than a cast: the caller already holds the object at its real
    static type, so returning it narrowed would buy nothing a `cast` at the call
    site does not already express.

    Args:
        client: Candidate client object, typically caller-supplied.
        protocol: A `runtime_checkable` Protocol describing the contract.

    Raises:
        IncompatibleClientError: If the client lacks any protocol member.

    Note:
        Test doubles must be spec'd — `MagicMock(spec=IS3Client)` or
        `create_autospec(IS3Client, instance=True)`. A bare `MagicMock` only
        grows attributes on access and so satisfies no protocol; spec'ing also
        gives the async members `AsyncMock` children, which is what callers
        await anyway.

    """
    if isinstance(client, protocol):
        return
    raise IncompatibleClientError(
        expected=protocol.__name__,
        got=type(client).__name__,
        missing=missing_members(client, protocol),
    )
