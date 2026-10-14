"""Session scoping for methods of objects that carry their own database."""

from collections.abc import Callable
from functools import wraps
from typing import Concatenate, Protocol, overload

from .database import Coro, Database

type Method[T, **P, RT] = Callable[Concatenate[T, P], Coro[RT]]


class IDatabaseBound(Protocol):
    """Structural contract: an object exposing the :class:`Database` it works against."""

    @property
    def db(self) -> Database:
        """Return the database this object works against."""
        ...


@overload
def scoped[T: IDatabaseBound, **P, RT](
    func: Method[T, P, RT],
    /,
) -> Method[T, P, RT]: ...


@overload
def scoped[T: IDatabaseBound, **P, RT](
    *,
    retries: int = 0,
    dbschema: str | None = None,
) -> Callable[[Method[T, P, RT]], Method[T, P, RT]]: ...


def scoped[T: IDatabaseBound, **P, RT](
    func: Method[T, P, RT] | None = None,
    /,
    *,
    retries: int = 0,
    dbschema: str | None = None,
) -> Method[T, P, RT] | Callable[[Method[T, P, RT]], Method[T, P, RT]]:
    """Wrap a method so it runs inside a session scope on ``self.db``.

    The method-level form of :meth:`Database.scoped`, for a class that
    receives its database at construction time and so has none to name at
    class-definition time. Used bare (``@scoped``) or with options
    (``@scoped(retries=2, dbschema="tenant_a")``).

    Args:
        func: The coroutine method to scope, when used bare.
        retries: Extra attempts on a transient error — see
            :meth:`Database.run_in_scope`.
        dbschema: Multi-tenant schema-translate target for the scope.

    Returns:
        The wrapped method, or a decorator producing it when called with
        options.

    """

    def decorate(
        target: Method[T, P, RT],
    ) -> Method[T, P, RT]:
        @wraps(target)
        async def wrapper(self: T, *args: P.args, **kwargs: P.kwargs) -> RT:
            return await self.db.run_in_scope(
                lambda: target(self, *args, **kwargs),
                retries=retries,
                dbschema=dbschema,
            )

        return wrapper

    if func is None:
        return decorate
    return decorate(func)
