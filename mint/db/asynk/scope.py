"""Session scoping for methods of objects that carry their own database."""

from collections.abc import Callable
from functools import wraps
from typing import Concatenate, Protocol

from .database import Coro, Database


class IDatabaseBound(Protocol):
    """Structural contract: an object exposing the :class:`Database` it works against."""

    @property
    def db(self) -> Database:
        """Return the database this object works against."""
        ...


def scoped[T: IDatabaseBound, **P, RT](
    func: Callable[Concatenate[T, P], Coro[RT]],
) -> Callable[Concatenate[T, P], Coro[RT]]:
    """Wrap a method so it runs inside a session scope on ``self.db``.

    The method-level form of :meth:`Database.scoped`, for a class that
    receives its database at construction time and so has none to name at
    class-definition time.

    Args:
        func: The coroutine method to scope.

    Returns:
        The wrapped method.

    """

    @wraps(func)
    async def wrapper(self: T, *args: P.args, **kwargs: P.kwargs) -> RT:
        async with self.db.session_scope():
            return await func(self, *args, **kwargs)

    return wrapper
