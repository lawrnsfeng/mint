---
paths:
  - "**/*.py"
---
# Coding Style

## Python version

Target Python 3.13+. Always use current syntax — no backports, no compatibility shims.

## Type annotations (MANDATORY)

All functions, methods, and class attributes must have type annotations.

Use PEP 695 syntax for generics and type aliases:

```python
# type alias
type Vector = list[float]

# generic function
def first[T](items: list[T]) -> T: ...

# generic class
class Stack[T]:
    def push(self, item: T) -> None: ...
```

### Type parameter naming

Generic type parameters are named `T`, or `SomethingT` when a descriptive name adds
clarity (e.g. multiple type parameters that would otherwise collide, or a name that
disambiguates intent like `KeyT`/`ValueT`). Never use a bare descriptive noun like
`Item` or `Value` as a type parameter name.

- Single type parameter → always `T`.
- Multiple type parameters → suffix each with `T`: `KeyT`, `ValueT`, `ResultT`.

```python
# Wrong
class Fetcher[Item](Protocol): ...

# Correct — single param
class Fetcher[T](Protocol): ...

# Correct — multiple params
class Cache[KeyT, ValueT]: ...
```

Use `TypeVar` only when a third-party API requires it.

Never add `from __future__ import annotations`. Resolve forward references with
`if TYPE_CHECKING:` blocks or by reordering imports.

## Line length

Max 100 characters.

## Guard clauses — fail fast

Validate preconditions at the top of functions. Return or raise early rather than
nesting the happy path.

```python
# Wrong
def process(items: list[str]) -> str:
    if items:
        if len(items) > 1:
            return items[0]
    return ""

# Correct
def process(items: list[str]) -> str:
    if not items:
        return ""
    if len(items) <= 1:
        return ""
    return items[0]
```

## RORO pattern

Functions that accept multiple parameters should receive an object; functions that
return multiple values should return an object, not a bare tuple.

## Data classes

Three tiers — pick the right one:

| Use case | Type |
|---|---|
| Complex data needing validation + serialization | `pydantic.BaseModel` |
| Intermediate structs needing validation | `pydantic.dataclasses.dataclass` |
| Simple value containers, no validation | `dataclasses.dataclass` |

## OOP over functional

Prefer classes and methods over standalone functions. A free function whose first
parameter is an instance of a local struct is an implicit method — make it explicit.

See [common coding-style rule](~/.claude/rules/common/coding-style.md) for the
full OOP method placement decision tree.

## Datetime

Always pass `tz` to `datetime.now()`. Default to UTC:

```python
from datetime import UTC, datetime

now = datetime.now(tz=UTC)
```

## Boolean arguments

Boolean parameters must be keyword-only:

```python
# Wrong
def run(verbose: bool) -> None: ...

# Correct
def run(*, verbose: bool) -> None: ...
```

## Naming

Intermediate dict variable: `mp_<keytype>_<valuetype>` (e.g. `mp_str_int`).

### Module names

Never name a module after a stdlib module (`types.py`, `io.py`, `email.py`, ...) —
it shadows the stdlib name for every absolute import inside the package and forces
awkward relative-import gymnastics. Pick a distinguishing name instead
(`typedefs.py`, not `types.py`).

## Immutability

Return new objects rather than mutating in place.

## No global variables

Limit global state. Pass dependencies explicitly.

## Ruff + ty

All code must pass `uv run ruff check` and `uv run ruff format --check` and
`uv run ty check` before a change is considered done.

## Type narrowing over hasattr/getattr

Prefer `isinstance()`/`issubclass()` to narrow a type over `hasattr()` or
`getattr(obj, name, default)`. `hasattr`/`getattr` with a default silently
accept any object shape, including the wrong one, and give the type
checker nothing to narrow on — the following line still sees the original
(possibly `Any`) type. `isinstance`/`issubclass` against a real type (a
concrete class, or a `Protocol`) narrow the checked variable's static type
for the rest of the block, so the type checker catches a shape mismatch
that `hasattr`/`getattr` would let through silently.

```python
# Wrong
if hasattr(schema, "is_deleted"):
    ...
owner = getattr(self, "owner", None)

# Correct
if issubclass(schema, IsDeletedMixin):
    ...
if isinstance(self, IScopedRepository):
    owner = self.owner
```

One caveat: `issubclass()` raises `TypeError` on a `Protocol` that has any
non-method (data) member — only a `Protocol` with exclusively method
members supports `issubclass()`. For a class-level check against a
data-bearing shape, check against a concrete class instead (as with
`IsDeletedMixin` above), not a data `Protocol`. `isinstance()` has no such
restriction and works on any `@runtime_checkable` `Protocol`, data members
included — prefer it for instance-level structural checks.

## No suppression comments

Fix the underlying issue — don't silence the linter/type-checker with
`# noqa`, `# type: ignore`, or similar. The only exception is a genuine
limitation of a third-party dependency: a library function/class that is
itself untyped or incorrectly typed, where no code change on our side can
make the call typed (e.g. calling a third-party constructor whose
`__init__` has no return annotation). In that case, suppress with the
narrowest possible code (e.g. `# type: ignore[no-untyped-call]`, never a
bare `# type: ignore`) and only on the exact line the third-party gap
forces.
