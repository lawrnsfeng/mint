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

## Immutability

Return new objects rather than mutating in place.

## No global variables

Limit global state. Pass dependencies explicitly.

## Ruff + ty

All code must pass `uv run ruff check` and `uv run ruff format --check` and
`uv run ty check` before a change is considered done.
