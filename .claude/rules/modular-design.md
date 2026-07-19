---
paths:
  - "**/*.py"
---
# Modular Design

## File structure

Organize by feature/domain, not by type. Each module has:

```
stevard/
  <feature>/
    __init__.py
    models.py       # data classes, pydantic models
    services.py     # business logic
    commands.py     # typer commands (controllers)
    exceptions.py   # module-specific exceptions
```

## File size

200–400 lines typical. Hard cap 800 lines. Extract when approaching the limit.

## Exceptions

Each module defines its own exception hierarchy rooted at a module-level base:

```python
class FeatureError(Exception): ...
class FeatureNotFoundError(FeatureError): ...
class FeatureValidationError(FeatureError): ...
```

Never raise bare `Exception` or `RuntimeError` from module code.

Never build an error message into a local variable and raise a generic
exception (or a generic field like `detail`) with it:

```python
# Wrong
_msg = f"unsupported ref type: {type(ref).__name__}"
raise InvalidArgumentsError(_msg)

# Correct — dedicated class, typed fields, no pre-formatted string
@dataclass
class UnsupportedRefTypeError(FileStorageError):
    TEMPLATE = "unsupported ref type for {path}: {ref_type}"
    path: str
    ref_type: str

raise UnsupportedRefTypeError(path, type(ref).__name__)
```

Every distinct error condition gets its own `TemplatedError` subclass (see
`mint/exc.py` for the shared base) with a `TEMPLATE` string and typed
fields for whatever data the caller already has — never a single opaque
`msg`/`detail` string assembled at the call site. This keeps exception
types precise enough to `except` individually and keeps messages complete
(every relevant value is a named field, not whatever the caller happened
to interpolate).

## Separation of concerns

- `models.py` — pure data, no I/O, no side effects
- `services.py` — business logic, orchestration, may do I/O
- `commands.py` — typer entry points only; delegate immediately to services
- `exceptions.py` — exception classes only
