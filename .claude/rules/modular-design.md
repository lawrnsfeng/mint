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

## Separation of concerns

- `models.py` — pure data, no I/O, no side effects
- `services.py` — business logic, orchestration, may do I/O
- `commands.py` — typer entry points only; delegate immediately to services
- `exceptions.py` — exception classes only
