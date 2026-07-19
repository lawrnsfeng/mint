---
paths:
  - "**/*.py"
---
# Documentation

## Docstrings

All public functions, methods, and classes must have docstrings.
Private helpers (leading `_`) need one only when the intent is non-obvious.

One-line docstring for simple cases:

```python
def greet(name: str) -> str:
    """Return a greeting string for the given name."""
    ...
```

Multi-line for complex cases:

```python
def process(items: list[str], *, reverse: bool = False) -> list[str]:
    """Process items and return transformed results.

    Args:
        items: Input strings to process.
        reverse: If True, reverse each string before processing.

    Returns:
        Transformed list of strings.

    Raises:
        ValueError: If items contains an empty string.
    """
    ...
```

## README

Update `README.md` when new commands or features are introduced.
