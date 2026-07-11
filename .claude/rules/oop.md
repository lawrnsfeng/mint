---
paths:
  - "**/*.py"
---
# OOP Method Placement (NON-NEGOTIABLE)

When writing a function, scan its parameter list for the first parameter that is an
**owned class** (defined in this repo). Ignore any leading dependency-injection params
that are third-party types we do not control — those do NOT trigger this rule.

Once the first owned-class param is identified, apply this decision tree:

## Case A → method on that class

All of the following hold:

- All remaining params are primitives (`str`, `int`, `float`, `bool`, `None`), enums,
  or types owned by the **same module** or an inner layer (`models`, `entities`).
- The function body only references symbols from the same module or those inner layers.
- The class is defined in the same module.

```python
# WRONG — dangling function
def process_expense(expense: Expense, owner_id: str) -> None: ...

# CORRECT — Case A
class Expense:
    def process(self, owner_id: str) -> None: ...
```

## Case B → method on a service class

The function needs a repository, external client, event publisher, or coordinates
multiple owned types → a service class MUST own the logic via constructor-injected
collaborators. The service lives in `services.py`.

```python
# WRONG — dangling function pulling outer-layer deps
def sync_expense(
    expense: Expense,
    repo: ExpenseRepo,
    pub: EventPublisher,
) -> None: ...

# CORRECT — Case B
class ExpenseService:
    def __init__(self, repo: ExpenseRepo, pub: EventPublisher) -> None:
        self._repo = repo
        self._pub = pub

    def sync(self, expense: Expense) -> None: ...
```

## What does NOT trigger this rule

- Functions with no owned-class params (pure utilities, factory functions, validators
  that only receive primitives).
- Typer command callbacks — these are framework entry points, not domain logic.
  Delegate immediately to a service method; keep the callback itself thin.
- `@classmethod` / `@staticmethod` factory methods are Case A variants and are allowed.

## Enforcement checklist

Before marking a function done:

- [ ] Does its first param have a type defined in this repo?
- [ ] If yes — is it a method on that class or on a service class?
- [ ] If it's a free function — does it meet one of the exemptions above?
