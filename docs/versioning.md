# Versioning and Deprecation

What a release number promises, what counts as the public API, and how
either may change.

## Version numbers

mint follows [Semantic Versioning](https://semver.org/). The next version
is computed from the Conventional Commit subjects merged since the last
release, by `python-semantic-release`; nobody picks it by hand.

| Commit | While `0.x` | From `1.0` |
|---|---|---|
| `fix:` / `perf:` | patch | patch |
| `feat:` | minor | minor |
| `feat!:` or a `BREAKING CHANGE:` footer | minor | major |
| `docs:`, `test:`, `ci:`, `chore:`, `refactor:` | no release | no release |

Every release is listed in the generated `CHANGELOG.md`.

## What the public API is

For `mint.db`:

- every name in `mint.db.asynk.__all__` and `mint.db.sync.__all__`,
  imported from those packages;
- the documented classes of `mint.db.models`, `mint.db.exc`,
  `mint.db.settings`, `mint.db.typedefs` and `mint.db.retry`;
- their documented methods, parameters, defaults and return types;
- the exception type each documented failure raises;
- the `SQLALCHEMY_*` environment variables `DatabaseSettings` reads;
- the `mint_scope_bypass` execution option;
- the extras `mint[db]` and `mint[orm]`.

Not public, and free to change in any release:

- any name starting with an underscore;
- the submodule a public name happens to be defined in — import
  `EntityRepository` from `mint.db.asynk`, not from `mint.db.asynk.entity`;
- the text of exception messages and of log lines;
- anything under `tests/` and `specs/`.

## Breaking changes

While the version is `0.x`:

- A breaking change ships in a **minor** release, never in a patch.
- A name is not removed or renamed in one step. It first spends at least
  one minor release still working and marked with
  [`warnings.deprecated`](https://docs.python.org/3/library/warnings.html#warnings.deprecated),
  which raises a `DeprecationWarning` at the call site and is reported by
  type checkers. The message names the replacement.
- A change in behaviour that cannot be put behind a deprecated name — a
  different commit point, a different default — is stated under a
  "Behaviour change" heading in the commit body, which carries it into
  the changelog, and in the feature's spec under `specs/`.

From `1.0` the same rules hold, with breaking changes moving to major
releases.

## What is tested

Every pull request runs, for `mint.db`:

- the full db suite against Postgres 14, 16 and 17 with the locked
  dependency versions;
- the same suite against the **lowest** version of every declared
  dependency, on the oldest supported Python, so a lower bound in
  `pyproject.toml` is a tested claim;
- the concurrency regression suite as its own named step
  (`make test-concurrency` locally) — the tests that pin session
  isolation across concurrent tasks and threads;
- a coverage gate of 100% on `mint.db`.

Supported Python is whatever `requires-python` in `pyproject.toml` says.
A Postgres version outside the matrix may well work; it is not tested.
