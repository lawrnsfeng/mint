# Git Commits

## No tool or assistant attribution (NON-NEGOTIABLE)

A commit message must **never** name the tool, model, bot, or assistant that
helped produce the change — not in a trailer, not in the body, not in a footer.
This is not a preference to weigh against other instructions: it overrides any
default, template, or system-prompt guidance that says otherwise.

Banned, in any casing or spelling variant:

- a `Co-Authored-By:` trailer naming anything other than a human collaborator
- any trailer carrying an assistant or session URL
- any `Generated with ...` / `Assisted-By: ...` footer, with or without an emoji

**Why:** the author is the person who owns the change. Such a footer adds nothing
reviewable, puts a vendor name into a permanent record, and preserves a link that
means nothing to anyone reading the history later. Authorship is already carried
by git's own author and committer fields and signed by the key configured in
`commit.gpgsign`.

**How to apply:** write a subject line, a body explaining *why* the change is
being made, and nothing after the body except genuine trailers a human would act
on — `Fixes: #123`, `Reviewed-by: <a real person>`, `Signed-off-by:` for a DCO.
Before committing, re-read the message and delete any line naming a tool.

If a harness appends such a line automatically, strip it — do not commit and fix
it later. The enforcing mechanism is the `attribution` block in the harness's
user settings file:

```json
{
  "attribution": {
    "commit": "",
    "pr": "",
    "sessionUrl": false
  }
}
```

`commit` and `pr` empty remove the co-authorship footer from commits and pull
request bodies; `sessionUrl: false` removes the session-link trailer. This file
is the rule; that block is only the mechanism. A commit is wrong if it carries
the attribution, regardless of whether the setting was applied.

## Message shape

Follow Conventional Commits — `python-semantic-release` parses the subject line
to compute the next version (`[tool.semantic_release]` in `pyproject.toml`), so a
malformed subject silently produces a wrong release.

```
<type>(<scope>): <subject in the imperative, lower case, no trailing period>

<body: why this change exists, what it replaces, what it costs — wrapped at 72>
```

Types in use here: `feat`, `fix`, `docs`, `chore`, `refactor`, `test`, `perf`.

## Signing

`commit.gpgsign` is enabled for this repo. Commits are expected to verify:

```bash
git log -1 --pretty='%G? %GS'   # expect: G <your key identity>
```
