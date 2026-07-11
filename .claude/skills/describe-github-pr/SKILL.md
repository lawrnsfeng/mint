---
name: describe-github-pr
description: Use whenever the user wants a GitHub pull request's description written, rewritten, updated, or cleaned up — e.g. "update the PR description", "describe this PR", "write a description for PR #12", "the PR body looks ugly, fix it", "generate a PR summary". Works with an explicit PR number or, if none is given, resolves the PR that belongs to the current git branch. Always prefer this over hand-rolling a PR body inline.
---

# Describe GitHub PR

Writes a concise, scannable PR description and pushes it to GitHub — never
just prints markdown and stops. Structure and formatting rules below come
from a synthesis of common PR-description guidance (Conventional-Commit
titles, five-core-elements bodies, brevity constraints); they're baked in so
you don't have to re-derive them each time.

## 1. Resolve owner/repo/PR number

```bash
git remote get-url origin   # parse `owner/repo` out of the SSH or HTTPS form
git branch --show-current   # only needed if PR number wasn't given
```

- If the user gave a PR number, use it directly.
- If not, find the PR for the current branch. Prefer the GitHub MCP tools
  over `gh` (may not be installed — check with `which gh` once, don't assume):
  `list_pull_requests` with `head: "<owner>:<branch>"`, or
  `search_pull_requests` with query `repo:<owner>/<repo> head:<branch> is:pr`.
- If more than one PR matches, or none do, stop and ask the user — don't guess.
- Remote URL and PR content can drift from what you remember earlier in a
  session (branches get rebased, PRs get replaced). Re-fetch fresh state —
  `pull_request_read` (`method: get`) — right before writing anything; don't
  reuse PR metadata from earlier in the conversation without confirming it
  still matches (same author, same head/base, same created_at). If it
  doesn't match what you expect, say so and stop rather than overwriting.

## 2. Gather real context — don't guess, don't fabricate

Pull whatever is needed to describe the change accurately:

- `pull_request_read` methods `get_commits` and `get_files` (paginate; `get_diff`
  can exceed tool output limits on large PRs — fall back to `get_files` or
  local `git diff <base>...<head> --stat` if it does).
- Read the actual changed files for anything non-obvious — don't describe a
  diff you haven't looked at.
- **Test/coverage numbers are the most common place this goes wrong.** Only
  state concrete figures (pass counts, coverage %) if you've actually run the
  suite yourself in this session, or the user just reported real output to
  you. Never invent a number because it "sounds plausible" — if you haven't
  run it, give the test *command* and let the reviewer see real output,
  don't claim a result.
- If the PR fixes a specific bug, say what was broken and why the fix works
  — one sentence of mechanism beats a paragraph of restated diff.

## 3. Write the body

Use this structure (skip a section if it's genuinely empty — don't pad):

```markdown
## 📝 Summary
<2–3 sentences max: what changed and why this approach, not a line-by-line
recap of the diff. Bold the load-bearing nouns (new dependency, breaking
change, migration).>

## ⚠️ Impact & Scope
- [ ] Breaking change
- [ ] Database schema migration
- [ ] New dependencies added
- [ ] None (standard feature/bugfix)
<Check what applies. Add one line for anything unusual — a bug fix bundled
in, a follow-up left out of scope, etc.>

## 🧪 How to Test
<Copy-pasteable commands. If this touches UI, say so and note that a
screenshot/GIF belongs here — don't fabricate one.>
- Call out anything **known-broken and pre-existing** that a reviewer would
  otherwise assume this PR caused. Say explicitly it's unrelated and out of
  scope, so it doesn't stall review.

## ✅ Checklist
- [ ] Self-reviewed diff
- [ ] Tests pass locally
- [ ] Lint/typecheck pass
- [ ] Docs updated if applicable
```

Only include a "🔗 Ticket" line if there's an actual issue link to point to
(from the branch name, commit messages, or the user telling you) — don't add
a placeholder for one that doesn't exist.

Formatting discipline: bullet fragments over paragraphs, bold the 2-3 words
that matter most per section, cut phrases like "In this PR I..." and start
directly with the substance. Reviewers are scanning, not reading prose.

## 4. Verify before publishing

Write the draft to a scratch file and read it back before sending anything to
GitHub — the same rule as any other content you're about to distribute
somewhere visible to others. This catches formatting mistakes (unrendered
literal `\n`, broken checkbox syntax) before they're live on someone's PR.

## 5. Push it

Prefer `gh pr edit <number> --body-file <path>` if `gh` is available and
authenticated; otherwise use the GitHub MCP `update_pull_request` tool with
the `body` field.

- A `403 Resource not accessible` error means the token/CLI lacks
  `pull_request:write` (or `repo`) scope on that repo — tell the user
  plainly what's missing and that you'll retry once they grant it. Don't
  loop retrying blindly.
- Report back the PR URL once done so the user can confirm it looks right.
