<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

---
name: audit-skill-md
description: Audit the user-facing skill at skills/datafusion_python/SKILL.md against the current public Python API. Find new APIs that should be documented, stale mentions of removed/renamed APIs, examples that drifted from current idiomatic style, and places that need a "requires datafusion-python NN or newer" note. Run after upstream syncs and before each release.
argument-hint: [scope] (e.g., "session-context", "dataframe", "expr", "functions", "patterns", "pitfalls", "version-notes", "all")
---

# Audit `skills/datafusion_python/SKILL.md`

You are auditing the user-facing skill at
[`skills/datafusion_python/SKILL.md`](../../skills/datafusion_python/SKILL.md)
against the current state of the Python API. The skill is the source of truth
for how AI coding assistants are taught to write `datafusion-python` code, so
it must match what the project actually ships. This skill identifies gaps
caused by upstream syncs, refactors, or renames, and (if asked) applies the
edits directly to `SKILL.md`.

The skill is most usefully run **after** the `check-upstream` step of an
upstream sync (see `dev/release/upstream-sync.md`) — once any new APIs are
exposed, this skill makes sure they get documented.

## What the skill covers

The user-facing `SKILL.md` documents these public surfaces. This list is not
exhaustive — if a new top-level area is added (e.g., a new `Catalog` API
exposed at the package root), include it.

| Surface | Module | Sections in SKILL.md |
|---|---|---|
| `SessionContext` | `python/datafusion/context.py` | "Data Loading" |
| `DataFrame` | `python/datafusion/dataframe.py` | "DataFrame Operations Quick Reference", "Executing and Collecting Results", "Idiomatic Patterns" |
| `Expr` | `python/datafusion/expr.py` | "Expression Building", "Common Pitfalls" |
| `functions` | `python/datafusion/functions/__init__.py` | "Available Functions (Categorized)", scattered uses throughout |
| `functions.spark` | `python/datafusion/functions/spark.py` | "Available Functions (Categorized)" → "Spark-Compatible Functions" subsection |
| Top-level helpers (`col`, `lit`, `WindowFrame`, ...) | `python/datafusion/__init__.py` | "Import Conventions", "Core Abstractions" |

## Scope argument

The user may specify a scope via `$ARGUMENTS` to limit the audit. If no scope
is given or `all` is specified, audit every area.

| Scope | Audit target |
|---|---|
| `session-context` | `SessionContext` methods and the "Data Loading" section |
| `dataframe` | `DataFrame` methods and the operations / executing / patterns sections |
| `expr` | `Expr` methods/operators and the "Expression Building" section |
| `functions` | `functions/__init__.py` `__all__` and the "Available Functions (Categorized)" section |
| `spark-functions` | `functions/spark.py` `__all__`, the "Spark-Compatible Functions" subsection, and the divergent-semantics table |
| `patterns` | "Idiomatic Patterns" section — confirm patterns still match recommended style |
| `pitfalls` | "Common Pitfalls" — confirm each pitfall still reproduces, drop ones fixed upstream |
| `version-notes` | Cross-check version annotations (see below) |
| `all` | Everything above |

## Inputs to read

Before producing the report:

1. `skills/datafusion_python/SKILL.md` — the document being audited.
2. The relevant Python module(s) for the chosen scope. Public surface is the
   `__all__` list (where defined) plus `class` and `def` symbols not prefixed
   with `_`.
3. `Cargo.toml` (root) for the current `datafusion-python` version — read
   the `version` field under `[workspace.package]` (format `NN.0.0`). The
   major version always matches the upstream `datafusion` crate, so a
   single `datafusion-python` version expresses both.
   `python/datafusion/__init__.py`'s `__version__` is the same value
   exposed at runtime.
4. Recent commits touching the relevant module(s) for context on what
   changed since the last sync:
   ```bash
   git log --oneline -- python/datafusion/dataframe.py | head -20
   ```

## What to look for

Walk through each scoped area and flag four kinds of issues.

### 1. New APIs not mentioned

For each public symbol in the module's `__all__` (or each public class
method), check whether it appears anywhere in `SKILL.md`. A symbol is
"covered" if it shows up in:

- A code block (the strongest signal — it's demonstrated).
- The "Available Functions (Categorized)" list.
- The SQL-to-DataFrame Reference table.

**Decide whether each missing symbol deserves an entry.** Not every public
symbol belongs in `SKILL.md` — the skill is curated for the patterns users
hit daily, not exhaustive API reference. Use these heuristics:

- **Add it** if it replaces or supersedes something already in the skill
  (e.g., a new operation that is the idiomatic alternative to a documented
  workaround).
- **Add it** if it fits a category already present (a new aggregate function
  goes in the aggregate list; a new join type goes in the joining section).
- **Add it** if it changes how a documented pattern should be written.
- **Skip it** if it is genuinely niche / advanced / experimental.
- **Skip it** if it is internal plumbing exposed for FFI but not user-facing.

When you flag a missing symbol, include a one-line proposed insertion point
(which section / which table row) so a reviewer can decide quickly.

### 2. Stale mentions

For each function name, method name, or import shown in `SKILL.md`, verify it
still exists in the current API:

- Function names mentioned in prose or in the categorized list should appear
  in `python/datafusion/functions/__init__.py`'s `__all__`.
- Spark function names mentioned in the "Spark-Compatible Functions"
  subsection should appear in `python/datafusion/functions/spark.py`'s
  `__all__`. Also confirm the divergent-semantics table still matches the
  current spark vs. main signatures.
- Method calls in code blocks should resolve against the current class.
- Imports (`from datafusion import ...`) should succeed against the current
  `__init__.py`.

A quick way to check imports without running them:

```bash
python -c "from datafusion import SessionContext, col, lit; from datafusion import functions as F; print('ok')"
```

For each stale mention, propose either:
- a rename to the current name, or
- removal if the API is gone with no replacement.

### 3. Examples that drifted from idiomatic style

The skill teaches a Pythonic style: prefer plain strings to `col(...)` when a
column reference is all you need; prefer raw Python values to `lit(...)`
where auto-wrapping applies. Recent refactors (see the `make-pythonic`
skill) keep moving more functions toward accepting native types.

For each code example in `SKILL.md`, check:

- Does it use `lit(value)` where a raw value would work? Comparison RHS,
  arithmetic with a column, etc. all auto-wrap. (Reserve `lit()` for the
  cases listed in pitfall #2.)
- Does it use `col("name")` where a plain string would work? `select(...)`,
  `aggregate([keys], ...)`, `sort(...)`, `sort_by(...)` all accept plain
  name strings.
- Do `functions.py` calls match the current pythonic signature for that
  function? If `make-pythonic` recently changed a signature (e.g.,
  `repeat(string, n: Expr | int)`), the example should pass `3` rather than
  `lit(3)`.
- Does any example use a deprecated or removed parameter name?

For drift, propose the updated snippet. If the change is purely stylistic
and the older form still works, mark the suggestion as **non-blocking**.

### 4. Missing or stale version notes

When an API depends on a specific version, the skill should say so —
otherwise an agent referencing the skill in an older project will write
code that fails at import or at runtime.

`datafusion-python` shares its major version number with the upstream
`datafusion` crate (e.g., `datafusion-python 53.x` tracks upstream
`datafusion 53`). Always express version requirements in terms of
`datafusion-python` only — there is no need to call out upstream and
package versions separately.

Add a version note when:

- A method or function shown in the skill was added in a specific release
  (e.g., a new `DataFrame` method that didn't exist before 53).
- A breaking change altered behavior in a specific release (signature
  change, default-value change, new required argument).
- A pitfall was fixed in a specific release. Either annotate the pitfall
  block with "fixed in datafusion-python NN, kept here for users on older
  versions" or remove it once the supported floor moves past that version.

Format for version notes (inline, italicized):

```markdown
*Requires datafusion-python 53 or newer.*
```

For each missing/stale version note, propose the exact line and where it
belongs.

## How to discover changes since the last audit

If the user supplies a previous version or commit SHA where the audit was
last run, diff against it:

```bash
# Public-API-relevant changes since SHA <prev>
git log --oneline <prev>..HEAD -- python/datafusion/

# Whose signatures actually moved
git diff <prev>..HEAD -- python/datafusion/functions.py | grep '^[+-]def '
```

If no prior audit point is given, fall back to "since the last upstream
sync" by inspecting commits that touch `Cargo.toml`'s `datafusion` pin:

```bash
git log --oneline -- Cargo.toml | grep -i datafusion | head -5
```

## Output Format

Produce a report grouped by scope. Each finding is one bullet with a
proposed action, so a maintainer can review the list quickly and apply
edits in order.

```
## SKILL.md Audit (scope: <scope>)

Audited against:
- skills/datafusion_python/SKILL.md @ <git SHA / "working tree">
- datafusion-python <version>

### New APIs to cover
- `DataFrame.foo()` — added in datafusion-python 53. Insert in "DataFrame Operations Quick Reference" under <subsection>.
  Proposed snippet:
  ```python
  df.foo(...)
  ```

### Stale mentions
- "old_function_name" referenced in the categorized list (line N) — renamed to "new_function_name". Replace.

### Drifted examples
- "Filtering" section, `df.filter(col("a") > lit(10))` — drop `lit(10)`, auto-wrap applies. (non-blocking)
- "Aggregation" section, `df.aggregate([col("region")], ...)` — pass `"region"` as a plain string per "Projection" guidance.

### Version notes
- `DataFrame.foo()` block needs *Requires datafusion-python 53 or newer.*
- "Common Pitfalls" #N — fixed in datafusion-python 53; remove the pitfall and update the SQL-to-DataFrame row to no longer flag the workaround.

### No-change confirmed
- `SessionContext` data-loading section — all entries match current API.
```

If asked to apply the changes, edit `skills/datafusion_python/SKILL.md`
directly with `Edit` tool calls, one finding at a time, and re-run the
relevant doctest sanity check at the end:

```bash
pytest --doctest-modules python/datafusion -q
```

## What NOT to flag

- **Internal helpers / underscored names.** Private symbols are not part of
  the user-facing surface.
- **Functions intentionally omitted.** Niche / advanced APIs (custom
  catalogs, raw FFI plumbing, low-level execution plan accessors) live in
  the API reference, not the skill. If an omission was deliberate and a
  comment / commit explains why, leave it out.
- **Style nits inside explanatory prose.** The skill mixes example code and
  prose; only enforce the pythonic style on actual code blocks.
- **Function-by-function coverage of every `functions.py` symbol.** The
  "Available Functions (Categorized)" list is curated by category, not
  exhaustive. Adding a single new aggregate to the aggregate list is
  enough — the user follows the pointer to the API reference for the rest.

## Coordination with other skills

- Run `/check-upstream` first to expose any missing upstream APIs into the
  Python layer. Without that, this skill cannot recommend documenting
  something that is not yet exposed.
- Run `/make-pythonic` before this skill if a Pythonic-signature pass is
  planned for a release — that way this skill can update examples to the
  final signature in one shot rather than churning them twice.
- The order during an upstream sync (PR 3 of `dev/release/upstream-sync.md`)
  is therefore: `/check-upstream` → `/make-pythonic` (optional) →
  `/audit-skill-md`.
