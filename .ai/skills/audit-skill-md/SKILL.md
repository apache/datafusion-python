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
description: Cross-reference the repo-root SKILL.md against the current public Python API (DataFrame, Expr, SessionContext, functions module) and report new APIs that need coverage and stale mentions that no longer exist. Use after upstream syncs or any PR that changes the public Python surface.
argument-hint: [area] (e.g., "functions", "dataframe", "expr", "context", "all")
---

# Audit SKILL.md Against the Python Public API

This skill keeps the repo-root `SKILL.md` (the agent-facing DataFrame API
guide) aligned with the actual Python surface exposed by the package. It is
a **diff-only audit** — it does not auto-edit `SKILL.md`. The output is a
report the user reviews and then asks the agent to act on.

Run this whenever the public Python API changes — most commonly:

- after an upstream DataFusion sync PR adds new functions or methods,
- after a PR that adds or removes a `DataFrame`, `Expr`, or `SessionContext`
  method,
- as a pre-release gate before cutting a new datafusion-python version.

The companion skill [`check-upstream`](../check-upstream/SKILL.md) reports
upstream APIs that are **not yet** exposed in the Python bindings. This skill
reports APIs that **are** exposed but are missing or misspelled in the
user-facing guide.

## Areas to Check

`$ARGUMENTS` selects a subset. If empty or `all`, audit every area.

### 1. Scalar / aggregate / window functions

**Source of truth:** `python/datafusion/functions.py` — the `__all__` list.
Only symbols in `__all__` are part of the public surface; helpers not listed
there are implementation details.

**Procedure:**

1. Load `python/datafusion/functions.py`, extract the `__all__` list.
2. Parse `SKILL.md`, collect every function reference — patterns to look for:
   - Inline `F.<name>(...)`, `F.<name>` references.
   - Bare backticked names in the "Available Functions (Categorized)"
     section (`sum`, `avg`, ...).
3. Cross-reference:
   - **In `__all__` but not mentioned in `SKILL.md`** → new API needing
     coverage. Flag unless it is an alias documented through a `See Also`
     in the primary function's docstring (see "Alias handling" below).
   - **Mentioned in `SKILL.md` but not in `__all__`** → stale reference, has
     been renamed or removed.

### 2. `DataFrame` methods

**Source of truth:** `python/datafusion/dataframe.py` — public methods on the
`DataFrame` class. A method is public if its name does not begin with an
underscore.

**Procedure:**

1. Import `DataFrame` and collect `dir(DataFrame)`, filtering to names that
   do not start with `_`.
2. Parse `SKILL.md` for method references — patterns:
   - `df.<name>(`, `.<name>(`, and backticked bare names in prose.
   - The method tables in "Core Abstractions" and the pitfalls/idiomatic
     patterns sections.
3. Flag:
   - **Public method, no mention in `SKILL.md`** → candidate addition.
     Weight the flag by whether the method would change how a user writes a
     query (e.g. `with_column`, `join`, `aggregate` are high-value; a new
     `explain_analyze_format` is low-value).
   - **Mentioned in `SKILL.md`, no longer a public method** → stale.

### 3. `Expr` methods and attributes

**Source of truth:** `python/datafusion/expr.py` — the `Expr` class. Also
include `Window`, `WindowFrame`, and `GroupingSet` if they are re-exported
from `datafusion.expr`.

**Procedure:** same as for `DataFrame`. Pay particular attention to operator
dunder methods mentioned in `SKILL.md` — the "Common Pitfalls" section
already covers `&`, `|`, `~`, `==`, the comparison operators, and arithmetic
operators on `Expr`. If a new operator is added (e.g. a new `__matmul__`),
it probably warrants a pitfall or pattern note.

### 4. `SessionContext` methods

**Source of truth:** `python/datafusion/context.py` — the `SessionContext`
class.

**Procedure:** same as for `DataFrame`. The high-value methods in `SKILL.md`
are the data-loading methods (`read_parquet`, `read_csv`, `read_json`,
`from_pydict`, `from_pylist`, `from_pandas`) and the SQL entry points
(`sql`, `register_*`, `table`). New additions in those families are
worth flagging for a sentence in the data-loading section.

### 5. Re-exports at package root

**Source of truth:** `python/datafusion/__init__.py` — the top-level
`from ... import ...` statements and `__all__`. A symbol re-exported at the
package root is part of the "import" examples in `SKILL.md` even if it
lives in a submodule.

**Procedure:** verify every name in the top-level `__all__` resolves. Flag
any new re-export that is not already mentioned in the "Import Conventions"
or "Core Abstractions" section.

## Alias handling

Many functions in the `functions` module are aliases — for example
`list_sort` aliases `array_sort`, and `character_length` aliases `length`.
The convention in this project is that alias function docstrings carry only
a one-line description and a `See Also` pointing at the primary function
(see `CLAUDE.md`). Do not flag an alias as missing from `SKILL.md` as long
as its primary function is already covered, unless the alias uses a name
that a user would reasonably reach for first (e.g. SQL-standard names).

## Output Format

Produce a report of this shape:

```
## SKILL.md Audit Report

### Summary
- Functions checked: N
- DataFrame methods checked: N
- Expr members checked: N
- SessionContext methods checked: N
- Package-root re-exports checked: N

### New APIs needing coverage in SKILL.md
- `functions.new_fn` — brief description. Suggested section: "String".
- `DataFrame.with_catalog` — brief description. Suggested section: "Core Abstractions".

### Stale mentions in SKILL.md
- `functions.old_fn` — referenced in "Available Functions" but no longer in `__all__`. Likely renamed to `new_fn` in <upstream PR/commit>.
- `DataFrame.show_limit` — referenced in a pitfall; method removed in favor of `DataFrame.show(num=...)`.

### Informational
- Alias `list_sort` covered transitively via `array_sort` — no action needed.
```

If every area is clean, state that explicitly ("All audited areas are in
sync. No action required."). An audit report that elides the summary line
is harder to scan in a release checklist.

## When to edit SKILL.md

This skill does not auto-edit. After reporting, wait for the user to
confirm which gaps are worth filling. New APIs often need a natural home
chosen by a human — the categorized function list and the pitfalls section
both have opinionated structure that an automated edit will not respect.

## Related

- Repo-root [`SKILL.md`](../../SKILL.md) — the file this skill audits.
- `.ai/skills/check-upstream/` — the complementary audit against upstream
  Rust APIs not yet exposed in Python.
- `.ai/skills/write-dataframe-code/` — how to write idiomatic DataFrame
  code in this repo.
