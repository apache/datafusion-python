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

# Upstream Sync Process

This document describes how to sync `datafusion-python` to a new version of the
upstream `apache/datafusion` Rust crates. This is a recurring task: between
official releases the `main` branch tracks DataFusion via crates.io or GitHub
dependencies, and we periodically bump those dependencies to pick up new
features and bug fixes.

The work is broken into **three sequential PRs** rather than landing as one
large change. Splitting reviews along these lines keeps each PR focused, makes
breakage easier to bisect, and lets reviewers concentrate on one concern at a
time.

## PR 1: Bump DataFusion crate dependencies and fix breakage

**Goal:** update the upstream `datafusion` crate version and make the project
build, test, and lint cleanly against it.

1. In the root `Cargo.toml`, update:
   - `[workspace.package].version` to the new major (the `datafusion-python`
     major tracks the upstream `datafusion` major, so a 53→54 bump moves
     this from `53.0.0` to `54.0.0`), and
   - every `datafusion` / `datafusion-*` entry in `[workspace.dependencies]`
     to the same new major.

   Per-crate manifests under `crates/` inherit these pins via
   `workspace = true` and need no edit.
2. Update `Cargo.lock` for the datafusion family only — leave unrelated
   transitives at their current pins so PR 2 can address them deliberately.
   List every `datafusion-*` workspace dependency with `-p`:

   ```bash
   cargo update \
     -p datafusion \
     -p datafusion-substrait \
     -p datafusion-proto \
     -p datafusion-ffi \
     -p datafusion-catalog \
     -p datafusion-common \
     -p datafusion-functions-aggregate \
     -p datafusion-functions-window \
     -p datafusion-expr
   ```

   Or pin exact versions with `--precise`, one crate at a time:

   ```bash
   cargo update -p datafusion --precise 54.0.0
   # repeat for each datafusion-* sibling
   ```

   A bare `cargo update` would refresh every transitive crate and blur the
   diff between PR 1 and PR 2.
3. Run the standard build and test commands and address compilation errors,
   API renames, signature changes, and behavior changes:
   - `cargo build`
   - `cargo test`
   - `pytest`
   - `pre-commit run --all-files`
4. Fix only what's needed to restore green CI. Resist the urge to bundle
   unrelated cleanups — those belong in their own PR.
5. If a breaking change in upstream requires a user-facing API change in
   `datafusion-python`, add the `api change` label and document the change
   in the PR description so it surfaces in the changelog.

**Reference PRs:** [#1311](https://github.com/apache/datafusion-python/pull/1311)
(DF51), [#1337](https://github.com/apache/datafusion-python/pull/1337) (DF52).

## PR 2: Consolidate transitive dependencies

**Goal:** after the upstream bump, the dependency tree may have multiple
versions of the same transitive crate (for example, two `arrow` versions, two
`object_store` versions). Reconcile these so we ship a single coherent set.

1. Inspect the lockfile for duplicates:
   ```bash
   cargo tree --duplicates
   ```
2. For each duplicate that matters (Arrow, `object_store`, `parquet`,
   `tokio`, `arrow-flight`, etc.), update our direct dependency declarations
   in `Cargo.toml` to versions compatible with what upstream DataFusion now
   pulls in. The goal is one version of each ecosystem-critical crate.
3. Re-run `cargo update` and re-run the full test matrix. Some duplicates are
   benign (small leaf crates with no FFI surface) and can be left alone if
   reconciliation would force a much larger change. Use judgment.
4. If consolidating forces a behavioral change visible to users (for example,
   a newer `pyarrow`-compatible Arrow version), call it out in the PR
   description.

Keeping this work separate from PR 1 means PR 1 stays a "make it compile"
review and PR 2 stays a "tidy the dependency graph" review.

## PR 3: Fill API and documentation gaps

**Goal:** with the upstream version locked in, identify new APIs that landed
upstream and decide whether to expose them, and update agent-facing
documentation so it still matches the surface we ship.

1. Run the `check-upstream` skill (`.ai/skills/check-upstream/SKILL.md`) to
   diff the upstream Rust API against what's exposed in
   `python/datafusion/`. The skill covers scalar/aggregate/window/table
   functions, `DataFrame` methods, `SessionContext` methods, and FFI types.
   Invoke it from the assistant with `/check-upstream` (optionally scoped to
   one area, e.g. `/check-upstream scalar functions`).
2. For each gap, decide whether to:
   - Expose it now (small, obvious additions can land in this PR).
   - File a tracking issue (anything non-trivial — separate PR per feature
     keeps reviews focused).
   - Skip it (internal-only or already covered by an existing API; record
     the decision in the "Evaluated and not requiring exposure" sections of
     the skill so future runs don't re-flag it).
3. (Optional) Run the `make-pythonic` skill
   (`.ai/skills/make-pythonic/SKILL.md`) over any newly exposed APIs to
   align signatures with the project's Pythonic style (accepting plain
   strings for column names, raw Python values where auto-wrapping
   applies, etc.). Invoke it from the assistant with `/make-pythonic`.
   Running this *before* the audit step means examples in `SKILL.md` get
   updated to the final signature in one pass instead of churning twice.
   Larger reshapes still belong in their own PR.
4. Run the `audit-skill-md` skill (`.ai/skills/audit-skill-md/SKILL.md`) to
   cross-reference the user-facing skill at
   [`skills/datafusion_python/SKILL.md`](../../skills/datafusion_python/SKILL.md)
   against the current public API. The skill flags stale function names,
   missing newly exposed APIs, examples that drifted from idiomatic style,
   and missing version notes. Invoke it from the assistant with
   `/audit-skill-md` (optionally scoped, e.g. `/audit-skill-md dataframe`).
   Apply the resulting edits to `SKILL.md` and to the relevant RST pages
   under `docs/source/user-guide/common-operations/`.
5. If new aggregate or window functions were exposed in step 2, also update:
   - `docs/source/user-guide/common-operations/aggregations.rst`
   - `docs/source/user-guide/common-operations/windows.rst`

## Why three PRs

- **Bisectable.** If a regression appears, `git bisect` lands on the
  responsible PR (compile fix, dependency consolidation, or API addition)
  rather than a single mega-commit.
- **Reviewable.** Each PR has a single concern. Reviewers reading PR 1 don't
  need to also reason about whether new APIs are well-named.
- **Skippable.** Some upstream syncs are pure version bumps with no new APIs
  worth exposing. PR 3 can be empty or merged as a no-op if the audit comes
  back clean.

## Related documents

- [`README.md`](README.md) — the broader release process (this sync work
  feeds into the next official release).
- [`.ai/skills/check-upstream/SKILL.md`](../../.ai/skills/check-upstream/SKILL.md)
  — API coverage audit.
- [`skills/datafusion_python/SKILL.md`](../../skills/datafusion_python/SKILL.md)
  — user-facing agent guide kept in sync via PR 3.
