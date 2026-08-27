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

# Agent Instructions for Contributors

This file is for agents working **on** the datafusion-python project (developing,
testing, reviewing). If you need to **use** the DataFusion DataFrame API (write
queries, build expressions, understand available functions), see the user-facing
skill at [`SKILL.md`](skills/datafusion_python/SKILL.md).

## Skills

This project uses AI agent skills stored in `.ai/skills/`. Each skill is a directory containing a `SKILL.md` file with instructions for performing a specific task.

Skills follow the [Agent Skills](https://agentskills.io) open standard. Each skill directory contains:

- `SKILL.md` — The skill definition with YAML frontmatter (name, description, argument-hint) and detailed instructions.
- Additional supporting files as needed.

To discover what skills are available, list `.ai/skills/` and read each
`SKILL.md`. The frontmatter `name` and `description` fields summarize the
skill's purpose. Some descriptions begin with `TRIGGER —`; those are not tasks
to run on request but conventions to read *before* writing code that meets the
stated condition.

## FFI Capsule Protocol

Before adding or changing any `__datafusion_*__` capsule getter, read
[`.ai/skills/ffi-capsule-protocol/SKILL.md`](.ai/skills/ffi-capsule-protocol/SKILL.md)
and enumerate the existing family:

```bash
grep -rn "__datafusion_[a-z_]*__" --include="*.rs" crates/ examples/*/src/
```

These methods are one protocol with a settled convention: a getter receives the
`SessionContext` it is being installed on and takes what the FFI constructor
needs from that session. Never construct a `SessionContext` inside an extension
library to satisfy a `TaskContextProvider` or codec parameter.

## Documentation Sources

Search and edit `docs/source/`. `docs/temp/` is gitignored build output that
`grep -r` will surface with stale copies of the same pages.

Before changing a public API, check
`docs/source/user-guide/upgrade-guides.md` for how the same API family was
migrated previously. Follow the established pattern rather than inventing one.

## Pull Requests

Every pull request must follow the template in
`.github/pull_request_template.md`. The description must include these sections:

1. **Which issue does this PR close?** — Link the issue with `Closes #NNN`.
2. **Rationale for this change** — Why the change is needed (skip if the issue
   already explains it clearly).
3. **What changes are included in this PR?** — Summarize the individual changes.
4. **Are there any user-facing changes?** — Note any changes visible to users
   (new APIs, changed behavior, new files shipped in the package, etc.). If
   there are breaking changes to public APIs, add the `api change` label **and
   add a section to `docs/source/user-guide/upgrade-guides.md`** showing the
   before and after. This applies to FFI hook method signatures, which
   extension libraries implement.

## Pre-commit Checks

Always run pre-commit checks **before** committing. The hooks are defined in
`.pre-commit-config.yaml` and run automatically on `git commit` if pre-commit
is installed as a git hook. To run all hooks manually:

```bash
pre-commit run --all-files
```

Fix any failures before committing.

## Python Function Docstrings

Every Python function must include a docstring with usage examples.

- **Examples are required**: Each function needs at least one doctest-style example
  demonstrating basic usage.
- **Optional parameters**: If a function has optional parameters, include separate
  examples that show usage both without and with the optional arguments. Pass
  optional arguments using their keyword name (e.g., `step=dfn.lit(3)`) so readers
  can immediately see which parameter is being demonstrated.
- **Reuse input data**: Use the same input data across examples wherever possible.
  The examples should demonstrate how different optional arguments change the output
  for the same input, making the effect of each option easy to understand.
- **Alias functions**: Functions that are simple aliases (e.g., `list_sort` aliasing
  `array_sort`) only need a one-line description and a `See Also` reference to the
  primary function. They do not need their own examples.

## Aggregate and Window Function Documentation

When adding or updating an aggregate or window function, ensure the corresponding
site documentation is kept in sync:

- **Aggregations**: `docs/source/user-guide/common-operations/aggregations.md` —
  add new aggregate functions to the "Aggregate Functions" list and include usage
  examples if appropriate.
- **Window functions**: `docs/source/user-guide/common-operations/windows.md` —
  add new window functions to the "Available Functions" list and include usage
  examples if appropriate.
