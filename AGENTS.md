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

# Agent Instructions

This project uses AI agent skills stored in `.ai/skills/`. Each skill is a directory containing a `SKILL.md` file with instructions for performing a specific task.

Skills follow the [Agent Skills](https://agentskills.io) open standard. Each skill directory contains:

- `SKILL.md` — The skill definition with YAML frontmatter (name, description, argument-hint) and detailed instructions.
- Additional supporting files as needed.

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
