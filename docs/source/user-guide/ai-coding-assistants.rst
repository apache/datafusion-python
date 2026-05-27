.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

Using AI Coding Assistants
==========================

If you write DataFusion Python code with an AI coding assistant, this
project ships machine-readable guidance so the assistant produces
idiomatic code rather than guessing from its training data.

What is published
-----------------

- `SKILL.md <https://github.com/apache/datafusion-python/blob/main/skills/datafusion_python/SKILL.md>`_ —
  a dense, skill-oriented reference covering imports, data loading,
  DataFrame operations, expression building, SQL-to-DataFrame mappings,
  idiomatic patterns, and common pitfalls. Follows the
  `Agent Skills <https://agentskills.io>`_ open standard.
- `llms.txt <https://datafusion.apache.org/python/llms.txt>`_ — an entry point for LLM-based tools following the
  `llmstxt.org <https://llmstxt.org>`_ convention. Categorized links to the
  skill, user guide, API reference, and examples.

Both files live at stable URLs so an agent can discover them without a
checkout of the repo.

Installing the skill
--------------------

**Preferred:** run

.. code-block:: shell

    npx skills add apache/datafusion-python

This installs the skill in any supported agent on your machine (Claude
Code, Cursor, Windsurf, Cline, Codex, Copilot, Gemini CLI, and others).
The command writes the pointer into the agent's configuration so that any
project you open that uses DataFusion Python picks up the skill
automatically.

**Manual:** if you are not using the ``skills`` registry, paste this
single line into your project's ``AGENTS.md`` or ``CLAUDE.md``::

    For DataFusion Python code, see https://github.com/apache/datafusion-python/blob/main/skills/datafusion_python/SKILL.md

Most assistants resolve that pointer the first time they see a
DataFusion-related prompt in the project.

What the skill covers
---------------------

Writing DataFusion Python code has a handful of conventions that are easy
for a model to miss — bitwise ``&`` / ``|`` / ``~`` instead of Python
``and`` / ``or`` / ``not``, the lazy-DataFrame immutability model, how
window functions replace SQL correlated subqueries, the ``case`` /
``when`` builder syntax, and the ``in_list`` / ``array_position`` options
for membership tests. The skill enumerates each of these with short,
copyable examples.

It is *not* a replacement for this user guide. Think of it as a distilled
reference the assistant keeps open while it writes code for you.

If you are an agent author
--------------------------

The skill file and ``llms.txt`` are the two supported integration
points. Both are versioned along with the release and follow open
standards — no project-specific handshake is required.
