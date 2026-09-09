# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Shape checks on the docstrings that ship in the wheel.

These are about *form*, not content: a docstring that has grown into an essay,
a documented API with no example, or a pointer at the guide that the rendered
HTML cannot follow. See "Python Function Docstrings" in ``AGENTS.md``.

Content is checked elsewhere. ``--doctest-modules`` executes the examples, and
Sphinx builds with ``--fail-on-warning`` so a broken ``:ref:`` fails the docs
build. Neither of those notices a docstring that is simply too long, or one
that never had an example to run.
"""

from __future__ import annotations

import ast
import inspect
import pathlib
import re

import datafusion
import pytest
from datafusion import SessionContext, extensions

PACKAGE_ROOT = pathlib.Path(datafusion.__file__).parent

# Above this, a docstring has stopped being a contract and become a
# narrative. Move the argument to a guide page under `docs/source/` and leave
# a one-line pointer. Raising this is not the fix.
#
# Deliberately has no waiver list: at the time of writing the longest
# docstring in the package is `udaf` at 92 lines, which is a legitimately
# long UDF-authoring reference with many worked examples. If a genuinely
# necessary docstring ever exceeds this, prefer splitting the API.
MAX_DOCSTRING_LINES = 95

# An *inclusion* list, to be grown — not an exclusion list to be shrunk.
# Enforcing "every public callable has an example" package-wide is a separate
# project; several hundred callables do not have one yet. These are the
# extension-protocol surface, where an undocumented method is the difference
# between an extension author succeeding and filing an issue.
DOCSTRINGS_REQUIRING_EXAMPLES: list[tuple[str, object]] = [
    *[
        (f"SessionContext.{name}", getattr(SessionContext, name))
        for name in (
            "with_extensions",
            "set_query_planner",
            "with_logical_extension_codec",
            "with_physical_extension_codec",
            "logical_extension_codec_ids",
            "physical_extension_codec_ids",
            "with_python_udf_inlining",
            "__datafusion_codec_id__",
            "__datafusion_logical_extension_codec__",
            "__datafusion_physical_extension_codec__",
            "__datafusion_query_planner__",
        )
    ],
    *[(name, getattr(extensions, name)) for name in extensions.__all__],
]


def _iter_docstrings() -> list[tuple[str, int]]:
    """Yield ``(location, line count)`` for every docstring under ``python/``."""
    found = []
    for path in sorted(PACKAGE_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(
                node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
            ):
                continue
            doc = ast.get_docstring(node, clean=True)
            if not doc:
                continue
            name = getattr(node, "name", "<module>")
            rel = path.relative_to(PACKAGE_ROOT.parent)
            line = getattr(node, "lineno", 1)
            found.append((f"{rel}:{line} {name}", len(doc.splitlines())))
    return found


def test_no_docstring_is_an_essay() -> None:
    """No docstring exceeds the length ceiling."""
    too_long = [
        (where, count)
        for where, count in _iter_docstrings()
        if count > MAX_DOCSTRING_LINES
    ]
    assert not too_long, (
        "These docstrings exceed "
        f"{MAX_DOCSTRING_LINES} lines and have become narrative rather than "
        "contract:\n"
        + "\n".join(f"  {where} — {count} lines" for where, count in too_long)
        + "\n\nMove the argument to a guide page under docs/source/ and leave a "
        "one-line pointer with a :ref:. See 'One canonical home per claim' in "
        "AGENTS.md."
    )


@pytest.mark.parametrize(
    ("name", "obj"),
    DOCSTRINGS_REQUIRING_EXAMPLES,
    ids=[name for name, _ in DOCSTRINGS_REQUIRING_EXAMPLES],
)
def test_extension_api_has_a_doctest(name: str, obj: object) -> None:
    """Every extension-protocol member carries at least one example."""
    doc = inspect.getdoc(obj)
    assert doc, f"{name} has no docstring"
    assert ">>>" in doc, (
        f"{name} has a docstring but no example. Everything on the extension "
        "protocol needs one, even if the realistic usage has to be marked "
        "+SKIP — in which case a runnable example goes above it. See "
        "'Examples that need a compiled extension' in AGENTS.md."
    )


def test_no_dead_pointers_at_the_guide() -> None:
    """A docstring naming the guide must link it, not just mention it."""
    role = re.compile(r":(ref|doc|py:\w+):`")
    dead = []
    for path in sorted(PACKAGE_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(
                node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
            ):
                continue
            doc = ast.get_docstring(node, clean=True)
            if not doc or "guide" not in doc.lower():
                continue
            if role.search(doc):
                continue
            rel = path.relative_to(PACKAGE_ROOT.parent)
            name = getattr(node, "name", "<module>")
            dead.append(f"{rel}:{getattr(node, 'lineno', 1)} {name}")
    assert not dead, (
        "These docstrings send the reader to a guide without a resolvable "
        "link, which is a dead end in the rendered HTML:\n"
        + "\n".join(f"  {where}" for where in dead)
        + "\n\nUse :ref:`some_label` naming the specific section."
    )
