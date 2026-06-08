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

"""Drift guards between the public API surface and the mkdocs reference site.

Two checks:

1. Every symbol in ``datafusion.__all__`` is covered by an ``mkdocstrings``
   ``:::`` directive somewhere under ``docs/source/reference/``. Coverage may
   be direct (``::: datafusion.dataframe.DataFrame``) or by whole-module
   autodoc (``::: datafusion.functions``).
2. Every ``:::`` directive in the reference pages resolves to a real,
   importable Python object. Renames or removals that orphan a stub fail the
   suite instead of silently producing an empty doc page.
"""

from __future__ import annotations

import importlib
import inspect
import re
from pathlib import Path

import datafusion

REF_DIR = Path(__file__).resolve().parents[2] / "docs" / "source" / "reference"
_DIRECTIVE_RE = re.compile(r"^:::\s+(\S+)\s*$", re.MULTILINE)


def _all_directives() -> set[str]:
    paths: set[str] = set()
    for md in REF_DIR.rglob("*.md"):
        paths.update(_DIRECTIVE_RE.findall(md.read_text()))
    return paths


def _is_covered(qual: str, directives: set[str]) -> bool:
    if qual in directives:
        return True
    parent = qual.rsplit(".", 1)[0] if "." in qual else None
    return parent in directives if parent else False


def test_public_api_documented() -> None:
    directives = _all_directives()
    assert directives, f"no :::  directives found under {REF_DIR}"

    missing: list[str] = []
    for name in datafusion.__all__:
        obj = getattr(datafusion, name)
        if inspect.ismodule(obj):
            mod_name = obj.__name__
            if mod_name in directives or any(
                d.startswith(mod_name + ".") for d in directives
            ):
                continue
            missing.append(f"{name} (module {mod_name})")
            continue

        module = getattr(obj, "__module__", None) or "datafusion"
        qual = f"{module}.{name}"
        if _is_covered(qual, directives) or module in directives:
            continue
        missing.append(f"{name} (expected '::: {qual}' or '::: {module}')")

    assert not missing, (
        "Public API symbols missing from docs/source/reference/*.md:\n  "
        + "\n  ".join(missing)
    )


def test_directive_targets_resolve() -> None:
    bad: list[str] = []
    for path in sorted(_all_directives()):
        parts = path.split(".")
        obj = None
        remainder: list[str] = []
        for i in range(len(parts), 0, -1):
            try:
                obj = importlib.import_module(".".join(parts[:i]))
                remainder = parts[i:]
                break
            except ImportError:
                continue
        if obj is None:
            bad.append(f"{path} (no importable prefix)")
            continue
        try:
            for attr in remainder:
                obj = getattr(obj, attr)
        except AttributeError:
            bad.append(f"{path} (attribute chain broken)")

    assert not bad, (
        "Doc ::: directives reference symbols that no longer exist:\n  "
        + "\n  ".join(bad)
    )
