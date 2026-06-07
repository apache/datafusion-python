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

"""Rewrite Sphinx / MyST cross-reference roles to Markdown links.

Operates on:
- python/datafusion/*.py docstrings
- docs/source/**/*.md
- docs/source/**/*.ipynb (markdown cells)

Conversions:

    :py:class:`~datafusion.x.Y`        -> [`Y`][datafusion.x.Y]
    :py:func:`~mod.fn`                 -> [`fn`][mod.fn]
    :py:meth:`X.do <X.do>`             -> [`X.do`][X.do]
    {py:class}`~datafusion.x.Y`        -> [`Y`][datafusion.x.Y]
    {py:func}`mod.fn`                  -> [`mod.fn`][mod.fn]
    {py:mod}`mod`                      -> [`mod`][mod]
    {code}`text`                       -> `text`
    {doc}`path/to/page`                -> [path/to/page](path/to/page.md)
    {doc}`Label <path/to/page>`        -> [Label](path/to/page.md)
    {ref}`anchor`                      -> [anchor](anchor)  (best-effort)
    {ref}`Label <anchor>`              -> [Label](anchor)
    (label)=  (alone on a line)        -> removed
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]

ROLE_PATTERNS = [
    # Sphinx RST roles: :py:class:`~mod.Name`, :class:`~mod.Name`, plus
    # the `Name <mod.Name>` long form. Both `py:` and bare role names.
    (
        re.compile(
            r":(?:py:)?(?:class|func|meth|mod|attr|obj|data|exc):`~?\.?([\w.]+)`"
        ),
        lambda m: f"[`{m.group(1).split('.')[-1]}`][{m.group(1)}]",
    ),
    (
        re.compile(
            r":(?:py:)?(?:class|func|meth|mod|attr|obj|data|exc):`([^<`]+)\s*<\.?([\w.]+)>`"
        ),
        lambda m: f"[`{m.group(1).strip()}`][{m.group(2)}]",
    ),
    # MyST roles: {py:class}`~mod.Name` and the bare {class}`~mod.Name` aliases.
    (
        re.compile(
            r"\{(?:py:)?(?:class|func|meth|mod|attr|obj|data|exc)\}`~?\.?([\w.]+)`"
        ),
        lambda m: f"[`{m.group(1).split('.')[-1]}`][{m.group(1)}]",
    ),
    (
        re.compile(
            r"\{(?:py:)?(?:class|func|meth|mod|attr|obj|data|exc)\}`([^<`]+)\s*<\.?([\w.]+)>`"
        ),
        lambda m: f"[`{m.group(1).strip()}`][{m.group(2)}]",
    ),
    # {code}`text` -> `text`
    (re.compile(r"\{code\}`([^`]+)`"), lambda m: f"`{m.group(1)}`"),
    # {doc}`Label <path>` -> [Label](path.md)
    (
        re.compile(r"\{doc\}`([^<`]+)\s*<([^>]+)>`"),
        lambda m: f"[{m.group(1).strip()}]({m.group(2)}.md)",
    ),
    # {doc}`path` -> [path](path.md)
    (re.compile(r"\{doc\}`([^`<]+)`"), lambda m: f"[{m.group(1)}]({m.group(1)}.md)"),
    # {ref}`Label <anchor>` -> [Label](anchor)
    (
        re.compile(r"\{ref\}`([^<`]+)\s*<([^>]+)>`"),
        lambda m: f"[{m.group(1).strip()}]({m.group(2)})",
    ),
    # {ref}`anchor` -> [anchor](anchor)
    (re.compile(r"\{ref\}`([^`<]+)`"), lambda m: f"[{m.group(1)}]({m.group(1)})"),
]

# Drop standalone (label)= anchor lines (MyST cross-reference targets)
ANCHOR_LINE = re.compile(r"^\([a-zA-Z0-9_-]+\)=\s*$", re.MULTILINE)


def rewrite(text: str) -> str:
    for pattern, repl in ROLE_PATTERNS:
        text = pattern.sub(repl, text)
    return ANCHOR_LINE.sub("", text)


def process_file(path: Path, *, dry_run: bool = False) -> int:
    if path.suffix == ".ipynb":
        original = path.read_text()
        nb = json.loads(original)
        changed = False
        for cell in nb.get("cells", []):
            if cell.get("cell_type") != "markdown":
                continue
            old = cell["source"]
            text = "".join(old) if isinstance(old, list) else old
            new = rewrite(text)
            if new != text:
                cell["source"] = new
                changed = True
        if changed and not dry_run:
            path.write_text(json.dumps(nb, indent=1) + "\n")
        return 1 if changed else 0

    original = path.read_text()
    new = rewrite(original)
    if new != original:
        if not dry_run:
            path.write_text(new)
        return 1
    return 0


def main() -> int:
    dry = "--dry-run" in sys.argv
    paths = (
        list((REPO / "python" / "datafusion").rglob("*.py"))
        + list((REPO / "docs" / "source").rglob("*.md"))
        + list((REPO / "docs" / "source").rglob("*.ipynb"))
    )
    changed = 0
    for p in paths:
        changed += process_file(p, dry_run=dry)
    print(f"changed: {changed} files" + (" (dry run)" if dry else ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
