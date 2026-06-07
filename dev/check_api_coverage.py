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

"""Check that every symbol in datafusion.__all__ is documented.

Walks every Markdown file under docs/source/reference/ and collects:

1. The dotted target of every ``::: <dotted.path>`` mkdocstrings directive.
2. Every Markdown heading (``##``, ``###``, etc.).

A ``__all__`` entry is considered documented if its name appears as:

- The leaf of a ``::: <...>`` directive, OR
- The leaf of a ``### name`` heading.

Run from the repo root::

    python dev/check_api_coverage.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
REFERENCE_DIR = REPO_ROOT / "docs" / "source" / "reference"


def collect_documented_names() -> set[str]:
    documented: set[str] = set()
    directive_re = re.compile(r"^:::\s+([A-Za-z0-9_.]+)")
    heading_re = re.compile(r"^#{1,6}\s+([A-Za-z0-9_]+)")
    for md in REFERENCE_DIR.rglob("*.md"):
        if md.stem != "index":
            documented.add(md.stem)
        for line in md.read_text().splitlines():
            m = directive_re.match(line.strip())
            if m:
                dotted = m.group(1)
                documented.add(dotted.split(".")[-1])
                documented.add(dotted)
                continue
            m = heading_re.match(line)
            if m:
                documented.add(m.group(1))
    return documented


def main() -> int:
    sys.path.insert(0, str(REPO_ROOT / "python"))
    import datafusion  # noqa: PLC0415

    documented = collect_documented_names()
    missing = sorted(name for name in datafusion.__all__ if name not in documented)
    if missing:
        print("Undocumented entries in datafusion.__all__:")
        for name in missing:
            print(f"  - {name}")
        print(f"\n{len(missing)} symbol(s) missing from docs/source/reference/")
        return 1
    print(f"All {len(datafusion.__all__)} __all__ entries are documented.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
