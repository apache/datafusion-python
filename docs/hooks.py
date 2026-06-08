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

"""MkDocs hooks for datafusion-python docs.

Auto-injects a shared `markdown-exec` setup block at the top of every
user-guide page that contains executable Python code blocks. Page
authors write `.md` files with just prose plus `python exec="1"
session="..."` fences — they never have to copy/paste the shared
imports, `chdir`, or formatter configuration.

The injected block uses the same session slug as the first executable
block on the page, so its imports and `chdir` carry over to the rest of
the page through markdown-exec's per-session globals.
"""

from __future__ import annotations

import re
from typing import Any

# Matches `python exec="1" ... session="<slug>"` (slug captured).
# Tolerates attribute reordering.
_EXEC_FENCE = re.compile(
    r'```python\s+[^\n]*?exec="1"[^\n]*?session="(?P<slug>[\w-]+)"',
)

_SETUP_TEMPLATE = """```python exec="1" session="{slug}"
import os
import pathlib

import datafusion  # noqa: F401
import datafusion.dataframe
from datafusion import (  # noqa: F401
    SessionContext,
    col,
    column,
    lit,
    literal,
)
from datafusion import functions as f  # noqa: F401
from datafusion.dataframe_formatter import configure_formatter

# mkdocs build runs from the repo root; mkdocs serve from `docs/`. Walk
# the local candidates to find the demo data so pages resolve
# `pokemon.csv` regardless of which one is in use.
for _candidate in ("docs/source", "source", "."):
    _p = pathlib.Path(_candidate)
    if (_p / "pokemon.csv").exists():
        os.chdir(_p)
        break

configure_formatter(max_rows=10, show_truncation_message=False)


# `DataFrame.show()` writes through Rust's libc stdout (fd 1), bypassing
# Python's `sys.stdout` redirect that markdown-exec installs. Override
# it to route through Python's print() so the table appears in the
# captured output. The Python `__repr__` produces the same ASCII table.
def _show(self, *_args, **_kwargs):
    print(self)


datafusion.dataframe.DataFrame.show = _show


# `DataFrame.__repr__` appends a literal "Data truncated." footer that
# the HTML-side `show_truncation_message=False` option does not affect.
# Strip it so the rendered docs do not advertise truncation on every
# example DataFrame.
_orig_repr = datafusion.dataframe.DataFrame.__repr__


def _repr(self):
    text = _orig_repr(self).rstrip()
    if text.endswith("Data truncated."):
        text = text[: -len("Data truncated.")].rstrip()
    return text


datafusion.dataframe.DataFrame.__repr__ = _repr
```
"""


def on_page_markdown(markdown: str, **_: Any) -> str:
    """Prepend a setup `markdown-exec` block when the page uses code execution."""
    match = _EXEC_FENCE.search(markdown)
    if match is None:
        return markdown
    return _SETUP_TEMPLATE.format(slug=match.group("slug")) + "\n" + markdown
