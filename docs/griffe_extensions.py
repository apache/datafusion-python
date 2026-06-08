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

"""Griffe extensions for datafusion-python docs.

`SphinxRefsToAutorefs` rewrites sphinx-style cross-reference roles
(``:func:`~path`, :class:`~path``, etc.) inside docstrings into
mkdocstrings autoref syntax (``[`tail`][path]``) so that the same
docstring renders as a clickable cross-reference both in JetBrains-style
IDEs (which understand sphinx roles) and on the published docs site
(which understands mkdocstrings autorefs).
"""

from __future__ import annotations

import re
from typing import Any

from griffe import Extension, Object

_ROLE_RE = re.compile(
    r":(?:py:)?(?P<role>func|class|meth|attr|mod|obj|exc|const|data)"
    r":`(?P<tilde>~?)(?P<target>[\w.]+)`"
)


def _rewrite(text: str) -> str:
    def repl(match: re.Match[str]) -> str:
        target = match.group("target")
        tail = target.rsplit(".", 1)[-1]
        return f"[`{tail}`][{target}]"

    return _ROLE_RE.sub(repl, text)


class SphinxRefsToAutorefs(Extension):
    """Convert sphinx-style cross-references into mkdocstrings autorefs."""

    def on_object(self, *, obj: Object, **_: Any) -> None:
        docstring = obj.docstring
        if docstring is None:
            return
        new = _rewrite(docstring.value)
        if new != docstring.value:
            docstring.value = new
