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

"""Pytest configuration for doctest namespace injection."""

import sys
from pathlib import Path

# Ensure ``python/`` is reachable by ``import``. The ``tests`` package lives at
# ``python/tests`` and spawn-based multiprocessing tests need workers to be
# able to resolve ``tests._pickle_multiprocessing_helpers`` by its real dotted
# name when unpickling task args. Editable installs add this path via a .pth
# file, but the wheel install used in CI does not, which led to spawn workers
# dying with ``ModuleNotFoundError`` and ``Pool.map`` hanging.
#
# Append (don't prepend) so the wheel-installed ``datafusion`` in
# site-packages still wins over the source tree at ``python/datafusion`` —
# the source tree has no compiled ``_internal`` module on a fresh checkout.
_python_dir = str(Path(__file__).parent / "python")
if _python_dir not in sys.path:
    sys.path.append(_python_dir)

import datafusion as dfn  # noqa: E402
import numpy as np  # noqa: E402
import pyarrow as pa  # noqa: E402
import pytest  # noqa: E402
from datafusion import col, lit  # noqa: E402
from datafusion import functions as F  # noqa: E402


@pytest.fixture(autouse=True)
def _doctest_namespace(doctest_namespace: dict) -> None:
    """Add common imports to the doctest namespace."""
    doctest_namespace["dfn"] = dfn
    doctest_namespace["np"] = np
    doctest_namespace["pa"] = pa
    doctest_namespace["col"] = col
    doctest_namespace["lit"] = lit
    doctest_namespace["F"] = F
