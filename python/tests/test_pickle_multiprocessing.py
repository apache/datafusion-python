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

"""Cross-process pickle tests for :class:`Expr`.

Workers run with each :mod:`multiprocessing` start method (``fork``,
``forkserver``, ``spawn``). Python UDFs (scalar, aggregate, window) travel
with the pickled expression and need no worker-side pre-registration.
Worker-side helpers live in ``_pickle_multiprocessing_helpers`` — the
underscore prefix avoids pytest collection so the module imports under
its real name in worker subprocesses.
"""

from __future__ import annotations

import functools
import multiprocessing as mp
import pickle
import sys
from pathlib import Path

import pytest
from datafusion import col, lit

from . import _pickle_multiprocessing_helpers as helpers

# `pytest --import-mode=importlib` (used in CI) does not put the test parent
# directory on `sys.path`; pytest loads `tests` via its own importlib hook.
# multiprocessing forkserver / spawn workers receive only the parent's
# `sys.path` snapshot, not pytest's hook, so they fail to import
# `tests._pickle_multiprocessing_helpers` with `ModuleNotFoundError: No module
# named 'tests'`. Append (not prepend) the parent directory of the `tests`
# package so workers can resolve it the standard way, *without* shadowing the
# installed `datafusion` wheel — the source tree's `python/datafusion/` has
# no `_internal` extension module (that lives in the wheel under
# site-packages), so prepending would break `from datafusion._internal
# import ...`. Fork start method is unaffected (inherits the already-imported
# module object).
_TESTS_PARENT = str(Path(__file__).resolve().parent.parent)
if _TESTS_PARENT not in sys.path:
    sys.path.append(_TESTS_PARENT)


@functools.cache
def _multiprocessing_available() -> tuple[bool, str]:
    """Return (available, reason). Some sandboxed environments deny semaphore
    creation; without semaphores, ``multiprocessing.Pool`` cannot start.

    Cached so the probe Pool only spawns once per session, and only when a
    test in this module is actually about to run — collection-only runs
    (e.g. ``pytest --collect-only`` on the full suite) skip the probe.
    """
    try:
        ctx = mp.get_context("spawn")
        with ctx.Pool(processes=1) as pool:
            pool.map(int, [0])
    except (PermissionError, OSError) as exc:
        return False, f"multiprocessing.Pool unavailable: {exc}"
    return True, ""


@pytest.fixture(autouse=True)
def _skip_if_multiprocessing_unavailable():
    available, reason = _multiprocessing_available()
    if not available:
        pytest.skip(reason)


START_METHODS = [
    pytest.param(
        "fork",
        marks=pytest.mark.skipif(
            sys.platform in ("darwin", "win32"),
            reason="fork start method is not supported on Windows "
            "and unsafe with PyArrow/tokio on macOS",
        ),
    ),
    pytest.param(
        "forkserver",
        marks=pytest.mark.skipif(
            sys.platform == "win32",
            reason="forkserver start method is not supported on Windows",
        ),
    ),
    "spawn",
]


@pytest.mark.parametrize("start_method", START_METHODS)
@pytest.mark.timeout(120)
def test_builtin_pickle_via_pool(start_method):
    """Built-in expressions round-trip in every start method."""
    expr = col("a") + lit(1)
    blob = pickle.dumps(expr)

    ctx = mp.get_context(start_method)
    with ctx.Pool(processes=2) as pool:
        results = pool.map(helpers.unpickle_and_describe, [blob, blob, blob])

    assert all(r == expr.canonical_name() for r in results)


@pytest.mark.parametrize("start_method", START_METHODS)
@pytest.mark.timeout(120)
def test_udf_pickle_self_contained(start_method):
    """Scalar UDF travels inside the proto blob — no worker pre-registration.

    Workers start with no UDF registered. The Rust-side ``PythonUDFCodec``
    reconstructs the UDF from bytes embedded in the pickle blob.
    """
    udf_obj = helpers.make_double_udf()
    expr = udf_obj(col("a"))
    blob = pickle.dumps(expr)

    ctx = mp.get_context(start_method)
    with ctx.Pool(processes=2) as pool:
        results = pool.starmap(
            helpers.unpickle_and_evaluate,
            [(blob, [1, 2, 3]), (blob, [10, 20, 30])],
        )

    assert results[0] == [2, 4, 6]
    assert results[1] == [20, 40, 60]


@pytest.mark.parametrize("start_method", START_METHODS)
@pytest.mark.timeout(120)
def test_closure_capturing_udf_via_pool(start_method):
    """Cloudpickle preserves closure state across the codec boundary."""
    udf_obj = helpers.make_times_seven_udf()
    expr = udf_obj(col("a"))
    blob = pickle.dumps(expr)

    ctx = mp.get_context(start_method)
    with ctx.Pool(processes=2) as pool:
        result = pool.apply(helpers.unpickle_and_evaluate, (blob, [1, 2, 3]))

    assert result == [7, 14, 21]
