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

import pytest
from datafusion import col, lit

from . import _pickle_multiprocessing_helpers as helpers


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
            sys.platform == "darwin",
            reason="fork start method is unsafe with PyArrow/tokio on macOS",
        ),
    ),
    "forkserver",
    "spawn",
]


@pytest.mark.parametrize("start_method", START_METHODS)
@pytest.mark.timeout(120)
def test_builtin_pickle_via_pool(start_method):
    """Built-in expressions round-trip in every start method."""
    helpers._diag(f"test_builtin_pickle_via_pool[{start_method}]: enter")
    expr = col("a") + lit(1)
    blob = pickle.dumps(expr)

    ctx = mp.get_context(start_method)
    helpers._diag(f"test_builtin_pickle_via_pool[{start_method}]: creating Pool")
    with ctx.Pool(processes=2, initializer=helpers.diag_init) as pool:
        helpers._diag(f"test_builtin_pickle_via_pool[{start_method}]: pool ready, map")
        results = pool.map(helpers.unpickle_and_describe, [blob, blob, blob])
    helpers._diag(f"test_builtin_pickle_via_pool[{start_method}]: pool closed")

    assert all(r == expr.canonical_name() for r in results)


@pytest.mark.parametrize("start_method", START_METHODS)
@pytest.mark.timeout(120)
def test_udf_pickle_self_contained(start_method):
    """Scalar UDF travels inside the proto blob — no worker pre-registration.

    Workers start with no UDF registered. The Rust-side ``PythonUDFCodec``
    reconstructs the UDF from bytes embedded in the pickle blob.
    """
    helpers._diag(f"test_udf_pickle_self_contained[{start_method}]: enter")
    udf_obj = helpers.make_double_udf()
    expr = udf_obj(col("a"))
    blob = pickle.dumps(expr)

    ctx = mp.get_context(start_method)
    helpers._diag(f"test_udf_pickle_self_contained[{start_method}]: creating Pool")
    with ctx.Pool(processes=2, initializer=helpers.diag_init) as pool:
        helpers._diag(
            f"test_udf_pickle_self_contained[{start_method}]: pool ready, starmap"
        )
        results = pool.starmap(
            helpers.unpickle_and_evaluate,
            [(blob, [1, 2, 3]), (blob, [10, 20, 30])],
        )
    helpers._diag(f"test_udf_pickle_self_contained[{start_method}]: pool closed")

    assert results[0] == [2, 4, 6]
    assert results[1] == [20, 40, 60]


@pytest.mark.parametrize("start_method", START_METHODS)
@pytest.mark.timeout(120)
def test_closure_capturing_udf_via_pool(start_method):
    """Cloudpickle preserves closure state across the codec boundary."""
    helpers._diag(f"test_closure_capturing_udf_via_pool[{start_method}]: enter")
    udf_obj = helpers.make_times_seven_udf()
    expr = udf_obj(col("a"))
    blob = pickle.dumps(expr)

    ctx = mp.get_context(start_method)
    helpers._diag(f"test_closure_capturing_udf_via_pool[{start_method}]: creating Pool")
    with ctx.Pool(processes=2, initializer=helpers.diag_init) as pool:
        helpers._diag(
            f"test_closure_capturing_udf_via_pool[{start_method}]: pool ready, apply"
        )
        result = pool.apply(helpers.unpickle_and_evaluate, (blob, [1, 2, 3]))
    helpers._diag(f"test_closure_capturing_udf_via_pool[{start_method}]: pool closed")

    assert result == [7, 14, 21]
