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

# The leading underscore is load-bearing: pytest with --import-mode=importlib
# (used in CI) assigns synthetic module names to test modules, which breaks
# subprocess imports during multiprocessing. An underscore-prefixed module is
# not collected as a test module, so it imports under its normal __name__
# inside worker processes.

from __future__ import annotations

import pyarrow as pa
from datafusion import SessionContext, udf
from datafusion.ipc import clear_worker_ctx, set_worker_ctx


def make_double_udf():
    """Build the canonical UDF used in the multiprocessing tests."""
    return udf(
        lambda arr: pa.array([(v.as_py() or 0) * 2 for v in arr]),
        [pa.int64()],
        pa.int64(),
        volatility="immutable",
        name="double",
    )


def make_times_seven_udf():
    """Closure-capturing UDF — verifies cloudpickle preserves closed-over state."""
    multiplier = 7

    def fn(arr):
        return pa.array([(v.as_py() or 0) * multiplier for v in arr])

    return udf(
        fn,
        [pa.int64()],
        pa.int64(),
        volatility="immutable",
        name="times_seven",
    )


def init_worker_empty():
    """Pool initializer: install an empty SessionContext (no UDFs)."""
    set_worker_ctx(SessionContext())


def init_worker_clear():
    """Pool initializer: explicitly clear any prior worker context."""
    clear_worker_ctx()


def unpickle_and_describe(blob: bytes) -> str:
    """Unpickle a proto-bytes blob and return its canonical name."""
    import pickle

    expr = pickle.loads(blob)  # noqa: S301
    return expr.canonical_name()


def unpickle_and_evaluate(blob: bytes, batch: list[int]) -> list[int]:
    """Unpickle an expression and evaluate it against an in-memory batch.

    Returns the result column as a Python list. Used to verify that
    cloudpickled UDFs (including closure state) execute correctly in
    a fresh worker process.
    """
    import pickle

    expr = pickle.loads(blob)  # noqa: S301
    ctx = SessionContext()
    df = ctx.from_pydict({"a": batch})
    out = df.with_column("result", expr).select("result")
    return out.to_pydict()["result"]
