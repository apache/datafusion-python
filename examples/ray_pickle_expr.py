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

"""Distribute DataFusion expressions to Ray actors.

This example shows the worker-init pattern from the user guide adapted to
Ray's actor model: each actor builds its own :class:`SessionContext`.
Python scalar UDFs travel inside the pickle blob via the Rust-side
``PythonUDFCodec`` — no actor-side pre-registration is required. The
worker context (set via :func:`datafusion.ipc.set_worker_ctx`) is still
useful for aggregate/window UDFs or other registry-only entries; we set
one here to show the pattern.

Prerequisites:
    pip install ray

Run:
    python examples/ray_pickle_expr.py
"""

import pickle

import pyarrow as pa
import ray
from datafusion import SessionContext, col, lit, udf
from datafusion.ipc import set_worker_ctx


def _build_double_udf():
    """Return the demo UDF used by the actors."""
    return udf(
        lambda arr: pa.array([(v.as_py() or 0) * 2 for v in arr]),
        [pa.int64()],
        pa.int64(),
        volatility="immutable",
        name="double",
    )


@ray.remote
class DataFusionWorker:
    """A Ray actor with a private :class:`SessionContext`."""

    def __init__(self) -> None:
        ctx = SessionContext()
        ctx.register_udf(_build_double_udf())
        # The worker context is what Expr.__setstate__ consults when
        # pickled expressions arrive at this actor.
        set_worker_ctx(ctx)
        self._ctx = ctx

    def evaluate(self, expr_blob: bytes, batch_pylist: list[int]) -> list[int]:
        """Unpickle an Expr, run it over an in-memory batch, return results."""
        expr = pickle.loads(expr_blob)
        df = self._ctx.from_pydict({"a": batch_pylist})
        out = df.with_column("result", expr).select("result")
        return out.to_pydict()["result"]


def main() -> None:
    ray.init(ignore_reinit_error=True)

    sender = SessionContext()
    sender.register_udf(_build_double_udf())
    expr = _build_double_udf()(col("a")) + lit(1)
    blob = pickle.dumps(expr)
    print(f"pickled expression: {len(blob)} bytes")

    workers = [DataFusionWorker.remote() for _ in range(2)]
    batches = [[1, 2, 3], [10, 20, 30], [100, 200, 300]]
    futures = [
        workers[i % len(workers)].evaluate.remote(blob, batch)
        for i, batch in enumerate(batches)
    ]
    for batch, result in zip(batches, ray.get(futures), strict=True):
        print(f"input {batch} -> {result}")

    ray.shutdown()


if __name__ == "__main__":
    main()
