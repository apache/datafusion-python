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

For background — the shipped-expression model, what travels inline vs
by name, portability requirements, and the security threat model —
see ``docs/source/user-guide/distributing-work.rst``.

Build an expression in the driver, ship it to a pool of Ray actors, and
have each actor evaluate it against its own slice of data. Python UDFs
travel with the shipped expression — no actor-side registration needed.

Prerequisites:
    pip install ray

Run:
    python examples/ray_pickle_expr.py
"""

import pyarrow as pa
import ray
from datafusion import Expr, SessionContext, col, lit, udf


def _build_double_udf():
    """Return the demo UDF used by the driver."""
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
        self._ctx = SessionContext()

    def evaluate(self, expr: Expr, batch_pylist: list[int]) -> list[int]:
        """Run the expression against an in-memory batch."""
        # `expr` arrived here via Ray's automatic argument serialization;
        # the Python UDF inside it was reconstructed from the bytes — no
        # pre-registration on this actor required.
        df = self._ctx.from_pydict({"a": batch_pylist})
        out = df.with_column("result", expr).select("result")
        return out.to_pydict()["result"]


def main() -> None:
    ray.init(ignore_reinit_error=True)

    expr = _build_double_udf()(col("a")) + lit(1)

    workers = [DataFusionWorker.remote() for _ in range(2)]
    batches = [[1, 2, 3], [10, 20, 30], [100, 200, 300]]
    futures = [
        workers[i % len(workers)].evaluate.remote(expr, batch)
        for i, batch in enumerate(batches)
    ]
    for batch, result in zip(batches, ray.get(futures), strict=True):
        print(f"input {batch} -> {result}")

    ray.shutdown()


if __name__ == "__main__":
    main()
