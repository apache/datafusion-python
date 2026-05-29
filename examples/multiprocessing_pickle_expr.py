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

"""Distribute different DataFusion expressions to worker processes.

For background — the shipped-expression model, what travels inline vs
by name, portability requirements, and the security threat model —
see ``docs/source/user-guide/distributing-work.rst``.

Builds a list of parametric expressions in the driver — each closing
over a different threshold value — ships one per worker via
``multiprocessing.Pool``, and collects the results back. The closure
state forces the cloudpickle path (a by-name registration would lose
the captured threshold), so this is a real test of the expression-
pickling story rather than a same-expression fan-out.

Worker layout:

* Each worker receives a different ``(label, expr)`` task.
* Each worker materializes the shared dataset locally and runs its
  own expression against it.
* The result and the worker's PID travel back to the driver, so the
  output makes it visible that the work was spread across processes.

Run:
    python examples/multiprocessing_pickle_expr.py
"""

from __future__ import annotations

import multiprocessing as mp
import os

import pyarrow as pa
from datafusion import Expr, SessionContext, col, udaf, udf
from datafusion import functions as F
from datafusion.user_defined import Accumulator, AggregateUDF, ScalarUDF

# A shared input dataset. In a production pipeline this would live on
# object storage; here we hand-roll a small batch so the example runs
# without any I/O setup.
DATASET = {
    "value": [3, 17, 42, 5, 88, 21, 9, 56, 4, 73, 12, 31],
}


def make_above_threshold_udf(threshold: int) -> ScalarUDF:
    """Build a scalar UDF that returns 1 where ``value > threshold`` else 0.

    The threshold is captured in the closure, so cloudpickle has to
    walk into the function body to ship the value across processes —
    a by-name registration on the worker would collapse every
    threshold into the same callable and lose the per-task state.
    """

    def above(arr: pa.Array) -> pa.Array:
        # `v.as_py() or 0` coerces nulls to 0 — the demo dataset has no
        # nulls, but real-world code should decide explicitly how nulls
        # compare against the threshold.
        return pa.array([1 if (v.as_py() or 0) > threshold else 0 for v in arr])

    return udf(
        above,
        [pa.int64()],
        pa.int64(),
        volatility="immutable",
        name=f"above_{threshold}",
    )


class _SumAccumulator(Accumulator):
    """Tiny aggregate UDF state used to demonstrate UDAFs travel too."""

    def __init__(self) -> None:
        self._total = 0

    def state(self) -> list[pa.Scalar]:
        return [pa.scalar(self._total, type=pa.int64())]

    def update(self, values: pa.Array) -> None:
        for v in values:
            self._total += v.as_py() or 0

    def merge(self, states: list[pa.Array]) -> None:
        for s in states:
            self._total += s[0].as_py()

    def evaluate(self) -> pa.Scalar:
        return pa.scalar(self._total, type=pa.int64())


def _build_sum_udaf() -> AggregateUDF:
    return udaf(
        _SumAccumulator,
        [pa.int64()],
        pa.int64(),
        [pa.int64()],
        "immutable",
        name="my_sum",
    )


def evaluate_in_worker(task: tuple[str, Expr]) -> tuple[str, int, int]:
    """Run one expression against the shared dataset.

    ``task`` arrived here via the pool's automatic pickling. The Python
    callable inside the expression (including its captured threshold)
    was reconstructed by the codec — the worker did not have to
    register anything before this call.
    """
    label, expr = task
    ctx = SessionContext()
    df = ctx.from_pydict(DATASET)
    # ``expr`` is an aggregate over the whole batch; ``aggregate`` keeps
    # a single row of output, which we read as a Python int.
    result_df = df.aggregate([], [expr.alias("result")])
    result = result_df.to_pydict()["result"][0]
    return label, result, os.getpid()


def build_tasks() -> list[tuple[str, Expr]]:
    """Return ``(label, expr)`` pairs — one task per worker invocation.

    Mixes scalar-UDF-in-aggregate and pure-aggregate work to show both
    UDF kinds round-tripping through pickle.
    """
    sum_udaf = _build_sum_udaf()
    tasks: list[tuple[str, Expr]] = []

    # Three "count values strictly above threshold T" tasks built from
    # closure-capturing scalar UDFs.
    for threshold in (10, 30, 60):
        above_udf = make_above_threshold_udf(threshold)
        tasks.append((f"count_above_{threshold}", F.sum(above_udf(col("value")))))

    # One pure aggregate UDF task.
    tasks.append(("custom_sum", sum_udaf(col("value"))))

    return tasks


def main() -> None:
    tasks = build_tasks()

    # ``forkserver`` works on every POSIX platform and is the Python 3.14
    # default for POSIX. ``spawn`` would also work; ``fork`` is unsafe
    # with pyarrow/tokio on macOS.
    mp_ctx = mp.get_context("forkserver")
    with mp_ctx.Pool(processes=min(4, len(tasks))) as pool:
        results = pool.map(evaluate_in_worker, tasks)

    print(f"driver pid: {os.getpid()}")
    for label, value, worker_pid in results:
        print(f"  [{label:>16}] = {value:>6}   (worker pid: {worker_pid})")


if __name__ == "__main__":
    main()
