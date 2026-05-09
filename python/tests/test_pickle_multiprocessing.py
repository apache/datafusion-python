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

"""End-to-end multiprocessing tests for :py:class:`datafusion.Expr` pickling.

Motivation: users want to fan work out across processes
via :py:mod:`multiprocessing` and pass an :py:class:`Expr` to each worker.

The ``spawn`` start method is forced so each worker is a fresh interpreter and
the ``Expr`` argument is genuinely sent through pickle. Under ``fork`` the
child inherits the parent's address space and would never exercise the wire
format.

Two scenarios are covered:

1. A built-in expression — the workers can
   resolve everything against a default :py:class:`SessionContext`.
2. A custom Python UDF — the worker must register the UDF on its global
   context *before* unpickling, since ``Expr.__setstate__`` resolves
   function references by name against the global context.
"""

from __future__ import annotations

import multiprocessing as mp

import pyarrow as pa
import pyarrow.compute as pc
from datafusion import SessionContext, col, lit, udf

# Module-scope helpers — must be importable by name so the `spawn` workers
# can resolve them after re-importing this module.

_UDF_NAME = "mp_pickle_add_ten"


def _add_ten_impl(array: pa.Array) -> pa.Array:
    return pc.add(array, 10)


def _build_add_ten_udf():
    return udf(
        _add_ten_impl,
        [pa.int64()],
        pa.int64(),
        volatility="immutable",
        name=_UDF_NAME,
    )


def _register_udf_on_global_ctx() -> None:
    """Pool initializer: install a global ctx in the worker that knows the UDF.

    ``Expr.__setstate__`` resolves UDF references by name against the
    *global* context, so the registration must happen before any task arg is
    unpickled — i.e. in the Pool's ``initializer``, not in the task body.
    """
    ctx = SessionContext()
    ctx.register_udf(_build_add_ten_udf())
    ctx.set_as_global()


def _apply_builtin_expr(args: tuple) -> list:
    expr, values = args
    ctx = SessionContext()
    batch = pa.RecordBatch.from_arrays([pa.array(values, type=pa.int64())], names=["a"])
    df = ctx.create_dataframe([[batch]], name="t")
    return df.select(expr.alias("out")).collect()[0].column(0).to_pylist()


def _apply_udf_expr(args: tuple) -> list:
    expr, values = args
    # Reuse the worker's global ctx so the UDF registered by the initializer
    # is visible during execution as well as during arg unpickling.
    ctx = SessionContext.global_ctx()
    batch = pa.RecordBatch.from_arrays([pa.array(values, type=pa.int64())], names=["a"])
    df = ctx.create_dataframe([[batch]], name="t_udf")
    return df.select(expr.alias("out")).collect()[0].column(0).to_pylist()


def test_builtin_expr_through_multiprocessing_pool() -> None:
    """A built-in ``Expr`` survives a real ``multiprocessing.Pool`` dispatch."""
    spawn_ctx = mp.get_context("spawn")
    expr = (col("a") * lit(2)) + lit(1)
    chunks = [[1, 2, 3], [10, 20, 30]]

    with spawn_ctx.Pool(processes=2) as pool:
        results = pool.map(_apply_builtin_expr, [(expr, c) for c in chunks])

    assert results == [[3, 5, 7], [21, 41, 61]]


def test_udf_expr_through_multiprocessing_pool() -> None:
    """A UDF-backed ``Expr`` survives ``Pool.map`` when the worker registers
    the UDF on its global context via the Pool initializer."""
    spawn_ctx = mp.get_context("spawn")
    add_ten = _build_add_ten_udf()
    expr = add_ten(col("a"))
    chunks = [[1, 2, 3], [10, 20, 30]]

    with spawn_ctx.Pool(processes=2, initializer=_register_udf_on_global_ctx) as pool:
        results = pool.map(_apply_udf_expr, [(expr, c) for c in chunks])

    assert results == [[11, 12, 13], [20, 30, 40]]
