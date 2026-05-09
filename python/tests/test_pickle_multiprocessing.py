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

Worker-side helpers live in :mod:`tests._pickle_multiprocessing_helpers`
rather than in this test module, so spawn workers can resolve them by their
real dotted name regardless of how pytest imported the test module.
"""

from __future__ import annotations

import multiprocessing as mp

from datafusion import col, lit

from tests._pickle_multiprocessing_helpers import (
    apply_builtin_expr,
    apply_udf_expr,
    build_add_ten_udf,
    register_udf_on_global_ctx,
)


def test_builtin_expr_through_multiprocessing_pool() -> None:
    """A built-in ``Expr`` survives a real ``multiprocessing.Pool`` dispatch."""
    spawn_ctx = mp.get_context("spawn")
    expr = (col("a") * lit(2)) + lit(1)
    chunks = [[1, 2, 3], [10, 20, 30]]

    with spawn_ctx.Pool(processes=2) as pool:
        results = pool.map(apply_builtin_expr, [(expr, c) for c in chunks])

    assert results == [[3, 5, 7], [21, 41, 61]]


def test_udf_expr_through_multiprocessing_pool() -> None:
    """A UDF-backed ``Expr`` survives ``Pool.map`` when the worker registers
    the UDF on its global context via the Pool initializer."""
    spawn_ctx = mp.get_context("spawn")
    add_ten = build_add_ten_udf()
    expr = add_ten(col("a"))
    chunks = [[1, 2, 3], [10, 20, 30]]

    with spawn_ctx.Pool(processes=2, initializer=register_udf_on_global_ctx) as pool:
        results = pool.map(apply_udf_expr, [(expr, c) for c in chunks])

    assert results == [[11, 12, 13], [20, 30, 40]]
