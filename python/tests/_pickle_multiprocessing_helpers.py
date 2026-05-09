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

"""Helpers for :mod:`test_pickle_multiprocessing`.

Spawn workers re-import the module that defines a pickled function by the
function's ``__module__`` attribute. Pytest's ``--import-mode=importlib``
loads test modules under synthetic names that the worker cannot resolve via
the normal import machinery, which can cause ``Pool.map`` to hang waiting
for a worker that died during unpickling.

Keeping the helpers in this regular (non-test) module side-steps that: it
is importable under its real dotted name (``tests._pickle_multiprocessing_helpers``)
in both parent and worker, and the leading underscore keeps pytest from
collecting it as a test module.
"""

from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc
from datafusion import SessionContext, udf

UDF_NAME = "mp_pickle_add_ten"


def add_ten_impl(array: pa.Array) -> pa.Array:
    return pc.add(array, 10)


def build_add_ten_udf():
    return udf(
        add_ten_impl,
        [pa.int64()],
        pa.int64(),
        volatility="immutable",
        name=UDF_NAME,
    )


def register_udf_on_global_ctx() -> None:
    """Pool initializer: install a global ctx in the worker that knows the UDF.

    ``Expr.__setstate__`` resolves UDF references by name against the
    *global* context, so the registration must happen before any task arg is
    unpickled — i.e. in the Pool's ``initializer``, not in the task body.
    """
    ctx = SessionContext()
    ctx.register_udf(build_add_ten_udf())
    ctx.set_as_global()


def apply_builtin_expr(args: tuple) -> list:
    expr, values = args
    ctx = SessionContext()
    batch = pa.RecordBatch.from_arrays([pa.array(values, type=pa.int64())], names=["a"])
    df = ctx.create_dataframe([[batch]], name="t")
    return df.select(expr.alias("out")).collect()[0].column(0).to_pylist()


def apply_udf_expr(args: tuple) -> list:
    expr, values = args
    # Reuse the worker's global ctx so the UDF registered by the initializer
    # is visible during execution as well as during arg unpickling.
    ctx = SessionContext.global_ctx()
    batch = pa.RecordBatch.from_arrays([pa.array(values, type=pa.int64())], names=["a"])
    df = ctx.create_dataframe([[batch]], name="t_udf")
    return df.select(expr.alias("out")).collect()[0].column(0).to_pylist()
