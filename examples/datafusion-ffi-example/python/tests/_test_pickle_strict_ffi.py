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

"""Strict-mode Expr round-trip with an FFI-capsule scalar UDF.

Verifies the by-name path: an FFI-imported UDF (no
``PythonFunctionScalarUDF`` downcast on the codec) serializes by name
and resolves from the receiver's function registry on decode. Covers
both the explicit ``Expr.to_bytes(ctx)`` / ``Expr.from_bytes(ctx=...)``
API and the ``pickle.dumps`` / ``pickle.loads`` route through the
sender / worker context slots.
"""

from __future__ import annotations

import pickle

import pyarrow as pa
import pytest
from datafusion import Expr, SessionContext, col, udf
from datafusion.ipc import (
    clear_sender_ctx,
    clear_worker_ctx,
    set_sender_ctx,
    set_worker_ctx,
)
from datafusion_ffi_example import IsNullUDF


@pytest.fixture(autouse=True)
def _reset_thread_locals():
    """Ensure no sender / worker context leaks across tests."""
    clear_worker_ctx()
    clear_sender_ctx()
    yield
    clear_worker_ctx()
    clear_sender_ctx()


def _strict_session_with_ffi_udf():
    """Build a strict-mode session with the FFI ``IsNullUDF`` registered."""
    ctx = SessionContext().with_python_udf_inlining(enabled=False)
    my_udf = udf(IsNullUDF())
    ctx.register_udf(my_udf)
    return ctx, my_udf


def test_strict_ffi_udf_expr_roundtrip_via_to_bytes():
    """Strict-mode encode emits a by-name payload; receiver resolves
    ``my_custom_is_null`` from its registered functions and the decoded
    expression evaluates to the same result as the original."""
    sender, my_udf = _strict_session_with_ffi_udf()
    receiver, _ = _strict_session_with_ffi_udf()

    expr = my_udf(col("a"))
    blob = expr.to_bytes(sender)
    restored = Expr.from_bytes(blob, ctx=receiver)

    assert "my_custom_is_null" in restored.canonical_name()

    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, None, 4], type=pa.int64())], names=["a"]
    )
    receiver.register_record_batches("t", [[batch]])
    out = receiver.table("t").select(restored.alias("r")).collect()
    expected = pa.array([False, False, True, False], type=pa.bool_())
    assert out[0].column(0) == expected


def test_strict_ffi_udf_pickle_roundtrip_via_thread_locals():
    """Driver installs a strict sender context; worker installs a
    matching strict receiver. ``pickle.dumps`` / ``pickle.loads`` route
    through them and the FFI UDF resolves by name on decode."""
    sender, my_udf = _strict_session_with_ffi_udf()
    receiver, _ = _strict_session_with_ffi_udf()

    expr = my_udf(col("a"))

    set_sender_ctx(sender)
    try:
        blob = pickle.dumps(expr)
    finally:
        clear_sender_ctx()

    set_worker_ctx(receiver)
    try:
        restored = pickle.loads(blob)  # noqa: S301
    finally:
        clear_worker_ctx()

    assert "my_custom_is_null" in restored.canonical_name()


def test_strict_ffi_udf_smaller_than_inline_python_udf():
    """Sanity-check the wire size claim: strict-mode FFI UDF bytes are
    a small by-name payload, dramatically smaller than the inline form
    of a Python UDF with the same arity. Confirms the encode path
    actually took the by-name branch instead of falling through to an
    inline path."""
    sender, my_udf = _strict_session_with_ffi_udf()
    ffi_blob = my_udf(col("a")).to_bytes(sender)

    inline_ctx = SessionContext()
    py_udf = udf(
        lambda arr: pa.array([v.as_py() is None for v in arr]),
        [pa.int64()],
        pa.bool_(),
        volatility="immutable",
        name="py_is_null",
    )
    py_blob = py_udf(col("a")).to_bytes(inline_ctx)

    assert len(ffi_blob) < len(py_blob) // 4
