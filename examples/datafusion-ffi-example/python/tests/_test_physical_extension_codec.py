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

from __future__ import annotations

import pyarrow as pa
from datafusion import ExecutionPlan, SessionContext
from datafusion_ffi_example import MyPhysicalExtensionCodec


def _setup_session_with_codec() -> tuple[SessionContext, MyPhysicalExtensionCodec]:
    base = SessionContext()
    batch = pa.RecordBatch.from_arrays(
        [pa.array([-1, -2, -3])],
        names=["a"],
    )
    base.register_record_batches("t", [[batch]])
    codec = MyPhysicalExtensionCodec()
    ctx = base.with_physical_extension_codec(codec)
    return ctx, codec


def test_ffi_physical_codec_install_and_export():
    ctx, _codec = _setup_session_with_codec()
    capsule = ctx.__datafusion_physical_extension_codec__()
    assert capsule is not None


def test_ffi_physical_codec_consulted_on_udf_encode():
    """Serializing through ctx.physical_codec() routes try_encode_udf to
    the user-installed FFI codec.

    Mirror of the logical-side dispatch test: verifies
    `PyExecutionPlan.to_bytes -> session.physical_codec ->
    PythonPhysicalCodec -> FFI_PhysicalExtensionCodec -> user impl`
    forwards correctly. Does not test Python-UDF-specific dispatch —
    PythonPhysicalCodec currently delegates all UDF encoding to its
    inner codec unconditionally.
    """
    ctx, codec = _setup_session_with_codec()
    df = ctx.sql("SELECT abs(a) AS x FROM t")
    plan = df.execution_plan()

    before = codec.encode_udf_calls()
    _ = plan.to_bytes(ctx)
    after = codec.encode_udf_calls()

    assert after > before, (
        f"Expected user FFI codec encode_udf to fire, before={before} after={after}"
    )


def test_ffi_physical_codec_roundtrip():
    """A plan referencing an FFI-imported UDF round-trips via the
    user-supplied physical codec. On decode, the receiver resolves the
    UDF from the function registry; `try_decode_udf` only fires when a
    codec inlines the UDF body, which the counting codec does not."""
    ctx, _codec = _setup_session_with_codec()
    df = ctx.sql("SELECT abs(a) AS x FROM t")
    original = df.execution_plan()
    blob = original.to_bytes(ctx)

    restored = ExecutionPlan.from_bytes(ctx, blob)
    assert str(original) == str(restored)
