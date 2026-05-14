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

from datafusion import LogicalPlan, SessionContext
from datafusion_ffi_example import MyLogicalExtensionCodec


def _setup_session_with_codec() -> tuple[SessionContext, MyLogicalExtensionCodec]:
    """Build a session with the user-supplied logical extension codec
    installed. Tests use a FROM-less query so plan serialization does
    not pull in `try_encode_table_provider`, which the default codec
    leaves unimplemented."""
    base = SessionContext()
    codec = MyLogicalExtensionCodec()
    ctx = base.with_logical_extension_codec(codec)
    return ctx, codec


def test_ffi_logical_codec_install_and_export():
    """Installing a user FFI codec replaces the session's logical
    codec; the capsule getter on the session re-exports it."""
    ctx, _codec = _setup_session_with_codec()
    capsule = ctx.__datafusion_logical_extension_codec__()
    assert capsule is not None


def test_ffi_logical_codec_consulted_on_udf_encode():
    """Serializing through ctx.logical_codec() routes try_encode_udf to
    the user-installed FFI codec.

    Verifies the dispatch chain
    `PyLogicalPlan.to_bytes -> session.logical_codec ->
    PythonLogicalCodec -> FFI_LogicalExtensionCodec -> user impl`
    is wired correctly. The user codec's atomic counter increments
    after a serialization pass, proving every hop forwards.

    Does not test any Python-UDF-specific dispatch — PythonLogicalCodec
    currently delegates all UDF encoding to its inner codec
    unconditionally. Python-vs-other branching lands when in-band
    scalar UDF encoding is added.
    """
    ctx, codec = _setup_session_with_codec()
    df = ctx.sql("SELECT abs(-1) AS x")
    plan = df.logical_plan()

    before = codec.encode_udf_calls()
    _ = plan.to_bytes(ctx)
    after = codec.encode_udf_calls()

    assert after > before, (
        f"Expected user FFI codec encode_udf to fire, before={before} after={after}"
    )


def test_ffi_logical_codec_roundtrip():
    """A plan referencing an FFI-imported UDF round-trips through the
    user-supplied logical codec (encode via codec, decode resolves from
    registry — `try_decode_udf` is only consulted when the UDF is not
    in the registry, which is the codec-inlined case)."""
    ctx, _codec = _setup_session_with_codec()
    df = ctx.sql("SELECT abs(-1) AS x")
    blob = df.logical_plan().to_bytes(ctx)

    restored = LogicalPlan.from_bytes(ctx, blob)
    assert restored is not None
