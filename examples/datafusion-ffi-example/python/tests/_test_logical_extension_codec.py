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
import pytest
from datafusion import Expr, LogicalPlan, SessionContext, col, udf
from datafusion_ffi_example import MyLogicalExtensionCodec, MyTableProvider


def _double_udf():
    return udf(
        lambda arr: pa.array([(v.as_py() or 0) * 2 for v in arr]),
        [pa.int64()],
        pa.int64(),
        volatility="immutable",
        name="double",
    )


def _encode_provider_plan(token: str) -> tuple[bytes, MyLogicalExtensionCodec]:
    """Serialize a plan over this library's table provider using a codec
    that stamps `token` on the encoded provider.

    Returns the blob and the codec, so callers can assert on its call
    counters. The token is chosen per test so a second codec installed
    later is provably unable to claim these bytes.
    """
    codec = MyLogicalExtensionCodec(provider_prefix=token)
    ctx = SessionContext().with_logical_extension_codec(codec)
    ctx.register_table("numbers", MyTableProvider(1, 4, 1))
    blob = ctx.sql('SELECT "A" FROM numbers').logical_plan().to_bytes(ctx)
    assert token.encode() in blob
    return blob, codec


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
    df_round_trip = ctx.create_dataframe_from_logical_plan(restored)
    assert df.collect() == df_round_trip.collect()


def test_ffi_logical_codec_composes_with_later_install():
    """Codecs compose: installing a second codec prepends it to the
    session's codec chain instead of replacing the first. The second
    codec here (a default-backed codec exported from a fresh session)
    cannot encode this library's table provider, so encoding falls
    through to the user codec installed first. Under replace semantics
    this test fails with `LogicalExtensionCodec is not provided`."""
    ctx, codec = _setup_session_with_codec()
    ctx = ctx.with_logical_extension_codec(
        SessionContext().__datafusion_logical_extension_codec__()
    )

    ctx.register_table("numbers", MyTableProvider(1, 4, 1))
    df = ctx.sql('SELECT "A" FROM numbers')
    plan = df.logical_plan()

    before = codec.table_provider_encode_calls()
    blob = plan.to_bytes(ctx)
    assert codec.table_provider_encode_calls() > before

    restored = LogicalPlan.from_bytes(ctx, blob)
    df_round_trip = ctx.create_dataframe_from_logical_plan(restored)
    assert df.collect() == df_round_trip.collect()


def test_most_recently_installed_codec_encodes_first():
    """Encoding walks the chain front to back, and the front is the most
    recently installed codec. Both codecs here can encode the provider,
    so the winner is decided purely by install order.

    Both orders are exercised in one test on purpose. Asserting a single
    order would also pass under replace semantics, where the second
    install simply discards the first codec; swapping the order and
    getting the other token proves the losing codec was still installed
    and merely lost the race.
    """
    for winner, loser in (("TOKENAAA", "TOKENBBB"), ("TOKENBBB", "TOKENAAA")):
        loser_codec = MyLogicalExtensionCodec(provider_prefix=loser)
        winner_codec = MyLogicalExtensionCodec(provider_prefix=winner)
        ctx = SessionContext().with_logical_extension_codec(loser_codec)
        ctx = ctx.with_logical_extension_codec(winner_codec)

        ctx.register_table("numbers", MyTableProvider(1, 4, 1))
        blob = ctx.sql('SELECT "A" FROM numbers').logical_plan().to_bytes(ctx)

        assert winner.encode() in blob
        assert loser.encode() not in blob
        assert winner_codec.table_provider_encode_calls() == 1
        assert loser_codec.table_provider_encode_calls() == 0


def test_decode_falls_through_to_earlier_installed_codec():
    """A codec that does not own the payload signals "not mine" by
    erroring, and the chain keeps walking. The bytes here are stamped
    with the first codec's token, so the more recently installed second
    codec must decline and let the first one decode."""
    blob, first = _encode_provider_plan("TOKENAAA")

    second = MyLogicalExtensionCodec(provider_prefix="TOKENBBB")
    ctx = SessionContext().with_logical_extension_codec(first)
    ctx = ctx.with_logical_extension_codec(second)

    restored = LogicalPlan.from_bytes(ctx, blob)
    assert ctx.create_dataframe_from_logical_plan(restored).collect()

    assert first.table_provider_decode_calls() == 1
    assert second.table_provider_decode_calls() == 0


def test_decode_failure_aggregates_every_codec_error():
    """When no codec in the chain claims the payload, the error names
    the number of codecs tried and carries each one's message, so an
    operator can see which library was expected to own the bytes."""
    blob, _owner = _encode_provider_plan("TOKENBBB")

    # Neither installed codec owns TOKENBBB, so the chain is exhausted:
    # two example codecs plus DataFusion's default codec.
    ctx = SessionContext().with_logical_extension_codec(
        MyLogicalExtensionCodec(provider_prefix="TOKENCCC")
    )
    ctx = ctx.with_logical_extension_codec(
        MyLogicalExtensionCodec(provider_prefix="TOKENDDD")
    )

    with pytest.raises(Exception, match="None of the 3 composed extension codecs"):
        LogicalPlan.from_bytes(ctx, blob)


def test_single_codec_chain_error_is_returned_verbatim():
    """A session with no extra codec has a one-entry chain, so a decode
    failure surfaces DataFusion's own error rather than the aggregated
    wrapper. Keeps error messages unchanged for the common case where
    nobody composed anything."""
    blob, _owner = _encode_provider_plan("TOKENEEE")

    # DataFusion's own wording for "no codec claimed this", surfaced
    # unwrapped because the chain has a single entry.
    with pytest.raises(
        Exception, match="LogicalExtensionCodec is not provided"
    ) as excinfo:
        LogicalPlan.from_bytes(SessionContext(), blob)

    assert "composed extension codecs" not in str(excinfo.value)


def test_udf_inlining_setting_survives_codec_install():
    """Installing an extension codec must not silently re-enable inline
    Python UDF encoding on a session that opted out. Regression guard in
    both directions: the encoder still emits the by-name form, and the
    decoder still refuses an inline payload.

    The codec installed here delegates UDF encoding to DataFusion's
    default codec. A codec exported from another `SessionContext` would
    not work as a probe: that export is itself a Python-aware codec with
    inlining enabled, so the strict outer codec would delegate to it and
    the inline payload would reappear.
    """
    strict = SessionContext().with_python_udf_inlining(enabled=False)
    extended = strict.with_logical_extension_codec(
        MyLogicalExtensionCodec(provider_prefix="TOKENFFF")
    )

    e = _double_udf()(col("a"))
    assert b"DFPYUDF" not in e.to_bytes(extended)

    inline_blob = e.to_bytes(SessionContext())
    assert b"DFPYUDF" in inline_blob
    with pytest.raises(Exception, match="inlining is disabled"):
        Expr.from_bytes(inline_blob, ctx=extended)
