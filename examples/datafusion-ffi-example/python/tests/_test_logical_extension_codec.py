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

    The codec is installed under ``token`` as its id as well, so a
    caller can reinstall the same instance elsewhere and have the tag on
    these bytes resolve. Identity would otherwise be derived from the
    class, which every instance shares.
    """
    codec = MyLogicalExtensionCodec(provider_prefix=token)
    ctx = SessionContext().with_logical_extension_codec(codec, codec_id=token)
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


def test_first_installed_codec_encodes():
    """Encoding walks the chain in install order, so the earliest
    installed codec that can claim an object gets it.

    Both orders run in one test on purpose. Asserting a single order
    would also pass under replace semantics, where the second install
    simply discards the first codec; swapping the order and getting the
    other token proves the losing codec was still installed and merely
    lost the race.

    The two instances need explicit ids: identity is otherwise derived
    from the class, and these are two instances of one class owning
    disjoint slices of the wire format.
    """
    for winner, loser in (("TOKENAAA", "TOKENBBB"), ("TOKENBBB", "TOKENAAA")):
        winner_codec = MyLogicalExtensionCodec(provider_prefix=winner)
        loser_codec = MyLogicalExtensionCodec(provider_prefix=loser)
        ctx = SessionContext().with_logical_extension_codec(
            winner_codec, codec_id=winner
        )
        ctx = ctx.with_logical_extension_codec(loser_codec, codec_id=loser)

        ctx.register_table("numbers", MyTableProvider(1, 4, 1))
        blob = ctx.sql('SELECT "A" FROM numbers').logical_plan().to_bytes(ctx)

        assert winner.encode() in blob
        assert loser.encode() not in blob
        assert winner_codec.table_provider_encode_calls() == 1
        assert loser_codec.table_provider_encode_calls() == 0


def test_installing_a_codec_cannot_hijack_an_earlier_codecs_objects():
    """Appending is additive: a later install can claim objects nothing
    else claimed, but never takes over an object an earlier codec was
    already encoding.

    This is why install order is append rather than prepend. Under
    prepend, adding an unrelated library would silently change how an
    existing library's objects encode -- and, once payloads are tagged,
    would renumber ids that older payloads already reference.
    """
    first = MyLogicalExtensionCodec(provider_prefix="TOKENAAA")
    ctx = SessionContext().with_logical_extension_codec(first, codec_id="TOKENAAA")
    ctx.register_table("numbers", MyTableProvider(1, 4, 1))
    before = ctx.sql('SELECT "A" FROM numbers').logical_plan().to_bytes(ctx)

    later = MyLogicalExtensionCodec(provider_prefix="TOKENBBB")
    ctx = ctx.with_logical_extension_codec(later, codec_id="TOKENBBB")
    after = ctx.sql('SELECT "A" FROM numbers').logical_plan().to_bytes(ctx)

    # Provider tokens are minted per encode, so the payloads differ in the
    # token id. What must not change is which codec claimed the provider.
    assert b"TOKENAAA" in after
    assert b"TOKENBBB" not in after
    assert later.table_provider_encode_calls() == 0
    assert len(before) == len(after)


def test_decode_dispatches_to_the_codec_that_encoded():
    """A payload names the codec that wrote it, so decoding consults
    exactly that codec and never offers the bytes to any other.

    The blob here is written by ``first`` in its own session. Installing
    ``second`` alongside it must not put ``second`` anywhere near those
    bytes -- under trial-and-error dispatch it would be asked first, and
    a codec that decodes structurally similar protobuf would answer.
    """
    blob, first = _encode_provider_plan("TOKENAAA")

    second = MyLogicalExtensionCodec(provider_prefix="TOKENBBB")
    ctx = SessionContext().with_logical_extension_codec(first, codec_id="TOKENAAA")
    ctx = ctx.with_logical_extension_codec(second, codec_id="TOKENBBB")

    restored = LogicalPlan.from_bytes(ctx, blob)
    assert ctx.create_dataframe_from_logical_plan(restored).collect()

    assert first.table_provider_decode_calls() == 1
    assert second.table_provider_decode_calls() == 0


def test_decode_survives_a_different_install_order():
    """Dispatch keys off codec identity, not chain position, so the
    decoding session may install the same codecs in any order.

    This is the case positional dispatch cannot handle: the encoding
    session has the owning codec at index 0 and the decoding session has
    it at index 1. Keying on position would hand the payload to whatever
    sits at index 0 in the decoder -- silently, and with a plausible
    result.
    """
    owner = MyLogicalExtensionCodec(provider_prefix="TOKENAAA")
    other = MyLogicalExtensionCodec(provider_prefix="TOKENBBB")

    encoder = SessionContext().with_logical_extension_codec(owner, codec_id="TOKENAAA")
    encoder = encoder.with_logical_extension_codec(other, codec_id="TOKENBBB")
    encoder.register_table("numbers", MyTableProvider(1, 4, 1))
    blob = encoder.sql('SELECT "A" FROM numbers').logical_plan().to_bytes(encoder)

    # Same codecs, opposite order.
    decoder = SessionContext().with_logical_extension_codec(other, codec_id="TOKENBBB")
    decoder = decoder.with_logical_extension_codec(owner, codec_id="TOKENAAA")

    restored = LogicalPlan.from_bytes(decoder, blob)
    assert decoder.create_dataframe_from_logical_plan(restored).collect()
    assert other.table_provider_decode_calls() == 0


def test_decode_names_the_codec_that_is_not_installed():
    """When the owning codec is absent the error names it and lists what
    is installed, instead of reporting DataFusion's generic "not
    provided" from whichever codec was tried last."""
    blob, _owner = _encode_provider_plan("TOKENBBB")

    ctx = SessionContext().with_logical_extension_codec(
        MyLogicalExtensionCodec(provider_prefix="TOKENCCC"), codec_id="lib_c.Codec"
    )

    with pytest.raises(Exception, match="TOKENBBB") as excinfo:
        LogicalPlan.from_bytes(ctx, blob)

    message = str(excinfo.value)
    # Names the codec the payload belongs to, and what is actually here.
    assert "not installed on this session" in message
    assert "lib_c.Codec" in message


def test_installing_two_codecs_under_one_id_is_rejected():
    """Identity is derived from the class, so installing two instances of
    one class collides. Rejecting at install time is the point: two
    codecs sharing an id are indistinguishable when a payload is decoded,
    and only the caller knows whether they write the same wire format."""
    ctx = SessionContext().with_logical_extension_codec(MyLogicalExtensionCodec())

    with pytest.raises(ValueError, match="already installed"):
        ctx.with_logical_extension_codec(MyLogicalExtensionCodec())


def test_bare_capsule_codec_is_session_local():
    """A bare PyCapsule exposes nothing stable to derive an identity
    from -- every capsule reports the same type -- so it is tagged with a
    session-local id. A plan it encodes fails on an unrelated session
    with an error naming the fix, rather than being decoded by whichever
    codec happens to sit at the same position."""
    exporter = SessionContext().with_logical_extension_codec(
        MyLogicalExtensionCodec(provider_prefix="TOKENAAA")
    )
    encoder = SessionContext().with_logical_extension_codec(
        exporter.__datafusion_logical_extension_codec__()
    )
    encoder.register_table("numbers", MyTableProvider(1, 4, 1))
    blob = encoder.sql('SELECT "A" FROM numbers').logical_plan().to_bytes(encoder)

    with pytest.raises(Exception, match="bare PyCapsule") as excinfo:
        LogicalPlan.from_bytes(SessionContext(), blob)
    assert "codec_id" in str(excinfo.value)


def test_default_only_session_writes_no_envelope():
    """A session with no extension codecs installed produces the same
    bytes as a build without codec chaining: the terminal codec writes
    unframed, so the envelope only appears once a codec is installed.

    Keeps the wire break scoped to sessions that actually compose."""
    ctx = SessionContext()
    blob = ctx.sql("SELECT abs(-1) AS x").logical_plan().to_bytes(ctx)
    assert b"DFPYCHN" not in blob


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
