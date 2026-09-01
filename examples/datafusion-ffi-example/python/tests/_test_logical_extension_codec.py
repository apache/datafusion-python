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
from datafusion_ffi_example import (
    MyLogicalExtensionCodec,
    MyTableProvider,
    NameOnlyFunction,
    NameOnlyUdfCodec,
)


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
    """Installing a user FFI codec adds it to the session's logical
    codec chain; the capsule getter on the session re-exports it."""
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
    """Codecs compose: installing a second codec appends it to the
    session's codec chain instead of replacing the first. The second
    codec here (a default-backed codec exported from a fresh session)
    cannot encode this library's table provider, so the first codec
    still claims it. Under replace semantics this test fails with
    `LogicalExtensionCodec is not provided`."""
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


def test_a_codec_id_defaults_to_its_class_module_and_qualname():
    """A codec that declares no identity is named by its class.

    That string goes on the wire in front of every payload the codec
    writes, so it is a compatibility surface: renaming the class or moving
    the module changes it, and plans stored by an earlier release stop
    decoding. A library that needs to rename declares
    ``__datafusion_codec_id__`` instead, which the next test covers.

    Pinned twice on purpose -- once against the literal, so a rename has to
    come here and be acknowledged, and once against the rule, so the
    literal cannot drift into something the code no longer derives.
    """
    ctx = SessionContext().with_logical_extension_codec(MyLogicalExtensionCodec())

    assert ctx.logical_extension_codec_ids() == [
        "datafusion_ffi_example.MyLogicalExtensionCodec"
    ]
    assert ctx.logical_extension_codec_ids() == [
        f"{MyLogicalExtensionCodec.__module__}.{MyLogicalExtensionCodec.__qualname__}"
    ]


class _CodecUnderItsOldName:
    """A library codec that pins its identity, so the class can be renamed.

    Delegates the capsule getter to a real FFI codec. ``session`` is
    forwarded, because that argument is how the underlying library reaches
    the session the codec is being installed on.
    """

    __datafusion_codec_id__ = "pinned.example.Codec"

    def __init__(self, inner: MyLogicalExtensionCodec) -> None:
        self._inner = inner

    def __datafusion_logical_extension_codec__(self, session: object = None) -> object:
        return self._inner.__datafusion_logical_extension_codec__(session)


class _CodecUnderItsNewName(_CodecUnderItsOldName):
    """The same codec after a rename. Same pinned id, different class."""


def test_a_pinned_codec_id_survives_a_class_rename():
    """``__datafusion_codec_id__`` decouples identity from the class name,
    which is the reason to declare one.

    A plan encoded by the codec under its old name decodes on a session
    that only knows the new name. Under the class-derived default the two
    would be different ids and the payload would be undecodable.
    """
    assert _CodecUnderItsOldName.__qualname__ != _CodecUnderItsNewName.__qualname__

    old = MyLogicalExtensionCodec(provider_prefix="TOKENAAA")
    encoder = SessionContext().with_logical_extension_codec(_CodecUnderItsOldName(old))
    assert encoder.logical_extension_codec_ids() == ["pinned.example.Codec"]

    encoder.register_table("numbers", MyTableProvider(1, 4, 1))
    blob = encoder.sql('SELECT "A" FROM numbers').logical_plan().to_bytes(encoder)

    # The pinned id, not the class name, is what the payload carries.
    assert b"pinned.example.Codec" in blob
    assert b"_CodecUnderItsOldName" not in blob

    new = MyLogicalExtensionCodec(provider_prefix="TOKENAAA")
    decoder = SessionContext().with_logical_extension_codec(_CodecUnderItsNewName(new))
    assert decoder.logical_extension_codec_ids() == ["pinned.example.Codec"]

    restored = LogicalPlan.from_bytes(decoder, blob)
    assert decoder.create_dataframe_from_logical_plan(restored).collect()
    assert new.table_provider_decode_calls() == 1


def test_installing_two_codecs_under_one_id_is_rejected():
    """Identity is derived from the class, so installing two instances of
    one class collides. Rejecting at install time is the point: two
    codecs sharing an id are indistinguishable when a payload is decoded,
    and only the caller knows whether they write the same wire format."""
    ctx = SessionContext().with_logical_extension_codec(MyLogicalExtensionCodec())

    with pytest.raises(ValueError, match="already installed"):
        ctx.with_logical_extension_codec(MyLogicalExtensionCodec())


def _encode_through_a_bare_capsule(token: str) -> tuple[bytes, list[str]]:
    """Serialize a provider plan through a codec installed as a bare
    capsule, which is the case with no derivable identity.

    Returns the blob and the encoding session's codec ids, so a caller
    can compare them against another session's.
    """
    exporter = SessionContext().with_logical_extension_codec(
        MyLogicalExtensionCodec(provider_prefix=token)
    )
    encoder = SessionContext().with_logical_extension_codec(
        exporter.__datafusion_logical_extension_codec__()
    )
    encoder.register_table("numbers", MyTableProvider(1, 4, 1))
    blob = encoder.sql('SELECT "A" FROM numbers').logical_plan().to_bytes(encoder)
    return blob, encoder.logical_extension_codec_ids()


def test_bare_capsule_codec_is_session_local():
    """A bare PyCapsule exposes nothing stable to derive an identity
    from -- every capsule reports the same type -- so it is tagged with a
    session-local id. A plan it encodes fails on an unrelated session
    with an error naming the fix, rather than being decoded by whichever
    codec happens to sit at the same position."""
    blob, _ids = _encode_through_a_bare_capsule("TOKENAAA")

    with pytest.raises(Exception, match="bare PyCapsule") as excinfo:
        LogicalPlan.from_bytes(SessionContext(), blob)
    assert "codec_id" in str(excinfo.value)


def test_a_bare_capsule_codec_id_is_not_re_mintable_by_another_session():
    """The id given to a capsule-installed codec must be one no other
    session can arrive at.

    Both sessions here install exactly one bare capsule, so any identity
    drawn from a namespace both sessions number the same way -- a
    counter, a position in the chain -- collides, and the payload is
    handed to the other library's codec. That failure is quiet: a codec
    offered bytes it does not recognise falls through to its own inner
    default codec, so the error names neither codec and no counter moves.
    Asserting on the message is what separates the two schemes.

    The empty-chain case in the test above passes under either scheme,
    because a lookup in an empty chain misses whatever the id is.
    """
    blob, encoder_ids = _encode_through_a_bare_capsule("TOKENAAA")

    # An unrelated session, also holding exactly one bare capsule.
    other = SessionContext().with_logical_extension_codec(
        MyLogicalExtensionCodec(provider_prefix="TOKENBBB")
    )
    decoder = SessionContext().with_logical_extension_codec(
        other.__datafusion_logical_extension_codec__()
    )
    decoder_ids = decoder.logical_extension_codec_ids()
    assert len(encoder_ids) == len(decoder_ids) == 1
    assert encoder_ids != decoder_ids

    with pytest.raises(Exception, match="bare PyCapsule") as excinfo:
        LogicalPlan.from_bytes(decoder, blob)
    assert "codec_id" in str(excinfo.value)


def test_installing_a_session_as_a_codec_uses_a_per_session_id():
    """A context installed as a codec is identified by its session, not by
    its class.

    Every ``SessionContext`` shares one class, so a class-derived id would
    name all of them: two contexts could not coexist on one target, and a
    payload written through one would resolve to the other on decode. The
    sources are held in locals because the imported codecs resolve their
    task context against them.
    """
    src_a = SessionContext().with_logical_extension_codec(
        MyLogicalExtensionCodec(provider_prefix="TOKENAAA")
    )
    src_b = SessionContext().with_logical_extension_codec(
        MyLogicalExtensionCodec(provider_prefix="TOKENBBB")
    )
    assert src_a.__datafusion_codec_id__ != src_b.__datafusion_codec_id__

    # Both sources compose onto one session, which a shared id would refuse.
    both = SessionContext().with_logical_extension_codec(src_a)
    both = both.with_logical_extension_codec(src_b)
    assert both.logical_extension_codec_ids() == [
        src_a.__datafusion_codec_id__,
        src_b.__datafusion_codec_id__,
    ]

    # A payload written through one source does not resolve to the other.
    encoder = SessionContext().with_logical_extension_codec(src_a)
    encoder.register_table("numbers", MyTableProvider(1, 4, 1))
    blob = encoder.sql('SELECT "A" FROM numbers').logical_plan().to_bytes(encoder)

    decoder = SessionContext().with_logical_extension_codec(src_b)
    with pytest.raises(Exception, match="not installed on this session") as excinfo:
        LogicalPlan.from_bytes(decoder, blob)
    assert src_a.__datafusion_codec_id__ in str(excinfo.value)


def test_a_derived_handle_reports_its_session_id():
    """Handles derived from one session share its id, so only one of them
    can be installed on a given target. Their payloads would be
    indistinguishable on decode, so refusing is the right answer."""
    base = SessionContext()
    derived = base.with_python_udf_inlining(enabled=False)
    assert derived.__datafusion_codec_id__ == base.__datafusion_codec_id__

    target = SessionContext().with_logical_extension_codec(base)
    with pytest.raises(ValueError, match="already installed"):
        target.with_logical_extension_codec(derived)


def test_name_only_codec_round_trips_without_a_payload():
    """A codec may own functions that need no payload: the name is the
    whole encoding. ``try_encode_udf`` writes nothing, and the decoder
    rebuilds the function from the name with no registry entry.

    DataFusion supports this directly -- an empty ``fun_definition``
    sends the decoder to the registry first and the codec second. This
    test pins that arm from the Python side, because it is the one path
    where a payload is still offered to every installed codec: there are
    no bytes, so there is no identity to dispatch on.

    It is also the guard against a plausible "improvement". Wrapping
    every chained encode in the identity envelope would make this
    payload non-empty, which sets ``fun_definition`` and permanently
    skips the registry lookup -- breaking both this codec and ordinary
    by-name round trips, with nothing else in the suite noticing.
    """
    codec = NameOnlyUdfCodec()
    name = NameOnlyUdfCodec.function_name()

    # FROM-less, so serialization never reaches try_encode_table_provider --
    # this codec owns functions, not providers.
    encoder = SessionContext().with_logical_extension_codec(codec)
    encoder.register_udf(udf(NameOnlyFunction()))
    blob = encoder.sql(f"SELECT {name}(1) AS x").logical_plan().to_bytes(encoder)

    # The name is the entire encoding, so the codec contributed no bytes
    # and the payload carries no identity envelope for it.
    assert codec.encode_udf_calls() > 0
    assert b"DFPYCHN" not in blob

    # A fresh session that never registered the function: only the codec
    # can supply it, and only from the name.
    decoder = SessionContext().with_logical_extension_codec(codec)
    restored = LogicalPlan.from_bytes(decoder, blob)

    assert codec.decode_udf_calls() > 0
    assert decoder.create_dataframe_from_logical_plan(restored).collect()


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
