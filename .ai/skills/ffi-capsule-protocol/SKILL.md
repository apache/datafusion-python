---
name: ffi-capsule-protocol
description: "TRIGGER — read before adding, changing, or reviewing any __datafusion_*__ capsule getter, any FFI_* export that asks for a TaskContextProvider or an extension codec, any code that calls FFI_QueryPlanner::new / FFI_TableProvider::new / FFI_{Logical,Physical}ExtensionCodec::new, or anything in crates/core/src/codec.rs that decides which extension codec handles a payload. These methods are one protocol with a settled convention. Do not design it fresh; do not construct a SessionContext inside an extension library; do not dispatch a codec chain by trying codecs until one succeeds."
argument-hint: "[getter name] (e.g., \"__datafusion_query_planner__\", \"table provider\", \"codec\", \"codec chain\", or omit to review the whole family)"
---

<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# FFI Capsule Protocol

`datafusion-python` shares Rust objects with extension libraries through
PyCapsules. Every hook is a dunder method named `__datafusion_<thing>__` that
returns a capsule wrapping an FFI-safe struct. They are **one protocol**, not a
collection of unrelated methods, and they have a settled convention that has
already been migrated once (see `docs/source/user-guide/upgrade-guides.md`,
DataFusion 52.0.0 and 55.0.0).

## Rule 1 — enumerate the family before you change a member

Do this first, every time. It takes one command and it is the whole point of
this skill:

```bash
grep -rn "__datafusion_[a-z_]*__" --include="*.rs" crates/ examples/*/src/
```

Compare the signature you are about to write against what the others already
do. If yours is shaped differently, that is a finding about your design, not
about theirs.

## Rule 2 — a getter takes the session it is being installed on

```rust
fn __datafusion_physical_extension_codec__<'py>(
    &self,
    py: Python<'py>,
    session: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyCapsule>> { ... }
```

The host calls the getter and passes itself. That argument is how an extension
library reaches things only the session has.

`SessionContext` implements the same getters and ignores the argument, so a
session satisfies the protocol too — `ctx.__datafusion_query_planner__()` and
`ctx.__datafusion_query_planner__(ctx)` are both valid.

## Rule 3 — never construct a `SessionContext` in an extension library

The FFI constructors ask for things a library does not have:

| Constructor | Wants | Take it from |
|---|---|---|
| `FFI_{Logical,Physical}ExtensionCodec::new` | `TaskContextProvider` | `ffi_task_context_provider_from_pycapsule(&session)` |
| `FFI_TableProvider::new_with_ffi_codec` | logical codec | `ffi_logical_codec_from_pycapsule(session, None)` |
| `FFI_QueryPlanner::new_with_ffi_codecs` | both codecs | `ffi_{logical,physical}_codec_from_pycapsule(session, None)` |

`Arc::new(SessionContext::new())` is the wrong answer to all three, for two
independent reasons:

1. **It is the wrong registry.** Decode callbacks resolve names against
   whatever provider the codec carries. An empty session resolves nothing, so a
   function the host registered with `register_udf` is invisible to a node that
   references it by name.
2. **It dangles.** `FFI_TaskContextProvider` downgrades its provider to a
   `Weak`. A context built inline in the getter is dropped before the capsule
   is ever used, and every callback then fails with `TaskContextProvider went
   out of scope over FFI boundary`.

Prefer the `*_with_ffi_codec(s)` constructors when they exist. They take
prebuilt codecs that already carry the host's provider, so there is no provider
parameter to get wrong.

## Rule 4 — the helpers live in `crates/util/src/lib.rs`

`ffi_logical_codec_from_pycapsule`, `ffi_physical_codec_from_pycapsule`,
`ffi_query_planner_from_pycapsule`, `ffi_task_context_provider_from_pycapsule`,
`table_provider_from_pycapsule`. Each takes the object and, where relevant, an
`Option<&Bound<PyAny>>` session:

- `Some(session)` — importing a *foreign* object; the getter needs the session.
- `None` — the object already *is* a session and is being asked for what it
  holds.

Adding a getter means adding a helper here, not hand-rolling capsule
extraction at the call site.

## Rule 5 — changing a getter's signature is a breaking change

Extension libraries implement these methods. A signature change breaks every
one of them, and the failure is a bare `TypeError` from a `call1`. So:

- Add a section to `docs/source/user-guide/upgrade-guides.md` with before/after
  Rust, matching the 52.0.0 and 55.0.0 entries.
- Add the `api change` label to the PR.
- Map the `TypeError` to a diagnosable message. `call_capsule_getter` in
  `crates/util/src/lib.rs` already does this; reuse it.
- Update `python/datafusion/context.py` and
  `python/datafusion/user_defined.py`, where the `Protocol` type hints for
  these methods live.

## Rule 6 — a session keeps one `Arc<SessionContext>` for life

`FFI_TaskContextProvider` holds its provider **weakly**, and every codec handed
to a foreign object carries one. A registered catalog provider upgrades that
handle on every `supports_filters_pushdown` and every `scan`. The handle is
bound to an `Arc<SessionContext>` *allocation*, so anything that replaces the
allocation orphans every handle bound to the old one:
`TaskContextProvider went out of scope over FFI boundary`.

So mutate `SessionState` in place — `*self.ctx.state_ref().write() = ...`, the
way `add_physical_optimizer_rule` and `set_session_query_planner` both do —
rather than deriving a replacement `SessionContext`. Carry the session id
across the rewrite; `SessionStateBuilder::new_from_existing` drops it and
`build` mints a fresh one, which desyncs `session_id()` from every
`TaskContext` the session hands out.

Do not try to repair it after the fact:

- **You cannot rebind what you cannot reach.** A codec embedded in a registered
  `FFI_CatalogProvider`, and in every `FFI_SchemaProvider` and
  `FFI_TableProvider` minted from it, has no Python-side handle.
- **A codec must not retain its session.** Codecs are routinely handed to a
  provider that is registered straight back into the session that built them,
  closing `SessionContext -> catalog -> FFI provider -> FFI codec ->
  SessionContext`.

`test_registered_providers_survive_a_planner_install` in
`examples/datafusion-ffi-query-planner-example/python/tests/_test_three_library_query_planner.py`
guards this. Its `WHERE` clause is load-bearing: filter pushdown upgrades the
weak handle during logical optimization, before plan serialization could fail
first for an unrelated reason.

`SessionContext.enable_url_table` is the one method that mints a second
allocation for a session. Its result must not outlive the receiver.

## Rule 7 — installing a planner mutates the session, and says so

`set_query_planner` returns `None`, matching `add_physical_optimizer_rule`. The
query planner lives in `SessionState`, so it belongs to the session and not to
a handle on it; every context sharing that session plans through it. Do not
reintroduce a `with_query_planner` that pretends otherwise — the only way to
give a handle its own planner is a fresh `Arc<SessionContext>`, which is what
Rule 6 forbids.

Installing a codec rebuilds the installed planner against it, and that rebuild
reaches exactly one layer. `FFI_QueryPlanner::new_with_ffi_codecs` unwraps one
`ForeignQueryPlanner`; a fallback that planner resolved at install time sits in
its library's private data with no handle on this side, and cannot re-derive
codecs itself because `FFI_QueryPlanner` holds them by value and `Session`
exposes no accessor for the host's current ones. So do not promise that install
order is free — for a layered planner it is not. The examples cannot show this:
their fallback lives in the same cdylib as its wrapper, and `datafusion-ffi`
short-circuits a same-library hop rather than serializing. A fix has to come
from upstream; tracked in
[apache/datafusion#24762](https://github.com/apache/datafusion/issues/24762).

The session's planner also tracks whichever handle wrote it last, so
re-installing a planner on the original handle rebinds the session back to that
handle's codecs. `test_reinstalling_a_planner_rebinds_the_session_to_that_handles_codecs`
pins that; changing it should be deliberate.

## Rule 8 — a codec chain dispatches by identity, never by trying codecs in turn

A session holds a chain of extension codecs. `Python{Logical,Physical}Codec`
wraps every payload a chained codec writes in an envelope naming that codec,
and decoding consults exactly the codec named. The envelope is applied and
stripped in `crates/core/src/codec.rs`; a third-party codec receives the bytes
it wrote and never sees it. **Adding a codec to the chain asks nothing of the
codec's author.**

Do not replace this with "try each codec until one returns `Ok`". That is
unsound, and it has already shipped and been reverted upstream: it is how
`ComposedPhysicalExtensionCodec` decoded a Parquet payload as CSV
([apache/datafusion#16980](https://github.com/apache/datafusion/issues/16980),
fixed in [#16986](https://github.com/apache/datafusion/pull/16986)). Protobuf
carries no type identity — a `prost` message decodes cleanly from an unrelated
message's bytes whenever their leading field numbers and wire types line up, and
an all-defaults message encodes to zero bytes, which decodes as *anything*.
A byte-prefix convention does not rescue it, because the natural implementation
is `MyMessage::decode(buf)`, which has no prefix to check and cannot decline.

**Key on identity, not chain position.** Upstream's composed codec, Ballista's
`FileFormatProto`, and `datafusion-distributed` all stamp the encoder's *index*.
That is sound for them because their lists cannot disagree: Ballista's is a
compile-time constant, and `datafusion-distributed` pins its own codec at index
0 and appends user codecs rebuilt from the same startup code. A
`datafusion-python` chain is assembled by user Python, and
`Expr.to_bytes(ctx1)` / `Expr.from_bytes(ctx2)` puts two independently built
sessions on either end of one payload — so an index would name a different codec
in the decoder whenever install order differs. Copying the upstream design here
would reintroduce the same class of silent mis-decode from the other direction.

Identity resolves as: explicit `codec_id=` → `__datafusion_codec_id__` on the
exporting object → the exporting class's `module.QualName` → a session-local id
for a bare `PyCapsule`, which has nothing stable to derive from. Installing two
codecs under one id raises rather than shadowing.

**Two payloads are never framed, and both are load-bearing.**

1. The terminal codec writes bare, so a session with no extension codecs
   installed serializes byte-identically to a build without chaining.
2. An encode that writes nothing stays empty. `Ok` with an empty buffer is
   DataFusion's encode-by-name signal: it leaves `fun_definition` unset, and the
   decoder then tries the `FunctionRegistry` **first** and the codec second —
   `datafusion-proto`'s `from_proto.rs`, the
   `None => ctx.udf(..).or_else(|_| codec.try_decode_udf(name, &[]))` arm.
   Framing an empty payload sets the field and skips that registry lookup
   permanently. Note upstream's `encode_protobuf` gets this wrong; do not copy
   it.

That second point is the one plausible "tidy-up" that breaks things silently, so
it has a dedicated guard: `NameOnlyUdfCodec` in the FFI example owns functions
reconstructible from their names alone, encodes no bytes, and is exercised by
`test_name_only_codec_round_trips_without_a_payload` against a session that
never registered the function. That empty-buffer decode is also the one path
where every codec is still consulted in turn, because there are no bytes to
carry an identity — sound here because the question is name-scoped, and two
codecs disagreeing means they claim one function name, already a registry
collision.

**Encoding walks the chain in install order and the first codec to write bytes
wins**, so a codec appends. Prepending would let an unrelated install take over
an earlier library's objects, and would renumber nothing but still change which
codec claims what. Order affects decoding not at all.

## Where the truth is

- `docs/source/contributor-guide/ffi.md` — the protocol, the fork caveat.
- `docs/source/user-guide/upgrade-guides.md` — every past migration.
- `crates/core/src/codec.rs` — the codec chain: the envelope, identity dispatch,
  and the two unframed cases from Rule 8.
- `examples/datafusion-ffi-example/src/` — provider, catalog, function, codec
  getters, all in current form. `name_only_codec.rs` is the codec that encodes
  nothing.
- `examples/datafusion-ffi-query-planner-example/src/planner.rs` — planner
  getter.
- `examples/datafusion-ffi-query-planner-example/python/tests/_test_three_library_query_planner.py`
  — `require_udf_on_decode` proves which session a decode callback resolves
  against. Extend these when touching the protocol.
