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

(extension_codecs)=

# Extension codecs

A codec is what lets your objects survive being serialized into a plan and
rebuilt somewhere else — another process, or another program. If your library
contributes table providers, functions, or execution plan nodes and those plans
have to leave the process, you need one.

Codecs are contributed to a session either through an
{ref}`extension bundle <extension_bundles>`, which is the recommended route,
or one at a time through
{py:meth}`~datafusion.SessionContext.with_logical_extension_codec` and
{py:meth}`~datafusion.SessionContext.with_physical_extension_codec`.

## Codecs compose

Each call to `with_logical_extension_codec` or
`with_physical_extension_codec` **appends** the codec to the session's codec
chain rather than replacing prior codecs. One session can therefore carry
codecs from several independent libraries at once.

**Nothing is asked of the codec itself.** Implement `LogicalExtensionCodec` or
`PhysicalExtensionCodec` exactly as you would for a session that installs only
yours. When your codec writes bytes into a serialized plan, datafusion-python
records which codec wrote them, and strips that record off again before handing
the bytes back. So your codec receives, byte for byte, the payload it wrote,
and is never offered a payload another codec wrote.

A codec that also ships to hosts which dispatch differently may still want its
own guard against foreign payloads. Keeping one is fine; it is simply not
needed for the datafusion-python path.

(extension_codec_durable_metadata)=

## Encode metadata, not a handle to a live object

Your payload has to be enough to rebuild the object somewhere your process is
not. Write the metadata a fresh instance can be constructed from — a path, a
connection string, a schema, the options the object was created with.

The example codecs in this repository do not do this, and it is worth knowing
before copying them. They keep a process-local `HashMap` of live providers and
encode an integer token into it: encoding inserts, decoding removes. That makes
Rust type identity observable across three separately loaded libraries in one
test, which is what the examples exist to show. It also means a decode consumes
its token, so the same bytes cannot be decoded twice, one encoded plan cannot
fan out to several readers, and a plan that never reaches a decoder keeps its
provider alive for the life of the process. A real codec has none of those
properties because it does not park the object anywhere.

(extension_codec_ids)=

## Codec ids

That record is the codec's **id**: a short string stored inside the plan,
naming the codec that wrote each payload. Because plans are decoded in another
process — or another program — the id has to name the same codec there as it
did where the plan was written.

Ids are assigned for you. A codec's id is normally its exporting class's import
path, such as `my_library.Codec`, which is what you will see in
{py:meth}`~datafusion.SessionContext.logical_extension_codec_ids` and in decode
errors. You choose one yourself in three cases:

- **Two instances of one class.** Both get the same id, so the second install
  raises `ValueError`. Pass `codec_id=` to tell them apart.
- **A bare `PyCapsule`.** A capsule has no class to take a name from, so
  installing one through `with_logical_extension_codec` or
  `with_physical_extension_codec` gives it an id private to the session that
  installed it; plans it encodes fail with a clear error on any other session
  rather than being decoded by the wrong codec. Pass `codec_id=` if those plans
  have to cross sessions.

  {py:meth}`~datafusion.SessionContext.with_extensions` takes no `codec_id=`,
  so it refuses a bare capsule outright and tells you to wrap it. See
  {ref}`extension_bundles_codecs_are_objects`.
- **A class you intend to rename.** The id follows the class name, so renaming
  stops older plans from decoding. Declare `__datafusion_codec_id__` on the
  exporting object to pin an id that survives the rename.

{py:meth}`~datafusion.SessionContext.logical_extension_codec_ids` and its
physical counterpart list the ids installed on a session, which is also what a
decode failure names.

Installing one context's codec stack on another session composes the two
sessions rather than copying codecs out of one: the imported codecs resolve
their task context against the original and stop working when it is dropped —
see {ref}`ffi_internals_one_arc`. Pass the context itself rather than the
capsule it exports, so its codecs get an id that other sessions can decode.

## Functions whose name is the whole encoding

A codec may own functions that need no payload at all, where the name is the
whole encoding: `try_encode_udf` writes nothing and `try_decode_udf` rebuilds
the function from `name`. That is supported and needs no id, because an `Ok`
with an empty buffer is read as "no opinion" and passes the object to the next
codec. `NameOnlyUdfCodec` in [`datafusion-ffi-example`] is the worked case.
Anything no installed codec claims falls through to
`Default{Logical,Physical}ExtensionCodec`.

This is the one case where your decoder is consulted about something you may
not own, because an empty payload has no id to route on. `try_decode_udf` and
its aggregate and window siblings can therefore be called with an empty `buf`
and a `name` belonging to another library. Decide from `name` and return an
error if it is not yours; do not assume `buf` is non-empty.

:::{note}
The framing itself — how an id is stored alongside a payload and routed back,
and the two cases that stay unframed — is internal to datafusion-python and
documented in `crates/core/src/codec.rs` for anyone changing it.
:::

The current FFI logical codec supports providers and UDFs but not arbitrary
custom `LogicalPlan::Extension` nodes. See both example READMEs for the
supported flow and local build commands.

(extension_codec_order)=

## When codec order matters

Decoding is never order-dependent: a payload names its codec by id and the
chain dispatches straight to it. Encoding walks the chain in install order and
stops at the first codec that claims the node. Most of the time that is
invisible, because libraries claim disjoint things — one owns its table
providers, another its UDFs, a third its own execution plan nodes.

It stops being invisible when a codec claims *broadly*. A node that came from
another library arrives as an opaque `ForeignExecutionPlan`, and a codec that
claims any of those will take nodes it does not own from any library installed
after it. The query still succeeds. What changes is which library wrote the
bytes — so a plan that has to decode in another process now needs whichever
library happened to win, not the one whose node it is.
`MyPhysicalExtensionCodec` in [`datafusion-ffi-example`] claims this way, and
the query-planner example's test suite pins the consequence.

Two rules of thumb:

- **Writing a codec, claim narrowly.** Downcast to your own types. Claiming a
  broad category makes your library order-sensitive for everyone downstream of
  it.
- **Shipping plans out of the process, verify.** Do not assume your node
  reached your codec just because both are installed. Round-trip a plan through
  {py:meth}`ExecutionPlan.to_bytes <datafusion.ExecutionPlan.to_bytes>` /
  {py:meth}`~datafusion.ExecutionPlan.from_bytes` in a test and assert your
  codec did the work.

(extension_codec_decode_session)=

## A codec decodes against the session running the query

Because the task-context provider comes from the host — see
{ref}`extension_getter_argument` — a decode callback running inside an
extension library resolves names against the session running the query. A
function registered with `ctx.register_udf(...)` is visible to a foreign codec
decoding a node that references it by name, and the handle is live rather than
a snapshot, so a registration made after the codec is installed is visible too.

This is covered in
`examples/datafusion-ffi-query-planner-example/python/tests/_test_three_library_query_planner.py`,
where the example codecs take a `require_udf_on_decode` name and resolve it out
of the task context they are handed.

[`datafusion-ffi-example`]: https://github.com/apache/datafusion-python/tree/main/examples/datafusion-ffi-example
