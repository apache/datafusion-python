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

(extension_bundles)=

# Extension bundles

If your library ships codecs, functions, or a query planner, expose a **bundle**
and let callers install it with
{py:meth}`~datafusion.SessionContext.with_extensions`. This is the recommended
way to package an extension, and the rest of this page explains what the
bundle protocol asks of you and why.

## Why not have callers install the pieces

Installing the pieces by hand works, but it makes the caller responsible for
ordering: the codecs have to be installed before the planner, because a planner
is built against whatever codec chains exist when it is installed, and a codec
added afterwards rebinds it. Get that wrong and the planner encodes through a
chain that is missing a library.

`with_extensions` removes the ordering question. An extension library exposes a
bundle object implementing one or both of two hooks:

```python
class MyEngineExtension:
    def __datafusion_session_components__(self, ctx: SessionContext) -> SessionExtensionComponents:
        # Phase one. Create fresh components bound to `ctx` on every call.
        return SessionExtensionComponents(
            logical_extension_codecs=(self._make_logical_codec(ctx),),
            physical_extension_codecs=(self._make_physical_codec(ctx),),
            udfs=(MyScalarUDF(),),
        )

    def __datafusion_session_planner__(self, ctx: SessionContext, fallback):
        # Phase two. `ctx` now carries every bundle's codecs, and `fallback` is
        # the planner built so far. Wrapping it is what makes this library
        # compose with the other planners in the call.
        return self._make_planner(ctx, fallback=fallback)
```

Implement only the hooks you need. Codecs, functions, and tables all go in
`__datafusion_session_components__`, with the fields you do not use left empty,
so a codec-only library and a function-only library each define that one alone;
a library shipping nothing but an optimizing planner defines only
`__datafusion_session_planner__`. The caller then writes:

```python
ctx = SessionContext(config).with_extensions(lib_a.Extension(), lib_b.Extension())
```

Declare what you contribute rather than calling `register_udf` or
`register_table` on the `ctx` you were handed. Both put it on the session, but a
registration you make inside the hook is written the moment it runs — before the
other bundles have been called, too early to see their codecs, and not undone if
one of them raises. What you declare is instead resolved and checked while a
failure still costs nothing, then written once every bundle has succeeded. See
{ref}`extension_bundles_transaction`.

`MyPlannerExtension` in [`datafusion-ffi-query-planner-example`] is a complete
Rust implementation of the protocol, including taking the task-context provider
off the supplied context, wrapping its codecs in `BundledLogicalCodec` /
`BundledPhysicalCodec` so they carry declared ids, and constructing a Python
{py:class}`~datafusion.SessionExtensionComponents`.

## A bundle you can run

Before wiring up a cdylib, it is worth seeing the protocol work end to end in
pure Python. This runs against the plain wheel — the codec here re-exports the
host session's own capsule, where a real one would return its library's:

```python
from datafusion import SessionContext, SessionExtensionComponents


class Codec:
    """Wraps a capsule so it carries an id. A Rust library ships this shape."""

    def __init__(self, codec_id, capsule):
        self.__datafusion_codec_id__ = codec_id
        self._capsule = capsule

    def __datafusion_logical_extension_codec__(self, session=None):
        return self._capsule


class Bundle:
    def __init__(self, codec_id):
        self.codec_id = codec_id

    def __datafusion_session_components__(self, ctx):
        # Fresh components on every call, bound to the `ctx` handed in.
        # Never cache these, and never retain `ctx`.
        return SessionExtensionComponents(
            logical_extension_codecs=(
                Codec(self.codec_id, ctx.__datafusion_logical_extension_codec__()),
            )
        )


ctx = SessionContext().with_extensions(Bundle("tables.v1"), Bundle("engine.v1"))
ctx.logical_extension_codec_ids()
# ['tables.v1', 'engine.v1']
```

Three things this makes observable, each pinned by a test in
`python/tests/test_context.py`:

- **Ids accumulate in bundle order.** Decoding does not depend on that order;
  only encoding does. See {ref}`extension_codec_order`.
- **Two bundles claiming one id are refused**, with a `ValueError` naming the
  id — not resolved by position, since a positional id would break stored plans
  the first time a bundle reordered what it returns.
- **A hook that raises leaves the receiving session untouched.** Add a bundle
  whose hook raises and the source context's
  {py:meth}`~datafusion.SessionContext.logical_extension_codec_ids` is still
  empty afterwards.

(extension_bundles_two_phases)=

## Two phases, because codecs and planners compose differently

A session chains **many** codecs and dispatches between them by id. Codecs
therefore just accumulate: order affects encoding only, and decoding always
routes to the codec that wrote the payload. A session holds exactly **one**
query planner, so planners cannot accumulate — they compose by *nesting*, each
wrapping the one before it and delegating to it for work it does not handle.

So `with_extensions` runs every `__datafusion_session_components__` and installs
all the codecs, and only then runs each `__datafusion_session_planner__`, in
argument order, handing each the planner built so far. Two consequences worth
holding onto:

- **Bundle order matters differently for each.** For planners it sets the
  nesting: the last extension listed ends up outermost and is consulted first.
  For codecs it never affects decoding, and affects encoding only when two
  codecs would claim the same node — see {ref}`extension_codec_order`.
- **A planner is always built against the complete codec set**, including
  codecs from bundles listed after it. This is what the low-level chaining
  cannot give you, and it matters most for a nested planner: the rebuild that
  follows a later codec install reaches only the outermost layer (see
  {ref}`planner_codec_rebinding`), so a fallback captured before the codecs
  were complete would stay stale forever.

The two hooks therefore see the same session through different chains. Both
receive a handle on the one session, so the task-context provider taken off
either is the same and stays valid — but the `ctx` in phase one still carries
the chains the receiver had, since nothing is installed yet, while the `ctx` in
phase two carries every bundle's codecs. A bundle that reads the host's codec
chains — `MyPlannerExtension` does, to give its planner the host's codecs
rather than minting its own — must do that in the planner hook. Reading them in
phase one gets the chains from before the call, missing even the bundle's own
codecs.

## Returning a planner, or not

An extension that ignores `fallback` and returns an unrelated planner replaces
every layer beneath it, including any planner the session already had. That is
legal — a library that must be the only planner does it deliberately — but it
is not composable, and nothing detects it. Returning `None` contributes no
planner and leaves `fallback` in place.

`None` is the no-op, not `fallback`. The capsule handed to the first bundle
wraps the session's planner for export, so returning it unchanged installs that
planner as a foreign one and every plan built afterwards crosses an FFI
boundary it did not before. A bundle that decides at runtime it has nothing to
contribute returns `None`.

Three libraries that each ship a planner therefore install like this, with the
outermost last:

```python
ctx = SessionContext(config).with_extensions(
    tables.Extension(),        # codecs only
    functions.Extension(),     # codecs only
    optimizer.Extension(),     # planner, wrapping the session default
    distributed.Extension(),   # planner, wrapping the optimizer
)
```

(extension_bundles_order_conflict)=

## When the two orders conflict

Because codec position and planner position both come from one argument list, a
library can in principle need to be early for one and late for the other: its
codec must precede a broad claimer, while its planner must nest outside that
library's planner.

Do not try to satisfy both by reordering — contribute each half at its own
position. The two hooks are independent, so a three-line adapter each is
enough:

```python
class CodecsOf:
    """Contribute only the codec half of a bundle, at this position."""
    def __init__(self, inner):
        self.inner = inner

    def __datafusion_session_components__(self, ctx):
        return self.inner.__datafusion_session_components__(ctx)


class PlannerOf:
    """Contribute only the planner half of a bundle, at this position."""
    def __init__(self, inner):
        self.inner = inner

    def __datafusion_session_planner__(self, ctx, fallback):
        return self.inner.__datafusion_session_planner__(ctx, fallback)


ctx = SessionContext(config).with_extensions(
    CodecsOf(engine), CodecsOf(tables),   # engine's codec first
    PlannerOf(tables), PlannerOf(engine), # engine's planner outermost
)
```

This keeps everything `with_extensions` guarantees: one transaction, codecs
complete before any planner is built, codec ids untouched — an id is read off
the codec object, not off the extension that contributed it, so splitting a
bundle cannot re-tag its payloads. A library that expects to be composed this
way should expose the halves itself rather than make callers write the
adapters.

There is no attempt here to make every permutation expressible from one call.
Two positions per bundle covers the cases that arise; anything stranger is a
sign the libraries disagree about what they own, which is better fixed there.

The low-level
{py:meth}`~datafusion.SessionContext.with_logical_extension_codec` /
{py:meth}`~datafusion.SessionContext.with_physical_extension_codec` /
{py:meth}`~datafusion.SessionContext.set_query_planner` sequence also works,
and it is the right answer when the pieces do not come as bundles at all. But
it is a real downgrade, not just a more verbose spelling: you take back
responsibility for installing every codec before every planner, and a planner
you layer by hand keeps the codecs it captured — the
{ref}`one-level rebind <planner_codec_rebinding>` does not reach inside it.
Reach for it last.

(extension_bundles_codecs_are_objects)=

## Codecs are objects, not capsules

`with_extensions` requires each codec to be an object exposing the capsule
getter, and refuses a bare `PyCapsule`. A codec's id is read off the object it
is handed over as, and a capsule has no type to read one from; since this
method takes no `codec_id=`, there would be nothing left to name it by. A
library holding a raw capsule — which is what a Rust implementation has —
wraps it:

```python
class MyLogicalCodec:
    # Optional. Without it the id is this class's import path, which is already
    # stable; declare it if you may rename the class and need old plans to decode.
    __datafusion_codec_id__ = "my_library.logical.v1"

    def __init__(self, capsule):
        self._capsule = capsule

    def __datafusion_logical_extension_codec__(self, session=None):
        return self._capsule
```

Wrapping is not just bookkeeping. It ties the id to the codec rather than to
the bundle that contributed it, and that difference is load-bearing: an
application commonly presents several libraries as one bundle of its own, and
the id has to survive that. Were the id taken from the contributing bundle,
wrapping `my_engine.Extension` inside `my_app.Extension` would silently re-tag
the engine's payloads, and a scheduler that installs the engine's codec by its
documented id would fail to decode plans from composed clients while succeeding
for direct ones. The wrapper travels with the codec; the bundle does not.

The query planner is exempt — it carries no wire id, so it may be an object or
a capsule.

(extension_bundles_binding)=

## What a component is resolved against

Components split in two by what their capsule getter asks for, and it decides
where the host can resolve them:

- **Getters taking no argument** — the three function kinds and physical
  optimizer rules. Nothing is session-scoped, so a bundle may hand over either
  a wrapped object or the raw exportable.
- **Getters taking the session or a codec** — table functions, table providers,
  and catalog providers. These are resolved by the host against the *finished*
  handle, which is why you hand over the unwrapped value and a name rather than
  a {py:class}`~datafusion.user_defined.TableFunction` you built yourself.
  Wrapping one inside your components hook binds it to the context that hook
  received, which has none of the call's codecs — so it would capture a chain
  missing every library in the call, including your own.

(extension_bundles_collisions)=

## Two bundles claiming one name

Two extensions in one call may not declare a function of the same kind under
the same name. Doing so raises:

```text
ValueError: Two extensions declare a scalar function named 'normalize':
argument 0 (...) and argument 1 (...). ...
```

Codecs get away with sharing a chain because a payload carries the id of the
codec that wrote it, so decode routes to the right one. A function registry has
no such fall-through — one name holds one function — so the second registration
would quietly replace the first. The call refuses instead.

Which argument each claim came from is part of the message because it is what
picks the remedy. Two arguments colliding is the caller's to resolve, by
installing the two on separate sessions or by dropping a repeat; renaming is
not something a caller can do. One argument declaring a name twice is the
bundle author's own bug, and gets a different message saying so. Collisions are
keyed on position rather than on object identity, so passing one extension
twice reads as the caller's duplicate that it is, rather than as a bundle
colliding with itself.

Two cases this does *not* catch:

- **Different kinds never collide.** Names are compared within a kind, so a
  scalar function and an aggregate may both be called `normalize`.
- **Shadowing a built-in is allowed.** The registry already holds every
  DataFusion function, and replacing one by name is a supported thing to do —
  `enable_spark_functions` works that way.

Physical optimizer rules are exempt from all of this: they accumulate rather
than replace, so two bundles contributing one each is the normal case and there
is nothing to refuse. See {doc}`other-components`.

Tables go the other way: a declared table name that is *already* on the session
is an error too, so a table cannot shadow one the way a function can.
Catalogs are back on the function side of that line, and for a reason worth
reading before you declare one: see {doc}`table-providers`.

Strictly, whether a duplicate registration replaces or refuses is the
`SchemaProvider`'s own call, and the in-memory one a session starts with
refuses. `with_extensions` does not ask. It settles the question during resolve
and refuses whatever the destination would have done, which buys two things a
per-provider answer could not: the same rule wherever your table lands, and the
refusal arriving while a failure still costs nothing. A `SchemaProvider` that
would have replaced is therefore stricter through a bundle than through
{py:meth}`~datafusion.context.SessionContext.register_table`, which asks it
directly. Deregister the old table first if replacing is what you meant.

A table collides on where it lands rather than on how it was spelled. A name is
lowercased when it is parsed and filled out from the session's default catalog
and schema, so `Events`, `events` and `public.events` are one table, and two
bundles declaring any two of them collide. Comparing the spellings would let the
pair through resolution and leave the duplicate to surface from the insert, with
the first table already written — the one thing {ref}`the commit order
<ffi_internals_commit_order>` exists to prevent.

Your caller cannot rename your function, so stay out of the way: prefix the
names with something tied to your library.

(extension_bundles_transaction)=

## Failure and rollback

Nothing is written to the session until every factory has returned and every
component has been validated, so a factory that raises leaves the session
exactly as it was. A factory that mutates the context it is handed —
registering a table, say — is **not** rolled back, which is why bundle objects
must be configuration-only: create fresh components on each call, never cache
bound components, and do not retain the context passed in.

Declaring a component is what buys you that guarantee, and it is the whole
reason to prefer `udfs=(...)` over a `register_udf` call inside your hook.
Anything you declare is resolved and checked while a failure still costs
nothing, and is written only after every bundle in the call has succeeded.
Anything you register yourself is written immediately, before the other bundles
have even run. The ordering that makes this hold is recorded at
{ref}`ffi_internals_commit_order`.

The one thing that ordering costs you: functions are registered *after* the
planner hooks run, so `ctx.udfs()` inside your
`__datafusion_session_planner__` will not list a function declared in the same
call — not yours, and not another bundle's. Look one up at plan time instead,
where the registry is complete; a planner is called per query, long after the
install has finished. If you need a function at hook time, you already have the
object, because you are the one declaring it.

Tables are the single exception to committing last, and they are committed
first because of it. A declared table has its provider imported, its
destination schema resolved, and a name already taken refused, all while a
failure still costs nothing — but the insert itself goes through a
`SchemaProvider`, and a foreign one can still refuse what it reported as free.
Running that first means no planner is bound and no function is registered
behind it when it does.

Like every other derivation, the returned context is a handle on the *same*
session as the receiver — see {ref}`extension_sessions`. Only the Python-side
codec chains belong to the returned handle; the planner is installed on the
shared session and takes effect even if that handle is discarded.

`with_extensions` sidesteps the {ref}`one-level rebind <planner_codec_rebinding>`
entirely, and for nested planners too: every codec from every bundle is
installed before the first planner hook runs, so no layer — outer or fallback —
is ever captured against a partial chain. There is no "afterwards" within a
call. Prefer it over hand-layering whenever the planners you are composing all
ship as bundles.

[`datafusion-ffi-query-planner-example`]: https://github.com/apache/datafusion-python/tree/main/examples/datafusion-ffi-query-planner-example
