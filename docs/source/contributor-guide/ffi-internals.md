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

(ffi_internals)=

# FFI framing internals

Read this before changing how datafusion-python frames FFI components. You do
**not** need it to write an extension library — that is the
{ref}`Extension Guide <extension_guide>`.

The invariants here are the reasons the extension-facing rules are shaped the
way they are. Each one has an "obvious" simplification that does not work, and
the point of this page is to record why, so the next person does not spend a
week rediscovering it.

For the wire format itself — how a codec id is stored alongside a payload,
routed back on decode, and which two cases stay unframed — see the module
documentation in `crates/core/src/codec.rs`. It is the authority; this page
does not restate it.

(ffi_internals_one_arc)=

## One session, one `Arc<SessionContext>`

Every codec handed to a foreign object carries an `FFI_TaskContextProvider`,
and that type holds its provider **weakly**. A registered catalog provider
upgrades the handle on every `supports_filters_pushdown` and every `scan`.
Those handles are bound to one particular `Arc<SessionContext>` allocation, not
to the logical session, so anything that replaces the allocation orphans all of
them and the next query fails with `TaskContextProvider went out of scope over
FFI boundary`.

So a `PySessionContext` keeps the `Arc<SessionContext>` it was created with for
its whole life. Installing a query planner writes the new `SessionState` back
through `state_ref()`, exactly as `add_physical_optimizer_rule` does, rather
than deriving a replacement context. The session id is carried across that
rewrite — `SessionStateBuilder` mints a fresh one otherwise — so `session_id()`
and every `TaskContext` the session hands out keep agreeing.

Repairing the damage instead of avoiding it does not work in general. A context
can rebuild the codecs it holds in its own fields, but a codec already embedded
in a registered `FFI_CatalogProvider` — and in every `FFI_SchemaProvider` and
`FFI_TableProvider` minted from it — is not reachable from Python at all. Nor
can the codec simply retain the session that built it: a codec handed to a
provider is routinely registered straight back into that same session, which
would close the cycle
`SessionContext -> catalog -> FFI provider -> FFI codec -> SessionContext` and
leak it.

`SessionContext.enable_url_table` is the one exception. It clones the
underlying `SessionContext`, so the returned context has an allocation of its
own and must not outlive the receiver. It also forks the session's state while
keeping its id, so two handles report one `session_id()` with divergent
configuration. That is a bug rather than a design, tracked in
[apache/datafusion-python#1708](https://github.com/apache/datafusion-python/issues/1708);
do not copy the pattern.

(ffi_internals_rebinding)=

## Why planner codec rebinding is one level deep

Installing a codec on a session that already has a foreign planner rebuilds
that planner against the new chain. The rebuild swaps the codecs on the
installed `ForeignQueryPlanner` handle, and only that handle. A planner that
wraps a fallback resolved that fallback when *it* was installed, and holds the
result inside its own library's private data — behind a `create_physical_plan`
function pointer, with no Python-side handle. A codec installed afterwards
therefore reaches the outer planner and not the fallback, which keeps whichever
codecs were in force when it was imported.

Neither side can repair that:

- **The host cannot reach it.** `FFI_QueryPlanner::new_with_ffi_codecs` unwraps
  exactly one `ForeignQueryPlanner` layer. There is no deeper handle to unwrap
  — the same situation as a codec embedded in a registered
  `FFI_CatalogProvider`.
- **The planner library cannot re-derive it.** `FFI_QueryPlanner` holds its
  codecs by value, and `Session` exposes no accessor for the ones the host
  currently has, so `create_physical_plan` cannot pick them up from the session
  it is handed. The rebuild has to be eager, and an eager rebuild only sees the
  top layer.

A fix has to come from upstream, and is tracked in
[apache/datafusion#24762](https://github.com/apache/datafusion/issues/24762).

The stale codecs stay usable rather than dangling — they hold weak handles to
the one `Arc<SessionContext>` that the previous section keeps alive — so the
effect is a fallback hop serializing with an older codec, not a failure. It is
also invisible to the examples in this repository, which use one fallback in
the same cdylib as its wrapper; `datafusion-ffi` short-circuits a same-library
hop rather than serializing, so no codec runs. A fallback in a *different*
library would serialize, and would do it with the codecs it was imported with.

The extension-facing consequence — install codecs before a layered planner, and
prefer `with_extensions` — is documented at {ref}`planner_codec_rebinding`.

(ffi_internals_commit_order)=

## Why `with_extensions` commits last

`with_extensions` promises that a bundle which raises leaves the session as it
was. Keeping that promise is an ordering constraint on the implementation,
because the components a bundle declares no longer all live on the returned
handle — functions are registered on the shared `SessionState`, and the planner
is bound there too.

A call therefore splits into a part that may fail and a part that may not:

1. **Collect.** Every `__datafusion_session_components__` runs.
2. **Chains.** The codecs are assembled into the returned handle. Codec chains
   live on that handle rather than on the session, so this step writes nothing
   even though it can fail on a bad capsule or a duplicate id.
3. **Resolve.** Every declared function is wrapped and every name is checked,
   and every `__datafusion_session_planner__` runs against the completed
   chains.
4. **Commit.** The planner is bound and the functions are registered.

Only step 4 touches the session, and every step that can fail happens before
it. This is a rule for the next field added to
`SessionExtensionComponents`, not only a description of the current code: a new
kind of component must do its fallible work — importing a capsule, resolving a
name — in step 3, so that step 4 cannot raise part-way through.

There is nothing to roll back to if it does. The returned handle shares one
session with the receiver, so the damage is visible from every other handle;
and undoing a registration is not the same as restoring what it displaced,
because deregistering a function that shadowed a built-in removes the built-in
too. The split is cheaper than an undo log that cannot be written correctly.

The extension-facing statement of this is
{ref}`extension_bundles_transaction`, which says only that declaring a
component is safe where registering one during the hook is not.

## Two argument kinds for one convention

`CapsuleGetterArg` in `crates/util/src/lib.rs` distinguishes three cases: no
argument, the session, and the host's logical extension codec as a bare
capsule. Provider and catalog registration passes the codec; the extension
codecs, the query planner, and table functions get the session.

The distinction exists so that a `TypeError` from a getter that refused its
argument can be rewritten into an `ImportError` naming what the getter *should*
accept. Getting that message wrong sends an extension author to fix the wrong
signature, which is why every capsule getter routes through
`call_capsule_getter` rather than calling `getattr` directly. Three importers
previously each had their own copy of that logic and each missed later
corrections to it.

When adding a hook, decide which arm it needs by what its FFI constructor
requires: a task-context provider means it must have the session, since you
cannot get a provider off a capsule. A codec alone means either works, and
passing the codec keeps the host from handing out session handles it does not
need to.

The extension-facing statement of this is {ref}`extension_getter_argument`,
which deliberately describes the argument by capability — "something you can
read the host's logical codec off" — rather than by type.
