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

`SessionContext.enable_url_table` follows the same rule. It replaces only the
catalog list through `state_ref()` and returns a handle sharing the original
allocation. The idempotence check and catalog replacement share one write lock,
so concurrent calls cannot nest wrappers or overwrite a newer catalog list.

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
was, apart from anything a hook writes to the context it is handed
({ref}`extension_bundles_transaction`). Keeping that promise is an ordering constraint on the implementation, not
a property of any one step, because the planner is bound on the shared
`SessionState` rather than on the returned handle.

A call therefore splits into four steps, of which only the last writes:

1. **Collect.** Every `__datafusion_session_components__` runs and its codecs
   are gathered. Nothing is installed yet, so a hook that raises here has
   touched nothing.
2. **Chains.** The codecs are assembled into the returned handle. Codec chains
   live on that handle rather than on the session, so this step writes nothing
   to the session even though it can fail on a bad capsule or a duplicate id.
3. **Resolve.** Every `__datafusion_session_planner__` runs, in argument order,
   against the completed chains, and each supplied planner is exported to a
   capsule. Every hook that can raise has run by the end of this step.
4. **Commit.** The accumulated planner is re-imported from its capsule and
   bound, in a single `SessionState` rebuild. The bind is skipped entirely when
   the call installed nothing, so an empty call does not drag a planner sitting
   on another handle's codecs onto this one's.

Only step 4 touches the session, and it is not itself infallible:
`_install_extension_planner` runs `ffi_query_planner_from_pycapsule` before it
calls `set_session_query_planner`, which cannot fail. The property that keeps
the promise is therefore about order, not about any step being incapable of
raising — every fallible operation, including the ones inside the commit,
completes before the first write.

That is the rule for the next field added to `SessionExtensionComponents`, not
only a description of the current code: a new kind of component must do its
fallible work — importing a capsule, resolving a name — before anything is
written, so no failure can leave the session half-updated.

There would be nothing to roll back to if one did. The returned handle shares one
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
