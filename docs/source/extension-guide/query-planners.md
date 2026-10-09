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

(extension_planners)=

# Query planners

A query planner turns a logical plan into a physical one. Contributing your own
is how a library changes the way queries execute rather than what data they can
reach — a distributed engine is the motivating case, and an optimizing rewriter
is another.

The session owns the codecs used for the exchange and supplies them to your
planner. That is what lets the planner decode provider-owned objects, and lets
datafusion-python decode the physical plan the planner returns. Your planner
uses `FFI_QueryPlanner::new_with_ffi_codecs` with the two codecs it takes off
the session, and never touches a task-context provider directly. That also
matches what installation does anyway:
{py:meth}`~datafusion.SessionContext.set_query_planner` builds the planner
against the codecs of the session that will run the query.

`MyQueryPlanner` in [`datafusion-ffi-query-planner-example`] is the worked
implementation.

(planner_host_optimizer_rules)=

## Plan against your own optimizer rules

Your planner returns its plan as **protobuf**, not as a handle. Every query
therefore serializes what you produce, and anything in it that cannot be
encoded is your problem rather than a distant one.

That matters because of where physical optimization runs. Physical planning
applies `session.physical_optimizers()`, and when the session arrived over FFI
those rules are the *host's* — so each one runs back across the boundary and
hands you a `ForeignExecutionPlan` wrapping the result. `EnsureCooperative` is
on by default and will do exactly this. A stock `CooperativeExec` produced that
way has no reachable `try_to_proto`, so a node that is perfectly serializable
in the process that made it becomes unserializable in yours:

```text
Internal error: Unsupported plan and extension codec failed with
[This feature is not implemented: PhysicalExtensionCodec is not provided].
Plan: ForeignExecutionPlan { name: "CooperativeExec", ... }
```

A foreign node is also opaque to `downcast_ref`, so a planner that means to
*rewrite* the plan — inserting stages, say — cannot inspect what it was given.

Both problems go away if the rules run on your side. Wrap the session you were
handed in one that delegates everything except `physical_optimizers()`, and
return the stock rule set from there:

```rust
let local = LocalOptimizerSession::new(session);   // owns PhysicalOptimizer::default().rules
DefaultPhysicalPlanner::default()
    .create_physical_plan(logical_plan, &local)
    .await?
```

`LocalOptimizerSession` in
[`examples/distributed/engine-library`](https://github.com/apache/datafusion-python/tree/main/examples/distributed/engine-library)
is about twenty delegating methods and one override.

Delegating to a `fallback` avoids the problem differently, by not planning at
all: the plan comes back from whoever you delegated to, already concrete. That
is the right choice for a planner that only layers behaviour on another, and
the wrong one for a planner that needs to rewrite the result — you cannot
rewrite a subtree you hold an opaque handle to. A planner does one or the
other.

## One planner per session

A session holds exactly one query planner. Calling `set_query_planner` again
**replaces** the installed planner instead of layering another one.

To chain planners, have the new planner wrap the capsule returned by
{py:meth}`SessionContext.__datafusion_query_planner__ <datafusion.SessionContext.__datafusion_query_planner__>`,
captured before the new planner is installed, and delegate to it explicitly:

```python
fallback = ctx.__datafusion_query_planner__()
ctx.set_query_planner(MyPlanner(fallback=fallback))
```

`set_query_planner` returns nothing. The query planner lives in `SessionState`,
so it is a property of the session rather than of a handle on it, and
installing one is visible to every context sharing that session. See
{ref}`extension_sessions`.

If the planners you are composing all ship as
{ref}`bundles <extension_bundles>`, prefer `with_extensions` — it does the
nesting for you and cannot capture a partial codec chain.

(planner_codec_rebinding)=

## Install codecs before a layered planner

Installing a codec on a session that already has a foreign planner rebuilds
that planner against the new chain: there is one planner, and it has to carry
the codecs currently in force. But that rebuild swaps the codecs on the
installed `ForeignQueryPlanner` handle, and **only that handle**.

A planner that wraps a fallback resolved that fallback when *it* was installed,
and holds the result inside its own library's private data — behind a
`create_physical_plan` function pointer, with no Python-side handle. A codec
installed afterwards therefore reaches the outer planner and not the fallback,
which keeps whichever codecs were in force when it was imported.

The stale codecs stay usable rather than dangling — they hold weak handles to
the one `Arc<SessionContext>` the session keeps alive — so the effect is a
fallback hop serializing with an older codec, not a failure. Neither side can
repair it; the reasons are in {ref}`ffi_internals_rebinding`, and a fix has to
come from upstream
([apache/datafusion#24762](https://github.com/apache/datafusion/issues/24762)).

So, three rules:

- **Install the codecs before a layered planner.**
- If a codec has to go in afterwards, install the outer planner again *on the
  handle that holds the new codec* — that re-runs its getter, which re-imports
  the fallback against that handle's codecs. Re-installing on the original
  handle rebinds the session's planner back to the original handle's codecs
  instead, which is the trap the query-planner example's test suite pins.
- Better, use {ref}`extension_bundles`, where there is no "afterwards" within a
  call.

:::{note}
This is invisible in the examples here, which use one fallback in the same
cdylib as its wrapper; `datafusion-ffi` short-circuits a same-library hop
rather than serializing, so no codec runs. A fallback in a *different* library
would serialize, and would do it with the codecs it was imported with.
:::

[`datafusion-ffi-query-planner-example`]: https://github.com/apache/datafusion-python/tree/main/examples/datafusion-ffi-query-planner-example
