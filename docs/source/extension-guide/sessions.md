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

(extension_sessions)=

# Sessions, handles, and lifetimes

One `SessionContext` in Python is a *handle* on a session, not the session
itself. Several handles can share one session, and which handle you call
something on sometimes matters. This page is the set of rules that follow from
that — the ones an extension author trips over.

## The rule that surprises people

> The session's query planner carries the codecs of the handle that most
> recently installed one. Every other path — `Expr.to_bytes(ctx)`,
> `ExecutionPlan.to_bytes(ctx)`, registering a provider — uses the codecs of
> the handle you call it on.

Those can be different handles, and then one session has two codec chains in
effect at once:

```python
ctx = ctx.with_logical_extension_codec(codec_a)
ctx.set_query_planner(planner)
ctx.with_logical_extension_codec(codec_b)  # discarded

Expr.to_bytes(expr, ctx)   # encodes with [codec_a, default] -- ctx's own field
ctx.sql(...).collect()     # plans with [codec_b, codec_a, default] -- the discarded
                           # handle's chain, installed on the shared session
```

Chaining `ctx = ctx.with_...(...)` keeps the two in step, which is why every
example in this guide does. The query-planner example's test suite pins the
divergence.

## What a derived context shares

{py:meth}`~datafusion.SessionContext.enable_url_table`,
{py:meth}`~datafusion.SessionContext.with_logical_extension_codec`,
{py:meth}`~datafusion.SessionContext.with_physical_extension_codec`,
{py:meth}`~datafusion.SessionContext.with_python_udf_inlining`, and
{py:meth}`~datafusion.SessionContext.with_extensions` return a new
`SessionContext` wrapping the *same* underlying session. Only the Python-side
codec settings differ; catalogs, tables, registered functions, and
configuration are the one shared session, so a registration on either side is
visible to both.

There is one `Arc<SessionContext>` per session, which is what makes the weak
task-context-provider scheme work: a component bound through any handle stays
valid while *any* handle on that session is alive, so there is no way to bind a
component to an intermediate handle and have it dangle when that handle is
dropped. See {ref}`ffi_internals_one_arc` for why the allocation is kept
rather than replaced.

`set_query_planner` does not return anything, because the query planner lives
in `SessionState` and is therefore a property of the session rather than of a
handle on it. Installing one is visible to every context sharing that session —
including ones a `with_*` call returned earlier. Installing a codec on a
session that already has a foreign planner rebuilds that planner against the
new chain for the same reason. This happens on the shared session, so it takes
effect even if the returned context is discarded:
`ctx.with_python_udf_inlining(...)` whose result is thrown away still leaves
the session's planner carrying the codecs of that discarded handle. A call that
changes nothing is exempt — asking for the inlining setting a context already
has returns a handle without touching the session.

Order between installing codecs and installing a planner is a readability
preference rather than a requirement, since installing a codec after a planner
rebuilds the planner against it. The exception is a *layered* planner, where
codecs-first is a requirement: see {ref}`planner_codec_rebinding`.

## Keep a context alive

The session owns every installed component's task-context provider, and
dependent objects do not extend its lifetime. A `DataFrame`, logical plan, or
capsule can outlive every context on the session, but any operation that
reaches an FFI codec after the last one is collected fails with:

```text
TaskContextProvider went out of scope over FFI boundary
```

Keep a context alive for as long as objects derived from it are in use. This is
the rule most likely to reach your users as a bug report against your library,
so it is worth stating in your own documentation too — the user-facing version
is in {ref}`user_guide_extensions`.

The same rule applies to a capsule you take off a context inside your own code:
a codec capsule taken from a throwaway `SessionContext()` names a session that
is already gone and fails on first use.

Enabling URL tables takes effect on the shared session even if the returned
handle is discarded. The returned handle keeps the receiver's codec settings
unchanged, and repeated calls do not nest catalog wrappers.
