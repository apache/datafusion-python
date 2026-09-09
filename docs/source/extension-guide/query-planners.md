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
