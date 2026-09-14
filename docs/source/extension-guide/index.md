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

(ffi)=
(extension_guide)=

# Extension Guide

This section is for people **writing** a library that plugs into
datafusion-python — a package that contributes table providers, functions, a
catalog, extension codecs, or a query planner, usually written in Rust and
exposed through [PyO3](https://pyo3.rs).

Two neighbouring audiences are served elsewhere:

- **Using** an extension library someone else published:
  {ref}`user_guide_extensions`.
- **Changing datafusion-python itself**, including the framing that makes this
  protocol work: {doc}`../contributor-guide/index`.

The protocol described here is a public, versioned contract. When a hook's
signature changes, the change is documented in
{doc}`../user-guide/upgrade-guides` with a before-and-after, and objects built
against a different DataFusion major version are rejected with a clear error
rather than used as-is.

## Start here

If you have not built an extension before, read {doc}`why-ffi` and
{doc}`capsule-protocol` in order. Together they explain why your library must
not depend on the `datafusion-python` crate, and the one convention every hook
follows. After that the pages are independent — go to the one matching what
you are contributing.

## The three roles in a query

A single query can involve three independent native libraries, and much of
this section only makes sense once they are distinct in your head:

- **datafusion-python** — the host. Owns the session, and decodes whatever
  comes back from the other two.
- **A provider library** — owns table providers, catalogs, and functions, plus
  the codecs that serialize them.
- **A planner library** — owns a query planner and the configuration it needs.

The worked examples in this repository use two separate crates,
[`datafusion-ffi-example`] and [`datafusion-ffi-query-planner-example`], so
each role has a distinct shared-library identity. A real library may play more
than one role; keeping them separate in the examples is what makes the
boundaries observable.

The session owns the codecs used for the exchange and supplies them to the
foreign planner. That is what lets the planner decode provider-owned objects,
and lets datafusion-python decode the physical plan the planner returns.

## Hook reference

Every integration point is a dunder method named `__datafusion_*__`. The
convention is uniform enough to be worth stating once: your object exposes the
getter, datafusion-python calls it, and it returns a `PyCapsule` wrapping an
FFI-safe struct. See {doc}`capsule-protocol` for what that means and
{ref}`extension_getter_argument` for the argument every getter in the middle
group receives.

Each hook's capsule name follows from its own name by
{ref}`the naming rule <extension_capsule_protocol>`, so it is not repeated
here.

| Hook | Contributes | Argument | Documented on |
| --- | --- | --- | --- |
| `__datafusion_table_provider__` | one table | codec source | {doc}`table-providers` |
| `__datafusion_table_provider_factory__` | tables built by `CREATE EXTERNAL TABLE` | codec source | {doc}`table-providers` |
| `__datafusion_catalog_provider__` | a named set of schemas | codec source | {doc}`table-providers` |
| `__datafusion_catalog_provider_list__` | the whole catalog namespace | codec source | {doc}`table-providers` |
| `__datafusion_schema_provider__` | a named set of tables | codec source | {doc}`table-providers` |
| `__datafusion_table_function__` | a table-valued function | session | {doc}`functions` |
| `__datafusion_scalar_udf__` | a scalar function | none | {doc}`functions` |
| `__datafusion_aggregate_udf__` | an aggregate function | none | {doc}`functions` |
| `__datafusion_window_udf__` | a window function | none | {doc}`functions` |
| `__datafusion_logical_extension_codec__` | a logical codec | session | {doc}`codecs` |
| `__datafusion_physical_extension_codec__` | a physical codec | session | {doc}`codecs` |
| `__datafusion_codec_id__` | the wire id a codec's payloads carry | — | {ref}`extension_codec_ids` |
| `__datafusion_query_planner__` | a query planner | session | {doc}`query-planners` |
| `__datafusion_session_components__` | a bundle's codecs | `ctx` | {doc}`bundles` |
| `__datafusion_session_planner__` | a bundle's planner, wrapping `fallback` | `ctx`, `fallback` | {doc}`bundles` |
| `__datafusion_physical_optimizer_rule__` | a physical optimizer rule | none | {ref}`extension_other_hooks` |
| `__datafusion_extension_options__` | typed entries in the session config | none | {ref}`extension_other_hooks` |
| `__datafusion_task_context_provider__` | the host's task context | none | {ref}`extension_task_context_provider` |

Three rows are not like the others. `__datafusion_task_context_provider__` is
implemented by the **host**, not by your library — you read it off the session
you are handed. `__datafusion_codec_id__` is a plain string attribute rather
than a method returning a capsule. And the two `session_` hooks are dispatched
from Python and return objects rather than capsules.

"codec source" in the argument column means the value is something you can
read the host's logical extension codec off, which is not always a session.
{ref}`extension_getter_argument` explains why, and what to do with it.

`python/tests/test_docstrings.py` compares this table against the hook names
the package actually dispatches, so a hook added or renamed without touching
this page fails the suite.

```{toctree}
:maxdepth: 2

why-ffi
capsule-protocol
table-providers
functions
codecs
bundles
query-planners
other-components
sessions
checklist
```

[`datafusion-ffi-example`]: https://github.com/apache/datafusion-python/tree/main/examples/datafusion-ffi-example
[`datafusion-ffi-query-planner-example`]: https://github.com/apache/datafusion-python/tree/main/examples/datafusion-ffi-query-planner-example
