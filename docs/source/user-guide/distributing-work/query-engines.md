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

(distributed_query_engines)=

# Distributed query engines

A distributed query engine takes the query you already wrote and runs
it across several machines for you. You do not partition anything by
hand and you do not change your queries — the engine installs itself
on your session, rewrites the plan into stages, and executes those
stages on its own workers.

This is the counterpart to {ref}`distributed_expressions`, where
*you* decide the partitioning and ship one expression per slice of
data. Choose an engine when you want a single query spread across a
cluster; choose expressions when you already know how to split the
work and only need parallelism.

## How an engine attaches to your session

An engine library ships an object you hand to
{py:meth}`~datafusion.SessionContext.with_extensions`:

```python
from datafusion import SessionContext
import my_engine

ctx = SessionContext().with_extensions(my_engine.Extension("scheduler:50050"))

ctx.register_table("events", my_engine.TableProvider("s3://bucket/events"))
ctx.sql("SELECT country, count(*) FROM events GROUP BY country").show()
```

The `sql` call is unchanged from a single-process program. What
changed is underneath it: installing the engine gave the session a
query planner of the engine's own, and that planner is what turns
your plan into distributed stages.

Two consequences worth knowing:

- **The engine must be installed before you run the query**, not
  before you register tables. Registration order does not matter;
  `with_extensions` is a session-level setup step.
- **The engine has to be able to reach your tables and functions.**
  It ships the plan to its workers, so anything the plan references
  has to be reconstructible there. Table providers from the engine's
  own library always are. A Python UDF may or may not be — the engine
  library documents what it supports, and
  {ref}`distributed_udf_portability` describes the constraints that
  apply to Python callables crossing a process boundary in general.

If you install more than one library, pass them in one
`with_extensions` call so they can see each other. See
{ref}`user_guide_extensions` for the details of installing extension
libraries, and {ref}`ffi` if you want to write an engine yourself.

(distributed_worker_parity)=

## What a worker has to reproduce

An engine ships your plan to a process that has never seen your session. That
process has to be able to rebuild everything the plan refers to, and there is
**no way to snapshot a {py:class}`~datafusion.SessionContext` and restore it
somewhere else**: {py:class}`~datafusion.SessionConfig` is write-only from
Python, and while `information_schema.df_settings` can be read back, it lists
`datafusion.runtime.*` keys that have no configuration namespace to set them
into again.

So parity is not automatic. It is something you build the same way twice, and
these are the things that have to match. Most engines handle several of them
for you — check which.

- **Codec ids.** A plan records which codec wrote each payload, and decoding
  routes on that id. Pin ids with `__datafusion_codec_id__` rather than
  letting them default to a class's import path, and compare
  {py:meth}`~datafusion.SessionContext.physical_extension_codec_ids` on both
  sides before shipping anything. See {ref}`extension_codec_ids`.
- **Functions the plan names.** A function resolves either from the receiving
  session's registry or from a codec. Either is enough; neither is automatic.
  A Python UDF is the exception — it travels inside the plan.
- **Object stores**, registered *before* the plan is decoded rather than
  before it is executed. Decoding a Parquet scan resolves its store.
- **Config extensions**, installed before any namespaced key is set. Setting a
  key in a namespace that has not been declared is an error, not a no-op.
- **The Python minor version**, if any inline Python UDF is involved.
  Cloudpickle payloads are stamped with the sender's version and refuse to
  load on another. Launching workers with `sys.executable` makes this true by
  construction; a hardcoded `python` does not.
- **The `cloudpickle` version**, which is *not* stamped. Cross-version loading
  usually works and is not guaranteed. Pin it.
- **`target_partitions`**, if a worker re-plans anything. Left to default it
  follows the core count, so two differently-sized machines disagree.

Two more that are about lifetime rather than configuration:

- **One session per worker, alive for the whole process.** An FFI codec
  resolves names against the session captured when its bundle was installed,
  and that reference is weak — see {ref}`extension_sessions`.
- **Pass a context to `to_bytes`.** {py:meth}`~datafusion.ExecutionPlan.to_bytes`
  takes an optional context, and without one it uses an empty codec chain that
  cannot encode any library's nodes. The argument being optional makes this
  easy to miss, because the failure only appears once an extension node is in
  the plan.

And one that is about the engine rather than the session:

- **Where stages exchange results, scoped to one query.** An engine that
  publishes stage output to a location you name, and numbers its stages from
  scratch per plan, will read a previous query's results back if you point two
  queries at the same location. Ask your engine whether it isolates that for
  you; if it does not, give each query its own. This one is worth confirming
  rather than assuming, because when the two plans happen to agree on schema
  the wrong answer arrives without an error.

## Available engines

Query-level distribution is being built upstream. Neither project
below is usable from datafusion-python yet; both sections will fill
in as the integrations land. In the meantime the repository contains a
worked example you can read and run:
[`examples/distributed`](https://github.com/apache/datafusion-python/tree/main/examples/distributed)
splits a query across worker processes using three separate extension
libraries, and is written to make each of the requirements above visible —
including the ways they fail.

### datafusion-distributed

🚧 *Work in progress upstream — not yet usable from datafusion-python.*

[datafusion-distributed](https://github.com/apache/datafusion-distributed)
splits a single physical plan into stages and runs each stage on a
different worker node. The driver writes a SQL or DataFrame query
once; the runtime handles partitioning, shuffles, and reassembly.

A datafusion-python integration is in development. In the meantime,
{ref}`distributed_expressions` covers most use cases that do not
require automatic plan partitioning.

### Apache Ballista

🚧 *Work in progress upstream — not yet usable from datafusion-python.*

[Apache Ballista](https://github.com/apache/datafusion-ballista)
provides distributed query execution on top of DataFusion with a
scheduler / executor model better suited to long-lived cluster
deployments. A datafusion-python integration is on the roadmap.
