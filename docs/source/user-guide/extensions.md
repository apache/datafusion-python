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

(user_guide_extensions)=

# Using extension libraries

An extension library is a separate package that teaches a
{py:class}`~datafusion.SessionContext` something it does not know on its own —
a new data source, extra functions, or a different way of executing your
queries. You install it with `pip`, hand it to your session, and keep writing
the same SQL and DataFrame code.

Examples in the wild include [delta-rs](https://delta-io.github.io/delta-rs/),
which exposes Delta Lake tables to DataFusion, and the two worked examples in
this repository under
[`examples/`](https://github.com/apache/datafusion-python/tree/main/examples).

## How an extension reaches your session

Which route your library takes determines how much setup you do.

**Tables register directly.** If the library gives you a table, register it
the same way you would register a CSV file. No extra setup:

```python
from datafusion import SessionContext
import my_tables

ctx = SessionContext()
ctx.register_table("events", my_tables.TableProvider("s3://bucket/events"))
ctx.sql("SELECT count(*) FROM events").show()
```

**Libraries that change how queries run need to be installed on the session.**
A distributed engine, or anything that rewrites your query plan, has to be
attached to the session before it can do its work. That is what
{py:meth}`~datafusion.SessionContext.with_extensions` is for. The library
documents an object — often called `Extension` — that you pass to it:

```python
from datafusion import SessionContext
import my_engine

ctx = SessionContext().with_extensions(my_engine.Extension("scheduler:50050"))
ctx.register_table("events", my_engine.TableProvider("s3://bucket/events"))
ctx.sql("SELECT count(*) FROM events").show()
```

**Tables and functions arrive by whichever route their library chose.** A
library offering one or two hands you the objects themselves, and you register
each — a table as above, a function after wrapping it:

```python
from datafusion import udf

ctx.register_udf(udf(my_library.MyScalarUDF()))
```

A library shipping a set of them packages them in its `Extension` object
instead, so `with_extensions` installs them all along with everything else it
provides — and the library picks the names — leaving nothing per-item for you to
do. Its documentation says which.

`with_extensions` returns a context; use the returned one. It shares
everything else with the context you called it on, so tables you registered
before the call are still there — and a library shipping a table under a name
you have already used cannot replace it. The call fails and writes nothing, so
drop yours with {py:meth}`~datafusion.context.SessionContext.deregister_table`
first, or install onto a context that does not hold it. See
{ref}`extension_bundles_collisions`.

## Using more than one library

Pass them all to a single call:

```python
ctx = SessionContext().with_extensions(
    my_tables.Extension(),
    my_engine.Extension("scheduler:50050"),
)
```

One call rather than several is worth preferring: it lets the libraries see
each other, which they cannot do if you install them one at a time. Order
rarely matters. When a library needs a particular position — usually "list me
last" for something that wraps the others — it says so in its own
documentation.

## Three things that will bite you

**Two libraries can claim one function name.** If both ship a function of the
same kind under the same name, the call raises a `ValueError` naming both,
rather than letting one silently replace the other:

```text
ValueError: Two extensions declare a scalar function named 'normalize': ...
```

You cannot rename another library's function from your own code, so the fix is
to use two sessions, one per library, and query each for what only it provides.
Worth reporting upstream too: the library whose names are the less specific
should be prefixing them. A function shadowing a *built-in* is not a collision
and raises nothing — that is a supported thing for a library to do. See
{ref}`extension_bundles_collisions`.

Check the argument positions the message names before you go looking for a
second library. Passing one extension twice collides with itself, and an
extension list assembled from a plugin registry is the usual way that happens.

**Keep your context alive.** A `DataFrame` or a plan does not keep its session
alive on its own. If a context is garbage-collected while something built from
it is still in use, the next query fails with:

```text
TaskContextProvider went out of scope over FFI boundary
```

Almost always this is a helper that built a context locally and returned a
DataFrame:

```python
# Wrong — ctx is collected when the function returns.
def load():
    ctx = SessionContext().with_extensions(my_engine.Extension())
    return ctx.sql("SELECT * FROM events")

# Right — hand back the context too, or keep it on an object that lives
# as long as the frames derived from it.
def load():
    ctx = SessionContext().with_extensions(my_engine.Extension())
    return ctx, ctx.sql("SELECT * FROM events")
```

**Versions have to match.** An extension library is compiled against one
DataFusion version. A mismatch raises an `ImportError` naming the version it
found and the version expected, at the moment you register or install the
library — not silently at query time. If you see one, upgrade or downgrade the
extension library so its DataFusion version matches this package's. See
{ref}`extension_version_mismatch`.

## Checking what a session knows about

{py:meth}`~datafusion.SessionContext.logical_extension_codec_ids` and
{py:meth}`~datafusion.SessionContext.physical_extension_codec_ids` list which
libraries a session has been taught about. Useful when a query fails and you
want to confirm the library actually got installed:

```python
ctx = SessionContext().with_extensions(my_engine.Extension())
ctx.logical_extension_codec_ids()
# ['my_engine.LogicalCodec']
```

An empty list means nothing extra is installed.

For functions, {py:meth}`~datafusion.SessionContext.udfs`,
{py:meth}`~datafusion.SessionContext.udafs` and
{py:meth}`~datafusion.SessionContext.udwfs` return the names a session knows.
Both the library's and every DataFusion built-in are in there, so look for the
name rather than reading the whole list:

```python
"my_engine_normalize" in ctx.udfs()
# True
```

## Next steps

- {ref}`distributed_query_engines` — running your queries across several
  machines with an engine library.
- {ref}`distributed_expressions` — the other road to parallelism, where you
  decide the partitioning and ship expressions to a worker pool.
- {ref}`ffi` — writing an extension library of your own.
