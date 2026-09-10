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

## Two kinds of extension

Which one you have determines how much setup you do.

**Tables and functions register directly.** If the library gives you a table
or a function, register it the same way you would register a CSV file. No
extra setup:

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

`with_extensions` returns a context; use the returned one. It shares
everything else with the context you called it on, so tables you registered
before the call are still there.

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

## Two things that will bite you

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

## Next steps

- {ref}`distributed_query_engines` — running your queries across several
  machines with an engine library.
- {ref}`distributed_expressions` — the other road to parallelism, where you
  decide the partitioning and ship expressions to a worker pool.
- {ref}`ffi` — writing an extension library of your own.
