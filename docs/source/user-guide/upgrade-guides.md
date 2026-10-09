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

# Upgrade Guides

## DataFusion 56.0.0

### `sort` and `order_by` now order `NULLS LAST` by default

Calling `.sort(...)` on a DataFrame or using the `.order_by(...)` function now
orders rows with `NULLS LAST` by default, instead of `NULLS FIRST`. This follows
the same behavior as SQL's `ORDER BY` or the DataFrame's `sort_by(...)`.

To go back to the previous behavior, we need to explicitly specify
`nulls_first=True`. Example:

```python
# sort
df.sort(column("a"), nulls_first=True)

# order by
expr = f.first_value(column("a")).over(
    Window(
        partition_by=partition,
        order_by=f.order_by(column("b"), nulls_first=True)
    )
)
```

## DataFusion 55.0.0

### URL tables share the original session

`SessionContext.enable_url_table()` now enables URL tables on the existing session
and returns another handle on it. Previously it copied session state into a separate
session while retaining the same session id. Configuration and function registrations
could then diverge, and replacing the original handle could invalidate FFI providers.

Before, callers had to use the returned context to query file paths:

```python
ctx = SessionContext()
enabled = ctx.enable_url_table()
# Only enabled could query local file paths as tables.
```

After, both handles share configuration, registrations, and URL table support:

```python
ctx = SessionContext()
ctx.enable_url_table()  # Takes effect even when the returned handle is discarded.
```

Existing `ctx = ctx.enable_url_table()` calls continue to work and now retain FFI
providers bound to the original session. Repeated calls have no effect. To keep a
session without URL table support, create a separate `SessionContext` explicitly.

### FFI codec hooks receive the session

This release extends the change made in 52.0.0 to the remaining
{ref}`extension_capsule_protocol` hook methods. Users who contribute their own
`LogicalExtensionCodec` or
`PhysicalExtensionCodec` via FFI must update
`__datafusion_logical_extension_codec__` and
`__datafusion_physical_extension_codec__` to accept an additional
`session: Bound<PyAny>` parameter, and take the `TaskContextProvider` from that
session rather than constructing a `SessionContext` of their own.

Before:

```rust
fn __datafusion_physical_extension_codec__<'py>(
    &self,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyCapsule>> {
    let ctx_provider: Arc<dyn TaskContextProvider> = Arc::clone(&self.ctx_provider);
    let ffi = FFI_PhysicalExtensionCodec::new(inner, Some(runtime), &ctx_provider);
    PyCapsule::new_with_value(py, ffi, cr"datafusion_physical_extension_codec")
}
```

After:

```rust
fn __datafusion_physical_extension_codec__<'py>(
    &self,
    py: Python<'py>,
    session: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyCapsule>> {
    let ctx_provider = ffi_task_context_provider_from_pycapsule(&session)?;
    let ffi = FFI_PhysicalExtensionCodec::new(inner, Some(runtime), ctx_provider);
    PyCapsule::new_with_value(py, ffi, cr"datafusion_physical_extension_codec")
}
```

The dropped `&` on the last argument is not a typo. That parameter is
`impl Into<FFI_TaskContextProvider>`, so it accepts either an
`&Arc<dyn TaskContextProvider>`, as before, or an `FFI_TaskContextProvider`,
which is what `ffi_task_context_provider_from_pycapsule` hands back. Both forms
compile; the argument changes because the provider now comes from the session
rather than from a field.

A codec that keeps its own `SessionContext` still compiles, but its decode
callbacks resolve names against that empty session instead of the one running
the query, so a function registered with `SessionContext.register_udf` is not
visible to it. Taking the provider from `session` also removes a lifetime
hazard: `FFI_TaskContextProvider` holds its provider weakly, so a context
constructed inside the getter is already dropped by the time the capsule is
used.

`SessionContext` accepts the argument on its own capsule getters and ignores
it, so existing calls such as `ctx.__datafusion_logical_extension_codec__()`
continue to work unchanged.

The provider and catalog getters —
`__datafusion_table_provider_factory__`, `__datafusion_catalog_provider__`,
`__datafusion_catalog_provider_list__`, and `__datafusion_schema_provider__` —
now receive the host's logical extension codec as a bare
`datafusion_logical_extension_codec` capsule rather than a session.
`__datafusion_table_provider__` receives a session from
`SessionContext.register_table` and a codec capsule from
`Schema.register_table`. No code change is needed in either case: pass the
argument to `ffi_logical_codec_from_pycapsule`, which calls the codec getter
when the object has one and returns the object unchanged when it does not. Do
not inspect the argument's type. See {ref}`extension_getter_argument`.

New in this release, `__datafusion_query_planner__` follows the same protocol.
It receives the session and takes both extension codecs from it, so a planner
library never builds a `TaskContextProvider` at all. Install one with
`SessionContext.set_query_planner(planner)`, which mutates the session the same
way `add_physical_optimizer_rule` does and returns nothing — the query planner
lives in `SessionState`, so it belongs to the session rather than to a
particular handle on it. See {ref}`extension_planners` for the full protocol.

(extension_version_mismatch)=

### Mismatched extension libraries now fail loudly

Objects imported through the capsule protocol are checked against the major
version of `datafusion-ffi` this package was built with. A table provider,
extension codec, or query planner produced by a library built against a
different DataFusion major version now raises an `ImportError` naming the
version found and the one expected, instead of being used as-is. Table
providers previously performed no such check.

This is a diagnostic rather than a soundness guarantee — reading the version
out of the struct already assumes the local field layout — but it turns the
common "extension library built against the wrong DataFusion" mistake into a
clear message rather than undefined behaviour on first use.

`FFI_TaskContextProvider`, `FFI_TableProviderFactory`, and `FFI_ExtensionOptions`
carry no version field, so objects of those types cannot be checked.

### Extension codecs compose instead of replacing

`SessionContext.with_logical_extension_codec` and
`with_physical_extension_codec` previously replaced whichever codec was already
installed, so a session could only ever have one. Installing a second codec
silently discarded the first, and plans failed later with a confusing decode
error. Both methods now append to a chain, and a session can carry codecs from
several independent libraries at once.

**No change is required in an extension codec.** Keep implementing
`LogicalExtensionCodec` or `PhysicalExtensionCodec` exactly as before. Your codec
is still handed back exactly the bytes it wrote, and is never handed a payload
another library's codec wrote.

Callers relying on replacement semantics — installing a codec in order to remove
a previous one — are affected. There is no way to remove an installed codec.

A serialized plan now records which codec wrote each payload, as a short id taken
from the codec's class. Two behaviours follow from that:

- Installing two instances of one class raises a `ValueError`, because both would
  claim the same id. Pass `codec_id=` to tell them apart.
- A codec installed from a bare `PyCapsule` has no class to take an id from, so it
  gets one private to the session that installed it. It works normally on that
  session, but a plan it encodes cannot be decoded on an unrelated one. Pass
  `codec_id=` if those plans have to cross sessions.

```python
ctx = ctx.with_logical_extension_codec(lib_a.codec())
ctx = ctx.with_logical_extension_codec(lib_b.codec())  # no longer discards lib_a

# Two instances of one class need distinct ids.
ctx = ctx.with_logical_extension_codec(lib_a.Codec(), codec_id="lib_a.reader")
ctx = ctx.with_logical_extension_codec(lib_a.Codec(), codec_id="lib_a.writer")

ctx.logical_extension_codec_ids()
```

Serialized plans change shape once an extension codec is installed, because each
payload now records which codec wrote it. A session with no extension codecs
installed produces the same bytes as before, as do functions encoded by name.
Regenerate any plan you serialized with an earlier release and stored for later
use, if it was produced by a session with an extension codec installed.

### Capsule-getter protocols moved to `datafusion.extensions`

`PhysicalOptimizerRuleExportable` now lives in `datafusion.extensions`, next to
the other protocols an extension library implements against. It was previously
importable from `datafusion.context`, and that path is gone.

```python
from datafusion.context import PhysicalOptimizerRuleExportable  # before
from datafusion.extensions import PhysicalOptimizerRuleExportable  # after
```

This affects type annotations only. The protocol is structural and not
`@runtime_checkable`, so nothing imports it to call `isinstance`, and
`SessionContext.add_physical_optimizer_rule` is unchanged — a rule object that
worked before still works, whether or not its library names the protocol
anywhere.

The bundle protocols added in this release —  `QueryPlannerExportable`,
`SessionComponentsExportable`, and `SessionPlannerExportable` — are reached the
same way, through `datafusion.extensions` rather than the package root. They are
new in 55.0.0, so no earlier import path existed. `SessionExtensionComponents`
stays at the root, because a bundle constructs one rather than merely naming it:

```python
from datafusion import SessionExtensionComponents
from datafusion.extensions import SessionComponentsExportable
```

### `SessionContext.execute` renamed its second parameter

The parameter is a single partition index, not a count, and is now named
`partition` rather than `partitions`. Positional calls are unaffected; update
any call passing it by keyword.

```python
ctx.execute(plan, partitions=0)  # before
ctx.execute(plan, partition=0)  # after
```

### More aggregate functions accept `distinct`

{py:func}`~datafusion.functions.bit_and`,
{py:func}`~datafusion.functions.bit_or`,
{py:func}`~datafusion.functions.mean`,
{py:func}`~datafusion.functions.percentile_cont`,
{py:func}`~datafusion.functions.quantile_cont`, and
{py:func}`~datafusion.functions.string_agg` now accept a `distinct` argument.
As with `sum` and `avg` in 54.0.0, `distinct` is inserted *before* `filter`, so
code that passed `filter` (or, for `string_agg`, `order_by`) positionally must
pass it by keyword.

```python
f.bit_and(column("a"), my_filter)  # before
f.bit_and(column("a"), filter=my_filter)  # after
```

Passing `filter` to `mean` previously raised a `TypeError`, whether passed
positionally or by keyword; it now works when passed by keyword.

### Chaining keeps options already set

Chaining a builder method (`order_by`, `filter`, `distinct`, `null_treatment`,
`partition_by`, `window_frame`) or `over()` onto a function used to start from
an empty builder, so options set by the function's keyword arguments were
silently reset. On an aggregate they are now kept, which can change results:

```python
e = f.string_agg(col("s"), ",", order_by="s")
e.distinct().build()  # before: order_by dropped; after: kept
```

On a window function that already has a `partition_by`, `order_by`, or
`window_frame`, chaining now raises instead of dropping them. Set them in one
place, as described in {ref}`window_function_chaining`:

```python
e = f.lead(col("v"), 1, partition_by=[col("g")], order_by="t")
e.over(Window(order_by="t"))  # before: partition dropped; after: raises
f.lead(col("v"), 1).over(Window(partition_by=[col("g")], order_by="t"))  # after
```

Options that used to be dropped now take effect, so a chain that ran before
may now raise. For example, DISTINCT requires the ORDER BY expressions to be
among the arguments:

```python
f.array_agg(col("s"), distinct=True).order_by(col("v")).build()
# before: ran without DISTINCT
# after:  Execution error: In an aggregate with DISTINCT, ORDER BY expressions
#         must appear in argument list
```

Drop `distinct`, or order by the aggregated column, to get either of the
results the chain can actually produce.

An option that does not apply to the function now raises as soon as it is
set, anywhere in the chain. `filter` and `distinct` need an aggregate,
including one used as a window function, and `partition_by` and
`window_frame` need a window function. Later in a chain these were silently
dropped:

```python
f.sum(col("v")).filter(col("v") > lit(1)).partition_by(col("g"))
# before: partition_by dropped; after: raises
```

The default `RESPECT NULLS` set by `first_value`, `last_value`, and `nth_value`
is also kept, so their generated column names change:

```python
f.first_value(col("a")).order_by(col("b")).build()
# before: first_value(a) ORDER BY [b ASC NULLS FIRST]
# after:  first_value(a) RESPECT NULLS ORDER BY [b ASC NULLS FIRST]
```

This now matches the name from `f.first_value(col("a"), order_by=col("b"))`.
Code that selects the result by its generated name should `alias()` it instead.

### `over()` keeps options set on an aggregate

`Expr.over()` on an aggregate used to drop the `filter`, `distinct`,
`null_treatment`, and `order_by` options it was built with. The first three are
now kept, which can change results:

```python
f.avg(col("v"), distinct=True).over(Window())
# v = [1, 1, 4]; before: 2.0, after: 2.5
```

An `order_by` on the aggregate now raises instead of being dropped, as it does
with `OVER` in SQL. Remove it, or move it into the `Window` if it was meant to
order the rows:

```python
# before: order_by dropped; after: raises
f.first_value(col("v"), order_by=col("i").sort(ascending=False)).over(
    Window(partition_by=[col("g")])
)

# after: the Window orders the rows the aggregate sees
f.first_value(col("v")).over(
    Window(partition_by=[col("g")], order_by=[col("i").sort(ascending=False)])
)
```

A `WITHIN GROUP` function such as `percentile_cont` still accepts
an ascending `sort_expression`, and raises on a descending one, which used to
give the ascending result. See {ref}`aggregate_over_options`.

### Python aggregate UDFs reject `DISTINCT`

A Python {py:class}`~datafusion.user_defined.Accumulator` cannot deduplicate its
input, so a Python aggregate UDF with `DISTINCT` counted every row. It now
raises instead of returning that result:

```python
my_sum(col("v")).over(Window()).distinct().build()
# v = [1, 1, 1, 5]; before: 8.0; after: DISTINCT is not supported ...
```

When the optimizer rewrites the query to group by the distinct values first,
as it does for SQL's `SELECT my_sum(DISTINCT v) FROM t`, the query still runs
and gives the distinct result.

### Percentile functions keep the sort direction

{py:func}`~datafusion.functions.percentile_cont`,
{py:func}`~datafusion.functions.quantile_cont`,
{py:func}`~datafusion.functions.approx_percentile_cont`, and
{py:func}`~datafusion.functions.approx_percentile_cont_with_weight` ignored the
direction of `sort_expression`, so a descending sort gave the ascending result.
Used as aggregates, they now match `WITHIN GROUP (ORDER BY ... DESC)` in SQL
(see {ref}`aggregate_over_options` for their use in a window):

```python
f.percentile_cont(col("a").sort(ascending=False), 0.25)
# a = [1, 2, 3, 4, 5]; before: 2.0, after: 4.0
```

Their generated column names now include the ordering, in the same form as SQL.
Code that selects the result by its generated name should `alias()` it instead.

```python
f.percentile_cont(col("a"), 0.25)
# before: percentile_cont(t.a,Float64(0.25))
# after:  percentile_cont(Float64(0.25)) WITHIN GROUP [t.a ASC NULLS FIRST]
```

### `fill_null(subset=[])` fills no columns

{py:meth}`~datafusion.dataframe.DataFrame.fill_null` with an empty `subset`
list used to fill every column, the same as `subset=None`. It now returns the
DataFrame unchanged, so a subset computed from the schema that matches nothing
no longer rewrites every column. The new
{py:meth}`~datafusion.dataframe.DataFrame.fill_nan` behaves the same way.

```python
df.fill_null(0, subset=[])  # before: fills all columns; after: fills none
df.fill_null(0)  # fills all columns, before and after
```

### `spark.last_day` renamed its parameter

The parameter of {py:func}`datafusion.functions.spark.last_day` is now named
`date`, matching `pyspark.sql.functions.last_day`. Positional calls are
unaffected; update any call passing it by keyword.

```python
spark.last_day(col=d)  # before
spark.last_day(date=d)  # after
```

### Changes to the `datafusion-python-util` crate

Extension libraries written in Rust usually depend on the
`datafusion-python-util` crate for the helpers that read these capsules. Two of
those helpers changed, because the getter they call now takes the session.

`ffi_logical_codec_from_pycapsule` takes a second argument. Pass `Some(session)`
when importing an object from another library, so its getter receives the
session it is being installed on. Pass `None` when the object *is* a session and
you are asking it for what it holds:

```rust
// Before
let codec = ffi_logical_codec_from_pycapsule(obj)?;

// After
let codec = ffi_logical_codec_from_pycapsule(obj, Some(session))?;
```

`physical_codec_from_pycapsule` has been **removed**. It called
`__datafusion_physical_extension_codec__` with no arguments, which no longer
matches the protocol, so against an updated codec it raised a bare `TypeError`
and against an outdated one it silently produced a codec bound to the wrong
session. Use `ffi_physical_codec_from_pycapsule`, which passes the session:

```rust
// Before
let codec: Arc<dyn PhysicalExtensionCodec> = physical_codec_from_pycapsule(&obj)?;

// After
let ffi = ffi_physical_codec_from_pycapsule(obj, Some(session))?;
let codec: Arc<dyn PhysicalExtensionCodec> = (&ffi).into();
```

`physical_optimizer_rule_from_pycapsule` and `task_context_from_pycapsule` are
unchanged. Their hooks take no session.

Calling a getter that still has the old signature now raises an `ImportError`
naming the method, with the original `TypeError` retained as its `__cause__`,
rather than a bare `TypeError`.

## DataFusion 54.0.0

The `Config` class has been removed. It was a standalone wrapper around
`ConfigOptions` that could not be connected to a `SessionContext`, making it
effectively unusable. Use {py:class}`~datafusion.context.SessionConfig` instead,
which is passed directly to `SessionContext`.

Before:

```python
from datafusion import Config

config = Config()
config.set("datafusion.execution.batch_size", "4096")
# config could not be passed to SessionContext
```

After:

```python
from datafusion import SessionConfig, SessionContext

config = SessionConfig().set("datafusion.execution.batch_size", "4096")
ctx = SessionContext(config)
```

The aggregate functions {py:func}`~datafusion.functions.sum` and
{py:func}`~datafusion.functions.avg` now accept a `distinct` argument, matching
the other aggregate functions. `distinct` is inserted *before* `filter` in the
argument list, so any code that passed `filter` positionally must be updated to
pass it as a keyword argument. The types are distinct so a type checker should flag this.

Before:

```python
f.sum(column("a"), my_filter)
f.avg(column("a"), my_filter)
```

Now:

```python
f.sum(column("a"), filter=my_filter)
f.avg(column("a"), filter=my_filter)
```

## DataFusion 53.0.0

This version includes an upgraded version of `pyo3`, which changed the way to extract an FFI
object. Example:

Before:

```rust
let codec = unsafe { capsule.reference::<FFI_LogicalExtensionCodec>() };
```

Now:

```rust
let data: NonNull<FFI_LogicalExtensionCodec> = capsule
    .pointer_checked(Some(c_str!("datafusion_logical_extension_codec")))?
    .cast();
let codec = unsafe { data.as_ref() };
```

## DataFusion 52.0.0

This version includes a major update to the {ref}`ffi` due to upgrades
to the [Foreign Function Interface](https://doc.rust-lang.org/nomicon/ffi.html).
Users who contribute their own `CatalogProvider`, `SchemaProvider`,
`TableProvider` or `TableFunction` via FFI must now provide access to a
`LogicalExtensionCodec` and a `TaskContextProvider`. The function signatures
for the methods to get these `PyCapsule` objects now requires an additional
parameter, which is a Python object that can be used to extract the
`FFI_LogicalExtensionCodec` that is necessary.

A complete example can be found in the [FFI example](https://github.com/apache/datafusion-python/tree/main/examples/datafusion-ffi-example).
Your FFI hook methods — `__datafusion_catalog_provider__`,
`__datafusion_schema_provider__`, `__datafusion_table_provider__`, and
`__datafusion_table_function__` — need to be updated to accept an additional
`session: Bound<PyAny>` parameter, as shown in this example.

```rust
#[pymethods]
impl MyCatalogProvider {
    pub fn __datafusion_catalog_provider__<'py>(
        &self,
        py: Python<'py>,
        session: Bound<PyAny>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let name = cr"datafusion_catalog_provider".into();

        let provider = Arc::clone(&self.inner) as Arc<dyn CatalogProvider + Send>;

        let codec = ffi_logical_codec_from_pycapsule(session)?;
        let provider = FFI_CatalogProvider::new_with_ffi_codec(provider, None, codec);

        PyCapsule::new(py, provider, Some(name))
    }
}
```

To extract the logical extension codec FFI object from the provided object you
can implement a helper method such as:

```rust
pub(crate) fn ffi_logical_codec_from_pycapsule(
    obj: Bound<PyAny>,
) -> PyResult<FFI_LogicalExtensionCodec> {
    let attr_name = "__datafusion_logical_extension_codec__";
    let capsule = if obj.hasattr(attr_name)? {
        obj.getattr(attr_name)?.call0()?
    } else {
        obj
    };

    let capsule = capsule.downcast::<PyCapsule>()?;
    validate_pycapsule(capsule, "datafusion_logical_extension_codec")?;

    let codec = unsafe { capsule.reference::<FFI_LogicalExtensionCodec>() };

    Ok(codec.clone())
}
```

The DataFusion FFI interface updates no longer depend directly on the
`datafusion` core crate. You can improve your build times and potentially
reduce your library binary size by removing this dependency and instead
using the specific datafusion project crates.

For example, instead of including expressions like:

```rust
use datafusion::catalog::MemTable;
```

Instead you can now write:

```rust
use datafusion_catalog::MemTable;
```
