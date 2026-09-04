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

# Python Extensions

The DataFusion in Python project is designed to allow users to extend its functionality in a few core
areas. Ideally many users would like to package their extensions as a Python package and easily
integrate that package with this project. This page serves to describe some of the challenges we face
when doing these integrations and the approach our project uses.

## The Primary Issue

Suppose you wish to use DataFusion and you have a custom data source that can produce tables that
can then be queried against, similar to how you can register a {ref}`CSV <io_csv>` or
{ref}`Parquet <io_parquet>` file. In DataFusion terminology, you likely want to implement a
{ref}`Custom Table Provider <io_custom_table_provider>`. In an effort to make your data source
as performant as possible and to utilize the features of DataFusion, you may decide to write
your source in Rust and then expose it through [PyO3](https://pyo3.rs) as a Python library.

At first glance, it may appear the best way to do this is to add the `datafusion-python`
crate as a dependency, provide a `PyTable`, and then to register it with the
`SessionContext`. Unfortunately, this will not work.

When you produce your code as a Python library and it needs to interact with the DataFusion
library, at the lowest level they communicate through an Application Binary Interface (ABI).
The acronym sounds similar to API (Application Programming Interface), but it is distinctly
different.

The ABI sets the standard for how these libraries can share data and functions between each
other. One of the key differences between Rust and other programming languages is that Rust
does not have a stable ABI. What this means in practice is that if you compile a Rust library
with one version of the `rustc` compiler and I compile another library to interface with it
but I use a different version of the compiler, there is no guarantee the interface will be
the same.

In practice, this means that a Python library built with `datafusion-python` as a Rust
dependency will generally **not** be compatible with the DataFusion Python package, even
if they reference the same version of `datafusion-python`. If you attempt to do this, it may
work on your local computer if you have built both packages with the same optimizations.
This can sometimes lead to a false expectation that the code will work, but it frequently
breaks the moment you try to use your package against the released packages.

You can find more information about the Rust ABI in their
[online documentation](https://doc.rust-lang.org/reference/abi.html).

## The FFI Approach

Rust supports interacting with other programming languages through it's Foreign Function
Interface (FFI). The advantage of using the FFI is that it enables you to write data structures
and functions that have a stable ABI. The allows you to use Rust code with C, Python, and
other languages. In fact, the [PyO3](https://pyo3.rs) library uses the FFI to share data
and functions between Python and Rust.

The approach we are taking in the DataFusion in Python project is to incrementally expose
more portions of the DataFusion project via FFI interfaces. This allows users to write Rust
code that does **not** require the `datafusion-python` crate as a dependency, expose their
code in Python via PyO3, and have it interact with the DataFusion Python package.

Early adopters of this approach include [delta-rs](https://delta-io.github.io/delta-rs/)
who has adapted their Table Provider for use in `` `datafusion-python` `` with only a few lines
of code. Also, the DataFusion Python project uses the existing definitions from
[Apache Arrow CStream Interface](https://arrow.apache.org/docs/format/CStreamInterface.html)
to support importing **and** exporting tables. Any Python package that supports reading
the Arrow C Stream interface can work with DataFusion Python out of the box! You can read
more about working with Arrow sources in the {ref}`Data Sources <user_guide_data_sources>`
page.

To learn more about the Foreign Function Interface in Rust, the
[Rustonomicon](https://doc.rust-lang.org/nomicon/ffi.html) is a good resource.

## Inspiration from Arrow

DataFusion is built upon [Apache Arrow](https://arrow.apache.org/). The canonical Python
Arrow implementation, [pyarrow](https://arrow.apache.org/docs/python/index.html) provides
an excellent way to share Arrow data between Python projects without performing any copy
operations on the data. They do this by using a well defined set of interfaces. You can
find the details about their stream interface
[here](https://arrow.apache.org/docs/format/CStreamInterface.html). The
[Rust Arrow Implementation](https://github.com/apache/arrow-rs) also supports these
`C` style definitions via the Foreign Function Interface.

In addition to using these interfaces to transfer Arrow data between libraries, `pyarrow`
goes one step further to make sharing the interfaces easier in Python. They do this
by exposing PyCapsules that contain the expected functionality.

You can learn more about PyCapsules from the official
[Python online documentation](https://docs.python.org/3/c-api/capsule.html). PyCapsules
have excellent support in PyO3 already. The
[PyO3 online documentation](https://pyo3.rs/main/doc/pyo3/types/struct.pycapsule) is a good source
for more details on using PyCapsules in Rust.

Two lessons we leverage from the Arrow project in DataFusion Python are:

- We reuse the existing Arrow FFI functionality wherever possible.
- We expose PyCapsules that contain a FFI stable struct.

## Implementation Details

The bulk of the code necessary to perform our FFI operations is in the upstream
[DataFusion](https://datafusion.apache.org/) core repository. You can review the code and
documentation in the [datafusion-ffi] crate.

Our FFI implementation is narrowly focused at sharing data and functions with Rust backed
libraries. This allows us to use the [abi_stable crate](https://crates.io/crates/abi_stable).
This is an excellent crate that allows for easy conversion between Rust native types
and FFI-safe alternatives. For example, if you needed to pass a `Vec<String>` via FFI,
you can simply convert it to a `RVec<RString>` in an intuitive manner. It also supports
features like `RResult` and `ROption` that do not have an obvious translation to a
C equivalent.

The [datafusion-ffi] crate has been designed to make it easy to convert from DataFusion
traits into their FFI counterparts. For example, if you have defined a custom
[TableProvider](https://docs.rs/datafusion/45.0.0/datafusion/catalog/trait.TableProvider.html)
and you want to create a sharable FFI counterpart, you could write:

```rust
let my_provider = MyTableProvider::default();
let ffi_provider = FFI_TableProvider::new(Arc::new(my_provider), false, None);
```

(ffi_pyclass_mutability)=

## PyO3 class mutability guidelines

PyO3 bindings should present immutable wrappers whenever a struct stores shared or
interior-mutable state. In practice this means that any `#[pyclass]` containing an
`Arc<RwLock<_>>` or similar synchronized primitive must opt into `#[pyclass(frozen)]`
unless there is a compelling reason not to.

The execution context illustrates the preferred pattern. `PySessionContext` in
{file}`src/context.rs` stays frozen even though it shares mutable state internally via
`SessionContext`. This ensures PyO3 tracks borrows correctly while Python-facing APIs
clone the inner `SessionContext` or return new wrappers instead of mutating the
existing instance in place:

```rust
#[pyclass(from_py_object, frozen, name = "SessionContext", module = "datafusion", subclass)]
#[derive(Clone)]
pub struct PySessionContext {
    pub ctx: SessionContext,
}
```

Occasionally a type must remain mutable—for example when PyO3 attribute setters need to
update fields directly. In these rare cases add an inline justification so reviewers and
future contributors understand why `frozen` is unsafe to enable. `DataTypeMap` in
{file}`src/common/data_type.rs` includes such a comment because PyO3 still needs to track
field updates:

```rust
// TODO: This looks like this needs pyo3 tracking so leaving unfrozen for now
#[derive(Debug, Clone)]
#[pyclass(from_py_object, name = "DataTypeMap", module = "datafusion.common", subclass)]
pub struct DataTypeMap {
    #[pyo3(get, set)]
    pub arrow_type: PyDataType,
    #[pyo3(get, set)]
    pub python_type: PythonType,
    #[pyo3(get, set)]
    pub sql_type: SqlType,
}
```

When reviewers encounter a mutable `#[pyclass]` without a comment, they should request
an explanation or ask that `frozen` be added. Keeping these wrappers frozen by default
helps avoid subtle bugs stemming from PyO3's interior mutability tracking.

If you were interfacing with a library that provided the above `FFI_TableProvider` and
you needed to turn it back into an `TableProvider`, you can turn it into a
`ForeignTableProvider` with implements the `TableProvider` trait.

```rust
let foreign_provider: ForeignTableProvider = ffi_provider.into();
```

If you review the code in [datafusion-ffi] you will find that each of the traits we share
across the boundary has two portions, one with a `FFI_` prefix and one with a `Foreign`
prefix. This is used to distinguish which side of the FFI boundary that struct is
designed to be used on. The structures with the `FFI_` prefix are to be used on the
**provider** of the structure. In the example we're showing, this means the code that has
written the underlying `TableProvider` implementation to access your custom data source.
The structures with the `Foreign` prefix are to be used by the receiver. In this case,
it is the `datafusion-python` library.

In order to share these FFI structures, we need to wrap them in some kind of Python object
that can be used to interface from one package to another. As described in the above
section on our inspiration from Arrow, we use `PyCapsule`. We can create a `PyCapsule`
for our provider thusly:

```rust
let name = CString::new("datafusion_table_provider")?;
let my_capsule = PyCapsule::new_bound(py, provider, Some(name))?;
```

On the receiving side, turn this pycapsule object into the `FFI_TableProvider`, which
can then be turned into a `ForeignTableProvider` the associated code is:

```rust
let capsule = capsule.cast::<PyCapsule>()?;
let data: NonNull<FFI_TableProvider> = capsule
    .pointer_checked(Some(name))?
    .cast();
let codec = unsafe { data.as_ref() };
```

By convention the `datafusion-python` library expects a Python object that has a
`TableProvider` PyCapsule to have this capsule accessible by calling a function named
`__datafusion_table_provider__`. You can see a complete working example of how to
share a `TableProvider` from one python library to DataFusion Python in the
[repository examples folder](https://github.com/apache/datafusion-python/tree/main/examples/datafusion-ffi-example).

This section has been written using `TableProvider` as an example. It is the first
extension that has been written using this approach and the most thoroughly implemented.
As we continue to expose more of the DataFusion features, we intend to follow this same
design pattern.

## Query Planners Across Multiple Libraries

A query can involve three independent native libraries: `datafusion-python`, a library
that owns table providers or functions, and a library that owns the query planner. The
examples use two separate extension crates so each role has a distinct shared-library
identity:

- [`datafusion-ffi-example`] owns providers, functions, and their codecs.
- [`datafusion-ffi-query-planner-example`] owns the planner and its configuration.

The `SessionContext` owns the codecs used for the exchange and supplies them to the
foreign planner. This lets the planner decode provider-owned objects and lets
`datafusion-python` decode the physical plan returned by the planner. The examples use
process-local tokens to demonstrate ownership; production codecs should serialize
durable metadata instead.

### Composable codecs

Extension codecs compose. Each call to `with_logical_extension_codec` or
`with_physical_extension_codec` appends the codec to the session's codec chain
rather than replacing prior codecs.

**Nothing is asked of the codec itself.** Implement `LogicalExtensionCodec` or
`PhysicalExtensionCodec` exactly as you would for a session that installs only
yours. When your codec writes bytes into a serialized plan, datafusion-python
records which codec wrote them, and strips that record off again before handing the
bytes back. So your codec receives, byte for byte, the payload it wrote, and is
never offered a payload another codec wrote.

A codec that also ships to hosts which dispatch differently may still want its own
guard against foreign payloads. Keeping one is fine; it is simply not needed for the
datafusion-python path.

That record is the codec's **id**: a short string stored inside the plan, naming the
codec that wrote each payload. Because plans are decoded in another process — or
another program — the id has to name the same codec there as it did where the plan
was written.

Ids are assigned for you. A codec's id is normally its exporting class's import
path, such as `my_library.Codec`, which is what you will see in
`logical_extension_codec_ids()` and in decode errors. You choose one yourself in
three cases:

- **Two instances of one class.** Both get the same id, so the second install
  raises `ValueError`. Pass `codec_id=` to tell them apart.
- **A bare `PyCapsule`.** A capsule has no class to take a name from. Installed
  through `with_extensions`, it is named after the extension that contributed it —
  an extension is a plain object, so its import path is library-owned and just as
  stable across processes as a codec class's. Installed directly through
  `with_logical_extension_codec` or `with_physical_extension_codec` there is nothing
  to fall back on, so it gets an id private to the session that installed it; plans
  it encodes fail with a clear error on any other session rather than being decoded
  by the wrong codec. Pass `codec_id=` if those plans have to cross sessions.

  One extension contributing two bare capsules of the same kind is refused, because
  both resolve to that one extension's id. Numbering them by position would be an id
  another library can mint the same value from, and would break stored plans the
  first time the extension reordered what it returns — so name one of them by
  wrapping it in an object declaring `__datafusion_codec_id__`.
- **A class you intend to rename.** The id follows the class name, so renaming stops
  older plans from decoding. Declare `__datafusion_codec_id__` on the exporting
  object to pin an id that survives the rename.

`SessionContext.logical_extension_codec_ids()` and its physical counterpart list the
ids installed on a session, which is also what a decode failure names.

Installing one context's codec stack on another session composes the two sessions
rather than copying codecs out of one: the imported codecs resolve their task context
against the original and stop working when it is dropped — see
[One session, one `Arc<SessionContext>`](#one-session-one-arcsessioncontext). Pass
the context itself rather than the capsule it exports, so its codecs get an id that
other sessions can decode.

Because decoding keys off the id rather than install position, registration order
between independent libraries does not affect decoding at all. It is visible only
on encoding, where codecs are consulted in install order and the first to claim an
object wins — so installing a library can claim objects nothing else claimed, but
never takes over an object an earlier codec was already encoding. Two libraries
that each own tables, functions, and a planner register like this:

```python
ctx = SessionContext(config)

# Codecs from both libraries. Order between libraries does not matter.
ctx = ctx.with_logical_extension_codec(lib_a.codec())
ctx = ctx.with_logical_extension_codec(lib_b.codec())
ctx = ctx.with_physical_extension_codec(lib_a.physical_codec())
ctx = ctx.with_physical_extension_codec(lib_b.physical_codec())

# A session holds one planner, so layering is explicit delegation. Install the
# codecs first: the fallback captured here keeps the codecs it was exported
# with. See "Rebinding a planner's codecs is one level deep" below.
ctx.set_query_planner(lib_a.Planner())
ctx.set_query_planner(lib_b.Planner(fallback=ctx.__datafusion_query_planner__()))

# Tables and functions — any time before the first query.
ctx.register_table("t", lib_a.TableProvider())
ctx.register_udf(udf(lib_b.SomeUDF()))
```

A codec may own functions that need no payload at all, where the name is the whole
encoding: `try_encode_udf` writes nothing and `try_decode_udf` rebuilds the function
from `name`. That is supported and needs no id, because an `Ok` with an empty
buffer is read as "no opinion" and passes the object to the next codec.
`NameOnlyUdfCodec` in the FFI example is the worked case. Anything no installed
codec claims falls through to `Default{Logical,Physical}ExtensionCodec`.

This is the one case where your decoder is consulted about something you may not
own, because an empty payload has no id to route on. `try_decode_udf` and
its aggregate and window siblings can therefore be called with an empty `buf` and a
`name` belonging to another library. Decide from `name` and return an error if it is
not yours; do not assume `buf` is non-empty.

The framing itself — how an id is stored alongside a payload and routed back, and the two cases
that stay unframed — is internal to datafusion-python and documented in
`crates/core/src/codec.rs` for anyone changing it.

The current FFI logical codec supports providers and UDFs but not arbitrary custom
`LogicalPlan::Extension` nodes. See both example READMEs for the supported flow and
local build commands.

### Extension bundles: `with_extensions`

The chaining above works, but it makes the caller responsible for ordering: the codecs
have to be installed before the planner, because a planner is built against whatever
codec chains exist when it is installed, and a codec added afterwards rebinds it. Get
that wrong and the planner encodes through a chain that is missing a library.

`SessionContext.with_extensions` removes the ordering question. An extension library
exposes a bundle object implementing `__datafusion_session_extension__`:

```python
class MyEngineExtension:
    def __datafusion_session_extension__(self, ctx: SessionContext) -> SessionExtensionComponents:
        # Create fresh components bound to `ctx` on every call. `ctx` is the
        # session the components will run on.
        return SessionExtensionComponents(
            logical_extension_codecs=(self._make_logical_codec(ctx),),
            physical_extension_codecs=(self._make_physical_codec(ctx),),
            query_planner=self._make_planner(ctx),
        )
```

The host passes the context to every factory, installs all the codecs, binds the
planner against the final codec chains, and returns a handle on that session in a
single step:

```python
ctx = SessionContext(config).with_extensions(lib_a.Extension(), lib_b.Extension())
ctx.register_table("t", lib_a.TableProvider())
ctx.register_udf(udf(lib_b.SomeUDF()))
```

Extensions are processed left to right and their codecs are appended to the chain in
that order. As above, order affects only encoding — decoding routes by id. At most one
extension per call may supply a query planner.

Nothing is written to the session until every factory has returned and every capsule
has been validated, so a factory that raises leaves the session exactly as it was. A
factory that mutates the context it is handed — registering a table, say — is not
rolled back, which is why bundle objects must be configuration-only: create fresh
components on each call, never cache bound components, and do not retain the context
passed in.

Like every other derivation, the returned context is a handle on the *same* session as
the receiver — see [What a derived context shares](#what-a-derived-context-shares).
Only the Python-side codec chains belong to the returned handle; the planner is
installed on the shared session and takes effect even if that handle is discarded.

The session owns every installed component's task-context provider, and dependent
objects do not extend its lifetime. A `DataFrame`, logical plan, or capsule can outlive
every context on the session, but any operation that reaches an FFI codec after the
last one is collected fails with `TaskContextProvider went out of scope over FFI
boundary`. Keep a context alive for as long as objects derived from it are in use.

`MyPlannerExtension` in [`datafusion-ffi-query-planner-example`] is a complete Rust
implementation of the protocol, including taking the task-context provider off the
supplied context and constructing a Python `SessionExtensionComponents`.

### Capsule getters receive the session they are installed on

`__datafusion_query_planner__`, `__datafusion_logical_extension_codec__`, and
`__datafusion_physical_extension_codec__` all take the `SessionContext` the object is
being installed on, the same way `__datafusion_table_provider__` does:

```rust
fn __datafusion_physical_extension_codec__<'py>(
    &self,
    py: Python<'py>,
    session: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyCapsule>> {
    let runtime = get_tokio_runtime().handle().clone();
    let ctx_provider = ffi_task_context_provider_from_pycapsule(&session)?;
    let ffi = FFI_PhysicalExtensionCodec::new(inner, Some(runtime), ctx_provider);
    PyCapsule::new_with_value(py, ffi, cr"datafusion_physical_extension_codec")
}
```

This exists because the FFI constructors need things an extension library does not
have. `FFI_{Logical,Physical}ExtensionCodec::new` needs a `TaskContextProvider` for the
decode callbacks the codec will receive, and `FFI_QueryPlanner::new` needs both codecs
on top of that. Taking them from the session is what keeps a library from constructing
a `SessionContext` purely to satisfy a parameter — an empty one resolves nothing, and
`FFI_TaskContextProvider` holds it weakly, so a context built inline in the getter is
already dropped by the time the capsule is used.

A planner uses `FFI_QueryPlanner::new_with_ffi_codecs` with the two codecs it takes off
the session, and never touches a provider directly. That also matches what installation
does anyway: `set_query_planner` builds the planner against the codecs of the session
that will run the query.

`SessionContext` accepts the argument on all three getters and ignores it, so a session
satisfies the same protocol an extension library implements. When you export the current
planner to wrap it, `ctx.__datafusion_query_planner__()` and
`ctx.__datafusion_query_planner__(ctx)` are both fine.

### A codec decodes against the session that is running the query

Because the provider comes from the host, a decode callback running inside an extension
library resolves names against the session running the query. A function registered with
`ctx.register_udf(...)` is visible to a foreign codec decoding a node that references it
by name, and the handle is live rather than a snapshot, so a registration made after the
codec is installed is visible too.

This is covered in
`examples/datafusion-ffi-query-planner-example/python/tests/_test_three_library_query_planner.py`,
where the example codecs take a `require_udf_on_decode` name and resolve it out of the
task context they are handed.

### One session, one `Arc<SessionContext>`

Every codec handed to a foreign object carries an `FFI_TaskContextProvider`, and that
type holds its provider **weakly**. A registered catalog provider upgrades the handle on
every `supports_filters_pushdown` and every `scan`. Those handles are bound to one
particular `Arc<SessionContext>` allocation, not to the logical session, so anything that
replaces the allocation orphans all of them and the next query fails with
`TaskContextProvider went out of scope over FFI boundary`.

So a `PySessionContext` keeps the `Arc<SessionContext>` it was created with for its whole
life. Installing a query planner writes the new `SessionState` back through
`state_ref()`, exactly as `add_physical_optimizer_rule` does, rather than deriving a
replacement context. The session id is carried across that rewrite — `SessionStateBuilder`
mints a fresh one otherwise — so `session_id()` and every `TaskContext` the session hands
out keep agreeing.

Repairing the damage instead of avoiding it does not work in general. A context can
rebuild the codecs it holds in its own fields, but a codec already embedded in a
registered `FFI_CatalogProvider` — and in every `FFI_SchemaProvider` and
`FFI_TableProvider` minted from it — is not reachable from Python at all. Nor can the
codec simply retain the session that built it: a codec handed to a provider is routinely
registered straight back into that same session, which would close the cycle
`SessionContext -> catalog -> FFI provider -> FFI codec -> SessionContext` and leak it.

`SessionContext.enable_url_table` is the one exception. It clones the underlying
`SessionContext`, so the returned context has an allocation of its own and must not
outlive the receiver. It also forks the session's state while keeping its id, so two
handles report one `session_id()` with divergent configuration. That is a bug rather
than a design, tracked in
[apache/datafusion-python#1708](https://github.com/apache/datafusion-python/issues/1708);
do not copy the pattern.

### What a derived context shares

`with_logical_extension_codec`, `with_physical_extension_codec`,
`with_python_udf_inlining`, and `with_extensions` return a new `SessionContext` wrapping
the *same* underlying session. Only the Python-side codec settings differ; catalogs,
tables, registered functions, and configuration are the one shared session, so a
registration on either side is visible to both.

There is one `Arc<SessionContext>` per session, which is what makes the weak
`FFI_TaskContextProvider` scheme work: a component bound through any handle stays valid
while *any* handle on that session is alive, so there is no way to bind a component to
an intermediate handle and have it dangle when that handle is dropped.

`set_query_planner` does not return anything. The query planner lives in `SessionState`,
so it is a property of the session rather than of a handle on it, and installing one is
visible to every context sharing that session — including ones a `with_*` call returned
earlier. Installing a codec on a session that already has a foreign planner rebuilds
that planner against the new chain for the same reason: there is one planner, and it has
to carry the codecs currently in force. This happens on the shared session, so it takes
effect even if the returned context is discarded — `ctx.with_python_udf_inlining(...)`
whose result is thrown away still leaves the session's planner carrying the codecs of
that discarded handle. A call that changes nothing is exempt: asking for the inlining
setting a context already has returns a handle without touching the session.

The rule that falls out of this is worth stating on its own, because it is the one thing
that surprises people:

> The session's query planner carries the codecs of the handle that most recently
> installed one. Every other path — `Expr.to_bytes(ctx)`, `ExecutionPlan.to_bytes(ctx)`,
> registering a provider — uses the codecs of the handle you call it on.

Those can be different handles, and then one session has two codec chains in effect at
once:

```python
ctx = ctx.with_logical_extension_codec(codec_a)
ctx.set_query_planner(planner)
ctx.with_logical_extension_codec(codec_b)  # discarded

Expr.to_bytes(expr, ctx)   # encodes with [codec_a, default] -- ctx's own field
ctx.sql(...).collect()     # plans with [codec_b, codec_a, default] -- the discarded
                           # handle's chain, installed on the shared session
```

Chaining `ctx = ctx.with_...(...)`, as the example below does, keeps the two in step.
`test_the_planner_and_the_handle_can_hold_different_codecs` pins the divergence.

```python
ctx = SessionContext(config)
ctx = ctx.with_logical_extension_codec(provider_logical_codec)
ctx = ctx.with_physical_extension_codec(provider_physical_codec)
ctx.set_query_planner(planner)
ctx.register_udf(my_udf)
```

Order is a readability preference rather than a requirement — installing a codec after a
planner rebuilds the planner against it.

A session holds exactly one query planner. Calling `set_query_planner` again replaces the
installed planner instead of layering another one. To chain planners, have the new
planner wrap the capsule returned by `SessionContext.__datafusion_query_planner__()`,
captured before the new planner is installed, and delegate to it explicitly.

### Rebinding a planner's codecs is one level deep

The rebuild above swaps the codecs on the installed `ForeignQueryPlanner` handle, and
only that handle. A planner that wraps a fallback resolved that fallback when *it* was
installed, and holds the result inside its own library's private data — behind a
`create_physical_plan` function pointer, with no Python-side handle. A codec installed
afterwards therefore reaches the outer planner and not the fallback, which keeps
whichever codecs were in force when it was imported.

Neither side can repair that:

- **The host cannot reach it.** `FFI_QueryPlanner::new_with_ffi_codecs` unwraps exactly
  one `ForeignQueryPlanner` layer. There is no deeper handle to unwrap — the same
  situation as a codec embedded in a registered `FFI_CatalogProvider`.
- **The planner library cannot re-derive it.** `FFI_QueryPlanner` holds its codecs by
  value, and `Session` exposes no accessor for the ones the host currently has, so
  `create_physical_plan` cannot pick them up from the session it is handed. The rebuild
  has to be eager, and an eager rebuild only sees the top layer.

A fix has to come from upstream, and is tracked in
[apache/datafusion#24762](https://github.com/apache/datafusion/issues/24762).

The stale codecs stay usable rather than dangling — they hold weak handles to the one
`Arc<SessionContext>` that Rule 6 keeps alive — so the effect is a fallback hop
serializing with an older codec, not a failure. It is also invisible to the examples
here, which use one fallback in the same cdylib as its wrapper; `datafusion-ffi`
short-circuits a same-library hop rather than serializing, so no codec runs. A fallback
in a *different* library would serialize, and would do it with the codecs it was
imported with.

So install the codecs before a layered planner. If a codec has to go in afterwards,
install the outer planner again *on the handle that holds the new codec* — that re-runs
its getter, which re-imports the fallback against that handle's codecs. Re-installing on
the original handle rebinds the session's planner back to the original handle's codecs
instead, which is the trap
`test_reinstalling_a_planner_rebinds_the_session_to_that_handles_codecs` pins.

`with_extensions` sidesteps the ordering question entirely: it installs every codec
before it binds the planner, so there is no "afterwards" for a bundle's own planner.

## Alternative Approach

Suppose you needed to expose some other features of DataFusion and you could not wait
for the upstream repository to implement the FFI approach we describe. In this case
you decide to create your dependency on the `datafusion-python` crate instead.

As we discussed, this is not guaranteed to work across different compiler versions and
optimization levels. If you wish to go down this route, there are two approaches we
have identified you can use.

1. Re-export all of `datafusion-python` yourself with your extensions built in.
2. Carefully synchronize your software releases with the `datafusion-python` CI build
   system so that your libraries use the exact same compiler, features, and
   optimization level.

We currently do not recommend either of these approaches as they are difficult to
maintain over a long period. Additionally, they require a tight version coupling
between libraries.

## Status of Work

At the time of this writing, the FFI features are under active development. To see
the latest status, we recommend reviewing the code in the [datafusion-ffi] crate.

[datafusion-ffi]: https://crates.io/crates/datafusion-ffi
[`datafusion-ffi-example`]: https://github.com/apache/datafusion-python/tree/main/examples/datafusion-ffi-example
[`datafusion-ffi-query-planner-example`]: https://github.com/apache/datafusion-python/tree/main/examples/datafusion-ffi-query-planner-example
