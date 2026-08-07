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
`with_physical_extension_codec` adds the codec to the front of the session's codec
chain rather than replacing prior codecs. During encoding and decoding, the most
recently installed codec is consulted first, falling through codec by codec to
DataFusion's default codec. A codec signals "not mine" by returning an error, which
sends the chain on to the next codec. Two conventions keep this dispatch sound:

- Frame your payloads with a distinct byte prefix (pick a `DF` namespace plus a
  crate-specific suffix) and only decode payloads carrying your prefix.
- Return an error for objects and payloads you do not own. A codec that answers
  success for objects outside its family shadows every codec installed before it.

Because dispatch keys off payload prefixes rather than install position, codec
registration order between independent libraries does not matter.

The current FFI logical codec supports providers and UDFs but not arbitrary custom
`LogicalPlan::Extension` nodes. See both example READMEs for the supported flow and
local build commands.

### One planner per session, with explicit fallback

Unlike codecs, a `SessionState` holds exactly one query planner — installing another
replaces it. Planner layering is therefore explicit: a planner that wants to handle
only some queries should accept a fallback planner and delegate the rest to it. The
current planner can be exported for that purpose with
`ctx.__datafusion_query_planner__()`.

One ordering rule applies: a planner capsule captures the session's codecs at export
time and cannot be rebound afterward. Codec changes made after installing a single
planner are rebound automatically, but a planner wrapped inside another planner as a
fallback is opaque and keeps the codecs it was exported with. **Install all extension
codecs before exporting or chaining planners.**

### Extension bundles: `with_extensions`

Codec and planner capsules carry an `FFI_TaskContextProvider` holding a *weak*
reference to the `SessionContext` they were created against. A capsule does not keep
that context alive, and a component bound to one context cannot be rebound to another.
Chaining the low-level `with_*` methods by hand therefore risks binding components to
an intermediate context that is later garbage collected, which fails at query time
with `TaskContextProvider went out of scope over FFI boundary` — or worse, silently
reads stale session state.

`SessionContext.with_extensions` avoids this by construction. An extension library
exposes a bundle object implementing the `__datafusion_session_extension__` protocol:

```python
class MyEngineExtension:
    def __datafusion_session_extension__(self, ctx: SessionContext) -> SessionExtensionComponents:
        # Create fresh components bound to `ctx` on every call. `ctx` is the
        # exact context the host will return from with_extensions.
        return SessionExtensionComponents(
            logical_extension_codecs=(self._make_logical_codec(ctx),),
            physical_extension_codecs=(self._make_physical_codec(ctx),),
            query_planner=self._make_planner(ctx),
        )
```

The host creates one destination context, passes it to every factory, installs all
codecs, binds the planner against the final codec chains, and returns the context in
a single step:

```python
ctx = SessionContext(config).with_extensions(lib_a.Extension(), lib_b.Extension())
ctx.register_table("t", lib_a.TableProvider())
ctx.register_udf(udf(lib_b.SomeUDF()))
```

Extensions are processed left to right and prepend to the codec chain, so codecs from
later extensions are consulted first. At most one extension may supply a query
planner. If any factory fails, the source context's state is unchanged.

Bundle objects must be configuration-only: create fresh components on each call, never
cache bound components, and do not retain the context passed in. Catalogs are shared
with the source context, so registrations made during binding are not rolled back on
failure.

The returned context is the strong owner of every installed component's task-context
provider, and dependent objects do not extend its lifetime. A `DataFrame`, logical
plan, or capsule can outlive the context, but any operation that reaches an FFI codec
after the context is collected fails with `TaskContextProvider went out of scope over
FFI boundary`. Keep the context alive for as long as objects derived from it are in
use.

`MyPlannerExtension` in [`datafusion-ffi-query-planner-example`] is a complete Rust
implementation of this protocol, including extracting the task-context provider from
the supplied context and constructing a Python `SessionExtensionComponents`.

### Advanced: chaining the low-level methods

The `with_logical_extension_codec`, `with_physical_extension_codec`, and
`with_query_planner` methods remain available for advanced use. Putting them together
for a session using two extension libraries that each provide tables, functions, and
a query planner:

```python
ctx = SessionContext(config)

# 1. Codecs from both libraries. Order between libraries does not matter.
ctx = ctx.with_logical_extension_codec(lib_a.codec())
ctx = ctx.with_logical_extension_codec(lib_b.codec())
ctx = ctx.with_physical_extension_codec(lib_a.physical_codec())
ctx = ctx.with_physical_extension_codec(lib_b.physical_codec())

# 2. Planners, innermost fallback first. Library A's planner falls back to
#    DataFusion's default planner; library B's planner falls back to A's.
ctx = ctx.with_query_planner(lib_a.Planner())
ctx = ctx.with_query_planner(
    lib_b.Planner(fallback=ctx.__datafusion_query_planner__())
)

# 3. Tables and functions — any time before the first query.
ctx.register_table("t", lib_a.TableProvider())
ctx.register_udf(udf(lib_b.SomeUDF()))
```

When chaining by hand, keep the final context assigned to `ctx` as the single owner:
components created against earlier intermediate contexts (for example a codec
constructed with a context that is later discarded) hold weak references that break
once that intermediate context is collected. Prefer `with_extensions` whenever the
extension library provides a bundle.

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
