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

## DataFusion 55.0.0

This release extends the change made in 52.0.0 to the remaining {ref}`ffi` hook
methods. Users who contribute their own `LogicalExtensionCodec` or
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

New in this release, `__datafusion_query_planner__` follows the same protocol.
It receives the session and takes both extension codecs from it, so a planner
library never builds a `TaskContextProvider` at all. Install one with
`SessionContext.set_query_planner(planner)`, which mutates the session the same
way `add_physical_optimizer_rule` does and returns nothing — the query planner
lives in `SessionState`, so it belongs to the session rather than to a
particular handle on it. See the {ref}`ffi` guide for the full protocol.

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
`LogicalExtensionCodec` or `PhysicalExtensionCodec` exactly as before. Payloads
are wrapped in an envelope naming their author by `datafusion-python`, which
strips it again before your codec sees the bytes.

Callers relying on replacement semantics — installing a codec in order to remove
a previous one — are affected. There is no way to remove an installed codec.

Two behaviours are worth knowing:

- Installing two codecs under the same identity raises a `ValueError`. Identity
  is derived from the exporting class's module and qualified name, so this comes
  up when installing two instances of one class. Pass `codec_id=` to distinguish
  them.
- A codec installed from a bare `PyCapsule` has no portable identity, because
  every capsule reports the same type. It is tagged with a session-local
  identity and works normally within that session, but a plan it encodes cannot
  be decoded on an unrelated session. Pass `codec_id=` if plans must cross
  sessions.

```python
ctx = ctx.with_logical_extension_codec(lib_a.codec())
ctx = ctx.with_logical_extension_codec(lib_b.codec())  # no longer discards lib_a

# Two instances of one class need distinct identities.
ctx = ctx.with_logical_extension_codec(lib_a.Codec(), codec_id="lib_a.reader")
ctx = ctx.with_logical_extension_codec(lib_a.Codec(), codec_id="lib_a.writer")

ctx.logical_extension_codec_ids()
```

Serialized plans change shape once an extension codec is installed: payloads
written by a chained codec now carry an identity envelope. A session with no
extension codecs installed is unaffected and produces the same bytes as before,
as do functions encoded by name. Plans serialized by an earlier release and
stored for later use should be regenerated if they were produced by a session
with an extension codec installed.

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
