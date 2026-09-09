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

(extension_capsule_protocol)=

# The capsule protocol

Every integration point in this section works the same way. Your library
exposes a dunder method, datafusion-python calls it, and it hands back a
`PyCapsule` wrapping an FFI-safe struct. This page describes that mechanism
once; the pages after it describe what goes inside the capsule for each kind
of component.

The bulk of the code necessary to perform our FFI operations is in the
upstream [DataFusion](https://datafusion.apache.org/) core repository. You can
review the code and documentation in the [datafusion-ffi] crate.

## FFI-safe types

Our FFI implementation is narrowly focused on sharing data and functions with
Rust backed libraries. This allows us to use the
[stabby crate](https://crates.io/crates/stabby), which converts between Rust
native types and FFI-safe alternatives. For example, if you needed to pass a
`Vec<String>` via FFI, you can convert it to a
`stabby::vec::Vec<stabby::string::String>` — the crate's own examples alias
these as `SVec` and `SString`, which is the convention [datafusion-ffi] follows
too.

For `Option` and `Result`, [datafusion-ffi] defines its own `FFI_Option<T>` and
`FFI_Result<T>` rather than using stabby's. Stabby's versions require
`T: IStable` for niche optimization, and many of the `FFI_*` structs hold
self-referential function pointers that cannot implement it.

## `FFI_` on the provider, `Foreign` on the receiver

The [datafusion-ffi] crate has been designed to make it easy to convert from
DataFusion traits into their FFI counterparts. For example, if you have
defined a custom
[TableProvider](https://docs.rs/datafusion/45.0.0/datafusion/catalog/trait.TableProvider.html)
and you want to create a sharable FFI counterpart, you could write:

```rust
let my_provider = Arc::new(MyTableProvider::default());
let ffi_provider = FFI_TableProvider::new_with_ffi_codec(my_provider, false, None, codec);
```

where `codec` is the host's logical codec, read off the argument your getter
was handed — see {ref}`extension_getter_argument`.

If you were interfacing with a library that provided the above
`FFI_TableProvider` and you needed a usable `TableProvider` back, you convert
it into an `Arc<dyn TableProvider>`:

```rust
let provider: Arc<dyn TableProvider> = (&ffi_provider).into();
```

If you review the code in [datafusion-ffi] you will find that each of the
traits we share across the boundary has two portions, one with an `FFI_`
prefix and one with a `Foreign` prefix. This is used to distinguish which side
of the FFI boundary that struct is designed to be used on. The structures with
the `FFI_` prefix are to be used on the **provider** of the structure. In the
example we're showing, this means the code that has written the underlying
`TableProvider` implementation to access your custom data source. The
structures with the `Foreign` prefix are to be used by the receiver. In this
case, it is the `datafusion-python` library.

Convert to the trait object rather than naming `ForeignTableProvider` yourself.
The conversion compares the provider's library marker against the receiver's:
when both sides turn out to be the same shared library it hands back the
original `Arc` and skips the boundary entirely, and only otherwise wraps it in
a `ForeignTableProvider`. Which one you get is an implementation detail, and
both implement `TableProvider`.

## Wrapping it in a capsule

In order to share these FFI structures, we need to wrap them in some kind of
Python object that can be used to interface from one package to another. As
described in {ref}`extension_why_ffi`, we use `PyCapsule`. We can create a
`PyCapsule` for our provider thusly:

```rust
PyCapsule::new_with_value(py, ffi_provider, cr"datafusion_table_provider")
```

On the receiving side, read the `FFI_TableProvider` back out of the capsule and
convert it, which is what `table_provider_from_pycapsule` in `crates/util` does:

```rust
validate_pycapsule(capsule, "datafusion_table_provider")?;
let data: NonNull<FFI_TableProvider> = capsule
    .pointer_checked(Some(c"datafusion_table_provider"))?
    .cast();
let ffi_provider = unsafe { data.as_ref() };
check_ffi_version("table provider", unsafe { (ffi_provider.version)() })?;
let provider: Arc<dyn TableProvider> = ffi_provider.into();
```

## The naming rule

The getter's name and the capsule's name are both fixed by the protocol, and
they follow one rule with no exceptions:

- The method is `__datafusion_<thing>__`.
- The capsule it returns is named `datafusion_<thing>` — the same string
  without the underscores.

So a table provider is reached by calling `__datafusion_table_provider__` and
must return a capsule named `datafusion_table_provider`. Return a capsule with
the wrong name and the import fails with an error naming both the name found
and the name expected, rather than reading the pointer as the wrong type.

The full list of hooks is in the
{ref}`hook reference <extension_guide>`. `TableProvider` was the first
extension written this way and is the most thoroughly implemented; every hook
added since follows the same pattern.

## Version checking

Objects imported through this protocol are checked against the major version of
`datafusion-ffi` that datafusion-python was built with. A component produced by
a library built against a different DataFusion major version raises an
`ImportError` naming the version found and the version expected.

This is a diagnostic rather than a soundness guarantee — reading the version
out of the struct already assumes the local field layout — but it turns the
common "extension library built against the wrong DataFusion" mistake into a
clear message rather than undefined behaviour on first use. See
{ref}`extension_version_mismatch`.

Three FFI structs carry no version field and so cannot be checked:
`FFI_TaskContextProvider`, `FFI_TableProviderFactory`, and
`FFI_ExtensionOptions`.

(extension_getter_argument)=

## What your getter receives

Most getters take one positional argument beyond `py`. The
{ref}`hook reference <extension_guide>` says which, and the group that does
looks like this:

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

This exists because the FFI constructors need things an extension library does
not have. `FFI_{Logical,Physical}ExtensionCodec::new` needs a
`TaskContextProvider` for the decode callbacks the codec will receive, and
`FFI_QueryPlanner::new` needs both codecs on top of that. Taking them from the
argument is what keeps a library from constructing a `SessionContext` purely to
satisfy a parameter — an empty one resolves nothing, and
`FFI_TaskContextProvider` holds it weakly, so a context built inline in the
getter is already dropped by the time the capsule is used.

### It is not always a session

The parameter is conventionally named `session`, and for the codec and planner
hooks it genuinely is one. For the provider and catalog hooks it may instead be
a bare `datafusion_logical_extension_codec` capsule: `SessionContext.register_table`
passes the session, while `Schema.register_table`,
`SessionContext.register_catalog_provider`, `register_catalog_provider_list`,
and `register_table_factory` pass the host's codec directly.

This is why the helpers accept either. `ffi_logical_codec_from_pycapsule`
calls the codec getter if the object has one and returns the object untouched
if it does not, so the same line works in both cases:

```rust
let codec = ffi_logical_codec_from_pycapsule(session, None)?;
```

The rule to hold onto is therefore about capability rather than type: **the
argument is something you can read the host's logical extension codec off.**
The hooks that need more than a codec — the two extension codecs and the query
planner, which need a task-context provider — are exactly the hooks that are
always handed a real session.

### Duck-type it

Do not check the argument's type. Beyond the codec-capsule case above, even
when it *is* a session it is the PyO3 context the binding installs through, not
the `datafusion.context.SessionContext` wrapper. It carries every capsule
getter and `__datafusion_codec_id__` — everything the protocol asks of it — but
`isinstance(session, SessionContext)` is `False` in Python even though its
`repr` reads `datafusion.SessionContext`.

The two bundle hooks are the exception: `__datafusion_session_components__` and
`__datafusion_session_planner__` are dispatched from Python by
{py:meth}`~datafusion.SessionContext.with_extensions`, so they receive the
wrapper. See {doc}`bundles`.

`SessionContext` accepts the argument on its own codec and planner getters and
ignores it, so a session satisfies the same protocol an extension library
implements. When you export the current planner in order to wrap it,
`ctx.__datafusion_query_planner__()` and `ctx.__datafusion_query_planner__(ctx)`
are both fine.

(extension_task_context_provider)=

## `__datafusion_task_context_provider__`

This is the one hook implemented by the **host** rather than by your library.
You never define it; you read it off the session you were handed, which is what
`ffi_task_context_provider_from_pycapsule` does. Nothing in datafusion-python
calls it on a foreign object.

Taking the host's provider means your decode callbacks resolve names against
the session that is actually running the query — see
{ref}`extension_codec_decode_session` — and it removes any need for your
library to construct a `SessionContext` of its own.

[datafusion-ffi]: https://crates.io/crates/datafusion-ffi
