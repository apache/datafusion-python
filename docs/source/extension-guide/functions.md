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

(extension_functions)=

# Functions and table functions

Four hooks contribute functions written in Rust. Users can also define
functions in pure Python — see
{doc}`../user-guide/common-operations/udf-and-udfa` — and the two roads meet at
the same registration methods.

| Hook | Contributes | Wrapped by | Registered with | Declared in a bundle as |
| --- | --- | --- | --- | --- |
| `__datafusion_scalar_udf__` | scalar function | {py:func}`datafusion.udf` | {py:meth}`~datafusion.SessionContext.register_udf` | `udfs` |
| `__datafusion_aggregate_udf__` | aggregate function | {py:func}`datafusion.udaf` | {py:meth}`~datafusion.SessionContext.register_udaf` | `udafs` |
| `__datafusion_window_udf__` | window function | {py:func}`datafusion.udwf` | {py:meth}`~datafusion.SessionContext.register_udwf` | `udwfs` |
| `__datafusion_table_function__` | function returning a table | {py:func}`datafusion.udtf` | {py:meth}`~datafusion.SessionContext.register_udtf` | `udtfs`, as `(name, func)` |

All four are implemented in [`datafusion-ffi-example`], one per file. The last
column is the {py:class}`~datafusion.SessionExtensionComponents` field a
{ref}`bundle <extension_bundles>` declares the function in; table functions
have no such field yet, so they are always registered by the caller with
{py:meth}`~datafusion.SessionContext.register_udtf`.

## The three scalar-shaped hooks

The scalar, aggregate, and window getters take **no argument** beyond `py` —
they need no codec and no task-context provider, because a function is
identified by name and signature rather than by anything session-scoped:

```rust
#[pymethods]
impl MyScalarUDF {
    fn __datafusion_scalar_udf__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let udf = Arc::new(ScalarUDF::from(self.clone()));
        let ffi = FFI_ScalarUDF::from(udf);

        PyCapsule::new_with_value(py, ffi, cr"datafusion_scalar_udf")
    }
}
```

Aggregate and window follow identically with `FFI_AggregateUDF` /
`FFI_WindowUDF` and the matching capsule names.

Your users wrap the object once and register the result:

```python
from datafusion import udf

ctx.register_udf(udf(my_library.MyScalarUDF()))
```

If your library ships more than a function or two, do not make your users write
that line once per function. Ship a {ref}`bundle <extension_bundles>` declaring
them, so one call installs the lot:

```python
ctx = SessionContext().with_extensions(my_library.MyFunctionExtension())
```

The bundle is yours to write, and like the rest of the protocol it is an object
exposing a getter — which your cdylib can export directly. That is what
`MyFunctionExtension` in [`datafusion-ffi-example`] does for this crate's three
functions; spelled in Python, it is:

```python
from datafusion import SessionExtensionComponents


class MyFunctionExtension:
    def __datafusion_session_components__(self, ctx):
        return SessionExtensionComponents(
            udfs=(IsNullUDF(),),
            udafs=(MySumUDF(),),
            udwfs=(MyRankUDF(),),
        )
```

Declare either the raw exportable, as here, or an already-wrapped
{py:class}`~datafusion.user_defined.ScalarUDF`; the registered name comes off
the function either way. Declare rather than calling `register_udf` inside the
hook — see {ref}`extension_bundles_transaction` for why — and pick names that
will not collide with another library's
({ref}`extension_bundles_collisions`).

## Table functions

A table function takes literal `Expr` arguments and returns a table provider,
so it needs the host's logical codec the way a
{ref}`table provider <extension_providers>` does:

```rust
fn __datafusion_table_function__<'py>(
    &self,
    py: Python<'py>,
    session: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyCapsule>> {
    let func = self.clone();
    let codec = ffi_logical_codec_from_pycapsule(session, None)?;
    let provider = FFI_TableFunction::new_with_ffi_codec(Arc::new(func), None, codec);

    PyCapsule::new_with_value(py, provider, cr"datafusion_table_function")
}
```

Only literal expressions are supported as arguments. The Python side is
described under
{doc}`Table Functions <../user-guide/common-operations/udf-and-udfa>`.

Because that getter takes the session, a table function is declared on a bundle
as a `(name, func)` pair and the host wraps it — not as a
{py:class}`~datafusion.user_defined.TableFunction` you built, which would
capture the codec chain from before the call:

```python
return SessionExtensionComponents(udtfs=(("expand", my_library.MyTableFunction()),))
```

The name is given here rather than read off the capsule, which is the other way
this differs from the three above. See {ref}`extension_bundles_binding`.

## Serializing functions

A function that appears in a plan leaving the process has to be reconstructible
on the far side. Functions are the one case where a codec often needs **no
payload at all**: the name is the whole encoding, `try_encode_udf` writes
nothing, and `try_decode_udf` rebuilds the function from `name`. See
{ref}`extension_codecs` for how that works and for the one obligation it puts
on your decoder — with an empty payload there is no id to route on, so your
`try_decode_udf` can be called with a `name` belonging to another library.

`NameOnlyUdfCodec` in [`datafusion-ffi-example`] is the worked case.

[`datafusion-ffi-example`]: https://github.com/apache/datafusion-python/tree/main/examples/datafusion-ffi-example
