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

| Hook | Contributes | Wrapped by | Registered with |
| --- | --- | --- | --- |
| `__datafusion_scalar_udf__` | scalar function | {py:func}`datafusion.udf` | {py:meth}`~datafusion.SessionContext.register_udf` |
| `__datafusion_aggregate_udf__` | aggregate function | {py:func}`datafusion.udaf` | {py:meth}`~datafusion.SessionContext.register_udaf` |
| `__datafusion_window_udf__` | window function | {py:func}`datafusion.udwf` | {py:meth}`~datafusion.SessionContext.register_udwf` |
| `__datafusion_table_function__` | function returning a table | {py:func}`datafusion.udtf` | {py:meth}`~datafusion.SessionContext.register_udtf` |

All four are implemented in [`datafusion-ffi-example`], one per file.

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

## Serializing functions

A function that appears in a plan leaving the process has to be reconstructible
on the far side. Functions are the one case where a codec often needs **no
payload at all**: the name is the whole encoding, `try_encode_udf` writes
nothing, and `try_decode_udf` rebuilds the function from `name`. See
{ref}`extension_codecs` for how that works and for the one obligation it puts
on your decoder — with an empty payload there is no id to route on, so your
`try_decode_udf` can be called with a `name` belonging to another library.

`NameOnlyUdfCodec` in [`datafusion-ffi-example`] is the worked case.

(extension_other_hooks)=

## Other session components

Two further hooks contribute things that are neither data nor functions. Both
take no argument and both are implemented in [`datafusion-ffi-example`].

**`__datafusion_physical_optimizer_rule__`** contributes a rule that rewrites
physical plans, installed with
{py:meth}`~datafusion.SessionContext.add_physical_optimizer_rule`. Reach for
this rather than a {doc}`query planner <query-planners>` when you want to
adjust the plan DataFusion produced rather than produce it yourself — it is
much the smaller commitment, and rules accumulate where planners nest.

```rust
fn __datafusion_physical_optimizer_rule__<'py>(
    &self,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyCapsule>> {
    let rule: Arc<dyn PhysicalOptimizerRule + Send + Sync> = Arc::new(self.clone());
    let runtime = get_tokio_runtime().handle().clone();
    let ffi = FFI_PhysicalOptimizerRule::new(rule, Some(runtime));

    PyCapsule::new_with_value(py, ffi, cr"datafusion_physical_optimizer_rule")
}
```

**`__datafusion_extension_options__`** contributes typed configuration entries
that your components can read back out of the session config, installed with
{py:meth}`SessionConfig.with_extension <datafusion.SessionConfig.with_extension>`.
`FFI_ExtensionOptions` carries no version field, so it is one of the three
components that cannot be version-checked on import.

```rust
fn __datafusion_extension_options__<'py>(
    &self,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyCapsule>> {
    let mut config = FFI_ExtensionOptions::default();
    config
        .add_config(self)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    PyCapsule::new_with_value(py, config, cr"datafusion_extension_options")
}
```

[`datafusion-ffi-example`]: https://github.com/apache/datafusion-python/tree/main/examples/datafusion-ffi-example
