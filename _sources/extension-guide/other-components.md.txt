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

(extension_other_hooks)=

# Optimizer rules and configuration

Two hooks contribute things that are neither data nor functions: a rewrite pass
over physical plans, and typed entries in the session config. Both take no
argument and both are implemented in [`datafusion-ffi-example`].

## A physical optimizer rule

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

## Typed configuration

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
