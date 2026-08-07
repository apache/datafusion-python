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

# DataFusion Python FFI query planner example

This crate is an independent query-planner Python extension. Together with [`../datafusion-ffi-example`](../datafusion-ffi-example/) it demonstrates a real three-library plan exchange:

- **A — `datafusion-python`:** owns the session and final execution.
- **B — `datafusion-ffi-example`:** owns a table provider, UDF, and provider codecs.
- **C — this crate:** owns the query planner and its custom configuration.

Two extension crates are used rather than placing the planner in the provider crate. Loading distinct `cdylib` images gives each library a distinct DataFusion marker and proves that foreign sessions, providers, and plans survive the actual ABI boundary.

## Running the example

From the repository root, build and install all three extensions, then run the
integration tests:

```bash
maturin develop --uv
uv run maturin develop --manifest-path examples/datafusion-ffi-example/Cargo.toml
uv run maturin develop \
  --manifest-path examples/datafusion-ffi-query-planner-example/Cargo.toml
uv run pytest \
  examples/datafusion-ffi-query-planner-example/python/tests/_test*.py
```

The preferred setup uses `SessionContext.with_extensions` with extension bundles:

```python
config = SessionConfig().with_extension(PlannerConfig(max_rows=3))
ctx = SessionContext(config).with_extensions(provider_bundle, MyPlannerExtension())
ctx.register_table("numbers", provider)
ctx.register_udf(provider_udf)
```

`MyPlannerExtension` implements the `__datafusion_session_extension__` protocol: it
receives the destination context, binds fresh codec and planner components to that
context's task-context provider, and returns them as `SessionExtensionComponents`.
The host installs everything in one step, so no component can end up bound to an
intermediate context that is later collected.

The integration tests also cover the low-level chaining setup:

```python
config = SessionConfig().with_extension(PlannerConfig(max_rows=3))
ctx = SessionContext(config)
ctx = ctx.with_logical_extension_codec(provider_logical_codec)
ctx = ctx.with_physical_extension_codec(provider_physical_codec)
ctx.register_table("numbers", provider)
ctx.register_udf(provider_udf)
ctx = ctx.with_query_planner(MyQueryPlanner())
```

`PlannerConfig` is transferred through the foreign session. `MyQueryPlanner` reads `ffi_query_planner.max_rows`, creates the plan with `DefaultPhysicalPlanner`, and adds a built-in `GlobalLimitExec`. The test changes the setting with `SET` and verifies the new row limit.

The provider's codec pair is attached to the planner when the derived context is created and is also used to decode the returned physical plan in `datafusion-python`. Extension codecs compose: each `with_logical_extension_codec` / `with_physical_extension_codec` call prepends to the session's codec chain, so several libraries can install codecs on the same session. This planner owns no serializable types of its own and deliberately uses only built-in physical nodes. Install codecs before the planner; derived contexts rebind codecs after a planner is installed directly, but a planner exported as a fallback for another planner keeps the codecs captured at export time.

The pinned FFI logical codec cannot encode arbitrary custom `LogicalPlan::Extension` nodes. The example therefore demonstrates table-provider, UDF, and physical-plan interoperability without claiming custom logical extension support.
