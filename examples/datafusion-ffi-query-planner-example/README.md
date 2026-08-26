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

The integration test follows this setup:

```python
config = SessionConfig().with_extension(MyPlannerConfig(max_rows=3))
ctx = SessionContext(config)
ctx = ctx.with_logical_extension_codec(provider_logical_codec)
ctx = ctx.with_physical_extension_codec(provider_physical_codec)
ctx.register_table("numbers", provider)
ctx.register_udf(provider_udf)
ctx = ctx.with_query_planner(MyQueryPlanner())
```

`MyPlannerConfig` is transferred through the foreign session. `MyQueryPlanner` reads `ffi_query_planner.max_rows`, creates the plan with `DefaultPhysicalPlanner`, and adds a built-in `GlobalLimitExec`. The test changes the setting with `SET` and verifies the new row limit.

The provider's codec pair is attached to the planner when the derived context is created and is also used to decode the returned physical plan in `datafusion-python`. This planner deliberately uses only built-in physical nodes. Install the codecs before the planner where possible; derived contexts rebind codecs after planner installation, but planner-last order is easier to audit.

For the limits behind that choice — why there is one external codec owner rather than a registry, which node kinds survive the boundary, and what a derived context shares with the context it came from — see [Query Planners Across Multiple Libraries](../../docs/source/contributor-guide/ffi.md#query-planners-across-multiple-libraries) in the contributor guide.
