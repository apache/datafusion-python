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

# DataFusion Python FFI provider example

This crate is the **provider library** in the three-library query-planning example. It exports table providers, functions, and the logical and physical codecs needed to serialize objects owned by this library. The companion planner is in [`../datafusion-ffi-query-planner-example`](../datafusion-ffi-query-planner-example/).

The example intentionally uses separate `cdylib` crates for these roles:

1. **A — `datafusion-python`:** owns the `SessionContext` and executes the result.
2. **B — this crate:** owns table providers, functions, and provider execution plans.
3. **C — the planner crate:** receives the logical plan and returns a physical plan.

Separate shared libraries guarantee distinct DataFusion library markers. This catches type-identity mistakes that a planner and provider compiled into one shared library would hide.

## Codec behavior

`MyLogicalExtensionCodec` serializes this example's in-memory table providers, and `MyPhysicalExtensionCodec` serializes provider-owned memory scans and opaque FFI wrappers around them. Both use documented, process-local, one-shot token registries. The registries make ownership and callback routing visible without pretending to be a portable format. They assume trusted in-process payloads and consume each token during decoding. A production provider should instead encode durable metadata from which its provider and plans can be reconstructed.

Both codec getters take the `SessionContext` they are being installed on and pull the `TaskContextProvider` off it, so decode callbacks resolve session configuration and registered functions against the session that is running the query. Passing `require_udf_on_decode` to either constructor makes every decode call resolve a named scalar function out of that context, which is how the tests check where the registry came from.

This example makes the provider library the sole external codec owner. Register both provider codecs before installing the planner:

```python
ctx = ctx.with_logical_extension_codec(provider_logical_codec)
ctx = ctx.with_physical_extension_codec(provider_physical_codec)
ctx = ctx.with_query_planner(planner)
```

Derived contexts also rebind an installed planner when codecs change, so this order is a recommendation rather than a requirement. Planner-last states the ownership flow more clearly.

For the limits behind that choice — why there is one external codec owner rather than a registry, which node kinds survive the boundary, and what a derived context shares with the context it came from — see [Query Planners Across Multiple Libraries](../../docs/source/contributor-guide/ffi.md#query-planners-across-multiple-libraries) in the contributor guide.
