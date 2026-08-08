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

The example codecs do not inspect the callback `TaskContext`. A production codec that depends on session configuration or registered functions must ensure its exported FFI codec is bound to, and retains, the appropriate host `TaskContextProvider`.

Extension codecs compose: each `with_logical_extension_codec` / `with_physical_extension_codec` call prepends the codec to the session's codec chain, with the most recently installed codec consulted first and DataFusion's default codec as the terminal fallback. A codec signals "not mine" by returning an error, so several independent plugin libraries can install codecs on the same session as long as each only answers for payloads it owns (frame them with a distinct byte prefix). In this example the provider library is the only codec owner; the planner uses built-in physical nodes and receives the provider codecs from the host.

`MyLogicalExtensionCodec` takes an optional token argument (`MyLogicalExtensionCodec("TOKENAAA")`) that overrides the byte prefix it stamps on encoded table providers. It exists so the tests can install two instances that own disjoint slices of the wire format, which is what makes chain ordering and fall-through observable from Python. Real plugin libraries should hard-code a prefix unique to the library rather than accept one from the caller.

Register both provider codecs before installing the planner:

```python
ctx = ctx.with_logical_extension_codec(provider_logical_codec)
ctx = ctx.with_physical_extension_codec(provider_physical_codec)
ctx = ctx.with_query_planner(planner)
```

Derived contexts also rebind an installed planner when codecs change, but planner-last order is recommended because it states the ownership flow clearly.

Arbitrary custom `LogicalPlan::Extension` nodes are not supported by the current DataFusion FFI logical codec. This example covers foreign table providers, UDFs, and physical execution plans only.
