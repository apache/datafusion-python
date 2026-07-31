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

The current Python API installs one external logical codec and one external physical codec. It does not yet compose codecs from several independent plugin owners. This example therefore makes the provider library the sole external codec owner; the planner uses built-in physical nodes and receives the provider codecs from the host.

Register both provider codecs before installing the planner:

```python
ctx = ctx.with_logical_extension_codec(provider_logical_codec)
ctx = ctx.with_physical_extension_codec(provider_physical_codec)
ctx = ctx.with_query_planner(planner)
```

Derived contexts also rebind an installed planner when codecs change, but planner-last order is recommended because it states the ownership flow clearly.

Arbitrary custom `LogicalPlan::Extension` nodes are not supported by the current DataFusion FFI logical codec. This example covers foreign table providers, UDFs, and physical execution plans only.
