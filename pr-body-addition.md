# What changes are included in this PR? (additions to the existing list)

- New `docs/source/contributor-guide/ffi.md` subsection "What a derived context shares", documenting that a codec change on a session with a foreign planner installed forks the session state, and which parts of that state are shared (catalogs, tables, runtime environment) versus snapshotted (registered functions, configuration, optimizer rule lists).
- `examples/datafusion-ffi-example/pyproject.toml` declares `requires-python = ">=3.10"` to match the `abi3-py310` feature the crate has always built against. It previously declared `>=3.9`, which advertised support for an interpreter the wheel cannot load.

# Are there any user-facing changes?

New public APIs: `SessionContext.with_query_planner` and `SessionContext.__datafusion_query_planner__`. A new example crate ships under `examples/`. No breaking changes to existing APIs.

`with_query_planner` returns a context whose session state is forked from the receiver. Catalogs, tables, and the runtime environment remain shared with the original context, while registered functions, the session configuration, and the analyzer and optimizer rule lists are snapshotted at the time of the call. Installing a codec on a session that already has a foreign planner forks in the same way, because the planner has to be rebound to the new codec. Sessions with no foreign planner are unaffected and continue to share state as before. This is documented on the affected methods and in the contributor guide.
