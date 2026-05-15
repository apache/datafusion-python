# Plan: Decouple `Expr` pickle support from global `SessionContext`

## Context

[apache/datafusion-python#1517](https://github.com/apache/datafusion-python/pull/1517)
("Allow pickling PyExpr") adds `pickle`/`dill` support to `Expr` so that
expressions can be shipped across process boundaries — primarily for
`multiprocessing.Pool`-style fan-out within a single machine, with an eye toward
working around `datafusion-distributed` not being deployed everywhere.

This document proposes a redesign of how `__setstate__` resolves the
reconstruction context, replacing the current process-global singleton with an
explicit, framework-agnostic worker-scoped context. The new design works
identically under `fork`, `forkserver`, `spawn`, Ray actors, and any other
worker model that exposes an initialization hook.

The plan is staged: Phase 1 is the minimal structural change (small, low risk,
unblocks all the downstream patterns). Phase 2 is additive (cloudpickle-based
UDF inlining). Phase 3 is documentation/examples.

---

## What the PR currently does

Two structural changes from the existing commits on
`rerun-io:nick/pickle_expr`:

1. **Mutable global context.** `datafusion-python-util` promoted its global
   slot from `OnceLock<Arc<SessionContext>>` to `RwLock<Arc<SessionContext>>`,
   exposed as `set_global_ctx` (Rust) and `SessionContext.set_as_global`
   (Python). Users register UDFs on a context and call `set_as_global()` to
   make those UDFs visible to subsequent pickle reconstructions.

2. **Pickle protocol on `Expr`.** `Expr.to_bytes` / `Expr.from_bytes` were
   added on top of `datafusion-proto`'s `Serializeable` trait. The Python
   wrapper implements `__getstate__` (returns proto bytes) and `__setstate__`
   (decodes proto bytes by looking up function references against the
   *process-wide global* `SessionContext`). Built-in functions always
   roundtrip; user-defined functions roundtrip when registered on a context
   that has been installed via `SessionContext.set_as_global()`.

## Why the current design is fragile

### Coupling to start-method semantics

`SessionContext.set_as_global()` propagates to worker processes only when those
workers inherit the parent's memory. That is the `fork` start method, and
only `fork`.

- **fork (POSIX, pre-3.14 default):** worker is a COW clone; the global is
  present in the worker because it was present in the parent at fork time.
- **forkserver (POSIX, 3.14+ default):** a long-lived helper process forks
  workers. The helper started clean; module-level `set_as_global()` calls
  that ran in the parent are *not* in the helper, so workers don't see them
  unless setup re-runs.
- **spawn (Windows default, macOS default since 3.8, optional elsewhere):** worker
  is a fresh interpreter. Module-level setup runs again in the worker; whether
  `set_as_global()` ran depends on whether it sits in module-level code or in
  the parent's `__main__`.

Python 3.14 changed the POSIX default from `fork` to `forkserver`. The PR's
pattern silently degrades on the now-default start method unless the user
happens to structure their imports correctly.

### Pickle has no context channel

`__setstate__(state)` is invoked with exactly one positional argument: the
state blob returned by `__getstate__`. There is no idiomatic way for the
caller of `pickle.loads` to pass an explicit receiver context. The PR's
solution — consult a process-global — is the natural workaround, but it
couples deserialization to mutable ambient state in a way that:

- Makes the same blob deserialize differently depending on what the receiver
  has installed.
- Conflicts when multiple libraries try to use the global slot.
- Fails silently if a UDF name happens to collide with a different
  implementation registered on the receiver.

### Not extensible to other worker models

Ray actors, `concurrent.futures.ProcessPoolExecutor`, Dask workers, and other
multi-process frameworks all have per-worker initialization hooks. The PR's
pattern bypasses all of them by routing through a global. The result is that
each framework needs its own integration shim instead of a single uniform API.

---

## Proposed design

### Worker-scoped context, set by an initialization hook

Replace the "process-global ctx" lookup inside `__setstate__` with a
"worker-scoped ctx" lookup. The worker scope is populated by the framework's
worker-init hook (e.g. `Pool(initializer=...)`, Ray actor `__init__`,
`ProcessPoolExecutor(initializer=...)`). This is the pattern PySpark uses for
executor startup and Ray actors use for per-actor state.

```
+-- Driver process ---------+         +-- Worker process ----------+
|                           |         |                            |
| ctx = SessionContext()    | pickle  | init_worker():             |
| ctx.register_udf(...)     | ====>   |   ctx = SessionContext()   |
| blob = pickle.dumps(expr) |         |   ctx.register_udf(...)    |
|                           |         |   set_worker_ctx(ctx)      |
|                           |         |                            |
|                           |         | expr = pickle.loads(blob)  |
|                           |         |   # reads _worker_ctx()    |
+---------------------------+         +----------------------------+
```

The worker scope is just a module-level (or thread-local) variable in
`datafusion`. Each worker process has its own. No fork-time copy-on-write
dependency; no module-level magic; no global mutation that can collide.

### Envelope as the canonical wire format (Phase 2)

For UDFs the receiver doesn't already have registered, ship them *inside the
blob* via cloudpickle. The state stored by `__getstate__` becomes:

```python
ExprEnvelope = {
    "version": int,
    "proto_bytes": bytes,          # existing datafusion-proto encoding
    "udf_definitions": dict,       # {name: cloudpickled_callable_def} for Python UDFs
    "config_overrides": dict,      # subset of SessionConfig that affects evaluation
}
```

`__setstate__` resolves UDFs in priority order: (1) worker ctx if set, (2)
envelope's `udf_definitions`, (3) fail loudly listing what was needed and
what's available. The receiver only ever constructs a *fresh local*
`SessionContext` if the user hasn't set a worker ctx — never reaches for a
global.

### Explicit `from_bytes(buf, ctx=...)` as the canonical Python API

The pickle protocol is the *convenience* layer. The supported, documented
path is:

```python
env = expr.to_envelope(ctx=sender_ctx, include_udfs="referenced")
blob = cloudpickle.dumps(env)  # or any serializer

# Receiver:
env = cloudpickle.loads(blob)
expr = Expr.from_envelope(env, ctx=receiver_ctx)  # explicit, no globals
```

`__getstate__` / `__setstate__` delegate to these methods. Users who want
explicit control bypass pickle entirely.

---

## Implementation plan

### Phase 1 — Decouple from global ctx (minimum viable change)

Goal: stop `__setstate__` from depending on `set_as_global()`. Do not change
the wire format. Do not add cloudpickle. Just rewire where the reconstruction
ctx comes from.

**Python side (`python/datafusion/`):**

1. New module `python/datafusion/_ipc.py` (or `ipc.py` if public):
   ```python
   import threading
   _local = threading.local()

   def set_worker_ctx(ctx: "SessionContext") -> None:
       """Register the receiver context for Expr unpickling in this worker.

       Call once per worker process, typically from a Pool initializer or
       Ray actor __init__. Idempotent; overwrites any previous value.
       """
       _local.ctx = ctx

   def clear_worker_ctx() -> None:
       """Remove the worker ctx, restoring fresh-context fallback behavior."""
       if hasattr(_local, "ctx"):
           del _local.ctx

   def _get_worker_ctx() -> "SessionContext | None":
       return getattr(_local, "ctx", None)
   ```

2. Modify `Expr.__setstate__` to:
   - Call `_get_worker_ctx()`. If set, decode against it.
   - If not set, build a fresh `SessionContext()` and decode against that.
     This will succeed for built-in functions and fail informatively for
     unregistered UDFs.
   - Do *not* read from `set_as_global` / `get_global_ctx`.

3. Update `Expr.from_bytes` (Python wrapper) to take an explicit `ctx`
   parameter. Keep backward-compatible default — if `ctx=None`, behave as
   `__setstate__` does (worker ctx → fresh ctx → error).

**Rust side (`src/`):**

The underlying `RawExpr::from_bytes` likely already takes a `SessionContext`
parameter (this is how the current PR's global lookup wires through). Verify
this; if it does not, plumb one through. Do not introduce any new Rust-side
worker-ctx state — keep that entirely in Python.

The existing `set_global_ctx` / `SessionContext.set_as_global` can stay for
backward compatibility but are no longer consulted during pickle
reconstruction. Add a docstring note that `set_as_global` is legacy and
should not be relied on for pickle behavior.

**Tests:**

Replace the existing pickle tests with a parametrized matrix over
`(serializer, ctx_strategy)`:

| ctx_strategy           | expected behavior                                |
|------------------------|--------------------------------------------------|
| `worker_ctx_with_udfs` | UDFs resolve, expression evaluates correctly     |
| `worker_ctx_empty`     | Built-ins work, UDF references raise            |
| `no_worker_ctx`        | Built-ins work, UDF references raise informatively |
| `global_ctx_set`       | Should NOT affect outcome (asserts decoupling)    |

Add three multiprocessing tests parametrized over start methods:

```python
@pytest.mark.parametrize("start_method", ["fork", "forkserver", "spawn"])
def test_pickle_roundtrip_via_pool(start_method):
    ctx = mp.get_context(start_method)
    with ctx.Pool(initializer=init_worker, initargs=()) as pool:
        results = pool.map(evaluate_one, [(expr, batch) for ...])
        assert results == expected
```

The worker initializer registers UDFs and calls `set_worker_ctx`. The
`init_worker` function must live in a non-test module (per the lessons learned
in commit `bdeedd7` about pytest-importlib synthetic module names — keep that
fix).

### Phase 2 — Envelope-based UDF inlining (additive)

Goal: support pickling expressions that reference Python UDFs the receiver
hasn't pre-registered. Sender embeds cloudpickled UDF definitions; receiver
unpacks them onto its ctx (or a fresh one) before decoding.

**Python side:**

1. Add `Expr.referenced_functions() -> list[str]`. Walks the expression tree,
   yields names of every UDF/UDAF/UDWF/builtin referenced. May need a small
   Rust-side helper that exposes the walker.

2. Define `ExprEnvelope` as a Python dataclass (kept Python-side; the Rust
   layer only deals with proto bytes):
   ```python
   @dataclass
   class ExprEnvelope:
       version: int
       proto_bytes: bytes
       udf_definitions: dict[str, bytes]   # cloudpickle blobs
       config_overrides: dict[str, str]
   ```

3. Add `Expr.to_envelope(ctx, *, include_udfs="referenced")`:
   - Walk `referenced_functions()`.
   - For each name, check if it's a built-in (skip; receiver always has it).
   - Otherwise look up the UDF on the sender's `ctx`, extract the underlying
     Python callable + signature + volatility, cloudpickle it, store under
     the name.
   - Rust-only UDFs (no Python callable) cannot be inlined — store just the
     name and document that the receiver must have a matching registration.
   - `include_udfs="none"` skips inlining entirely (envelope carries only
     proto bytes); `include_udfs="all"` walks and inlines every UDF on the
     sender ctx regardless of reference.

4. Add `Expr.from_envelope(env, ctx=None)`:
   - If `ctx` is None, use `_get_worker_ctx()` or build a fresh one.
   - For each `(name, blob)` in `env.udf_definitions`, cloudpickle.loads and
     register on `ctx`. If `ctx` already has a UDF named `name`, raise unless
     a `prefer_envelope=False` knob says otherwise (avoid silent override).
   - Apply `env.config_overrides` to `ctx`.
   - Decode `env.proto_bytes` against `ctx`.

5. Rewire `__getstate__` / `__setstate__` to delegate to `to_envelope` /
   `from_envelope`. The state stored by `__getstate__` is the `ExprEnvelope`
   itself (which pickle/cloudpickle then serializes recursively).

**Dependencies:**

Add `cloudpickle` to runtime deps (not just dev). `dill` stays as optional;
the test matrix can keep covering both.

**Tests:**

- Lambda UDF roundtrip via envelope, no pre-registered worker ctx (envelope
  is fully self-contained).
- Closure-capturing UDF roundtrip; verify captured state is reconstructed.
- Name collision between envelope UDF and worker ctx UDF — verify the
  documented resolution rule (raise by default).
- Rust UDF referenced but not registered on receiver — verify clear error
  message identifying which UDFs are missing.
- Cross-version cloudpickle (different Python minors) — document expected
  failure mode if any.

### Phase 3 — Documentation and integration examples

Goal: make the new patterns discoverable and lower the integration cost for
Ray/Dask/concurrent.futures users.

1. **User guide section** "Distributing expressions across processes":
   - Recommended `Pool(initializer=...)` pattern with full example.
   - Note about Python 3.14 default start method change.
   - Trade-offs of envelope inlining vs pre-registered UDFs (blob size,
     closure capture surprises, Rust UDFs can't be inlined).
   - Security note: envelope deserialization runs cloudpickle, which
     executes arbitrary code. Only deserialize blobs from trusted sources.

2. **Ray integration example** (separate file, not a runtime dep):
   ```python
   import ray
   from datafusion import SessionContext
   from datafusion.ipc import set_worker_ctx

   @ray.remote
   class DataFusionWorker:
       def __init__(self, udf_defs):
           ctx = SessionContext()
           for name, fn in udf_defs.items():
               ctx.register_udf(name, fn)
           set_worker_ctx(ctx)
           self.ctx = ctx

       def evaluate(self, expr, batch):
           return self.ctx.evaluate(expr, batch)
   ```

3. **Migration note for `set_as_global`:** explain that the API is still
   available but no longer affects pickle behavior. Recommend
   `set_worker_ctx` for new code. Consider a `DeprecationWarning` if
   `set_as_global` is called (separate decision — may break existing users
   who rely on it for non-pickle reasons).

---

## Open design questions

Things worth deciding explicitly before implementation, ideally on the PR or
the linked issue:

1. **Should `__setstate__` fall back to a fresh ctx when no worker ctx is
   set, or raise immediately?** Falling back is friendlier for trivial cases
   (no UDFs, just built-ins) but hides the "you forgot to set up the
   worker" error in the common case. Suggest: fall back, but log a debug
   message; raise only if the proto references a non-built-in UDF that
   can't be resolved.

2. **Name collision policy in `from_envelope`.** If the worker ctx has a
   UDF named `foo` and the envelope also carries one named `foo`, what
   wins? Suggest: raise by default with a clear error; offer
   `prefer="worker" | "envelope"` parameter. Silent override is the worst
   outcome.

3. **Is `ipc` the right module name?** Alternatives: `_runtime`, `dist`,
   `workers`, `transport`. Whatever is chosen should be stable since the
   `set_worker_ctx` symbol becomes user-facing API.

4. **Should `set_as_global` be deprecated?** It has uses outside pickle
   (e.g. controlling the default ctx for `read_*` module helpers). Suggest:
   keep without deprecation, just stop using it from pickle paths, document
   the new boundary.

5. **Cloudpickle as a runtime dep.** Adds ~50KB and a transitive dep on
   nothing serious. Probably fine, but worth flagging in the PR. Alternative:
   make Phase 2 require `pip install datafusion[distributed]` with
   cloudpickle as an optional extra.

6. **Should the envelope version be wire-compatible across DataFusion
   releases?** The proto encoding has its own version story; the envelope
   adds another layer. Suggest: explicit `version: int` field, refuse to
   decode envelopes from a future major.

---

## Testing strategy summary

The test pyramid for both phases:

- **Unit:** envelope construction/round-trip, UDF enumeration, error paths
  (missing UDFs, name collisions, version mismatches).
- **Integration:** pickle roundtrip across all three start methods via
  `mp.get_context(method).Pool`. Run under CI on Linux at minimum, ideally
  also macOS (where `spawn` is default).
- **Negative:** ensure setting `set_as_global` does *not* affect a worker
  that doesn't call `set_worker_ctx` — verifies decoupling.
- **Stress:** large expressions with many UDFs, expressions with deeply
  nested case/when, expressions referencing both built-ins and UDFs.
- **Pool hang regression:** the original PR hit a deadlock when worker
  processes died during unpickle due to pytest-importlib synthetic module
  names. Keep `tests/_pickle_multiprocessing_helpers.py` (or equivalent
  non-test module) for worker-side function definitions. Keep the
  `timeout-minutes: 30` job cap as a backstop for future regressions.

---

## Implementation outcome (commit `aa08438`, local branch `feat/pickle-pyexpr`)

> **Workflow note (2026-05-14).** The work captured below was prototyped on
> `feat/pickle-pyexpr` but is *not* the merge target. We are landing the
> codec consistency work first as PR1 on a separate branch
> (`feat/proto-codecs`, off `main`), then rebasing the pickle work onto
> PR1 as PR2. See "Phasing the consistency work" below for the revised
> ordering. The text in this section describes the prototype state on
> `feat/pickle-pyexpr` and is preserved as design context for PR2.

Phases 1 and 2 landed with a design change worth recording: instead of a
Python-side `ExprEnvelope` dataclass, Python scalar UDFs are serialized
directly into the proto wire format by a new Rust-side
`LogicalExtensionCodec` (`crates/core/src/codec.rs`). The codec's
`try_encode_udf` downcasts the DataFusion `ScalarUDF` to the private
`PythonFunctionScalarUDF` struct, cloudpickles
`(name, callable, input_schema, return_field, volatility)` into the
proto `fun_definition` field, and tags the payload with the `DFPYUDF1`
magic prefix. `try_decode_udf` reverses it. Single self-contained wire
format — no two-layer envelope, no Python-side UDF registry on
`SessionContext`.

`Expr.__reduce__` simply calls `to_bytes` / `from_bytes`, both of which
route through the codec. The worker-scoped ctx
(`datafusion.ipc.set_worker_ctx`) is still consulted on decode for
references the codec can't inline — currently aggregate UDFs, window
UDFs, and FFI capsule UDFs.

UDAF / UDWF inlining was descoped: their Python state is held inside
opaque factory closures on the Rust side, and the codec needs a
downcastable named struct to extract the callable. A future change can
refactor `RustAccumulator` / `MultiColumnWindowUDF` to expose the same
shape as `PythonFunctionScalarUDF` and extend the codec.

> **Naming note.** The committed code uses `PythonUDFCodec` as the
> Rust struct name. The consistency proposal below renames it to
> `PythonLogicalCodec` — see [R1].

---

## Phase 4 — Cross-cutting consistency with other serialization paths

`feat/pickle-pyexpr` adds proto-based serialization to `Expr`. The
codebase already has, or has WIP for, two other serialization flows:

1. **`PyLogicalPlan.to_proto` / `from_proto`** on `main`
   (`crates/core/src/sql/logical.rs`) — hardcodes
   `DefaultLogicalExtensionCodec`.
2. **`PyExecutionPlan.to_proto` / `from_proto`** plus module-level
   `serialize_execution_plan` / `deserialize_execution_plan` /
   `serialize_physical_expr` / `deserialize_physical_expr` — on the WIP
   branch `poc_ffi_query_planner` (`src/physical_plan.rs`). Takes a
   codec PyCapsule and a `task_ctx_provider` PyCapsule.

Without a deliberate plan these flows will drift further apart. This
section proposes a consistency target so future merges (`Expr` pickle
landed, `LogicalPlan` codec stack, eventual POC merge) produce a
uniform API.

### Observed inconsistencies

| Concern             | `LogicalPlan` (main) | `Expr` (this branch) | `ExecutionPlan` / `PhysicalExpr` (POC) |
| ------------------- | -------------------- | -------------------- | -------------------------------------- |
| Codec source        | Hardcoded default    | Hardcoded `PythonUDFCodec` | Explicit codec PyCapsule per call |
| Method name         | `to_proto` / `from_proto` | `to_bytes` / `from_bytes` | `to_proto` / `from_proto` + module-level `serialize_*` / `deserialize_*` |
| Decoder input       | `PySessionContext`   | `PySessionContext`   | `task_ctx_provider` PyCapsule         |
| PyCapsule protocol on input | None         | None                 | Accepts both typed class + capsule-protocol objects |
| Reads `SessionContext.logical_codec` | No  | No                   | N/A (physical codec is separate)      |

`SessionContext` already stores
`logical_codec: Arc<FFI_LogicalExtensionCodec>`
(`crates/core/src/context.rs:368`) and exposes
`with_logical_extension_codec` plus `__datafusion_logical_extension_codec__`
getter. Nothing currently consults it for proto serialization — it is
only used to plumb codecs through the catalog/table FFI surface. That
field is the obvious home for the codec the recommendations below
prescribe.

### Recommendations

**R1. Rename `PythonUDFCodec` → `PythonLogicalCodec`; make it composable.**

The codec layer is *Python-side logical-layer extensions*, not just
scalar UDFs. Today it handles `try_encode_udf` / `try_decode_udf`; a
future patch adds UDAF/UDWF handling. Reserve the broader name now to
avoid renaming churn:

```rust
pub struct PythonLogicalCodec {
    /// Fallback for anything this codec does not handle directly:
    /// non-Python ScalarUDFs, UDAFs, UDWFs, table providers,
    /// extension nodes, etc. Typically supplied by the user via
    /// `SessionContext.with_logical_extension_codec(...)`. Defaults
    /// to `DefaultLogicalExtensionCodec` when no user codec is set.
    inner: Arc<dyn LogicalExtensionCodec>,
}

impl PythonLogicalCodec {
    pub fn new(inner: Arc<dyn LogicalExtensionCodec>) -> Self {
        Self { inner }
    }

    pub fn with_default_inner() -> Self {
        Self::new(Arc::new(DefaultLogicalExtensionCodec {}))
    }
}
```

Dispatch story (already implemented for `try_decode_udf`; generalize):

- **encode** — Downcast to the named Python impl struct
  (`PythonFunctionScalarUDF` today, `PythonAggregateUDF` /
  `PythonWindowUDF` later). On success: write magic-prefixed
  cloudpickle payload. On failure: delegate to `self.inner.try_encode_*`
  so user-supplied FFI codecs can handle their own types.
- **decode** — Check magic prefix:
  - `DFPYUDF1` → scalar UDF cloudpickle path
  - `DFPYUDA1` → aggregate UDF cloudpickle path (future)
  - `DFPYUDW1` → window UDF cloudpickle path (future)
  - anything else (including empty buf) → delegate to
    `self.inner.try_decode_*`, which either handles a user FFI payload
    or returns `not_impl_err` causing the caller (`parse_expr`) to fall
    back to the receiver's `FunctionRegistry`.

This gives a strict precedence: **Python inline → user FFI codec →
registry lookup**, decided per-payload by the magic prefix, with no
ambiguity about which path produced a given blob.

**R2. One codec lives on `SessionContext`; every serializer reads it.**

`SessionContext` already stores `logical_codec`. Wrap that field so the
session-attached codec *is* a `PythonLogicalCodec` whose inner is the
user-supplied FFI codec (or `Default` when none). Constructor sketch
(adapter glue elided — `FFI_LogicalExtensionCodec` already implements
`LogicalExtensionCodec`):

```rust
let inner: Arc<dyn LogicalExtensionCodec> = user_codec
    .map(|c| c as Arc<dyn LogicalExtensionCodec>)
    .unwrap_or_else(|| Arc::new(DefaultLogicalExtensionCodec {}));
let logical_codec = Arc::new(PythonLogicalCodec::new(inner));
```

`with_logical_extension_codec(new_inner)` rebuilds the
`PythonLogicalCodec` with the new inner. `PyExpr::to_bytes` /
`from_bytes` (this branch) drop their hardcoded
`PythonUDFCodec::new()` and read `session.logical_codec` instead.
`PyLogicalPlan::to_proto` / `from_proto` switch from hardcoded
`DefaultLogicalExtensionCodec` to the same field. Default behavior
unchanged for users who never call `with_logical_extension_codec`.

Same pattern on the physical side: introduce
`PythonPhysicalCodec` (mirror of `PythonLogicalCodec`, also
composable), park it on `SessionContext` as `physical_codec`, have
`PyExecutionPlan` / future `PyPhysicalExpr` consult it.

**R3. Single naming convention: `to_bytes` / `from_bytes` on the class.**

Choose one pair. Recommend `to_bytes` / `from_bytes` because:
- This branch already uses it for `Expr`.
- The proto encoding is the *byte* representation, not the proto
  object; `to_proto` was a slight misnomer on `LogicalPlan`.
- Symmetric with `Serializeable::{to_bytes, from_bytes_with_registry}`
  upstream.

Rename `PyLogicalPlan::to_proto` → `to_bytes` and `from_proto` →
`from_bytes` as a follow-up. Keep `to_proto` / `from_proto` as
deprecated aliases for at least one release. POC's module-level
`serialize_execution_plan` / `deserialize_execution_plan` collapse
into `PyExecutionPlan::to_bytes` / `from_bytes`; the FFI-bridging
PyCapsule-input variants stay as low-level helpers in the
`physical_plan` submodule for external Rust consumers (rename to
clarify their role, e.g. `bytes_from_capsule_plan`).

**R4. Decoders take `SessionContext`, not raw PyCapsule plumbing.**

The public Python API for every decoder accepts a `SessionContext`.
The Rust side pulls `task_ctx`, the function registry, and the codec
stack from that single argument. POC's split into
`(plan_bytes, task_ctx_provider_capsule, codec_capsule)` is FFI-glue
and should be wrapped behind `SessionContext.from_capsules(...)` for
callers who only have PyCapsules — same low-level access without
making it the default decoder shape.

**R5. PyCapsule protocol acceptance on every serialized type.**

POC's `PyExecutionPlan.from_pycapsule` and the dual-input
`serialize_execution_plan` pattern (extract typed class *or* capsule)
should generalize. `PyLogicalPlan`, `PyExpr` (this branch), and any
future serialized types expose `__datafusion_<name>__(py) -> PyCapsule`
getters and accept capsule-protocol objects on input. POC's
`from_pycapsule!` / `try_from_pycapsule!` macros (POC
`src/utils.rs:182–223`) move into `datafusion-python-util` and become
the canonical extractor pattern. Current main already uses them for
table/catalog providers; extend to plans/exprs.

**R6. Wire-format magic prefix registry.**

`PythonLogicalCodec` prepends a magic byte sequence per payload kind.
Establish a convention table:

| Codec layer + kind            | Magic prefix    | Owner             |
| ----------------------------- | --------------- | ----------------- |
| `PythonLogicalCodec` (scalar) | `DFPYUDF1`      | this branch       |
| `PythonLogicalCodec` (agg)    | `DFPYUDA1`      | future            |
| `PythonLogicalCodec` (window) | `DFPYUDW1`      | future            |
| `PythonPhysicalCodec` (expr)  | `DFPYPE1`       | future / POC merge |
| User FFI extension codec      | user-supplied   | downstream crates |
| Default codec (no magic)      | (none)          | upstream          |

`PythonLogicalCodec` decode dispatches by prefix; unknown prefixes (or
no prefix) delegate to the configured `inner` codec. User FFI codecs
must pick non-colliding magic prefixes (recommend a `DF` namespace plus
crate-specific suffix).

**R7. Cross-language story made explicit.**

The pre-existing "Out of scope" note that *"the bare `to_bytes` /
`from_bytes` proto path remains for cross-language use"* is no longer
accurate: with `PythonLogicalCodec` in the default stack, blobs
containing Python scalar UDFs are not portable to non-Python decoders.
Two remediations:

a. **Optional codec override on `to_bytes`** —
   `expr.to_bytes(cross_language=True)` builds bytes using only the
   inner codec (typically `DefaultLogicalExtensionCodec` or a user
   FFI codec), skipping Python-side encoding. Decoder must have UDFs
   pre-registered or supply a matching FFI codec.
b. **Document the trade-off** in the user guide — the default codec
   stack is Python-aware; the wire format is Python-receiver-only
   unless the sender opts out.

**R8. Move helpers into shared crate.**

POC plumbing (`from_pycapsule!`, `try_from_pycapsule!`,
`task_context_from_pycapsule`, `physical_codec_from_pycapsule`,
`validate_pycapsule`) is duplicated material. Park them in
`crates/util` (already houses `create_logical_extension_capsule`,
`ffi_logical_codec_from_pycapsule`) so every consumer pulls from one
place.

**R9. Pickle = `to_bytes` + worker ctx, for every serialized type.**

`Expr.__reduce__` (this branch) is the template. When `LogicalPlan` /
`ExecutionPlan` pickle support is added, they use the same shape:
`__reduce__` returns `(cls._reconstruct, (self.to_bytes(),))`;
`_reconstruct` resolves the receiver context via
`datafusion.ipc._resolve_ctx(None)` and calls `cls.from_bytes`. No
per-type envelope dataclass. The worker-ctx mechanism in
`datafusion.ipc` is reused unchanged.

### Phasing the consistency work

Reordered (2026-05-14) after deciding to land codec consistency *before*
pickle work, on a fresh branch off `main` (`feat/proto-codecs`). The
pickle prototype on `feat/pickle-pyexpr` (local commit `aa08438`)
rebases onto the codec PR after it merges. Two PRs:

**PR1 — Logical + physical codec consistency (`feat/proto-codecs`).**

1. **R1 — Introduce `PythonLogicalCodec` and `PythonPhysicalCodec`.**
   Both follow the composable-inner pattern (Python-aware dispatch
   first, delegate to a user-supplied inner codec, then registry
   fallback). Both implement scalar-UDF inline encoding via the
   shared `DFPYUDF1` magic prefix ported from `aa08438` — the
   `PhysicalExtensionCodec` trait exposes `try_encode_udf` /
   `try_decode_udf` (and the UDAF/UDWF variants) the same way the
   logical trait does, so an `ExecutionPlan` or `PhysicalExpr` that
   references a Python `ScalarUDF` round-trips only if the physical
   codec inlines it too. Factor the cloudpickle tuple
   `(name, callable, input_schema, return_field, volatility)` and
   the magic-prefix framing into a shared helper that both codecs
   call. What stays deferred on the physical side is the
   `DFPYPE*` namespace: encoding for Python-defined physical
   extension nodes (`ExecutionPlan` impls) and Python-defined
   `PhysicalExpr` impls. No concrete Python-side physical extension
   type exists today, so no prefix assignment in PR1.
2. **R2 — Park codecs on `SessionContext`.** Replace the existing
   `logical_codec: Arc<FFI_LogicalExtensionCodec>` field with
   `Arc<PythonLogicalCodec>`; add `physical_codec: Arc<PythonPhysicalCodec>`.
   `with_logical_extension_codec` and a new `with_physical_extension_codec`
   rebuild the wrapper with the supplied inner. Every serializer reads
   from these session fields.
3. **R3 — Rename `PyLogicalPlan::to_proto` → `to_bytes`** and
   `from_proto` → `from_bytes`. Keep `to_proto` / `from_proto` as
   deprecated thin wrappers that emit `DeprecationWarning` and call
   the new names. Targeted for removal one release after PR1.
4. **R5 — PyCapsule protocol on serialized types.** Move
   `from_pycapsule!` / `try_from_pycapsule!` from POC `src/utils.rs`
   into `datafusion-python-util`. `PyExecutionPlan` and `PyPhysicalExpr`
   expose `__datafusion_<name>__` capsule getters and accept
   capsule-protocol input on `from_pycapsule`. `PyLogicalPlan` is
   excluded: `datafusion-ffi` does not expose a `FFI_LogicalPlan`
   representation, so there is no stable capsule shape to publish.
   Round-tripping a `LogicalPlan` across a process boundary goes
   through `to_bytes` / `from_bytes` only.
5. **R6 — Magic-prefix registry doc comment** in
   `crates/core/src/codec.rs`. `DFPYUDF1` in use; `DFPYUDA1`,
   `DFPYUDW1`, `DFPYPE1` reserved.
6. **R8 — Shared helpers into `crates/util`.**
   `task_context_from_pycapsule`, `physical_codec_from_pycapsule`,
   `validate_pycapsule`, and the capsule macros land here. Core and
   downstream consumers pull from one place.
7. **POC equivalent surface area.** Pull
   `PyExecutionPlan::to_bytes` / `from_bytes`, `PyPhysicalExpr::to_bytes` /
   `from_bytes`, and `task_ctx_provider` capsule extraction off
   `poc_ffi_query_planner`. Module-level `serialize_execution_plan` /
   `deserialize_execution_plan` / `serialize_physical_expr` /
   `deserialize_physical_expr` collapse into the class methods; the
   FFI-bridging capsule-input variants stay as low-level helpers in
   the `physical_plan` submodule (rename for clarity, e.g.
   `bytes_from_capsule_plan`).

**PR2 — Pickle and worker-scoped distribution (rebase of `aa08438`).**

1. `PyExpr::to_bytes` / `from_bytes` reading `session.logical_codec`
   (no hardcoded `PythonUDFCodec`).
2. `Expr.__reduce__` + `datafusion.ipc.set_worker_ctx` worker-scoped
   fallback (Phase 1 of the original plan).
3. Multiprocessing tests across `fork`, `forkserver`, `spawn`.
4. **R9 — Pickle template reused on other serialized types.** Apply
   `__reduce__` returning `(cls._reconstruct, (self.to_bytes(),))` to
   `PyLogicalPlan` and `PyExecutionPlan` once the pickle infra exists.
5. **R7 — `to_bytes(cross_language=True)` opt-out** for blobs that
   must round-trip through non-Python decoders.

**Deferred (separate follow-ups after PR2).**

- **UDAF / UDWF inline encoding.** Needs `RustAccumulator` /
  `MultiColumnWindowUDF` refactored to expose a downcastable named
  struct. Magic prefixes `DFPYUDA1` / `DFPYUDW1` reserved.
- **`PythonPhysicalCodec` extension-node / physical-expr inline
  payloads.** Scalar-UDF (and the UDAF/UDWF variants, once those
  ship logical-side) round-trip through `DFPYUDF1` /
  `DFPYUDA1` / `DFPYUDW1` from PR1 onward. Distinct from those:
  Python-defined physical extension nodes (`ExecutionPlan` impls)
  and Python-defined `PhysicalExpr` impls. Once a concrete
  Python-side physical extension type exists, assign a magic
  prefix from the `DFPYPE*` namespace and add encode/decode.

---

## Out of scope

Things this plan deliberately does not address — flag as follow-ups if
needed:

- **Cross-language serialization.** Blobs produced by the default codec
  stack are not cross-language because they may contain cloudpickle
  payloads. The plan addresses this in Phase 4 R7 (opt-out
  `to_bytes(cross_language=True)`); full cross-language interop with a
  Rust-only receiver is otherwise out of scope.
- **Distributed catalog / object store / table provider replication.**
  Expressions don't typically reference tables directly (that's a `LogicalPlan`
  concern). If pickle support is later extended to `LogicalPlan` or
  `DataFrame`, those will have their own envelope concerns.
- **Authentication of envelopes.** Cloudpickle is arbitrary-code-execution
  on deserialize. If untrusted-source deserialization becomes a real use
  case, add a signed-envelope variant; not part of this work.
- **Replacing `datafusion-distributed`.** This is for local multi-process
  fan-out and Python-orchestrated frameworks (Ray, Dask). True distributed
  execution remains datafusion-distributed's domain.

---

## References

- PR #1517: <https://github.com/apache/datafusion-python/pull/1517>
- Issue #1520 (motivating): <https://github.com/apache/datafusion-python/issues/1520>
- Python 3.14 multiprocessing default change:
  <https://docs.python.org/3/whatsnew/3.14.html> — `forkserver` is now the
  POSIX default for `ProcessPoolExecutor` and `multiprocessing`.
- `datafusion-proto` `Serializeable` trait (the underlying wire format
  the envelope wraps).
- `cloudpickle`: <https://github.com/cloudpipe/cloudpickle>
- Ray custom serializer API:
  `ray.util.register_serializer(cls, serializer, deserializer)`
- Relevant commits on `rerun-io:nick/pickle_expr`:
  - `23543fc` — `OnceLock` → `RwLock` for global ctx (subject of decoupling)
  - `5796b53` — proto-based `to_bytes`/`from_bytes` and pickle hooks
    (the to_bytes/from_bytes plumbing is reusable; the `__setstate__`
    global-ctx lookup is what changes)
  - `bdeedd7` — pytest-importlib helper-module fix (keep)
  - `106ea3c` — 30-minute job timeout (keep)
