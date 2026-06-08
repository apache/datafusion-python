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

# Distributing work

DataFusion supports splitting work across processes by shipping
serialized expressions to workers: the driver builds an
[`Expr`][datafusion.expr.Expr], each worker evaluates it against its
own slice of data. This pattern suits embarrassingly-parallel
workloads where the driver decides partitioning up front.

Query-level distribution — where the runtime partitions a single
logical or physical plan across worker nodes — is in progress
upstream via [datafusion-distributed](https://github.com/datafusion-contrib/datafusion-distributed) and [Apache
Ballista](https://datafusion.apache.org/ballista/). Both
have short sections at the end of this page; integration details
will land as those projects become usable from datafusion-python.

## Expression-level distribution

DataFusion expressions support distribution directly: pass one to a
worker process and Python's standard
[pickle](https://docs.python.org/3/library/pickle.html) machinery
serializes it transparently — the same machinery
[`map`][multiprocessing.pool.Pool.map], Ray's `@ray.remote`, and
similar libraries already use to ship function arguments. Python UDFs
— scalar, aggregate, and window — travel inside the serialized
expression; the receiver does not need to pre-register them.

### Basic worker-pool example

Define a worker function that takes the expression plus a batch and
returns the evaluated result:

```python
import pyarrow as pa
from datafusion import SessionContext


def evaluate(expr, batch):
    # `expr` arrived here via the pool's automatic pickling —
    # no manual serialization needed in user code.
    ctx = SessionContext()
    df = ctx.from_pydict({"a": batch})
    return df.with_column("result", expr).select("result").to_pydict()["result"]
```

Then build the expression in the driver and fan it out:

```python
import multiprocessing as mp
from datafusion import col, udf

double = udf(
    lambda arr: pa.array([(v.as_py() or 0) * 2 for v in arr]),
    [pa.int64()], pa.int64(), volatility="immutable", name="double",
)
expr = double(col("a"))

mp_ctx = mp.get_context("forkserver")
with mp_ctx.Pool(processes=4) as pool:
    results = pool.starmap(
        evaluate,
        [(expr, [1, 2, 3]), (expr, [10, 20, 30])],
    )
print(results)  # [[2, 4, 6], [20, 40, 60]]
```

!!! note

    When saved to a `.py` file and executed with the `spawn` or
    `forkserver` start method, wrap the driver block in
    `if __name__ == "__main__":` so worker processes can re-import
    the module without re-running it. This is a standard Python
    [`multiprocessing`][multiprocessing] requirement, not DataFusion-specific —
    see [Safe importing of main module](https://docs.python.org/3/library/multiprocessing.html#the-spawn-and-forkserver-start-methods)
    in the Python docs.

### What travels with the expression

- **Built-in functions** (`abs`, `length`, arithmetic, comparisons,
  etc.) — fully portable. Worker needs nothing pre-registered.

- **Python UDFs** — travel inline (subject to the two portability
  requirements below). The callable, its signature, and any state
  captured in closures travel inside the serialized expression and are
  reconstructed on the worker automatically. Applies equally to:

  - **scalar UDFs** ([`udf`][datafusion.user_defined.udf])
  - **aggregate UDFs** ([`udaf`][datafusion.user_defined.udaf])
  - **window UDFs** ([`udwf`][datafusion.user_defined.udwf])

- **UDFs imported via the FFI capsule protocol** — travel **by name
  only**. The worker must already have a matching registration on its
  [`SessionContext`][datafusion.context.SessionContext]. Without that registration, evaluation
  raises an error.

### Portability requirements for inline Python UDFs

Inline Python UDFs ride on [cloudpickle](https://github.com/cloudpipe/cloudpickle), which imposes two
requirements on the worker environment:

- **Matching Python minor version.** cloudpickle serializes Python
  bytecode, which is not stable across minor versions. A UDF pickled
  on 3.12 cannot be reconstructed on 3.11 or 3.13. The wire format
  stamps the sender's `(major, minor)`; mismatches raise a clear
  error naming both versions. Align the Python version on driver and
  workers.
- **Imported modules must be importable on the worker.** cloudpickle
  captures the callable *by value* (bytecode and closure cells travel
  whole), but names resolved through `import` are captured *by
  reference* — module path only. A UDF doing
  `from mylib import transform` requires `mylib` installed on the
  worker. Same applies to bound methods of imported classes.
  Self-contained UDFs (no imports beyond what the worker already has,
  e.g. `pyarrow`) avoid this entirely.

### Registering shared UDFs on workers

When an expression references an FFI capsule UDF (or any UDF the
worker must resolve from its registered functions), set up the
worker's [`SessionContext`][datafusion.context.SessionContext] once per process and install it
as the *worker context*:

```python
from datafusion import SessionContext
from datafusion.ipc import set_worker_ctx


def init_worker():
    ctx = SessionContext()
    ctx.register_udaf(my_ffi_aggregate)
    set_worker_ctx(ctx)


with mp.get_context("forkserver").Pool(
    processes=4, initializer=init_worker
) as pool:
    ...
```

Inside a worker, expressions arriving from the driver resolve their
by-name references against the installed worker context. If no worker
context is installed, the global [`SessionContext`][datafusion.context.SessionContext] is used —
fine for expressions that only reference built-ins and Python UDFs,
but FFI-capsule-backed registrations must be installed on the global
context to resolve.

### Python 3.14 default change

Python 3.14 changed the Linux default start method for
[`multiprocessing`][multiprocessing] from `fork` to `forkserver` (macOS has
defaulted to `spawn` since Python 3.8; Windows has always used
`spawn`). With `fork`, any state set in the parent was visible in
workers via copy-on-write; with `forkserver` and `spawn` it is
not. The [`set_worker_ctx`][datafusion.ipc.set_worker_ctx] pattern works on
every start method — prefer it over relying on inherited state.

### Practical considerations

- **Serialized size scales with what travels inline.** A serialized
  expression of just built-ins is small (tens of bytes). An
  expression carrying a Python UDF is hundreds of bytes (the callable
  and its signature). When the same UDF is shipped many times,
  registering an equivalent FFI-capsule UDF on each worker via
  [`set_worker_ctx`][datafusion.ipc.set_worker_ctx] and referring to it by
  name cuts the per-trip overhead.
- **Closure capture.** When a Python UDF closes over surrounding
  state — local variables, module-level objects, file paths — that
  state is captured at serialization time. Surprises are possible if
  the captured state is large, mutable, or not portable to the
  worker's environment. See [Portability requirements for inline
  Python UDFs](#portability-requirements-for-inline-python-udfs) for the Python-version and imported-module rules.

### Disabling Python UDF inlining

For a stricter wire format, call
[`SessionContext.with_python_udf_inlining(enabled=False)`][datafusion.context.SessionContext.with_python_udf_inlining] on the session
producing or consuming the bytes. With inlining disabled, Python
UDFs travel by name only — the same way FFI-capsule UDFs do — and
the receiver must have a matching registration.

Two use cases:

- **Cross-language portability.** A non-Python decoder cannot
  reconstruct a cloudpickled payload. Senders aimed at Java, C++,
  or another Rust binary disable inlining and rely on the receiver
  having compatible UDF registrations.
- **Untrusted-source decode.** With inlining disabled,
  [`from_bytes`][datafusion.expr.Expr.from_bytes] never calls `cloudpickle.loads` on
  the incoming bytes — an inline payload from a misbehaving sender
  raises a clear error instead of executing arbitrary Python code.

Mismatched configurations raise a descriptive error: an inline blob
fed to a strict receiver fails fast rather than silently dropping
into `cloudpickle.loads`.

To make the toggle apply through [`dumps`][pickle.dumps] (which
calls [`to_bytes`][datafusion.expr.Expr.to_bytes] with no context), install the strict
session as the driver's *sender context*:

```python
from datafusion import SessionContext
from datafusion.ipc import set_sender_ctx

set_sender_ctx(SessionContext().with_python_udf_inlining(enabled=False))
# Every subsequent pickle.dumps(expr) on this thread encodes
# without inlining the Python callable.
```

Pair with a matching strict worker context
([`set_worker_ctx`][datafusion.ipc.set_worker_ctx]) so the `pickle.loads`
side also refuses inline payloads. Explicit
[`Expr.to_bytes(ctx)`][datafusion.expr.Expr.to_bytes] and
[`Expr.from_bytes(blob, ctx=ctx)`][datafusion.expr.Expr.from_bytes] calls
honor the supplied `ctx` directly and ignore the sender / worker
contexts.

The toggle only narrows the [`from_bytes`][datafusion.expr.Expr.from_bytes] surface;
[`loads`][pickle.loads] on untrusted bytes remains unsafe regardless
of this setting. See the [Security] section below for the full
threat model.

### Security

!!! warning

    Reconstructing an expression containing a Python UDF executes
    arbitrary Python code on the receiver — pickle is doing the work
    under the hood and pickle is unsafe on untrusted input (see the
    [pickle module security warning](https://docs.python.org/3/library/pickle.html#module-pickle)
    in the Python standard library docs). Only accept expressions
    from trusted sources. For untrusted-source workflows, disable
    Python UDF inlining (see above), restrict senders to built-in
    functions and pre-registered Rust-side UDFs, and avoid
    [`loads`][pickle.loads] on externally supplied bytes entirely.

### Reference: session context slots

There is only one type — [`SessionContext`][datafusion.context.SessionContext]. It can occupy
up to four *slots* in a running program:

| Slot | Lifetime | Purpose | Set how |
|------|----------|---------|---------|
| User-held | Local variable / attribute | Build and run queries | `ctx = SessionContext(...)` |
| Global | Process singleton (lazy-init) | Backs module-level [`read_parquet`][datafusion.io.read_parquet], [`read_csv`][datafusion.io.read_csv], [`read_json`][datafusion.io.read_json], [`read_avro`][datafusion.io.read_avro]; final fallback for [`Expr.from_bytes`][datafusion.expr.Expr.from_bytes] | Implicit; access via [`global_ctx`][datafusion.context.SessionContext.global_ctx] |
| Sender | Thread-local on the driver | Codec settings for outbound `pickle.dumps` / [`Expr.to_bytes`][datafusion.expr.Expr.to_bytes] without `ctx` | [`set_sender_ctx`][datafusion.ipc.set_sender_ctx] |
| Worker | Thread-local on the worker | Function registry for inbound `pickle.loads` / [`Expr.from_bytes`][datafusion.expr.Expr.from_bytes] without `ctx` | [`set_worker_ctx`][datafusion.ipc.set_worker_ctx] |

The same [`SessionContext`][datafusion.context.SessionContext] object may occupy more than one
slot simultaneously — installing it into a slot is a reference, not
a copy. A non-distributed program only ever uses the user-held slot;
the global slot is invisible unless you call top-level `read_*`
helpers.

Resolution order on the worker side is *explicit argument →
worker context → global context.* Explicit `ctx=` on
[`from_bytes`][datafusion.expr.Expr.from_bytes] always wins; the sender slot is ignored
on decode and the worker slot is ignored on encode.

Sharp edges:

- Sender and worker slots are **thread-local**. Background threads
  on either side see `None` until they install their own.
- Under the `fork` start method, the parent's `threading.local()`
  values are copied into the child by copy-on-write — a forked
  worker initially observes whatever sender / worker slot the parent
  had set, until the worker writes its own value (or calls the
  matching `clear_*_ctx`). `spawn` and `forkserver` workers
  start with empty thread-local slots. Treat the slot as
  uninitialized on worker entry and install (or clear) it explicitly
  in the worker initializer; do not rely on inherited state.
- The global slot persists across `fork` workers (copy-on-write
  memory inherit) but not across `spawn` / `forkserver` workers
  (fresh process — register or install a worker context on
  start-up).
- The inlining toggle is per-context state, not a global switch.
  Two contexts with different toggles can coexist in one process.

## Query-level distribution via datafusion-distributed

🚧 *Work in progress upstream — not yet usable from datafusion-python.*

[datafusion-distributed](https://github.com/datafusion-contrib/datafusion-distributed)
splits a single physical plan into stages and runs each stage on a
different worker node. The driver writes a SQL or DataFrame query
once; the runtime handles partitioning, shuffles, and reassembly.

A datafusion-python integration is in development. This section will
document the integration once it lands. In the meantime, the
expression-level approach above covers most use cases that do not
require automatic plan partitioning.

## Query-level distribution via Apache Ballista

🚧 *Work in progress upstream — not yet usable from datafusion-python.*

[Apache Ballista](https://datafusion.apache.org/ballista/)
provides distributed query execution on top of DataFusion with a
scheduler / executor model better suited to long-lived cluster
deployments. A datafusion-python integration is on the roadmap; this
section will fill in once the integration is usable.

## See also

- [`ipc`][datafusion.ipc] — worker context API.
- [`examples/`](https://github.com/apache/datafusion-python/tree/main/examples) —
  runnable scripts for `multiprocessing.Pool` and Ray actor patterns,
  plus other end-to-end demos.
