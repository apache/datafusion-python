.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

Distributing DataFusion work
============================

Splitting a DataFusion workload across multiple processes — for
throughput, isolation, or to use a worker pool — comes in a few
different shapes depending on what is being split.

* **Expression-level distribution** ✅ *supported today*. The driver
  builds a DataFusion :py:class:`~datafusion.Expr`, sends it to
  worker processes, and each worker evaluates the expression against
  its own slice of data. Suits embarrassingly-parallel workloads
  where the driver decides up front how to partition.
* **Query-level distribution via datafusion-distributed** 🚧 *work in
  progress upstream*. A single logical / physical plan is split into
  stages and run across worker nodes. The driver writes one SQL or
  DataFrame query; the runtime decides partitioning.
* **Query-level distribution via Apache Ballista** 🚧 *work in
  progress upstream*. Similar query-level model, with a more
  cluster-management-oriented runtime.

Only the first option is ready for use from datafusion-python today.
The other two are documented below so the surrounding story is in
one place; integration details will land here as those projects
become usable from datafusion-python.

Expression-level distribution
-----------------------------

DataFusion expressions support distribution directly: pass one to a
worker process and Python's standard
`pickle <https://docs.python.org/3/library/pickle.html>`_ machinery
serializes it transparently — the same machinery
:py:meth:`multiprocessing.pool.Pool.map`, Ray's ``@ray.remote``, and
similar libraries already use to ship function arguments. Python UDFs
— scalar, aggregate, and window — travel inside the serialized
expression; the receiver does not need to pre-register them.

Basic worker-pool example
~~~~~~~~~~~~~~~~~~~~~~~~~

Define a worker function that takes the expression plus a batch and
returns the evaluated result:

.. code-block:: python

    import pyarrow as pa
    from datafusion import SessionContext


    def evaluate(expr, batch):
        # `expr` arrived here via the pool's automatic pickling —
        # no manual serialization needed in user code.
        ctx = SessionContext()
        df = ctx.from_pydict({"a": batch})
        return df.with_column("result", expr).select("result").to_pydict()["result"]

Then build the expression in the driver and fan it out:

.. code-block:: python

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

When saved to a ``.py`` file and executed with the ``spawn`` or
``forkserver`` start method, wrap the driver block in
``if __name__ == "__main__":`` so worker processes can re-import the
module without re-running it.


What travels with the expression
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* **Built-in functions** (``abs``, ``length``, arithmetic, comparisons,
  etc.) — fully portable. Worker needs nothing pre-registered.
* **Python UDFs** — fully portable. The callable, its signature, and
  any state captured in closures travel inside the serialized
  expression and are reconstructed on the worker automatically.
  Applies equally to:

  * **scalar UDFs** (:py:func:`datafusion.udf`)
  * **aggregate UDFs** (:py:func:`datafusion.udaf`)
  * **window UDFs** (:py:func:`datafusion.udwf`)
* **UDFs imported via the FFI capsule protocol** — travel **by name
  only**. The worker must already have a matching registration on its
  :py:class:`SessionContext`. Without that registration, evaluation
  raises an error.

Registering shared UDFs on workers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When an expression references an FFI capsule UDF (or any UDF the
worker must resolve from its registered functions), set up the
worker's :py:class:`SessionContext` once per process and install it
as the *worker context*:

.. code-block:: python

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

Inside a worker, expressions arriving from the driver resolve their
by-name references against the installed worker context. If no worker
context is installed, a fresh empty :py:class:`SessionContext` is
used — fine for expressions that only reference built-ins and Python
UDFs, but FFI-capsule-backed registrations will fail to resolve.

Python 3.14 default change
~~~~~~~~~~~~~~~~~~~~~~~~~~

Python 3.14 changed the POSIX default start method for
:py:mod:`multiprocessing` from ``fork`` to ``forkserver``. With
``fork``, any state set in the parent was visible in workers via
copy-on-write; with ``forkserver`` and ``spawn`` it is not. The
:py:func:`~datafusion.ipc.set_worker_ctx` pattern works on every
start method — prefer it over relying on inherited state.

Practical considerations
~~~~~~~~~~~~~~~~~~~~~~~~

* **Serialized size scales with what travels inline.** A serialized
  expression of just built-ins is small (tens of bytes). An
  expression carrying a Python UDF is hundreds of bytes (the callable
  and its signature). When the same UDF is shipped many times,
  registering an equivalent FFI-capsule UDF on each worker via
  :py:func:`~datafusion.ipc.set_worker_ctx` and referring to it by
  name cuts the per-trip overhead.
* **Closure capture.** When a Python UDF closes over surrounding
  state — local variables, module-level objects, file paths — that
  state is captured at serialization time. Surprises are possible if
  the captured state is large, mutable, or not portable to the
  worker's environment.

Disabling Python UDF inlining
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For a stricter wire format, call
:py:meth:`SessionContext.with_python_udf_inlining(False)
<datafusion.SessionContext.with_python_udf_inlining>` on the session
producing or consuming the bytes. With inlining disabled, Python
UDFs travel by name only — the same way FFI-capsule UDFs do — and
the receiver must have a matching registration.

Two use cases:

* **Cross-language portability.** A non-Python decoder cannot
  reconstruct a cloudpickled payload. Senders aimed at Java, C++,
  or another Rust binary disable inlining and rely on the receiver
  having compatible UDF registrations.
* **Untrusted-source decode.** With inlining disabled,
  :py:meth:`Expr.from_bytes` never calls ``cloudpickle.loads`` on
  the incoming bytes — an inline payload from a misbehaving sender
  raises a clear error instead of executing arbitrary Python code.

Mismatched configurations raise a descriptive error: an inline blob
fed to a strict receiver fails fast rather than silently dropping
into ``cloudpickle.loads``.

Note that :py:func:`pickle.loads` itself remains unsafe on untrusted
input regardless of this setting — an attacker producing the outer
pickle envelope can execute arbitrary code before the codec ever
sees the bytes (see the
`pickle module security warning
<https://docs.python.org/3/library/pickle.html#module-pickle>`_ in
the Python standard library docs). The toggle only protects the
:py:meth:`Expr.from_bytes` API surface.

Security
~~~~~~~~

.. warning::

   Reconstructing an expression containing a Python UDF executes
   arbitrary Python code on the receiver — pickle is doing the work
   under the hood and pickle is unsafe on untrusted input (see the
   `pickle module security warning
   <https://docs.python.org/3/library/pickle.html#module-pickle>`_
   in the Python standard library docs). Only accept expressions
   from trusted sources. For untrusted-source workflows, disable
   Python UDF inlining (see above), restrict senders to built-in
   functions and pre-registered Rust-side UDFs, and avoid
   :py:func:`pickle.loads` on externally supplied bytes entirely.

Query-level distribution via datafusion-distributed
---------------------------------------------------

🚧 *Work in progress upstream — not yet usable from datafusion-python.*

`datafusion-distributed <https://github.com/apache/datafusion-distributed>`_
splits a single physical plan into stages and runs each stage on a
different worker node. The driver writes a SQL or DataFrame query
once; the runtime handles partitioning, shuffles, and reassembly.

A datafusion-python integration is in development. This section will
document the integration once it lands. In the meantime, the
expression-level approach above covers most use cases that do not
require automatic plan partitioning.

Query-level distribution via Apache Ballista
--------------------------------------------

🚧 *Work in progress upstream — not yet usable from datafusion-python.*

`Apache Ballista <https://github.com/apache/datafusion-ballista>`_
provides distributed query execution on top of DataFusion with a
scheduler / executor model better suited to long-lived cluster
deployments. A datafusion-python integration is on the roadmap; this
section will fill in once the integration is usable.

See also
--------

* :py:mod:`datafusion.ipc` — worker context API.
* ``examples/ray_pickle_expr.py`` — runnable Ray actor example.
