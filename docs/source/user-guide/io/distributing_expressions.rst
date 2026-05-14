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

Distributing expressions across processes
=========================================

DataFusion expressions (:py:class:`~datafusion.Expr`) can be serialized and
shipped across process boundaries — useful for distributing work over a
``multiprocessing.Pool``, a Ray actor pool, or any framework that supports a
per-worker initialization hook.

Pickle support
--------------

:py:class:`~datafusion.Expr` implements the pickle protocol directly. Call
:py:func:`pickle.dumps` on an expression and ship the bytes; the receiver
calls :py:func:`pickle.loads`. Python *scalar UDFs* are cloudpickled into the
proto wire format by a Rust-side codec (``PythonUDFCodec``), so the blob is
self-contained — the receiver does not need to pre-register the UDF.

.. code-block:: python

    import multiprocessing as mp
    import pickle

    import pyarrow as pa
    from datafusion import SessionContext, col, lit, udf

    def init_worker():
        # Optional: install a worker context for aggregate / window UDFs,
        # table providers, or Rust-side function registrations. Not needed
        # for built-ins or Python scalar UDFs.
        pass

    def evaluate(blob_and_batch):
        blob, batch = blob_and_batch
        expr = pickle.loads(blob)
        ctx = SessionContext()
        df = ctx.from_pydict({"a": batch})
        return df.with_column("result", expr).select("result").to_pydict()["result"]

    if __name__ == "__main__":
        double = udf(
            lambda arr: pa.array([(v.as_py() or 0) * 2 for v in arr]),
            [pa.int64()], pa.int64(), volatility="immutable", name="double",
        )
        blob = pickle.dumps(double(col("a")))

        mp_ctx = mp.get_context("forkserver")
        with mp_ctx.Pool(processes=4) as pool:
            results = pool.map(
                evaluate,
                [(blob, [1, 2, 3]), (blob, [10, 20, 30])],
            )
        print(results)  # [[2, 4, 6], [20, 40, 60]]

Worker-scoped context
---------------------

For references the codec cannot inline — aggregate UDFs, window UDFs, FFI
capsule UDFs, or anything resolved through the
:class:`SessionContext`'s function registry — set a worker-scoped context
once per process using :py:func:`datafusion.ipc.set_worker_ctx`:

.. code-block:: python

    from datafusion import SessionContext
    from datafusion.ipc import set_worker_ctx

    def init_worker():
        ctx = SessionContext()
        ctx.register_udaf(my_aggregate)  # if needed
        set_worker_ctx(ctx)

    with mp.get_context("forkserver").Pool(
        processes=4, initializer=init_worker
    ) as pool:
        ...

Without a worker context, unpickling falls back to a fresh
:py:class:`SessionContext`. Built-in functions resolve; Python scalar UDFs
ride along inside the blob via the codec. References to aggregate / window
UDFs or other registry-only entries raise an informative error if not
registered on the worker.

Python 3.14 default change
--------------------------

Python 3.14 changed the POSIX default start method for
:py:mod:`multiprocessing` from ``fork`` to ``forkserver``. With ``fork``, a
context set in the parent was visible in workers via copy-on-write; with
``forkserver`` and ``spawn`` it is not. The codec + worker-init pattern works
on every start method — prefer it over relying on inherited state.

Trade-offs of inline UDFs
-------------------------

* **Blob size** — cloudpickled callables add bytes per blob. A trivial
  built-in expression is ~20 bytes; an expression referencing a Python scalar
  UDF is hundreds of bytes (the cloudpickled callable + signature). Pre-register
  shared UDFs on workers via :py:func:`~datafusion.ipc.set_worker_ctx` when
  the same UDF is shipped many times and you want to avoid the overhead.
* **Closure capture** — cloudpickle captures closure state. Surprises are
  possible if the UDF closes over large objects, module-level mutable state,
  or non-portable file paths.
* **FFI scalar UDFs cannot be inlined** — PyCapsule-backed UDFs have no
  Python callable to cloudpickle. The codec leaves their ``fun_definition``
  empty; the receiver must have a matching registration.
* **Aggregate and window UDFs cannot be inlined yet** — their Python state
  is held inside opaque factory closures on the Rust side. Pre-register on
  the worker.

Security
--------

.. warning::

   Pickle blobs containing inlined UDFs deserialize via :py:mod:`cloudpickle`,
   which executes arbitrary code on the receiver. Only :py:func:`pickle.loads`
   blobs from trusted sources. For untrusted-source workflows, restrict the
   sender to built-in functions and pre-registered Rust-side UDFs.

See also
--------

* :py:mod:`datafusion.ipc` — module-level API reference.
* ``examples/ray_pickle_expr.py`` — runnable Ray actor example.
