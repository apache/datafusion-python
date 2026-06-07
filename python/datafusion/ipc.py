# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Driver- and worker-side setup for distributing DataFusion expressions.

When a [`Expr`][datafusion.expr.Expr] is shipped to a worker process (e.g. through
[`Pool`][multiprocessing.Pool] or a Ray actor), the worker reconstructs the
expression against a `SessionContext`. If the expression references
UDFs imported via the FFI capsule protocol — or any UDF the worker would
otherwise resolve from its registered functions rather than from inside
the shipped expression — install a configured `SessionContext`
once per worker:

.. code-block:: python

    from datafusion import SessionContext
    from datafusion.ipc import set_worker_ctx

    def init_worker():
        ctx = SessionContext()
        ctx.register_udaf(my_ffi_aggregate)
        set_worker_ctx(ctx)

Built-in functions and Python UDFs (scalar, aggregate, window) travel
inside the shipped expression itself and do not need pre-registration
on the worker.

.. note:: Serialization model

   Expressions containing Python UDFs (scalar, aggregate, window) are
   serialized using [`cloudpickle`][cloudpickle]. The callable itself travels
   **by value** (bytecode and closure cells inlined), but any names the
   callable resolves via ``import`` are captured **by reference** and
   must be importable on the receiving worker.

   The serialized payload is stamped with the sender's Python
   ``(major, minor)`` version. Loading on a different minor version
   raises [`ValueError`][ValueError] with an actionable message — cloudpickle
   payloads are not portable across Python minor versions. See
   [`to_bytes`][datafusion.Expr.to_bytes] for examples of what travels by
   value vs. by reference.

On the driver side, call [`set_sender_ctx`][set_sender_ctx] to control how
[`dumps`][pickle.dumps] encodes expressions — for example, to apply
`with_python_udf_inlining` to every pickled
expression on this thread:

>>> import pickle
>>> from datafusion import SessionContext, col, lit
>>> from datafusion.ipc import clear_sender_ctx, set_sender_ctx
>>> driver_ctx = SessionContext().with_python_udf_inlining(enabled=False)
>>> set_sender_ctx(driver_ctx)
>>> try:
...     blob = pickle.dumps(col("a") + lit(1))
... finally:
...     clear_sender_ctx()
>>> isinstance(blob, bytes)
True

Without a sender context the default codec is used (Python UDF
inlining on). The sender context only affects pickle / ``to_bytes``
encoding; explicit ``expr.to_bytes(ctx)`` calls still use the supplied
``ctx``.

The thread-local sender context holds a strong reference to the
installed `SessionContext` until [`clear_sender_ctx`][clear_sender_ctx] is
called or the thread exits. Long-running driver threads that install a sender
context once and never clear it will retain that session for the
lifetime of the thread; pair [`set_sender_ctx`][set_sender_ctx] with
[`clear_sender_ctx`][clear_sender_ctx] (e.g. in a ``try``/``finally``) when the
sender context is only needed for a bounded scope.
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion.context import SessionContext


__all__ = [
    "clear_sender_ctx",
    "clear_worker_ctx",
    "get_sender_ctx",
    "get_worker_ctx",
    "set_sender_ctx",
    "set_worker_ctx",
]


_local = threading.local()


def set_worker_ctx(ctx: SessionContext) -> None:
    """Install this worker's `SessionContext` for shipped expressions.

    Call once per worker — typically from a ``multiprocessing.Pool``
    initializer or a Ray actor ``__init__``. Idempotent: overwrites any
    previous value. Stored in a thread-local slot, so each thread within a
    worker may install its own context independently.

    Examples:
        >>> from datafusion import SessionContext
        >>> from datafusion.ipc import set_worker_ctx, get_worker_ctx, clear_worker_ctx
        >>> set_worker_ctx(SessionContext())
        >>> get_worker_ctx() is not None
        True
        >>> clear_worker_ctx()
    """
    _local.ctx = ctx


def clear_worker_ctx() -> None:
    """Remove this worker's installed `SessionContext`.

    After clearing, expressions reconstructed in this worker fall back to
    the global `SessionContext` — adequate for built-ins and Python
    UDFs (scalar, aggregate, window), but anything imported via the FFI
    capsule protocol must be registered on the global context to resolve.

    Examples:
        >>> from datafusion import SessionContext
        >>> from datafusion.ipc import set_worker_ctx, clear_worker_ctx, get_worker_ctx
        >>> set_worker_ctx(SessionContext())
        >>> clear_worker_ctx()
        >>> get_worker_ctx() is None
        True
    """
    if hasattr(_local, "ctx"):
        del _local.ctx


def get_worker_ctx() -> SessionContext | None:
    """Return this worker's installed `SessionContext`, or ``None``.

    Examples:
        >>> from datafusion.ipc import get_worker_ctx, clear_worker_ctx
        >>> clear_worker_ctx()
        >>> get_worker_ctx() is None
        True
    """
    return getattr(_local, "ctx", None)


def set_sender_ctx(ctx: SessionContext) -> None:
    """Install this driver's `SessionContext` for outbound pickles.

    Controls how `dumps` encodes [`Expr`][datafusion.expr.Expr] instances on
    this thread. The most useful application is propagating a session
    configured with
    `with_python_udf_inlining` so the toggle takes
    effect through pickle (which otherwise calls
    `to_bytes` with no context and uses the default codec).

    Idempotent: overwrites any previous value. Stored in a thread-local
    slot, so worker threads on the driver may install their own contexts.
    Does not affect `to_bytes` calls that pass an explicit
    ``ctx`` — those continue to use the supplied context.

    Examples:
        >>> from datafusion import SessionContext
        >>> from datafusion.ipc import set_sender_ctx, get_sender_ctx
        >>> driver = SessionContext().with_python_udf_inlining(enabled=False)
        >>> set_sender_ctx(driver)
        >>> get_sender_ctx() is driver
        True
    """
    _local.sender_ctx = ctx


def clear_sender_ctx() -> None:
    """Remove this driver's installed sender `SessionContext`.

    After clearing, pickled expressions fall back to the default codec
    (Python UDF inlining on).

    Examples:
        >>> from datafusion import SessionContext
        >>> from datafusion.ipc import (
        ...     set_sender_ctx, clear_sender_ctx, get_sender_ctx,
        ... )
        >>> set_sender_ctx(SessionContext())
        >>> clear_sender_ctx()
        >>> get_sender_ctx() is None
        True
    """
    if hasattr(_local, "sender_ctx"):
        del _local.sender_ctx


def get_sender_ctx() -> SessionContext | None:
    """Return this driver's installed sender `SessionContext`, or ``None``.

    Examples:
        >>> from datafusion.ipc import get_sender_ctx, clear_sender_ctx
        >>> clear_sender_ctx()
        >>> get_sender_ctx() is None
        True
    """
    return getattr(_local, "sender_ctx", None)


def _resolve_ctx(
    explicit_ctx: SessionContext | None = None,
) -> SessionContext:
    """Resolve a context for Expr reconstruction.

    Priority: explicit argument > worker context > global context.
    Falling back to the global `SessionContext` (instead of a
    freshly constructed one) preserves any registrations the user has
    installed on it.

    Examples:
        >>> from datafusion import SessionContext
        >>> from datafusion.ipc import _resolve_ctx, clear_worker_ctx
        >>> clear_worker_ctx()
        >>> isinstance(_resolve_ctx(), SessionContext)
        True
        >>> ctx = SessionContext()
        >>> _resolve_ctx(ctx) is ctx
        True
    """
    if explicit_ctx is not None:
        return explicit_ctx
    worker = get_worker_ctx()
    if worker is not None:
        return worker
    # Lazy import: `datafusion/__init__.py` imports `datafusion.ipc`
    # before `datafusion.context`, so a module-top import would force
    # `datafusion.context` to load mid-init of `datafusion.ipc`. The
    # cycle is benign today (context.py only pulls expr.py at module
    # scope, neither pulls ipc.py back), but a single new import in
    # context.py's transitive deps could turn it into a real cycle.
    # Deferring keeps `datafusion.ipc` import-order-independent.
    from datafusion.context import SessionContext  # noqa: PLC0415

    return SessionContext.global_ctx()
