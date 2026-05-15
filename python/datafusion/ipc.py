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

When a :class:`Expr` is shipped to a worker process (e.g. through
:func:`multiprocessing.Pool` or a Ray actor), the worker reconstructs the
expression against a :class:`SessionContext`. If the expression references
UDFs imported via the FFI capsule protocol — or any UDF the worker would
otherwise resolve from its registered functions rather than from inside
the shipped expression — install a configured :class:`SessionContext`
once per worker:

>>> # doctest: +SKIP
>>> from datafusion import SessionContext
>>> from datafusion.ipc import set_worker_ctx
>>>
>>> def init_worker():
...     ctx = SessionContext()
...     ctx.register_udaf(my_ffi_aggregate)
...     set_worker_ctx(ctx)

Built-in functions and Python UDFs (scalar, aggregate, window) travel
inside the shipped expression itself and do not need pre-registration
on the worker.

On the driver side, call :func:`set_sender_ctx` to control how
:func:`pickle.dumps` encodes expressions — for example, to apply
:meth:`SessionContext.with_python_udf_inlining` to every pickled
expression on this thread:

>>> # doctest: +SKIP
>>> from datafusion import SessionContext
>>> from datafusion.ipc import set_sender_ctx
>>>
>>> driver_ctx = SessionContext().with_python_udf_inlining(False)
>>> set_sender_ctx(driver_ctx)
>>> pickle.dumps(expr)  # encoded with inlining disabled

Without a sender context the default codec is used (Python UDF
inlining on). The sender context only affects pickle / ``to_bytes``
encoding; explicit ``expr.to_bytes(ctx)`` calls still use the supplied
``ctx``.

See :doc:`/user-guide/io/distributing_work` for the full pattern.
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
    """Install this worker's :class:`SessionContext` for shipped expressions.

    Call once per worker — typically from a ``multiprocessing.Pool``
    initializer or a Ray actor ``__init__``. Idempotent: overwrites any
    previous value. Stored in a thread-local slot, so each thread within a
    worker may install its own context independently.
    """
    _local.ctx = ctx


def clear_worker_ctx() -> None:
    """Remove this worker's installed :class:`SessionContext`.

    After clearing, expressions reconstructed in this worker fall back to
    the global :class:`SessionContext` — adequate for built-ins and Python
    UDFs (scalar, aggregate, window), but anything imported via the FFI
    capsule protocol must be registered on the global context to resolve.
    """
    if hasattr(_local, "ctx"):
        del _local.ctx


def get_worker_ctx() -> SessionContext | None:
    """Return this worker's installed :class:`SessionContext`, or ``None``."""
    return getattr(_local, "ctx", None)


def set_sender_ctx(ctx: SessionContext) -> None:
    """Install this driver's :class:`SessionContext` for outbound pickles.

    Controls how :func:`pickle.dumps` encodes :class:`Expr` instances on
    this thread. The most useful application is propagating a session
    configured with
    :meth:`SessionContext.with_python_udf_inlining` so the toggle takes
    effect through pickle (which otherwise calls
    :meth:`Expr.to_bytes` with no context and uses the default codec).

    Idempotent: overwrites any previous value. Stored in a thread-local
    slot, so worker threads on the driver may install their own contexts.
    Does not affect :meth:`Expr.to_bytes` calls that pass an explicit
    ``ctx`` — those continue to use the supplied context.
    """
    _local.sender_ctx = ctx


def clear_sender_ctx() -> None:
    """Remove this driver's installed sender :class:`SessionContext`.

    After clearing, pickled expressions fall back to the default codec
    (Python UDF inlining on).
    """
    if hasattr(_local, "sender_ctx"):
        del _local.sender_ctx


def get_sender_ctx() -> SessionContext | None:
    """Return this driver's installed sender :class:`SessionContext`, or ``None``."""
    return getattr(_local, "sender_ctx", None)


def _resolve_ctx(
    explicit_ctx: SessionContext | None = None,
) -> SessionContext:
    """Resolve a context for Expr reconstruction.

    Priority: explicit argument > worker context > global context.
    Falling back to the global :class:`SessionContext` (instead of a
    freshly constructed one) preserves any registrations the user has
    installed on it.
    """
    if explicit_ctx is not None:
        return explicit_ctx
    worker = get_worker_ctx()
    if worker is not None:
        return worker
    from datafusion.context import SessionContext  # noqa: PLC0415

    return SessionContext.global_ctx()
