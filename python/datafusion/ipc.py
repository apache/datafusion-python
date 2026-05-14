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

"""Inter-process communication helpers for distributing DataFusion expressions.

This module provides a worker-scoped :class:`SessionContext` slot that
:meth:`Expr.__reduce__` consults when unpickling expressions across process
boundaries. Set the worker context once per worker process (typically from a
``multiprocessing.Pool`` initializer or a Ray actor ``__init__``):

>>> # doctest: +SKIP
>>> from datafusion import SessionContext
>>> from datafusion.ipc import set_worker_ctx
>>>
>>> def init_worker():
...     ctx = SessionContext()
...     # register Rust-backed UDFs / aggregates / window functions here
...     set_worker_ctx(ctx)

Python scalar UDFs do not need pre-registration: their definitions are
cloudpickled into the proto wire format by ``PythonLogicalCodec`` and
reconstructed on the receiver automatically. The worker context is only
needed when the expression references aggregate / window UDFs, table
providers, or Rust-side function registrations the receiver wouldn't
otherwise have.
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion.context import SessionContext


__all__ = [
    "clear_worker_ctx",
    "get_worker_ctx",
    "set_worker_ctx",
]


_local = threading.local()


def set_worker_ctx(ctx: SessionContext) -> None:
    """Register the receiver :class:`SessionContext` for this worker.

    Call once per worker process — typically from a ``Pool`` initializer or a
    Ray actor ``__init__``. Idempotent: overwrites any previous value.

    The worker context is stored in a thread-local slot, so each thread within
    a worker can install its own context independently.
    """
    _local.ctx = ctx


def clear_worker_ctx() -> None:
    """Remove the worker context, restoring fresh-context fallback behavior."""
    if hasattr(_local, "ctx"):
        del _local.ctx


def get_worker_ctx() -> SessionContext | None:
    """Return the worker context if set, else ``None``."""
    return getattr(_local, "ctx", None)


def _resolve_ctx(
    explicit_ctx: SessionContext | None = None,
) -> SessionContext:
    """Resolve a context for Expr reconstruction.

    Priority: explicit argument > worker context > fresh context.
    """
    if explicit_ctx is not None:
        return explicit_ctx
    worker = get_worker_ctx()
    if worker is not None:
        return worker
    from datafusion.context import SessionContext  # noqa: PLC0415

    return SessionContext()
