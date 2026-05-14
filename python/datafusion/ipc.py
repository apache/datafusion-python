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

"""Worker-side setup for distributing DataFusion expressions.

When a :class:`Expr` is shipped to a worker process (e.g. through
:func:`multiprocessing.Pool` or a Ray actor), the worker reconstructs the
expression against a :class:`SessionContext`. If the expression references
aggregate UDFs, window UDFs, table providers, or UDFs imported via the FFI
capsule protocol — anything the worker would otherwise resolve from its
registered functions — install a configured :class:`SessionContext` once
per worker:

>>> # doctest: +SKIP
>>> from datafusion import SessionContext
>>> from datafusion.ipc import set_worker_ctx
>>>
>>> def init_worker():
...     ctx = SessionContext()
...     ctx.register_udaf(my_aggregate)
...     set_worker_ctx(ctx)

Built-in functions and Python scalar UDFs travel inside the shipped
expression itself and do not need pre-registration on the worker.

See :doc:`/user-guide/io/distributing_expressions` for the full pattern.
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
    """Install this worker's :class:`SessionContext` for shipped expressions.

    Call once per worker — typically from a ``multiprocessing.Pool``
    initializer or a Ray actor ``__init__``. Idempotent: overwrites any
    previous value. Stored in a thread-local slot, so each thread within a
    worker may install its own context independently.
    """
    _local.ctx = ctx


def clear_worker_ctx() -> None:
    """Remove this worker's installed :class:`SessionContext`.

    After clearing, expressions reconstructed in this worker fall back to a
    fresh empty :class:`SessionContext` — adequate for built-ins and Python
    scalar UDFs, but anything that travels by name only (aggregate UDFs,
    window UDFs, FFI UDFs) will fail to resolve.
    """
    if hasattr(_local, "ctx"):
        del _local.ctx


def get_worker_ctx() -> SessionContext | None:
    """Return this worker's installed :class:`SessionContext`, or ``None``."""
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
