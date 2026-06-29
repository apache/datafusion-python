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

"""Bindings for datafusion-distributed workers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from datafusion.context import SessionContext

from ._internal import Worker as WorkerInternal
from ._internal import WorkerQueryContext as WorkerQueryContextInternal

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable


class WorkerQueryContext:
    """Context passed to :class:`WorkerSessionBuilder` callbacks."""

    def __init__(self, context: WorkerQueryContextInternal) -> None:
        """Wrap the internal worker query context.

        This is created by DataFusion when a worker receives a query; user code
        normally only sees it as the argument to ``build_session_state``.
        """
        self._raw = context

    @property
    def headers(self) -> dict[str, str]:
        """Return incoming gRPC request headers for the worker query."""
        return dict(self._raw.headers)

    def session_context(self) -> SessionContext:
        """Build a public :class:`SessionContext` from the upstream worker builder.

        The upstream builder is consumed, so this method can only be called once
        for each ``WorkerQueryContext``.

        Examples:
            >>> from datafusion import SessionContext
            >>> from datafusion.distributed import WorkerQueryContext
            >>> def build_session_state(
            ...     context: WorkerQueryContext,
            ... ) -> SessionContext:
            ...     return context.session_context()
        """
        wrapper = SessionContext.__new__(SessionContext)
        wrapper.ctx = self._raw.session_context()
        return wrapper


def _wrap_worker_session_builder(
    callback: Callable[[WorkerQueryContext], SessionContext],
) -> Callable[[WorkerQueryContextInternal], SessionContext]:
    def adapter(context: WorkerQueryContextInternal) -> SessionContext:
        wrapped_context = WorkerQueryContext(context)
        return callback(wrapped_context)

    return adapter


class Worker:
    """A datafusion-distributed worker service."""

    def __init__(
        self,
        session_builder: Callable[[WorkerQueryContext], SessionContext] | None = None,
    ) -> None:
        """Create a worker.

        Args:
            session_builder: Optional custom session builder callback or object.

        Examples:
            >>> from datafusion import Worker
            >>> worker = Worker()
            >>> isinstance(worker, Worker)
            True
        """
        if session_builder is None:
            self._raw = WorkerInternal()
        else:
            if not callable(session_builder):
                msg = "Expected session_builder to be callable"
                raise TypeError(msg)
            adapter = _wrap_worker_session_builder(session_builder)
            self._raw = WorkerInternal.from_session_builder(adapter)

    @classmethod
    def from_session_builder(
        cls,
        session_builder: Callable[[WorkerQueryContext], SessionContext],
    ) -> Worker:
        """Create a worker with a custom session builder.

        Examples:
            >>> from datafusion import SessionContext, Worker
            >>> from datafusion.distributed import WorkerQueryContext
            >>> def build_session_state(
            ...     context: WorkerQueryContext,
            ... ) -> SessionContext:
            ...     return context.session_context()
            >>> worker = Worker.from_session_builder(build_session_state)
            >>> isinstance(worker, Worker)
            True
        """
        return cls(session_builder=session_builder)

    def with_version(self, version: str) -> Worker:
        """Set the worker version string returned by the worker service.

        Examples:
            >>> from datafusion import Worker
            >>> Worker().with_version("local").__class__ is Worker
            True
        """
        self._raw = self._raw.with_version(version)
        return self

    def with_max_message_size(self, size: int) -> Worker:
        """Set the maximum FlightData chunk size for this worker.

        Examples:
            >>> from datafusion import Worker
            >>> Worker().with_max_message_size(1024).__class__ is Worker
            True
        """
        self._raw = self._raw.with_max_message_size(size)
        return self

    def serve(self, host: str = "127.0.0.1", port: int = 50051) -> None:
        """Run the worker service on a tonic server until it is stopped.

        Examples:
            >>> from datafusion import Worker
            >>> Worker().serve("127.0.0.1", 50051)  # doctest: +SKIP
        """
        self._raw.serve(host, port)

    def serve_async(
        self, host: str = "127.0.0.1", port: int = 50051
    ) -> Awaitable[None]:
        """Return an awaitable that serves this worker.

        Examples:
            >>> import asyncio
            >>> from datafusion import Worker
            >>> async def main() -> None:
            ...     task = asyncio.create_task(Worker().serve_async())
            ...     task.cancel()
            >>> asyncio.run(main())  # doctest: +SKIP
        """
        return self._raw.serve_async(host, port)


__all__ = [
    "Worker",
    "WorkerQueryContext",
]
