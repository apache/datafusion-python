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

"""Protocols and value types for installing extensions on a session context.

An *extension* is a reusable configuration object — typically shipped by a
separate compiled library — that contributes components to a
:py:class:`~datafusion.context.SessionContext`. It implements
:py:class:`SessionExtensionExportable` by returning a
:py:class:`SessionExtensionComponents` describing what it contributes, and is
installed with :py:meth:`~datafusion.context.SessionContext.with_extensions`::

    ctx = SessionContext().with_extensions(MyLibraryExtension())

Installing through ``with_extensions`` rather than by chaining the individual
``with_*`` methods matters for components that hold a task-context provider:
the extension is handed the destination context so every component binds to
the session that is actually returned. See the FFI extensions guide in the
contributor documentation for the full rationale.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from _typeshed import CapsuleType as _PyCapsule

    from datafusion.context import SessionContext
    from datafusion.user_defined import (
        LogicalExtensionCodecExportable,
        PhysicalExtensionCodecExportable,
    )

__all__ = [
    "QueryPlannerExportable",
    "SessionExtensionComponents",
    "SessionExtensionExportable",
]


class QueryPlannerExportable(Protocol):
    """Type hint for object that has a __datafusion_query_planner__ PyCapsule.

    The method returns a PyCapsule wrapping an ``FFI_QueryPlanner``, typically
    produced by a separate compiled extension. ``session`` is the
    :py:class:`~datafusion.context.SessionContext` the planner is being
    installed on; take the extension codecs from it rather than building your
    own.
    """

    def __datafusion_query_planner__(self, session: Any) -> object: ...  # noqa: D105


@dataclass(frozen=True)
class SessionExtensionComponents:
    """Components an extension contributes to a session context.

    Returned by :py:meth:`SessionExtensionExportable.__datafusion_session_extension__`
    and consumed by
    :py:meth:`~datafusion.context.SessionContext.with_extensions`. Every
    component must be created against the context passed to that method;
    components bound to any other context hold a task-context provider for the
    wrong session and cannot be rebound.
    """

    logical_extension_codecs: tuple[
        LogicalExtensionCodecExportable | _PyCapsule, ...
    ] = ()
    """Logical codecs to add to the session's codec chain, in declaration order."""

    physical_extension_codecs: tuple[
        PhysicalExtensionCodecExportable | _PyCapsule, ...
    ] = ()
    """Physical codecs to add to the session's codec chain, in declaration order."""

    query_planner: QueryPlannerExportable | _PyCapsule | None = None
    """Optional query planner.

    At most one extension per
    :py:meth:`~datafusion.context.SessionContext.with_extensions` call may
    supply one.
    """


class SessionExtensionExportable(Protocol):
    """Type hint for extension bundles installable via ``with_extensions``.

    Implementations are reusable configuration objects: they must not retain a
    :py:class:`~datafusion.context.SessionContext` and must create fresh
    components on every call using the context supplied by
    :py:meth:`~datafusion.context.SessionContext.with_extensions`. They should
    also avoid mutating global state during binding, since a failed
    installation discards the destination context.
    """

    def __datafusion_session_extension__(  # noqa: D105
        self, ctx: SessionContext
    ) -> SessionExtensionComponents: ...
