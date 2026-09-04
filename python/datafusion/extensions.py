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
the extension is handed the session its components will run on, and every
codec is installed before the query planner is bound against them, so no
planner is left carrying a codec chain that has since grown. See the FFI
extensions guide in the contributor documentation for the full rationale.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

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
    components bound to a different session hold a task-context provider for
    that other session and cannot be rebound.

    Codecs may be handed over either as objects exposing the capsule getters or
    as bare ``PyCapsule`` objects. A bare capsule carries no class to take a
    codec id from, so it is named after the extension that contributed it. An
    extension contributing two bare capsules of the same kind therefore has to
    name at least one of them itself, by wrapping it in an object declaring
    ``__datafusion_codec_id__``.

    Examples:
        A bundle that contributes nothing is valid, and is what the defaults
        describe:

        >>> from datafusion import SessionExtensionComponents
        >>> components = SessionExtensionComponents()
        >>> components.logical_extension_codecs
        ()
        >>> components.query_planner is None
        True

        A bundle that contributes one kind of component names it, leaving
        the rest empty:

        >>> components = SessionExtensionComponents(
        ...     query_planner=my_library.make_planner(ctx)
        ... )  # doctest: +SKIP
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


@runtime_checkable
class SessionExtensionExportable(Protocol):
    """Type hint for extension bundles installable via ``with_extensions``.

    Runtime-checkable, so ``isinstance`` answers whether an object implements
    the protocol. Only the presence of the method is checked, which is the same
    question :py:meth:`~datafusion.context.SessionContext.with_extensions` asks
    before calling it.

    Implementations are reusable configuration objects: they must create fresh
    components on every call using the context supplied by
    :py:meth:`~datafusion.context.SessionContext.with_extensions`, and must not
    retain that context or cache the components they bound to it, since the
    next call may install onto a different session. They should also avoid
    mutating the context they are handed — a registration made during binding
    is not rolled back if a later extension fails.

    Examples:
        >>> from datafusion import (
        ...     SessionExtensionComponents,
        ...     SessionExtensionExportable,
        ... )
        >>> class MyLibraryExtension:
        ...     def __datafusion_session_extension__(self, ctx):
        ...         return SessionExtensionComponents()
        >>> isinstance(MyLibraryExtension(), SessionExtensionExportable)
        True
        >>> isinstance(object(), SessionExtensionExportable)
        False
    """

    def __datafusion_session_extension__(  # noqa: D105
        self, ctx: SessionContext
    ) -> SessionExtensionComponents: ...
