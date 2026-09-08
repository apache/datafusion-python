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
codec is installed before any query planner is bound against them, so no
planner is left carrying a codec chain that has since grown. See the FFI
extensions guide in the contributor documentation for the full rationale.

Codecs and planners install in two phases, because they compose differently. A
session's codec chain holds many codecs and dispatches between them by id, so
codecs merely accumulate and their order does not affect decoding. A session
holds exactly *one* query planner, so planners compose by nesting: each wraps
the one before it. Phase one collects the codecs of every bundle implementing
:py:class:`SessionExtensionExportable` and installs them; phase two runs
:py:class:`SessionPlannerExportable` once for each bundle that implements it,
in argument order, handing each the planner built so far. A bundle implements
either hook or both, and one it does not implement is simply not called.

That split is what lets several libraries that each ship a planner coexist. It
also means bundle order is significant for planners and irrelevant for codecs.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
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
    "SessionPlannerExportable",
]


class QueryPlannerExportable(Protocol):
    """Type hint for object that has a __datafusion_query_planner__ PyCapsule.

    The method returns a PyCapsule wrapping an ``FFI_QueryPlanner``, typically
    produced by a separate compiled extension. ``session`` is a handle on the
    session the planner is being installed on; take the extension codecs from
    it rather than building your own.

    Duck-type that handle rather than checking its type. It is the PyO3
    context from ``datafusion._internal``, not the
    :py:class:`~datafusion.context.SessionContext` wrapper, so it exposes every
    capsule getter and ``__datafusion_codec_id__`` — which is all the protocol
    asks of it — but ``isinstance(session, SessionContext)`` is ``False`` even
    though its ``repr`` reads ``datafusion.SessionContext``. The same is true
    of the codec getters in :py:mod:`datafusion.user_defined`. The two bundle
    hooks are the exception: :py:class:`SessionExtensionExportable` and
    :py:class:`SessionPlannerExportable` are dispatched from Python and receive
    the wrapper.
    """

    def __datafusion_query_planner__(self, session: Any) -> object: ...  # noqa: D105


def _not_a_codec_iterable(field: str, value: object) -> str:
    """Message for a codec field that cannot be read as a collection."""
    return (
        f"{field} must be an iterable of codec objects, not a single "
        f"{type(value).__name__}. A lone codec is written as a one-element "
        f"tuple — {field}=(codec,) — and the trailing comma is what makes it one."
    )


@dataclass(frozen=True)
class SessionExtensionComponents:
    """Components an extension contributes to a session context.

    Returned by :py:meth:`SessionExtensionExportable.__datafusion_session_extension__`
    and consumed by
    :py:meth:`~datafusion.context.SessionContext.with_extensions`. Every
    component must be created against the context passed to that method;
    components bound to a different session hold a task-context provider for
    that other session and cannot be rebound.

    Query planners are not listed here. They install in a second phase so each
    can wrap the one before it — see :py:class:`SessionPlannerExportable`.

    Codecs must be objects exposing the capsule getters, never bare
    ``PyCapsule`` objects: a codec's id is read off the object it is handed
    over as, and a capsule has no type to read. A library holding a raw capsule
    wraps it in an object, which is also what gives the codec an identity of
    its own — one that does not change when the codec is contributed through a
    different extension.

    Examples:
        A bundle that contributes no codecs is valid — a planner-only library
        returns this, or omits the hook entirely:

        >>> from datafusion import SessionExtensionComponents
        >>> components = SessionExtensionComponents()
        >>> components.logical_extension_codecs
        ()
        >>> components.physical_extension_codecs
        ()

        A bundle that contributes one kind of component names it, leaving the
        rest empty. Here the codec is a capsule wrapped in an object that
        declares the id its payloads will carry:

        >>> from datafusion import SessionContext
        >>> class NamedCodec:
        ...     __datafusion_codec_id__ = "my_library.v1"
        ...
        ...     def __init__(self, capsule):
        ...         self._capsule = capsule
        ...
        ...     def __datafusion_logical_extension_codec__(self, session=None):
        ...         return self._capsule

        The context stays in scope for as long as the codec does. An
        ``FFI_LogicalExtensionCodec`` holds its task-context provider *weakly*,
        so a capsule taken off a throwaway ``SessionContext()`` names a session
        that is already gone and fails on first use with ``TaskContextProvider
        went out of scope over FFI boundary``:

        >>> ctx = SessionContext()
        >>> capsule = ctx.__datafusion_logical_extension_codec__()
        >>> components = SessionExtensionComponents(
        ...     logical_extension_codecs=(NamedCodec(capsule),)
        ... )
        >>> components.logical_extension_codecs[0].__datafusion_codec_id__
        'my_library.v1'
        >>> components.physical_extension_codecs
        ()

        Any iterable is accepted and stored as a tuple, so a bundle that builds
        its codecs with a list comprehension does not have to convert:

        >>> components = SessionExtensionComponents(
        ...     logical_extension_codecs=[NamedCodec(capsule)]
        ... )
        >>> type(components.logical_extension_codecs).__name__
        'tuple'

        A single codec is not an iterable of codecs, and forgetting the
        trailing comma is the easy way to write one by accident:

        >>> SessionExtensionComponents(logical_extension_codecs=NamedCodec(capsule))
        Traceback (most recent call last):
            ...
        TypeError: logical_extension_codecs must be an iterable of codec objects...
    """

    logical_extension_codecs: tuple[LogicalExtensionCodecExportable, ...] = ()
    """Logical codecs to add to the session's codec chain, in declaration order."""

    physical_extension_codecs: tuple[PhysicalExtensionCodecExportable, ...] = ()
    """Physical codecs to add to the session's codec chain, in declaration order."""

    def __post_init__(self) -> None:
        """Normalize each field to a tuple, rejecting what cannot become one.

        A bundle that writes ``logical_extension_codecs=codec`` instead of
        ``(codec,)`` is contributing one codec, not an iterable of them.
        Without this, the mistake surfaces inside
        :py:meth:`~datafusion.context.SessionContext.with_extensions` as
        ``'MyCodec' object is not iterable``, which names neither the field
        nor the hook that built it. Checking here puts the error in the
        extension library's own frame.

        Normalizing is worth doing on its own: the declared type is a tuple
        and the class is frozen, so a list left in place would be a mutable
        member of an immutable value, and a generator would be exhausted by
        the first read.

        Driven off :py:func:`dataclasses.fields` rather than a written-out
        list, so a codec field added later is normalized without anyone
        remembering to name it here. The ``_codecs`` suffix is what marks a
        field as one of them, leaving room for a future field that is not a
        codec collection and must not be turned into a tuple.
        """
        for field in fields(self):
            name = field.name
            if not name.endswith("_codecs"):
                continue
            value = getattr(self, name)
            # A str is iterable, so it would otherwise normalize into a tuple
            # of characters and fail much later as that many bogus codecs.
            if isinstance(value, (str, bytes)):
                raise TypeError(_not_a_codec_iterable(name, value))
            try:
                codecs = tuple(value)
            except TypeError:
                raise TypeError(_not_a_codec_iterable(name, value)) from None
            object.__setattr__(self, name, codecs)


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

    ``ctx`` is the right session but not yet the final codec chains: this hook
    runs before anything is installed, so ``ctx`` still carries whatever chains
    the receiver had. Take the task-context provider off it — that is bound to
    the session and is what the components need — but do not read its codec
    chains expecting to find this call's codecs, including your own.
    :py:class:`SessionPlannerExportable` is the hook that sees the completed
    chains, which is why a planner that wraps the host's codecs builds them
    there rather than here.

    A bundle that also contributes a query planner implements
    :py:class:`SessionPlannerExportable` alongside this protocol. Planners are
    installed in a second phase, so they are not part of the components
    returned here.

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


@runtime_checkable
class SessionPlannerExportable(Protocol):
    """Type hint for extension bundles that contribute a query planner.

    A session holds exactly one query planner, so planners compose by nesting
    rather than by chaining: each wraps the one before it and delegates to it
    for the work it does not handle.
    :py:meth:`~datafusion.context.SessionContext.with_extensions` runs this
    hook once per bundle that implements it, **in argument order**, handing
    each the planner built so far. Returning a planner that wraps ``fallback``
    puts this bundle *outside* the previous one, so the last bundle listed ends
    up outermost and is consulted first.

    The hook runs after every codec from every bundle is installed, and ``ctx``
    is the context carrying those final chains. That ordering is the point: a
    planner captured here sees the complete codec set, so a nested planner is
    not left encoding through a chain that a later bundle has grown.

    Return ``None`` to contribute no planner and leave ``fallback`` in place.
    That is the no-op, and it is not the same as returning ``fallback``: the
    capsule the first bundle receives wraps the session's planner for export, so
    handing it back installs it as a foreign planner and every later plan crosses
    an FFI boundary that was not there before. A bundle with nothing to
    contribute returns ``None``.

    Ignoring ``fallback`` and returning a planner that does not delegate to it
    is legal and means "replace" — but it discards every planner listed before
    this one, including any the session already had.

    Args:
        ctx: The session the planner will run on, carrying the final codec
            chains.
        fallback: The planner built so far, as a ``PyCapsule``. For the first
            bundle this is the session's existing planner, which is the
            DataFusion default unless one was installed earlier.

    Examples:
        >>> from datafusion import SessionPlannerExportable
        >>> class MyEngineExtension:
        ...     def __datafusion_session_planner__(self, ctx, fallback):
        ...         # A real library returns its own planner wrapping
        ...         # `fallback`, e.g. ``my_library.Planner(fallback=fallback)``.
        ...         # Handing it straight back is the degenerate wrap: legal,
        ...         # but it still installs `fallback` as a foreign planner.
        ...         # Return None instead to contribute nothing.
        ...         return fallback
        >>> isinstance(MyEngineExtension(), SessionPlannerExportable)
        True
        >>> isinstance(object(), SessionPlannerExportable)
        False
    """

    def __datafusion_session_planner__(  # noqa: D105
        self, ctx: SessionContext, fallback: _PyCapsule
    ) -> QueryPlannerExportable | _PyCapsule | None: ...
