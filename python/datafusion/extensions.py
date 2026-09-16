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
:py:class:`SessionComponentsExportable` by returning a
:py:class:`SessionExtensionComponents` describing what it contributes, and is
installed with :py:meth:`~datafusion.context.SessionContext.with_extensions`::

    ctx = SessionContext().with_extensions(MyLibraryExtension())

Codecs and planners install in two phases: every
:py:class:`SessionComponentsExportable` runs first and its codecs are installed,
then every :py:class:`SessionPlannerExportable` runs in argument order. A bundle
implements either hook or both. Bundle order is significant for planners, which
nest, and irrelevant for codecs, which accumulate.

Of the four names here, only the two bundle hooks are ``@runtime_checkable``,
because :py:meth:`~datafusion.context.SessionContext.with_extensions`
dispatches on them from Python. :py:class:`QueryPlannerExportable` is a type
hint only, matching the other capsule-getter protocols in
:py:mod:`datafusion.user_defined` and :py:mod:`datafusion.catalog`.

See :ref:`extension_bundles` in the online documentation for why the phases are
split and for a worked implementation.
"""

from __future__ import annotations

from dataclasses import dataclass, field, fields
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from _typeshed import CapsuleType as _PyCapsule

    from datafusion.context import SessionContext
    from datafusion.user_defined import (
        AggregateUDF,
        AggregateUDFExportable,
        LogicalExtensionCodecExportable,
        PhysicalExtensionCodecExportable,
        ScalarUDF,
        ScalarUDFExportable,
        WindowUDF,
        WindowUDFExportable,
    )

__all__ = [
    "PhysicalOptimizerRuleExportable",
    "QueryPlannerExportable",
    "SessionComponentsExportable",
    "SessionExtensionComponents",
    "SessionPlannerExportable",
]


class PhysicalOptimizerRuleExportable(Protocol):
    """Type hint for object that has a __datafusion_physical_optimizer_rule__ capsule.

    The method returns a PyCapsule wrapping an ``FFI_PhysicalOptimizerRule``,
    typically produced by a separate compiled extension. It takes **no
    argument**: a rule needs neither a codec nor a task-context provider, so
    there is nothing session-scoped to hand it.

    Rules accumulate rather than replace. Install one with
    :py:meth:`~datafusion.context.SessionContext.add_physical_optimizer_rule`,
    or declare it on a bundle as
    :py:attr:`SessionExtensionComponents.physical_optimizer_rules` — see
    :ref:`extension_other_hooks`.

    Examples:
        The getter is the whole protocol, and a capsule is what it must return
        — anything else is refused where it is installed rather than at plan
        time:

        >>> from datafusion import SessionContext
        >>> ctx = SessionContext()
        >>> ctx.add_physical_optimizer_rule(object())
        Traceback (most recent call last):
            ...
        RuntimeError: "Invalid datafusion_physical_optimizer_rule...

        Real usage. Skipped here (needs a built extension library); run for
        real by ``test_ffi_physical_optimizer_rule_runs_during_planning`` in
        ``datafusion-ffi-example``.

        >>> from datafusion_ffi_example import MyPhysicalOptimizerRule  # doctest: +SKIP
        >>> ctx.add_physical_optimizer_rule(MyPhysicalOptimizerRule())  # doctest: +SKIP
    """

    def __datafusion_physical_optimizer_rule__(self) -> object: ...  # noqa: D105


class QueryPlannerExportable(Protocol):
    """Type hint for object that has a __datafusion_query_planner__ PyCapsule.

    The method returns a PyCapsule wrapping an ``FFI_QueryPlanner``, typically
    produced by a separate compiled extension. ``session`` is a handle on the
    session the planner is being installed on; take the extension codecs from
    it rather than building your own, and duck-type it — see
    :py:class:`~datafusion.user_defined.LogicalExtensionCodecExportable` for
    ``session``.

    Unlike the two bundle hooks in this module, this protocol is a type hint
    only: it is not ``@runtime_checkable``, so ``isinstance`` against it raises
    ``TypeError``.

    Examples:
        A :py:class:`~datafusion.context.SessionContext` satisfies this
        protocol, which is what lets a foreign planner wrap the one a session
        already has:

        >>> from datafusion import SessionContext
        >>> ctx = SessionContext()
        >>> type(ctx.__datafusion_query_planner__(ctx)).__name__
        'PyCapsule'

        The protocol itself is not runtime-checkable:

        >>> from datafusion import QueryPlannerExportable
        >>> try:
        ...     isinstance(ctx, QueryPlannerExportable)
        ... except TypeError as e:
        ...     print("runtime_checkable" in str(e))
        True
    """

    def __datafusion_query_planner__(self, session: Any) -> object: ...  # noqa: D105


def _not_an_iterable(name: str, value: object, noun: str) -> str:
    """Message for a component field that cannot be read as a collection."""
    return (
        f"{name} must be an iterable of {noun} objects, not a single "
        f"{type(value).__name__}. A lone {noun} is written as a one-element "
        f"tuple — {name}=({noun},) — and the trailing comma is what makes it one."
    )


def _not_a_pair(name: str, item: object, noun: str) -> str:
    """Message for an item of a pair-shaped field that is not ``(name, value)``."""
    return (
        f"{name} must be an iterable of (name, {noun}) pairs, and {item!r} is "
        f"not one. A {noun} is written with its name beside it — "
        f'{name}=(("a_name", {noun}),) — and the inner parentheses are what '
        "make the two into one pair."
    )


def _pair_name_not_a_str(name: str, pair_name: object, noun: str) -> str:
    """Message for a pair whose first element is not the name."""
    return (
        f"The name in a {name} pair must be a str, not a "
        f"{type(pair_name).__name__}. The name comes first: "
        f'{name}=(("a_name", {noun}),).'
    )


def _components(noun: str, *, pairs: bool = False) -> Any:
    """Declare a field holding a tuple of contributed components.

    ``noun`` names what the field holds, for the error a bundle sees when it
    hands over one component instead of a collection of them. Carrying it in
    the field metadata is what lets ``__post_init__`` normalize a field it was
    never told about by name.

    ``pairs`` marks a field whose items are ``(name, value)`` rather than bare
    components, so ``__post_init__`` checks that shape too.
    """
    metadata: dict[str, Any] = {"datafusion_component": noun}
    if pairs:
        metadata["datafusion_component_pairs"] = True
    return field(default=(), metadata=metadata)


def _as_pairs(name: str, components: tuple[Any, ...], noun: str) -> tuple[Any, ...]:
    """Check every item of a pair-shaped field and normalize it to a tuple."""
    pairs = []
    for item in components:
        # A str is iterable and unpacks into two characters, so a two-letter
        # name would otherwise pass as a pair.
        if isinstance(item, (str, bytes)):
            raise TypeError(_not_a_pair(name, item, noun))
        try:
            pair_name, value = item
        except (TypeError, ValueError):
            raise TypeError(_not_a_pair(name, item, noun)) from None
        if not isinstance(pair_name, str):
            raise TypeError(_pair_name_not_a_str(name, pair_name, noun))
        pairs.append((pair_name, value))
    return tuple(pairs)


@dataclass(frozen=True)
class SessionExtensionComponents:
    """Components an extension contributes to a session context.

    Returned by :py:meth:`SessionComponentsExportable.__datafusion_session_components__`
    and consumed by
    :py:meth:`~datafusion.context.SessionContext.with_extensions`. Every
    component must be created against the context passed to that method;
    components bound to a different session hold a task-context provider for
    that other session and cannot be rebound.

    Query planners are not listed here. They install in a second phase so each
    can wrap the one before it — see :py:class:`SessionPlannerExportable`.

    Codecs are held by the returned handle; everything else is registered on
    the session the handle shares. Declaring a component is not the same as
    registering it yourself during the hook: declared components are resolved
    before anything is written, so a bundle that fails leaves nothing behind.
    See :ref:`extension_bundles_transaction`.

    Examples:
        A bundle that contributes no codecs is valid — a planner-only library
        returns this, or omits the hook entirely:

        >>> from datafusion import SessionExtensionComponents
        >>> components = SessionExtensionComponents()
        >>> components.logical_extension_codecs
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

        >>> ctx = SessionContext()
        >>> components = SessionExtensionComponents(
        ...     logical_extension_codecs=(NamedCodec(
        ...         ctx.__datafusion_logical_extension_codec__()
        ...     ),)
        ... )
        >>> components.logical_extension_codecs[0].__datafusion_codec_id__
        'my_library.v1'
        >>> components.physical_extension_codecs
        ()

        Functions are declared the same way, and register under the name the
        function itself reports:

        >>> import pyarrow as pa
        >>> from datafusion import udf
        >>> double = udf(
        ...     lambda arr: pa.array([v.as_py() * 2 for v in arr]),
        ...     [pa.int64()],
        ...     pa.int64(),
        ...     volatility="stable",
        ...     name="double",
        ... )
        >>> components = SessionExtensionComponents(udfs=(double,))
        >>> [fn.name for fn in components.udfs]
        ['double']

        A single component is not an iterable of them, and forgetting the
        trailing comma is the easy way to write one by accident:

        >>> SessionExtensionComponents(logical_extension_codecs=NamedCodec(ctx))
        Traceback (most recent call last):
            ...
        TypeError: logical_extension_codecs must be an iterable of codec objects...

        Tables and table functions carry their name beside the value, so there
        the same mistake is a missing *inner* pair of parentheses:

        >>> SessionExtensionComponents(udtfs=("expand", lambda: None))
        Traceback (most recent call last):
            ...
        TypeError: udtfs must be an iterable of (name, table function) pairs...
    """

    logical_extension_codecs: tuple[LogicalExtensionCodecExportable, ...] = _components(
        "codec"
    )
    """Logical codecs to add to the session's codec chain, in declaration order.

    Objects exposing ``__datafusion_logical_extension_codec__``, never bare
    ``PyCapsule`` objects — a codec's id is read off the object it is handed
    over as. Any iterable is accepted and stored as a tuple. See
    :ref:`extension_bundles_codecs_are_objects`.
    """

    physical_extension_codecs: tuple[PhysicalExtensionCodecExportable, ...] = (
        _components("codec")
    )
    """Physical codecs to add to the session's codec chain, in declaration order.

    As :py:attr:`logical_extension_codecs`, for
    ``__datafusion_physical_extension_codec__``.
    """

    udfs: tuple[ScalarUDF | ScalarUDFExportable, ...] = _components("function")
    """Scalar functions to register on the session.

    Either a :py:class:`~datafusion.user_defined.ScalarUDF` or an object
    exposing ``__datafusion_scalar_udf__``, which is wrapped with
    :py:func:`~datafusion.udf` on the way in. The registered name comes from
    the function itself, not from this field.

    Two extensions in one
    :py:meth:`~datafusion.context.SessionContext.with_extensions` call may not
    declare the same name; shadowing a function the session already has is
    allowed. See :ref:`extension_bundles_collisions`.
    """

    udafs: tuple[AggregateUDF | AggregateUDFExportable, ...] = _components("function")
    """Aggregate functions to register on the session.

    As :py:attr:`udfs`, for ``__datafusion_aggregate_udf__`` and
    :py:func:`~datafusion.udaf`. Names are compared within their own kind, so
    an aggregate may share a name with a scalar function.
    """

    udwfs: tuple[WindowUDF | WindowUDFExportable, ...] = _components("function")
    """Window functions to register on the session.

    As :py:attr:`udfs`, for ``__datafusion_window_udf__`` and
    :py:func:`~datafusion.udwf`.
    """

    udtfs: tuple[tuple[str, Any], ...] = _components("table function", pairs=True)
    """Table functions to register, as ``(name, function)`` pairs.

    Unlike the other three function kinds the name is **not** read off the
    capsule, so it is given here. The value is an object exposing
    ``__datafusion_table_function__``, or a plain Python callable.

    Pass the unwrapped value, not a
    :py:class:`~datafusion.user_defined.TableFunction`. Wrapping calls the
    capsule getter with the session, and the context a bundle is handed has not
    had this call's codecs installed yet — so a wrapper built inside the hook
    would be bound to the wrong chains. The host wraps these against the
    finished context instead. See :ref:`extension_bundles_binding`.

    Collides by name like :py:attr:`udfs`.
    """

    table_providers: tuple[tuple[str, Any], ...] = _components("table", pairs=True)
    """Tables to register, as ``(name, provider)`` pairs.

    Anything :py:meth:`~datafusion.context.SessionContext.register_table`
    accepts: an object exposing ``__datafusion_table_provider__``, a
    :py:class:`~datafusion.catalog.Table`, a
    :py:class:`~datafusion.dataframe.DataFrame`, or a PyArrow dataset. Names may
    be qualified (``"cat.schema.events"``); an unqualified one lands in the
    session's default schema.

    Bound to the finished context for the same reason as :py:attr:`udtfs`.

    A name that is **already registered** is an error, so unlike a function a
    table cannot shadow one. Two declarations collide when they resolve to one
    table, not when they match as strings. Both rules hold wherever the table
    lands, even where
    :py:meth:`~datafusion.context.SessionContext.register_table` would have
    replaced: see :ref:`extension_bundles_collisions`.
    """

    physical_optimizer_rules: tuple[PhysicalOptimizerRuleExportable, ...] = _components(
        "optimizer rule"
    )
    """Physical optimizer rules to install on the session.

    Objects exposing ``__datafusion_physical_optimizer_rule__``. Unlike
    functions these never collide — they accumulate. All the rules in one call
    install together, in declaration order. See :ref:`extension_other_hooks`.
    """

    def __post_init__(self) -> None:
        """Normalize each component field, rejecting what cannot become a tuple."""
        # A bundle that writes `logical_extension_codecs=codec` instead of
        # `(codec,)` is contributing one codec, not an iterable of them.
        # Without this, the mistake surfaces inside `with_extensions` as
        # `'MyCodec' object is not iterable`, which names neither the field nor
        # the hook that built it. Checking here puts the error in the extension
        # library's own frame.
        #
        # Normalizing is worth doing on its own: the declared type is a tuple
        # and the class is frozen, so a list left in place would be a mutable
        # member of an immutable value, and a generator would be exhausted by
        # the first read.
        #
        # Driven off `dataclasses.fields` rather than a written-out list, so a
        # component field added later is normalized without anyone remembering
        # to name it here. `_components` metadata is what marks a field as one
        # of them, leaving room for a future field that is not a collection and
        # must not be turned into a tuple.
        for spec in fields(self):
            noun = spec.metadata.get("datafusion_component")
            if noun is None:
                continue
            name = spec.name
            value = getattr(self, name)
            # A str is iterable, so it would otherwise normalize into a tuple
            # of characters and fail much later as that many bogus components.
            if isinstance(value, (str, bytes)):
                raise TypeError(_not_an_iterable(name, value, noun))
            try:
                components = tuple(value)
            except TypeError:
                raise TypeError(_not_an_iterable(name, value, noun)) from None
            # The pair-shaped fields have a second version of the same mistake:
            # `udtfs=("expand", func)` is one pair with its inner parentheses
            # left off, and normalizes into two components rather than failing.
            # Left unchecked it surfaces from `with_extensions` as
            # `'function' object is not subscriptable`, which is the very thing
            # this method exists to keep out of the caller's lap.
            if spec.metadata.get("datafusion_component_pairs"):
                components = _as_pairs(name, components, noun)
            object.__setattr__(self, name, components)


@runtime_checkable
class SessionComponentsExportable(Protocol):
    """Type hint for extension bundles installable via ``with_extensions``.

    Runtime-checkable, so ``isinstance`` answers whether an object implements
    the protocol. Only the presence of the method is checked, which is the same
    question :py:meth:`~datafusion.context.SessionContext.with_extensions` asks
    before calling it.

    Implementations are reusable configuration objects: they must create fresh
    components on every call using the context supplied by
    :py:meth:`~datafusion.context.SessionContext.with_extensions`, and must not
    retain that context or cache the components they bound to it, since the
    next call may install onto a different session. Declare what you contribute
    rather than registering it on the context you are handed: a registration
    made during the hook is not rolled back if a later extension fails, and it
    binds to the codec chains from before the call. See
    :ref:`extension_bundles_transaction`.

    A bundle that also contributes a query planner implements
    :py:class:`SessionPlannerExportable` alongside this protocol.

    Args:
        ctx: The session the components will run on. Take the task-context
            provider off it. Do **not** read its codec chains expecting to find
            this call's codecs, including your own: this hook runs before
            anything is installed, so ``ctx`` still carries whatever chains the
            receiver had. :py:class:`SessionPlannerExportable` is the hook that
            sees the completed chains — see :ref:`extension_bundles_two_phases`.

    Returns:
        The codecs this bundle contributes.

    Examples:
        >>> from datafusion import (
        ...     SessionExtensionComponents,
        ...     SessionComponentsExportable,
        ... )
        >>> class MyLibraryExtension:
        ...     def __datafusion_session_components__(self, ctx):
        ...         return SessionExtensionComponents()
        >>> isinstance(MyLibraryExtension(), SessionComponentsExportable)
        True
        >>> isinstance(object(), SessionComponentsExportable)
        False
    """

    def __datafusion_session_components__(  # noqa: D105
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
    not left encoding through a chain that a later bundle has grown. See
    :ref:`extension_bundles_two_phases`.

    **Return ``None`` to contribute no planner**, leaving ``fallback`` in
    place. That is the no-op, and it is not the same as returning ``fallback``:
    the capsule the first bundle receives wraps the session's planner for
    export, so handing it back installs that planner as a foreign one and every
    later plan crosses an FFI boundary that was not there before. A bundle that
    decides at runtime it has nothing to contribute returns ``None``.

    Ignoring ``fallback`` and returning a planner that does not delegate to it
    is legal and means "replace" — but it discards every planner listed before
    this one, including any the session already had.

    Args:
        ctx: The session the planner will run on, carrying the final codec
            chains.
        fallback: The planner built so far, as a ``PyCapsule``. For the first
            bundle this is the session's existing planner, which is the
            DataFusion default unless one was installed earlier.

    Returns:
        A planner wrapping ``fallback``, or ``None`` to contribute none.

    Examples:
        A real library returns its own planner wrapping ``fallback``, e.g.
        ``my_library.Planner(fallback=fallback)``. The two degenerate cases are
        worth contrasting, because both plan queries successfully and only one
        of them is the no-op:

        >>> from datafusion import SessionContext, SessionPlannerExportable
        >>> class Contributes:
        ...     def __datafusion_session_planner__(self, ctx, fallback):
        ...         return None  # the no-op: session keeps its own planner
        >>> class Replaces:
        ...     def __datafusion_session_planner__(self, ctx, fallback):
        ...         return fallback  # installs it as a *foreign* planner
        >>> for bundle in (Contributes(), Replaces()):
        ...     ctx = SessionContext().with_extensions(bundle)
        ...     ctx.sql("SELECT 1 AS n").collect()[0].column(0).to_pylist()
        [1]
        [1]

        >>> isinstance(Contributes(), SessionPlannerExportable)
        True
        >>> isinstance(object(), SessionPlannerExportable)
        False
    """

    def __datafusion_session_planner__(  # noqa: D105
        self, ctx: SessionContext, fallback: _PyCapsule
    ) -> QueryPlannerExportable | _PyCapsule | None: ...
