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

from __future__ import annotations

import gc

import pyarrow as pa
import pytest
from datafusion import Expr, SessionConfig, SessionContext, col, udf
from datafusion_ffi_example import (
    IsNullUDF,
    MyCatalogProvider,
    MyLogicalExtensionCodec,
    MyPhysicalExtensionCodec,
    MyPhysicalOptimizerRule,
    MyTableProvider,
)
from datafusion_ffi_query_planner_example import MyPlannerConfig, MyQueryPlanner


def configured_context(max_rows: int):
    config = SessionConfig().with_extension(MyPlannerConfig(max_rows=max_rows))
    logical_codec = MyLogicalExtensionCodec()
    physical_codec = MyPhysicalExtensionCodec()
    ctx = SessionContext(config)
    ctx = ctx.with_logical_extension_codec(logical_codec)
    ctx = ctx.with_physical_extension_codec(physical_codec)
    ctx.register_table("numbers", MyTableProvider(1, 6, 1))
    ctx.register_udf(udf(IsNullUDF()))
    return ctx, logical_codec, physical_codec


HOST_ONLY_UDF = "my_custom_is_null"
"""Scalar function registered on the host session and nowhere else."""

UNREGISTERED_UDF = "not_registered_anywhere"


def probe_context(
    *,
    logical_requires: str | None = None,
    physical_requires: str | None = None,
    max_rows: int = 3,
):
    """Three-library context whose codecs read the task context they are given.

    ``require_udf_on_decode`` makes each codec resolve a scalar function from
    the ``TaskContext`` handed to its FFI decode callback, which is otherwise
    unobservable: the example codecs restore objects from a token registry and
    never look at the registry they are passed.
    """
    config = SessionConfig().with_extension(MyPlannerConfig(max_rows=max_rows))
    logical_codec = MyLogicalExtensionCodec(require_udf_on_decode=logical_requires)
    physical_codec = MyPhysicalExtensionCodec(require_udf_on_decode=physical_requires)
    ctx = SessionContext(config)
    ctx = ctx.with_logical_extension_codec(logical_codec)
    ctx = ctx.with_physical_extension_codec(physical_codec)
    ctx.register_table("numbers", MyTableProvider(1, 6, 1))
    ctx.register_udf(udf(IsNullUDF()))
    ctx.set_query_planner(MyQueryPlanner())
    return ctx, logical_codec, physical_codec


def test_logical_codec_resolves_a_host_registered_udf():
    """``try_decode_table_provider`` sees the host session's registry.

    The codec takes its task context provider from the session it is installed
    on, so a function the host registered is resolvable inside a decode
    callback running in the other library.
    """
    ctx, logical_codec, _physical_codec = probe_context(logical_requires=HOST_ONLY_UDF)

    batches = ctx.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()
    assert batches[0].column(0).to_pylist() == [0, 1, 2]
    assert logical_codec.table_provider_decode_calls() > 0
    assert logical_codec.task_context_udf_resolutions() > 0


def test_physical_codec_resolves_a_host_registered_udf():
    """``try_decode`` sees the host session's registry, as above."""
    ctx, _logical_codec, physical_codec = probe_context(physical_requires=HOST_ONLY_UDF)

    batches = ctx.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()
    assert batches[0].column(0).to_pylist() == [0, 1, 2]
    assert physical_codec.execution_plan_decode_calls() > 0
    assert physical_codec.task_context_udf_resolutions() > 0


def test_codec_still_reports_a_name_registered_nowhere():
    """Negative control: resolution really is a lookup, not an unconditional pass."""
    ctx, _logical_codec, _physical_codec = probe_context(
        logical_requires=UNREGISTERED_UDF
    )

    with pytest.raises(
        Exception, match=rf"could not resolve scalar function '{UNREGISTERED_UDF}'"
    ):
        ctx.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()


def test_codec_sees_a_udf_registered_after_it_was_installed():
    """The provider is a live handle to the session, not a snapshot of it."""
    config = SessionConfig().with_extension(MyPlannerConfig(max_rows=3))
    logical_codec = MyLogicalExtensionCodec(require_udf_on_decode=HOST_ONLY_UDF)
    ctx = SessionContext(config)
    ctx = ctx.with_logical_extension_codec(logical_codec)
    ctx = ctx.with_physical_extension_codec(MyPhysicalExtensionCodec())
    ctx.register_table("numbers", MyTableProvider(1, 6, 1))
    # Registered after the codec was installed and bound to this session.
    ctx.register_udf(udf(IsNullUDF()))
    ctx.set_query_planner(MyQueryPlanner())

    batches = ctx.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()
    assert batches[0].column(0).to_pylist() == [0, 1, 2]
    assert logical_codec.task_context_udf_resolutions() > 0


def test_codec_sees_a_udf_registered_after_the_planner():
    """Installing a planner does not detach the codec from the session.

    The codec is bound to the session before the planner is installed and the
    function is registered afterwards. Installing writes through
    ``state_ref()`` rather than deriving a new ``SessionContext``, so the
    codec's task context provider still points at the one live session and
    sees the later registration.
    """
    config = SessionConfig().with_extension(MyPlannerConfig(max_rows=3))
    logical_codec = MyLogicalExtensionCodec(require_udf_on_decode=HOST_ONLY_UDF)
    ctx = SessionContext(config)
    ctx = ctx.with_logical_extension_codec(logical_codec)
    ctx = ctx.with_physical_extension_codec(MyPhysicalExtensionCodec())
    ctx.register_table("numbers", MyTableProvider(1, 6, 1))
    ctx.set_query_planner(MyQueryPlanner())
    # Registered after the planner, on the same session the codec is bound to.
    ctx.register_udf(udf(IsNullUDF()))

    batches = ctx.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()
    assert batches[0].column(0).to_pylist() == [0, 1, 2]
    assert logical_codec.table_provider_decode_calls() > 0
    assert logical_codec.task_context_udf_resolutions() > 0


@pytest.mark.parametrize("codecs_first", [True, False])
def test_registered_providers_survive_a_planner_install(codecs_first: bool):
    """Neither write path may orphan a previously registered provider.

    A foreign catalog provider is handed a codec carrying a *weak*
    ``FFI_TaskContextProvider`` pointing at the session it was registered on,
    and upgrades it on every ``supports_filters_pushdown`` and every ``scan``.
    That codec lives inside the registered ``FFI_CatalogProvider``, so nothing
    on the Python side can reach it to rebind it. Deriving a replacement
    ``SessionContext`` would drop the allocation those handles point at, and
    the next query would fail with ``TaskContextProvider went out of scope over
    FFI boundary``. Installing in place keeps the one allocation alive.

    Both parameters exercise that, because both write ``SessionState``:
    ``set_query_planner`` installs the planner, and installing a codec on a
    session that already has one rebuilds that planner against the new codec.

    The ``WHERE`` clause is load-bearing -- it forces filter pushdown, which
    upgrades the weak handle during logical optimization. It is also why both
    codecs have to be installed: without them the query fails at plan
    serialization with ``LogicalExtensionCodec is not provided``, which would
    mask a dangling handle rather than expose it.
    """
    config = SessionConfig().with_extension(MyPlannerConfig(max_rows=10))
    ctx = SessionContext(config)
    ctx.register_catalog_provider("ffi_catalog", MyCatalogProvider())

    def install_codecs(ctx):
        ctx = ctx.with_logical_extension_codec(MyLogicalExtensionCodec())
        return ctx.with_physical_extension_codec(MyPhysicalExtensionCodec())

    if codecs_first:
        ctx = install_codecs(ctx)
        ctx.set_query_planner(MyQueryPlanner())
    else:
        ctx.set_query_planner(MyQueryPlanner())
        ctx = install_codecs(ctx)
    gc.collect()

    batches = ctx.sql(
        "SELECT units FROM ffi_catalog.my_schema.my_table WHERE units > 5"
    ).collect()
    assert sorted(v for b in batches for v in b.column(0).to_pylist()) == [
        7,
        10,
        20,
        30,
    ]


def codec_context(max_rows: int = 3):
    """Context with both example codecs installed and a table to scan.

    Unlike :func:`probe_context` the codecs ask for no function, so the only
    thing they record is the session id of the task context they are handed.
    No planner yet -- the caller installs one.
    """
    config = SessionConfig().with_extension(MyPlannerConfig(max_rows=max_rows))
    logical_codec = MyLogicalExtensionCodec()
    physical_codec = MyPhysicalExtensionCodec()
    ctx = SessionContext(config)
    ctx = ctx.with_logical_extension_codec(logical_codec)
    ctx = ctx.with_physical_extension_codec(physical_codec)
    ctx.register_table("numbers", MyTableProvider(1, 6, 1))
    return ctx, logical_codec, physical_codec


def test_installing_a_planner_keeps_the_session_id():
    """A session and its decode callbacks must agree on the session id.

    Installing a planner rebuilds ``SessionState`` through
    ``SessionStateBuilder``, which mints a fresh id unless handed one, while
    ``SessionContext`` caches its id in a field of its own. Dropping the id
    there leaves the session reporting one id from ``session_id()`` and a
    different one from every ``TaskContext`` it gives a foreign codec.

    Asserting on ``session_id()`` alone cannot catch that: it reads the cached
    copy, which stays correct either way. The codec-side id is the only
    observable that moves, which is what makes this worth a test rather than a
    one-line equality check.
    """
    ctx, logical_codec, physical_codec = codec_context()
    session_id = ctx.session_id()

    ctx.set_query_planner(MyQueryPlanner())
    assert ctx.session_id() == session_id

    ctx.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()
    assert logical_codec.last_task_context_session_id() == session_id
    assert physical_codec.last_task_context_session_id() == session_id


def test_adding_a_physical_optimizer_rule_keeps_the_session_id():
    """The same guarantee for the other in-place ``SessionState`` rewrite.

    ``add_physical_optimizer_rule`` also rebuilds ``SessionState`` and writes
    it back, so a regenerated id would desync a context from itself.
    """
    ctx, logical_codec, physical_codec = codec_context()
    ctx.set_query_planner(MyQueryPlanner())
    session_id = ctx.session_id()

    ctx.add_physical_optimizer_rule(MyPhysicalOptimizerRule())
    assert ctx.session_id() == session_id

    ctx.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()
    assert logical_codec.last_task_context_session_id() == session_id
    assert physical_codec.last_task_context_session_id() == session_id


def test_replacing_a_planner_keeps_the_session_id():
    """Installing repeatedly must not drift the id.

    Each install rebuilds ``SessionState``, so an id carried over only on the
    first write would still be lost by the second.
    """
    ctx, logical_codec, _physical_codec = codec_context()
    session_id = ctx.session_id()

    ctx.set_query_planner(MyQueryPlanner())
    ctx.set_query_planner(MyQueryPlanner())
    assert ctx.session_id() == session_id

    ctx.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()
    assert logical_codec.last_task_context_session_id() == session_id


@pytest.mark.parametrize("raw_capsule", [False, True])
def test_three_library_query_planner(raw_capsule: bool):
    """Host, provider, and planner exchange a real non-empty plan over FFI."""
    ctx, logical_codec, physical_codec = configured_context(max_rows=3)
    planner = MyQueryPlanner()
    exported_planner = (
        planner.__datafusion_query_planner__(ctx) if raw_capsule else planner
    )
    ctx.set_query_planner(exported_planner)

    batches = ctx.sql(
        'SELECT "A", my_custom_is_null("A") AS is_null FROM numbers ORDER BY "A"'
    ).collect()
    assert batches[0].column(0).to_pylist() == [0, 1, 2]
    assert batches[0].column(1).to_pylist() == [False, False, False]
    assert planner.last_max_rows() == 3

    ctx.sql("SET ffi_query_planner.max_rows = 2").collect()
    batches = ctx.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()
    assert batches[0].column(0).to_pylist() == [0, 1]
    assert planner.last_max_rows() == 2

    assert planner.plan_calls() >= 2
    assert planner.foreign_session_observed()
    assert planner.foreign_provider_observed()
    assert planner.foreign_plan_observed()
    assert logical_codec.table_provider_encode_calls() > 0
    assert logical_codec.table_provider_decode_calls() > 0
    assert physical_codec.execution_plan_encode_calls() > 0
    assert physical_codec.execution_plan_decode_calls() > 0


def test_spawning_plan_across_three_libraries():
    """A plan that spawns Tokio tasks survives the full three-library round trip.

    ``target_partitions`` above one puts a ``RepartitionExec`` under the
    aggregate, and that operator spawns tasks while it runs. This exercises the
    codecs on a multi-node plan rather than the bare scan the other tests use.
    """
    config = SessionConfig().with_extension(MyPlannerConfig(max_rows=100))
    config = config.with_target_partitions(4)
    logical_codec = MyLogicalExtensionCodec()
    physical_codec = MyPhysicalExtensionCodec()
    ctx = SessionContext(config)
    ctx = ctx.with_logical_extension_codec(logical_codec)
    ctx = ctx.with_physical_extension_codec(physical_codec)
    ctx.register_table("numbers", MyTableProvider(1, 6, 3))

    planner = MyQueryPlanner()
    ctx.set_query_planner(planner)

    batches = ctx.sql(
        'SELECT "A" % 2 AS parity, count(*) AS n FROM numbers GROUP BY 1 ORDER BY 1'
    ).collect()
    counts = {
        row[0]: row[1]
        for batch in batches
        for row in zip(
            batch.column(0).to_pylist(), batch.column(1).to_pylist(), strict=True
        )
    }
    assert sum(counts.values()) == 6 + 7 + 8
    assert planner.plan_calls() > 0
    assert planner.foreign_provider_observed()


def test_planner_layers_on_the_session_planner():
    """A planner can wrap the one already installed and delegate to it.

    The capsule has to be captured before this planner is installed, because
    ``__datafusion_query_planner__`` exports whichever planner is installed when
    it is called. Capturing it afterwards would hand the planner a handle to
    itself, and planning would recurse.
    """
    ctx, logical_codec, physical_codec = configured_context(max_rows=3)
    fallback = ctx.__datafusion_query_planner__()
    planner = MyQueryPlanner(fallback=fallback)
    # The capsule's FFI codecs hold weak handles to this session. Installing in
    # place keeps that session alive, so the capsule stays usable; deriving a
    # replacement here would fail with "TaskContextProvider went out of scope
    # over FFI boundary".
    ctx.set_query_planner(planner)
    gc.collect()

    batches = ctx.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()
    assert batches[0].column(0).to_pylist() == [0, 1, 2]
    assert planner.plan_calls() > 0
    assert planner.used_fallback()
    assert logical_codec.table_provider_decode_calls() > 0
    assert physical_codec.execution_plan_decode_calls() > 0


def test_a_planner_can_fall_back_to_another_planner_library():
    """A fallback may be another foreign planner, not only a session.

    The fallback is imported when this planner is installed rather than when
    it is constructed, so its own getter receives the session. Importing it at
    construction time would mean calling that getter with no session, which
    only a ``SessionContext`` or a raw capsule tolerates -- and layering on
    another planner is the case a distributed engine actually needs.
    """
    ctx, logical_codec, physical_codec = configured_context(max_rows=3)
    inner = MyQueryPlanner()
    outer = MyQueryPlanner(fallback=inner)
    ctx.set_query_planner(outer)

    batches = ctx.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()
    assert batches[0].column(0).to_pylist() == [0, 1, 2]
    assert outer.plan_calls() > 0
    assert outer.used_fallback()
    # The delegation reached the inner planner rather than stopping at the
    # default physical planner.
    assert inner.plan_calls() > 0
    assert logical_codec.table_provider_decode_calls() > 0
    assert physical_codec.execution_plan_decode_calls() > 0


def test_a_session_fallback_delegates_to_its_installed_planner():
    """Passing a SessionContext delegates to whatever planner it holds."""
    ctx, _logical_codec, _physical_codec = configured_context(max_rows=3)
    first = MyQueryPlanner()
    ctx.set_query_planner(first)

    second = MyQueryPlanner(fallback=ctx)
    ctx.set_query_planner(second)

    batches = ctx.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()
    assert batches[0].column(0).to_pylist() == [0, 1, 2]
    assert second.used_fallback()
    assert first.plan_calls() > 0


def test_observations_accumulate_across_queries():
    """A later plain query must not retract what an earlier query observed.

    The ``*_observed`` accessors answer "was this ever seen". They are written
    with ``fetch_or`` rather than ``store`` so a query that touches no foreign
    object cannot clear a flag an earlier one set. Written with ``store``,
    ``SELECT 1`` here clears ``foreign_provider_observed``, and every other
    test asserting these flags after more than one query is a coincidence away
    from failing.
    """
    ctx, _logical_codec, _physical_codec = configured_context(max_rows=3)
    planner = MyQueryPlanner()
    ctx.set_query_planner(planner)

    ctx.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()
    assert planner.foreign_session_observed()
    assert planner.foreign_provider_observed()
    assert planner.foreign_plan_observed()

    # Touches no table, so this plan has no foreign provider of its own.
    ctx.sql("SELECT 1").collect()
    assert planner.foreign_session_observed()
    assert planner.foreign_provider_observed()
    assert planner.foreign_plan_observed()

    # `last_max_rows` is deliberately not cumulative; it reports the last plan.
    assert planner.last_max_rows() == 3
    assert planner.plan_calls() >= 2


def test_second_planner_replaces_the_first():
    """A session holds exactly one planner, so installing another replaces it."""
    ctx, _logical_codec, _physical_codec = configured_context(max_rows=2)
    first = MyQueryPlanner()
    second = MyQueryPlanner()
    ctx.set_query_planner(first)
    ctx.set_query_planner(second)

    batches = ctx.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()
    assert batches[0].column(0).to_pylist() == [0, 1]
    assert second.plan_calls() > 0
    assert first.plan_calls() == 0


def test_the_planner_reaches_every_handle_on_the_session():
    """The planner is session state, so all handles on that session use it.

    ``set_query_planner`` writes through ``state_ref()``. A context returned by
    an earlier ``with_*`` call shares that session, so it plans through the new
    planner too -- there is one session and one planner, not a family of
    diverging copies.
    """
    ctx, _logical_codec, _physical_codec = configured_context(max_rows=2)
    # Shares the session with `ctx`; predates the planner.
    sibling = ctx.with_python_udf_inlining(enabled=False)

    planner = MyQueryPlanner()
    ctx.set_query_planner(planner)

    batches = sibling.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()
    assert batches[0].column(0).to_pylist() == [0, 1]
    assert planner.plan_calls() > 0


def test_installed_codecs_outlive_python_exporters():
    ctx, logical_codec, physical_codec = configured_context(max_rows=2)
    del logical_codec, physical_codec
    gc.collect()

    ctx.set_query_planner(MyQueryPlanner())
    batches = ctx.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()
    assert batches[0].column(0).to_pylist() == [0, 1]


def test_provider_codecs_can_be_installed_after_planner():
    config = SessionConfig().with_extension(MyPlannerConfig(max_rows=2))
    planner = MyQueryPlanner()
    logical_codec = MyLogicalExtensionCodec()
    physical_codec = MyPhysicalExtensionCodec()
    ctx = SessionContext(config)
    ctx.set_query_planner(planner)
    ctx = ctx.with_logical_extension_codec(logical_codec)
    ctx = ctx.with_physical_extension_codec(physical_codec)
    ctx.register_table("numbers", MyTableProvider(1, 4, 1))

    batches = ctx.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()
    assert batches[0].column(0).to_pylist() == [0, 1]
    assert planner.last_max_rows() == 2
    assert logical_codec.table_provider_decode_calls() > 0
    assert physical_codec.execution_plan_decode_calls() > 0


def test_a_discarded_derived_context_still_rebinds_the_planner():
    """Installing a codec rebinds the planner even if the handle is thrown away.

    `with_logical_extension_codec` returns a context sharing this session, and
    rebuilding the installed planner against the new codec happens on that
    shared session rather than on the returned handle. So the rebind outlives
    the handle, and the codec below takes effect on `ctx` despite `ctx`'s own
    codec field never changing.

    That is spooky enough to be worth pinning as a decision. It is also forced:
    `FFI_QueryPlanner` holds its codecs by value, so a planner cannot read the
    session's current codecs at plan time and the rebuild has to be eager.

    A fresh codec instance is what makes it observable -- it carries its own
    counters, and the planner encodes the outbound logical plan with whichever
    codec it is holding.
    """
    ctx, _logical_codec, _physical_codec = configured_context(max_rows=3)
    ctx.set_query_planner(MyQueryPlanner())

    later = MyLogicalExtensionCodec()
    # Deliberately discarded. The rebind still lands on the shared session.
    ctx.with_logical_extension_codec(later)
    gc.collect()

    batches = ctx.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()
    assert batches[0].column(0).to_pylist() == [0, 1, 2]
    assert later.table_provider_encode_calls() > 0


def test_the_planner_and_the_handle_can_hold_different_codecs():
    """One session, two codecs in effect, depending on the path taken.

    The planner is session state and carries whichever codecs installed it
    last -- here, ones that arrived through a handle that was discarded.
    Everything else on a context uses that context's own codec field, which
    the discarded handle never touched. So `Expr.to_bytes(ctx)` and
    `ctx.sql(...)` encode with different codecs on the same `ctx`.

    Chaining ``ctx = ctx.with_...(...)`` keeps the two in step; this pins what
    happens when they are allowed to diverge.

    Inlining has to be off for the assertion to say anything: with it on, a
    Python UDF is encoded inline by ``PythonLogicalCodec`` and never reaches
    the installed codec's ``try_encode_udf``.
    """
    config = SessionConfig().with_extension(MyPlannerConfig(max_rows=3))
    handle_codec = MyLogicalExtensionCodec()
    ctx = SessionContext(config).with_python_udf_inlining(enabled=False)
    ctx = ctx.with_logical_extension_codec(handle_codec)
    ctx = ctx.with_physical_extension_codec(MyPhysicalExtensionCodec())
    ctx.register_table("numbers", MyTableProvider(1, 6, 1))
    ctx.set_query_planner(MyQueryPlanner())

    planner_codec = MyLogicalExtensionCodec()
    # Discarded, but the planner keeps its codec.
    ctx.with_logical_extension_codec(planner_codec)
    gc.collect()

    identity = udf(
        lambda arr: arr,
        [pa.int64()],
        pa.int64(),
        volatility="immutable",
        name="identity_i64",
    )
    ctx.register_udf(identity)
    Expr.to_bytes(identity(col("A")), ctx)

    # Serializing through `ctx` uses `ctx`'s own codec field.
    assert handle_codec.encode_udf_calls() > 0
    assert planner_codec.encode_udf_calls() == 0

    ctx.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()

    # Planning through the same `ctx` uses the codec the planner was rebound to.
    assert planner_codec.table_provider_encode_calls() > 0
    assert handle_codec.table_provider_encode_calls() == 0


def test_an_unchanged_inlining_setting_leaves_the_planner_alone():
    """A no-op toggle must not rebind the session's planner.

    ``with_python_udf_inlining`` rebuilds the handle's codecs and rebinds the
    session's planner to them. Asking for the setting a context already has
    changes nothing, so it must not pay that side effect.

    Observable only once the planner is holding some *other* handle's codec:
    without the guard, a defensive no-op toggle on `ctx` drags the planner back
    onto `ctx`'s codec and silently undoes the install below. The rebuilt
    codecs otherwise wrap the same inner codec, so nothing else distinguishes
    the two paths.
    """
    ctx, handle_codec, _physical_codec = codec_context()
    ctx.set_query_planner(MyQueryPlanner())

    planner_codec = MyLogicalExtensionCodec()
    ctx.with_logical_extension_codec(planner_codec)  # discarded; planner keeps it
    gc.collect()

    # The default is on, so this asks for what `ctx` already has.
    ctx.with_python_udf_inlining(enabled=True)
    gc.collect()

    ctx.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()
    assert planner_codec.table_provider_encode_calls() > 0
    assert handle_codec.table_provider_encode_calls() == 0


def test_reinstalling_a_planner_rebinds_the_session_to_that_handles_codecs():
    """A planner is built against the codecs of the handle it is installed from.

    The sequel to the test above, and the trap it sets up. Once a discarded
    derived handle has rebound the session's planner to its codec, installing
    the same planner again from the *original* handle rebuilds it against that
    handle's codec instead -- which never changed. The session's planner tracks
    whichever handle wrote it last, not the newest codec installed anywhere.

    So "re-install the planner after installing a codec" only repairs anything
    when it is done from the handle holding the new codec.
    """
    ctx, original_logical, _physical_codec = codec_context()
    planner = MyQueryPlanner()
    ctx.set_query_planner(planner)

    later = MyLogicalExtensionCodec()
    # Deliberately discarded, exactly as in the test above.
    ctx.with_logical_extension_codec(later)
    gc.collect()

    ctx.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()
    assert later.table_provider_encode_calls() > 0
    assert original_logical.table_provider_encode_calls() == 0

    # `ctx`'s own codec field never changed, so this rebuilds the planner
    # against `original_logical` and drops `later` from the session's planner.
    ctx.set_query_planner(planner)
    encodes_by_later = later.table_provider_encode_calls()

    ctx.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()
    assert original_logical.table_provider_encode_calls() > 0
    assert later.table_provider_encode_calls() == encodes_by_later


def test_query_planner_requires_provider_codec():
    config = SessionConfig().with_extension(MyPlannerConfig(max_rows=2))
    ctx = SessionContext(config)
    ctx.register_table("numbers", MyTableProvider(1, 3, 1))
    ctx.set_query_planner(MyQueryPlanner())

    with pytest.raises(Exception, match=r"LogicalExtensionCodec|TableProvider"):
        ctx.sql('SELECT "A" FROM numbers').collect()


@pytest.mark.parametrize("max_rows", ["0", "oops"])
def test_query_planner_rejects_invalid_config(max_rows: str):
    ctx, _logical_codec, _physical_codec = configured_context(max_rows=2)
    ctx.set_query_planner(MyQueryPlanner())

    with pytest.raises(Exception, match=r"max_rows|Invalid value"):
        ctx.sql(f"SET ffi_query_planner.max_rows = '{max_rows}'").collect()
