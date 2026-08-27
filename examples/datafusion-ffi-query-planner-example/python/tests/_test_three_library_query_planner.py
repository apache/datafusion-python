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

import pytest
from datafusion import SessionConfig, SessionContext, udf
from datafusion_ffi_example import (
    IsNullUDF,
    MyLogicalExtensionCodec,
    MyPhysicalExtensionCodec,
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
    ctx = ctx.with_query_planner(MyQueryPlanner())
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
    """The provider is a live handle to the session, not a snapshot of it.

    A function registered after the codec is installed is still visible to the
    decode callback. It has to be registered before the planner, though:
    installing a foreign planner forks the session, and the codec keeps
    pointing at the one it was built against.
    """
    config = SessionConfig().with_extension(MyPlannerConfig(max_rows=3))
    logical_codec = MyLogicalExtensionCodec(require_udf_on_decode=HOST_ONLY_UDF)
    ctx = SessionContext(config)
    ctx = ctx.with_logical_extension_codec(logical_codec)
    ctx = ctx.with_physical_extension_codec(MyPhysicalExtensionCodec())
    ctx.register_table("numbers", MyTableProvider(1, 6, 1))
    # Registered after the codec was installed and bound to this session.
    ctx.register_udf(udf(IsNullUDF()))
    ctx = ctx.with_query_planner(MyQueryPlanner())

    batches = ctx.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()
    assert batches[0].column(0).to_pylist() == [0, 1, 2]
    assert logical_codec.task_context_udf_resolutions() > 0


def test_codec_does_not_see_a_udf_registered_after_the_planner_fork():
    """Installing a planner forks; the codec still points at the pre-fork session.

    This is the same rule as the documented fork caveat, seen from the codec's
    side. Register before installing the planner, as the guide advises.
    """
    ctx, _logical_codec, _physical_codec = probe_context()
    ctx = ctx.with_logical_extension_codec(
        MyLogicalExtensionCodec(require_udf_on_decode="registered_after_fork")
    )

    with pytest.raises(
        Exception, match=r"could not resolve scalar function 'registered_after_fork'"
    ):
        ctx.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()


@pytest.mark.parametrize("raw_capsule", [False, True])
def test_three_library_query_planner(raw_capsule: bool):
    """Host, provider, and planner exchange a real non-empty plan over FFI."""
    ctx, logical_codec, physical_codec = configured_context(max_rows=3)
    planner = MyQueryPlanner()
    exported_planner = (
        planner.__datafusion_query_planner__(ctx) if raw_capsule else planner
    )
    ctx = ctx.with_query_planner(exported_planner)

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
    ctx = ctx.with_query_planner(planner)

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
    # Rebinding `ctx` drops the context that produced the capsule. The exported
    # codecs retain it, so the capsule stays usable. Without that the FFI
    # task-context handle is weak and planning fails with "TaskContextProvider
    # went out of scope over FFI boundary".
    ctx = ctx.with_query_planner(planner)
    gc.collect()

    batches = ctx.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()
    assert batches[0].column(0).to_pylist() == [0, 1, 2]
    assert planner.plan_calls() > 0
    assert planner.used_fallback()
    assert logical_codec.table_provider_decode_calls() > 0
    assert physical_codec.execution_plan_decode_calls() > 0


def test_second_planner_replaces_the_first():
    """A session holds exactly one planner, so installing another replaces it."""
    ctx, _logical_codec, _physical_codec = configured_context(max_rows=2)
    first = MyQueryPlanner()
    second = MyQueryPlanner()
    ctx = ctx.with_query_planner(first).with_query_planner(second)

    batches = ctx.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()
    assert batches[0].column(0).to_pylist() == [0, 1]
    assert second.plan_calls() > 0
    assert first.plan_calls() == 0


def test_planner_is_not_installed_on_the_original_context():
    """``with_query_planner`` returns a fork; the receiver keeps its planner."""
    ctx, _logical_codec, _physical_codec = configured_context(max_rows=2)
    planner = MyQueryPlanner()
    derived = ctx.with_query_planner(planner)

    ctx.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()
    assert planner.plan_calls() == 0

    derived.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()
    assert planner.plan_calls() > 0


def test_installed_codecs_outlive_python_exporters():
    ctx, logical_codec, physical_codec = configured_context(max_rows=2)
    del logical_codec, physical_codec
    gc.collect()

    ctx = ctx.with_query_planner(MyQueryPlanner())
    batches = ctx.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()
    assert batches[0].column(0).to_pylist() == [0, 1]


def test_provider_codecs_can_be_installed_after_planner():
    config = SessionConfig().with_extension(MyPlannerConfig(max_rows=2))
    planner = MyQueryPlanner()
    logical_codec = MyLogicalExtensionCodec()
    physical_codec = MyPhysicalExtensionCodec()
    ctx = SessionContext(config).with_query_planner(planner)
    ctx = ctx.with_logical_extension_codec(logical_codec)
    ctx = ctx.with_physical_extension_codec(physical_codec)
    ctx.register_table("numbers", MyTableProvider(1, 4, 1))

    batches = ctx.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()
    assert batches[0].column(0).to_pylist() == [0, 1]
    assert planner.last_max_rows() == 2
    assert logical_codec.table_provider_decode_calls() > 0
    assert physical_codec.execution_plan_decode_calls() > 0


def test_query_planner_requires_provider_codec():
    config = SessionConfig().with_extension(MyPlannerConfig(max_rows=2))
    ctx = SessionContext(config)
    ctx.register_table("numbers", MyTableProvider(1, 3, 1))
    ctx = ctx.with_query_planner(MyQueryPlanner())

    with pytest.raises(Exception, match=r"LogicalExtensionCodec|TableProvider"):
        ctx.sql('SELECT "A" FROM numbers').collect()


@pytest.mark.parametrize("max_rows", ["0", "oops"])
def test_query_planner_rejects_invalid_config(max_rows: str):
    ctx, _logical_codec, _physical_codec = configured_context(max_rows=2)
    ctx = ctx.with_query_planner(MyQueryPlanner())

    with pytest.raises(Exception, match=r"max_rows|Invalid value"):
        ctx.sql(f"SET ffi_query_planner.max_rows = '{max_rows}'").collect()
