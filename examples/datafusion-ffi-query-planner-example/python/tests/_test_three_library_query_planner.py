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
from datafusion import (
    SessionConfig,
    SessionContext,
    SessionExtensionComponents,
    udf,
)
from datafusion_ffi_example import (
    IsNullUDF,
    MyLogicalExtensionCodec,
    MyPhysicalExtensionCodec,
    MyTableProvider,
)
from datafusion_ffi_query_planner_example import (
    MyPlannerExtension,
    MyQueryPlanner,
    PlannerConfig,
)


def configured_context(max_rows: int):
    config = SessionConfig().with_extension(PlannerConfig(max_rows=max_rows))
    logical_codec = MyLogicalExtensionCodec()
    physical_codec = MyPhysicalExtensionCodec()
    ctx = SessionContext(config)
    ctx = ctx.with_logical_extension_codec(logical_codec)
    ctx = ctx.with_physical_extension_codec(physical_codec)
    ctx.register_table("numbers", MyTableProvider(1, 6, 1))
    ctx.register_udf(udf(IsNullUDF()))
    return ctx, logical_codec, physical_codec


@pytest.mark.parametrize("raw_capsule", [False, True])
def test_three_library_query_planner(raw_capsule: bool):
    """Host, provider, and planner exchange a real non-empty plan over FFI."""
    ctx, logical_codec, physical_codec = configured_context(max_rows=3)
    planner = MyQueryPlanner()
    exported_planner = (
        planner.__datafusion_query_planner__() if raw_capsule else planner
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


def test_installed_codecs_outlive_python_exporters():
    ctx, logical_codec, physical_codec = configured_context(max_rows=2)
    del logical_codec, physical_codec
    gc.collect()

    ctx = ctx.with_query_planner(MyQueryPlanner())
    batches = ctx.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()
    assert batches[0].column(0).to_pylist() == [0, 1]


def test_provider_codecs_can_be_installed_after_planner():
    config = SessionConfig().with_extension(PlannerConfig(max_rows=2))
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
    config = SessionConfig().with_extension(PlannerConfig(max_rows=2))
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


class ProviderCodecsExtension:
    """Bundles the provider library's codecs for ``with_extensions``.

    These codecs keep their own private task-context provider, so they only
    need to be created once; the bundle can hand out the same exporters on
    every call.
    """

    def __init__(self) -> None:
        self.logical_codec = MyLogicalExtensionCodec()
        self.physical_codec = MyPhysicalExtensionCodec()

    def __datafusion_session_extension__(
        self, ctx: SessionContext
    ) -> SessionExtensionComponents:
        return SessionExtensionComponents(
            logical_extension_codecs=(self.logical_codec,),
            physical_extension_codecs=(self.physical_codec,),
        )


def test_with_extensions_three_library_query():
    """One with_extensions call installs provider codecs and a planner bundle,
    and a real non-empty plan flows across the three libraries."""
    config = SessionConfig().with_extension(PlannerConfig(max_rows=3))
    provider_ext = ProviderCodecsExtension()
    planner_ext = MyPlannerExtension()
    ctx = SessionContext(config).with_extensions(provider_ext, planner_ext)
    ctx.register_table("numbers", MyTableProvider(1, 6, 1))
    ctx.register_udf(udf(IsNullUDF()))

    batches = ctx.sql(
        'SELECT "A", my_custom_is_null("A") AS is_null FROM numbers ORDER BY "A"'
    ).collect()
    assert batches[0].column(0).to_pylist() == [0, 1, 2]
    assert batches[0].column(1).to_pylist() == [False, False, False]
    assert planner_ext.plan_calls() >= 1
    assert planner_ext.last_max_rows() == 3
    assert planner_ext.foreign_session_observed()
    assert planner_ext.foreign_provider_observed()
    assert planner_ext.foreign_plan_observed()
    assert provider_ext.logical_codec.table_provider_encode_calls() > 0
    assert provider_ext.logical_codec.table_provider_decode_calls() > 0
    assert provider_ext.physical_codec.execution_plan_encode_calls() > 0
    assert provider_ext.physical_codec.execution_plan_decode_calls() > 0


def test_with_extensions_provider_targets_returned_context():
    """The bundle's task-context provider reads current state from the
    returned context, not the source it was derived from."""
    config = SessionConfig().with_extension(PlannerConfig(max_rows=3))
    source = SessionContext(config)
    source.register_table("numbers", MyTableProvider(1, 6, 1))
    planner_ext = MyPlannerExtension()
    result = source.with_extensions(ProviderCodecsExtension(), planner_ext)

    # Diverge the two live contexts. Config state is copied at derivation,
    # so after these statements source and result disagree.
    source.sql("SET ffi_query_planner.max_rows = 5").collect()
    result.sql("SET ffi_query_planner.max_rows = 2").collect()

    batches = result.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()
    assert batches[0].column(0).to_pylist() == [0, 1]
    assert planner_ext.last_max_rows() == 2

    # Codec decode calls resolve the task context through the weak provider
    # bound during with_extensions. Seeing 2 (never 5) proves the provider
    # targets the returned context rather than the source.
    seen = planner_ext.decode_max_rows_seen()
    assert seen, "expected the bundle codecs to observe at least one decode"
    assert set(seen) == {2}


def test_with_extensions_survives_dropping_source_and_bundles():
    """Neither the source context nor the bundle objects are needed to keep
    the installed components' task-context provider alive."""
    config = SessionConfig().with_extension(PlannerConfig(max_rows=2))
    ctx = SessionContext(config).with_extensions(
        ProviderCodecsExtension(), MyPlannerExtension()
    )
    ctx.register_table("numbers", MyTableProvider(1, 6, 1))
    gc.collect()

    batches = ctx.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()
    assert batches[0].column(0).to_pylist() == [0, 1]


def test_with_extensions_sees_state_changes_after_install():
    """Tables, UDFs, and config changes made after installation are visible
    to the planner and to provider callbacks."""
    config = SessionConfig().with_extension(PlannerConfig(max_rows=4))
    planner_ext = MyPlannerExtension()
    ctx = SessionContext(config).with_extensions(ProviderCodecsExtension(), planner_ext)

    ctx.register_table("numbers", MyTableProvider(1, 6, 1))
    ctx.register_udf(udf(IsNullUDF()))
    ctx.sql("SET ffi_query_planner.max_rows = 2").collect()

    batches = ctx.sql(
        'SELECT "A", my_custom_is_null("A") AS is_null FROM numbers ORDER BY "A"'
    ).collect()
    assert batches[0].column(0).to_pylist() == [0, 1]
    assert planner_ext.last_max_rows() == 2
    seen = planner_ext.decode_max_rows_seen()
    assert seen
    assert set(seen) == {2}


def test_with_extensions_bundle_is_reusable():
    """Installing the same bundle into two contexts binds fresh components to
    each destination."""
    planner_ext = MyPlannerExtension()

    config_a = SessionConfig().with_extension(PlannerConfig(max_rows=2))
    ctx_a = SessionContext(config_a).with_extensions(
        ProviderCodecsExtension(), planner_ext
    )
    ctx_a.register_table("numbers", MyTableProvider(1, 6, 1))

    config_b = SessionConfig().with_extension(PlannerConfig(max_rows=3))
    ctx_b = SessionContext(config_b).with_extensions(
        ProviderCodecsExtension(), planner_ext
    )
    ctx_b.register_table("numbers", MyTableProvider(1, 6, 1))

    batches = ctx_a.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()
    assert batches[0].column(0).to_pylist() == [0, 1]
    assert planner_ext.last_max_rows() == 2

    batches = ctx_b.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()
    assert batches[0].column(0).to_pylist() == [0, 1, 2]
    assert planner_ext.last_max_rows() == 3


def test_with_extensions_failure_leaves_source_usable():
    """A failing factory after a successful one leaves the source context
    fully functional."""

    class BoomExtension:
        def __datafusion_session_extension__(
            self, ctx: SessionContext
        ) -> SessionExtensionComponents:
            msg = "boom"
            raise RuntimeError(msg)

    config = SessionConfig().with_extension(PlannerConfig(max_rows=2))
    source = SessionContext(config)
    source.register_table("numbers", MyTableProvider(1, 6, 1))

    with pytest.raises(RuntimeError, match="boom"):
        source.with_extensions(MyPlannerExtension(), BoomExtension())

    # No planner was installed, so the default planner runs unrestricted.
    batches = source.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()
    assert batches[0].column(0).to_pylist() == [0, 1, 2, 3, 4, 5]


def test_dataframe_outliving_context_fails_cleanly():
    """A DataFrame does not keep its SessionContext alive. FFI components
    resolve the task context through a weak reference, so using the
    DataFrame after dropping the context raises a clean error instead of
    crashing. This locks in the documented ownership contract: the context
    must outlive DataFrames that depend on FFI codecs."""
    config = SessionConfig().with_extension(PlannerConfig(max_rows=2))
    ctx = SessionContext(config).with_extensions(
        ProviderCodecsExtension(), MyPlannerExtension()
    )
    ctx.register_table("numbers", MyTableProvider(1, 6, 1))
    df = ctx.sql('SELECT "A" FROM numbers ORDER BY "A"')
    del ctx
    gc.collect()

    with pytest.raises(Exception, match="went out of scope"):
        df.collect()


def test_composed_codecs_with_query_planner():
    """A second pair of codecs installed on top of the provider codecs
    composes with them instead of replacing them. The extra codecs
    (default-backed exports from a fresh session) decline everything,
    so planner-driven encode/decode falls through to the provider
    codecs and the query still succeeds end to end."""
    ctx, logical_codec, physical_codec = configured_context(max_rows=2)
    other = SessionContext()
    ctx = ctx.with_logical_extension_codec(
        other.__datafusion_logical_extension_codec__()
    )
    ctx = ctx.with_physical_extension_codec(
        other.__datafusion_physical_extension_codec__()
    )
    ctx = ctx.with_query_planner(MyQueryPlanner())

    batches = ctx.sql('SELECT "A" FROM numbers ORDER BY "A"').collect()
    assert batches[0].column(0).to_pylist() == [0, 1]
    assert logical_codec.table_provider_encode_calls() > 0
    assert logical_codec.table_provider_decode_calls() > 0
    assert physical_codec.execution_plan_decode_calls() > 0
