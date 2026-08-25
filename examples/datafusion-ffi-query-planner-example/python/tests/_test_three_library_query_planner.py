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
from datafusion_ffi_query_planner_example import MyQueryPlanner, PlannerConfig


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
