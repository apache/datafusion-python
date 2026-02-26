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

import pytest
from datafusion import (
    ExecutionPlan,
    LogicalPlan,
    Metric,
    MetricsSet,
    SessionContext,
)


# Note: We must use CSV because memory tables are currently not supported for
# conversion to/from protobuf.
@pytest.fixture
def df():
    ctx = SessionContext()
    return ctx.read_csv(path="testing/data/csv/aggregate_test_100.csv").select("c1")


def test_logical_plan_to_proto(ctx, df) -> None:
    logical_plan_bytes = df.logical_plan().to_proto()
    logical_plan = LogicalPlan.from_proto(ctx, logical_plan_bytes)

    df_round_trip = ctx.create_dataframe_from_logical_plan(logical_plan)

    assert df.collect() == df_round_trip.collect()

    original_execution_plan = df.execution_plan()
    execution_plan_bytes = original_execution_plan.to_proto()
    execution_plan = ExecutionPlan.from_proto(ctx, execution_plan_bytes)

    assert str(original_execution_plan) == str(execution_plan)


def test_metrics_tree_walk() -> None:
    ctx = SessionContext()
    ctx.sql("CREATE TABLE t AS VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    df = ctx.sql("SELECT * FROM t WHERE column1 > 1")
    df.collect()
    plan = df.execution_plan()

    results = plan.collect_metrics()
    assert len(results) >= 1
    found_metrics = False
    for name, ms in results:
        assert isinstance(name, str)
        assert isinstance(ms, MetricsSet)
        if ms.output_rows is not None and ms.output_rows > 0:
            found_metrics = True
    assert found_metrics


def test_metric_properties() -> None:
    ctx = SessionContext()
    ctx.sql("CREATE TABLE t AS VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    df = ctx.sql("SELECT * FROM t WHERE column1 > 1")
    df.collect()
    plan = df.execution_plan()

    for _, ms in plan.collect_metrics():
        r = repr(ms)
        assert isinstance(r, str)
        for metric in ms.metrics():
            assert isinstance(metric, Metric)
            assert isinstance(metric.name, str)
            assert len(metric.name) > 0
            assert metric.partition is None or isinstance(metric.partition, int)
            assert isinstance(metric.labels(), dict)
            mr = repr(metric)
            assert isinstance(mr, str)
            assert len(mr) > 0
            return
    pytest.skip("No metrics found")


def test_no_metrics_before_execution() -> None:
    ctx = SessionContext()
    ctx.sql("CREATE TABLE t AS VALUES (1), (2), (3)")
    df = ctx.sql("SELECT * FROM t")
    plan = df.execution_plan()
    ms = plan.metrics()
    assert ms is None or ms.output_rows is None or ms.output_rows == 0


def test_collect_partitioned_metrics() -> None:
    ctx = SessionContext()
    ctx.sql("CREATE TABLE t AS VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    df = ctx.sql("SELECT * FROM t WHERE column1 > 1")

    df.collect_partitioned()
    plan = df.execution_plan()

    found_metrics = False
    for _, ms in plan.collect_metrics():
        if ms.output_rows is not None and ms.output_rows > 0:
            found_metrics = True
    assert found_metrics


def test_execute_stream_metrics() -> None:
    ctx = SessionContext()
    ctx.sql("CREATE TABLE t AS VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    df = ctx.sql("SELECT * FROM t WHERE column1 > 1")

    for _ in df.execute_stream():
        pass

    plan = df.execution_plan()
    found_metrics = False
    for _, ms in plan.collect_metrics():
        if ms.output_rows is not None and ms.output_rows > 0:
            found_metrics = True
    assert found_metrics


def test_execute_stream_partitioned_metrics() -> None:
    ctx = SessionContext()
    ctx.sql("CREATE TABLE t AS VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    df = ctx.sql("SELECT * FROM t WHERE column1 > 1")

    for stream in df.execute_stream_partitioned():
        for _ in stream:
            pass

    plan = df.execution_plan()
    found_metrics = False
    for _, ms in plan.collect_metrics():
        if ms.output_rows is not None and ms.output_rows > 0:
            found_metrics = True
    assert found_metrics
