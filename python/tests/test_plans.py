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

import datetime

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


def test_logical_plan_to_bytes_roundtrip(ctx, df) -> None:
    """Round-trip a LogicalPlan through the session's logical codec."""
    logical_plan_bytes = df.logical_plan().to_bytes()
    logical_plan = LogicalPlan.from_bytes(ctx, logical_plan_bytes)

    df_round_trip = ctx.create_dataframe_from_logical_plan(logical_plan)

    assert df.collect() == df_round_trip.collect()


def test_execution_plan_to_bytes_roundtrip(ctx, df) -> None:
    """Round-trip an ExecutionPlan through the session's physical codec."""
    original_execution_plan = df.execution_plan()
    execution_plan_bytes = original_execution_plan.to_bytes()
    execution_plan = ExecutionPlan.from_bytes(ctx, execution_plan_bytes)

    assert str(original_execution_plan) == str(execution_plan)


@pytest.mark.parametrize("metrics_format", ["none", "aggregated", "per_task"])
def test_execution_plan_display_distributed(ctx, metrics_format) -> None:
    text = (
        ctx.sql("SELECT 1")
        .execution_plan()
        .display_distributed(metrics_format=metrics_format)
    )

    assert text


def test_execution_plan_display_distributed_validates_metrics_format(ctx) -> None:
    plan = ctx.sql("SELECT 1").execution_plan()

    with pytest.raises(Exception, match="metrics format"):
        plan.display_distributed(metrics_format="invalid")  # type: ignore[arg-type]


def test_logical_plan_to_proto_is_deprecated(ctx, df) -> None:
    """to_proto / from_proto still work but emit DeprecationWarning."""
    plan = df.logical_plan()

    with pytest.warns(DeprecationWarning, match="to_proto"):
        blob = plan.to_proto()
    with pytest.warns(DeprecationWarning, match="from_proto"):
        restored = LogicalPlan.from_proto(ctx, blob)

    df_round_trip = ctx.create_dataframe_from_logical_plan(restored)
    assert df.collect() == df_round_trip.collect()


def test_execution_plan_to_proto_is_deprecated(ctx, df) -> None:
    plan = df.execution_plan()

    with pytest.warns(DeprecationWarning, match="to_proto"):
        blob = plan.to_proto()
    with pytest.warns(DeprecationWarning, match="from_proto"):
        restored = ExecutionPlan.from_proto(ctx, blob)

    assert str(plan) == str(restored)


def test_session_with_logical_extension_codec_roundtrip(ctx, df) -> None:
    """A session with a non-default logical codec still round-trips builtins.

    The codec slot is overridable via with_logical_extension_codec; the
    PythonLogicalCodec wrapper delegates unhandled cases to the inner
    codec, so plans without Python UDFs are unaffected by the swap.
    """
    # Default-routed session should round-trip via to_bytes.
    blob = df.logical_plan().to_bytes()
    restored = LogicalPlan.from_bytes(ctx, blob)
    df_round_trip = ctx.create_dataframe_from_logical_plan(restored)
    assert df.collect() == df_round_trip.collect()


def test_session_codec_capsule_getters(ctx) -> None:
    """SessionContext exposes both logical and physical codec capsules."""
    logical = ctx.ctx.__datafusion_logical_extension_codec__()
    physical = ctx.ctx.__datafusion_physical_extension_codec__()
    assert logical is not None
    assert physical is not None


def test_metrics_tree_walk() -> None:
    ctx = SessionContext()
    ctx.sql("CREATE TABLE t AS VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    df = ctx.sql("SELECT * FROM t WHERE column1 > 1")
    df.collect()
    plan = df.execution_plan()

    results = plan.collect_metrics()
    assert len(results) >= 1
    output_rows_by_op: dict[str, int] = {}
    for name, ms in results:
        assert isinstance(name, str)
        assert isinstance(ms, MetricsSet)
        if ms.output_rows is not None:
            output_rows_by_op[name] = ms.output_rows

    # The filter passes rows where column1 > 1, so exactly
    # 2 rows from (1,'a'),(2,'b'),(3,'c').
    # At least one operator must report exactly 2 output rows (the filter).
    assert 2 in output_rows_by_op.values(), (
        f"Expected an operator with output_rows=2, got {output_rows_by_op}"
    )


def test_metric_properties() -> None:
    ctx = SessionContext()
    ctx.sql("CREATE TABLE t AS VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    df = ctx.sql("SELECT * FROM t WHERE column1 > 1")
    df.collect()
    plan = df.execution_plan()

    found_any_metric = False
    for _, ms in plan.collect_metrics():
        r = repr(ms)
        assert isinstance(r, str)
        for metric in ms.metrics():
            found_any_metric = True
            assert isinstance(metric, Metric)
            assert isinstance(metric.name, str)
            assert len(metric.name) > 0
            assert metric.partition is None or isinstance(metric.partition, int)
            assert metric.value is None or isinstance(
                metric.value, int | datetime.datetime
            )
            assert isinstance(metric.labels(), dict)
            mr = repr(metric)
            assert isinstance(mr, str)
            assert len(mr) > 0
    assert found_any_metric, "Expected at least one metric after execution"


def test_no_meaningful_metrics_before_execution() -> None:
    ctx = SessionContext()
    ctx.sql("CREATE TABLE t AS VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    df = ctx.sql("SELECT * FROM t WHERE column1 > 1")
    plan_before = df.execution_plan()

    # Some plan nodes (e.g. DataSourceExec) eagerly initialize a MetricsSet,
    # so metrics() may return a set even before execution.  However, no rows
    # should have been processed yet — output_rows must be absent or zero.
    for _, ms in plan_before.collect_metrics():
        rows = ms.output_rows
        assert rows is None or rows == 0, (
            f"Expected 0 output_rows before execution, got {rows}"
        )

    # After execution, at least one operator must report rows processed.
    df.collect()
    plan_after = df.execution_plan()
    output_rows_after = [
        ms.output_rows
        for _, ms in plan_after.collect_metrics()
        if ms.output_rows is not None and ms.output_rows > 0
    ]
    assert len(output_rows_after) > 0, "Expected output_rows > 0 after execution"


def test_collect_partitioned_metrics() -> None:
    ctx = SessionContext()
    ctx.sql("CREATE TABLE t AS VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    df = ctx.sql("SELECT * FROM t WHERE column1 > 1")

    df.collect_partitioned()
    plan = df.execution_plan()

    output_rows_values = [
        ms.output_rows for _, ms in plan.collect_metrics() if ms.output_rows is not None
    ]
    assert 2 in output_rows_values, f"Expected 2 in {output_rows_values}"


def test_execute_stream_metrics() -> None:
    ctx = SessionContext()
    ctx.sql("CREATE TABLE t AS VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    df = ctx.sql("SELECT * FROM t WHERE column1 > 1")

    for _ in df.execute_stream():
        pass

    plan = df.execution_plan()
    output_rows_values = [
        ms.output_rows for _, ms in plan.collect_metrics() if ms.output_rows is not None
    ]
    assert 2 in output_rows_values, f"Expected 2 in {output_rows_values}"


def test_execute_stream_partitioned_metrics() -> None:
    ctx = SessionContext()
    ctx.sql("CREATE TABLE t AS VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    df = ctx.sql("SELECT * FROM t WHERE column1 > 1")

    for stream in df.execute_stream_partitioned():
        for _ in stream:
            pass

    plan = df.execution_plan()
    output_rows_values = [
        ms.output_rows for _, ms in plan.collect_metrics() if ms.output_rows is not None
    ]
    assert 2 in output_rows_values, f"Expected 2 in {output_rows_values}"


def test_value_as_datetime() -> None:
    ctx = SessionContext()
    ctx.sql("CREATE TABLE t AS VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    df = ctx.sql("SELECT * FROM t WHERE column1 > 1")
    df.collect()
    plan = df.execution_plan()

    for _, ms in plan.collect_metrics():
        for metric in ms.metrics():
            if metric.name in ("start_timestamp", "end_timestamp"):
                dt = metric.value_as_datetime
                assert dt is None or isinstance(dt, datetime.datetime)
                if dt is not None:
                    assert dt.tzinfo is not None
            else:
                assert metric.value_as_datetime is None


def test_metric_names_and_labels() -> None:
    """Verify that known metric names appear and labels are well-formed."""
    ctx = SessionContext()
    ctx.sql("CREATE TABLE t AS VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    df = ctx.sql("SELECT * FROM t WHERE column1 > 1")
    df.collect()
    plan = df.execution_plan()

    all_metric_names: set[str] = set()
    for _, ms in plan.collect_metrics():
        for metric in ms.metrics():
            all_metric_names.add(metric.name)
            # Labels must be a dict of str->str
            labels = metric.labels()
            for k, v in labels.items():
                assert isinstance(k, str)
                assert isinstance(v, str)

    # After a filter query, we expect at minimum these standard metric names.
    assert "output_rows" in all_metric_names, (
        f"Expected 'output_rows' in {all_metric_names}"
    )
    assert "elapsed_compute" in all_metric_names, (
        f"Expected 'elapsed_compute' in {all_metric_names}"
    )


def test_collect_twice_has_metrics() -> None:
    ctx = SessionContext()
    ctx.sql("CREATE TABLE t AS VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    df = ctx.sql("SELECT * FROM t WHERE column1 > 1")

    df.collect()
    df.collect()

    plan = df.execution_plan()
    output_rows_values = [
        ms.output_rows for _, ms in plan.collect_metrics() if ms.output_rows is not None
    ]
    assert len(output_rows_values) > 0
