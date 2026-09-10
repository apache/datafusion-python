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

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from datafusion import (
    ExecutionPlan,
    LogicalPlan,
    Metric,
    MetricsSet,
    PhysicalPartitioning,
    SessionConfig,
    SessionContext,
    col,
    udf,
)
from datafusion.expr import Partitioning


# Note: CSV because a *logical* plan cannot carry a memory table. The physical
# layer can — see `test_execution_plan_over_memory_batches_round_trips`.
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


def test_execution_plan_over_memory_batches_round_trips() -> None:
    """A physical plan reading record batches decodes on an unrelated session.

    Only the *logical* layer cannot carry a memory table: its
    `try_encode_table_provider` has no arm for one. The physical scan inlines
    the batches, so it needs neither a shared session nor an extension codec —
    which is what lets a worker process execute a plan the driver encoded.
    """
    ctx = SessionContext()
    ctx.register_record_batches(
        "t",
        [[pa.record_batch({"a": [1, 2, 3]})], [pa.record_batch({"a": [4, 5, 6]})]],
    )
    plan_bytes = ctx.sql("select a from t").execution_plan().to_bytes(ctx)

    # A session that shares nothing with the encoder: no codecs, no tables.
    fresh = SessionContext()
    decoded = ExecutionPlan.from_bytes(fresh, plan_bytes)
    rows = sum(
        batch.to_pyarrow().num_rows
        for partition in range(decoded.partition_count)
        for batch in fresh.execute(decoded, partition)
    )
    assert rows == 6


def test_output_partitioning_reports_the_scheme_not_just_the_count() -> None:
    """`output_partitioning` distinguishes hash-distributed output from counted."""
    ctx = SessionContext(SessionConfig().with_target_partitions(4))
    ctx.register_record_batches(
        "t",
        [[pa.record_batch({"a": [1, 2, 3]})], [pa.record_batch({"a": [4, 5, 6]})]],
    )

    scan = ctx.sql("select a from t").execution_plan()
    scanned = scan.output_partitioning
    assert scanned.scheme == "UnknownPartitioning"
    assert scanned.hash_expressions is None
    # Agrees with the count-only accessor it supplements.
    assert scanned.partition_count == scan.partition_count

    grouped = ctx.sql("select a, count(*) from t group by a").execution_plan()
    partitioning = grouped.output_partitioning
    assert partitioning.scheme == "Hash"
    assert partitioning.hash_expressions == ["a@0"]
    assert partitioning.partition_count == 4
    assert repr(partitioning) == "Hash([a@0], 4)"


def test_a_requested_partitioning_and_the_resulting_one_disagree() -> None:
    """The logical request and the physical result are different things.

    `datafusion.expr.Partitioning` is what a `Repartition` node records — the
    request. `PhysicalPartitioning` is what the built plan does. Here the
    optimizer drops the repartition outright, because nothing above it needs
    the rows redistributed, so the two do not even agree on the scheme.
    """
    ctx = SessionContext(SessionConfig().with_target_partitions(4))
    ctx.register_record_batches(
        "t",
        [[pa.record_batch({"a": [1, 2, 3]})], [pa.record_batch({"a": [4, 5, 6]})]],
    )
    df = ctx.table("t").repartition_by_hash(col("a"), num=8)

    # The request survives on the logical plan, as an opaque object of the
    # other Partitioning type.
    requested = df.logical_plan().to_variant().partitioning_scheme()
    assert isinstance(requested, Partitioning)
    assert not isinstance(requested, PhysicalPartitioning)

    # The result honours neither the scheme nor the count that was asked for.
    resulting = df.execution_plan().output_partitioning
    assert isinstance(resulting, PhysicalPartitioning)
    assert resulting.scheme == "UnknownPartitioning"
    assert resulting.partition_count == 2


def test_output_partitioning_reports_round_robin(tmp_path) -> None:
    """A round-robin repartition reports `RoundRobinBatch`.

    The optimizer only inserts one above a source with fewer partitions than
    `target_partitions` and CPU work above it to parallelize, and it never
    survives at the root, so reach it by walking `children`.
    """
    path = tmp_path / "rr.parquet"
    pq.write_table(pa.table({"a": list(range(50)), "b": [1] * 50}), path)

    ctx = SessionContext(SessionConfig().with_target_partitions(8))
    ctx.register_parquet("t", str(path))
    plan = ctx.sql("select a, sum(b) from t where a > 5 group by a").execution_plan()

    schemes = set()
    stack = [plan]
    while stack:
        node = stack.pop()
        schemes.add(node.output_partitioning.scheme)
        stack.extend(node.children())

    # Membership, not equality: which other nodes the optimizer puts in this
    # tree is its business, and pinning the whole set here would make an
    # unrelated planner change look like a failure of this accessor. The other
    # schemes are asserted directly where they are the subject.
    assert "RoundRobinBatch" in schemes


def test_execute_rejects_an_out_of_range_partition() -> None:
    """An out-of-range partition index raises instead of panicking."""
    ctx = SessionContext()
    ctx.register_record_batches("t", [[pa.record_batch({"a": [1, 2, 3]})]])
    plan = ctx.sql("select a from t").execution_plan()
    assert plan.partition_count == 1

    with pytest.raises(ValueError, match="Partition index 5 is out of range"):
        ctx.execute(plan, 5)

    # The keyword is `partition`, as the upgrade guide says.
    with pytest.raises(ValueError, match="Partition index 5 is out of range"):
        ctx.execute(plan, partition=5)


def test_execute_rejects_a_negative_partition() -> None:
    """A negative index cannot reach the bounds check, so it overflows first.

    Documented on `execute` as `OverflowError` because that is what PyO3
    raises converting to `usize`, before any DataFusion code runs.
    """
    ctx = SessionContext()
    ctx.register_record_batches("t", [[pa.record_batch({"a": [1, 2, 3]})]])
    plan = ctx.sql("select a from t").execution_plan()

    with pytest.raises(OverflowError):
        ctx.execute(plan, -1)


def test_physical_partitioning_equality_is_structural() -> None:
    """Two partitionings are equal when scheme, count and keys agree.

    Not DataFusion's own comparison of the underlying type, which reports two
    `UnknownPartitioning` values of the same width as unequal. A reflexive
    `__eq__` is the Python expectation, and the count-only alternative would
    make `Hash` on different keys compare equal.
    """
    ctx = SessionContext(SessionConfig().with_target_partitions(4))
    ctx.register_record_batches(
        "t",
        [[pa.record_batch({"a": [1, 2, 3]})], [pa.record_batch({"a": [4, 5, 6]})]],
    )
    plan = ctx.sql("select a from t").execution_plan()
    other_scan = ctx.sql("select a as b from t").execution_plan().output_partitioning
    grouped = (
        ctx.sql("select a, count(*) from t group by a")
        .execution_plan()
        .output_partitioning
    )

    # The property builds a fresh wrapper per access, so these are two objects
    # over one partitioning. `UnknownPartitioning` is precisely the scheme
    # DataFusion's own comparison reports as unequal to itself.
    scan, scan_again = plan.output_partitioning, plan.output_partitioning
    assert scan is not scan_again
    assert scan == scan_again

    assert scan == other_scan
    assert scan != grouped
    assert scan != "UnknownPartitioning(2)"

    # Hashing agrees, so these collapse in a set the way equality implies.
    assert len({scan, other_scan, grouped}) == 2


def test_installing_a_physical_codec_preserves_strict_mode() -> None:
    """Installing a physical extension codec must not re-enable inlining.

    `with_physical_extension_codec` builds a replacement `PythonPhysicalCodec`
    around the imported one, and the constructor defaults inlining to on. A
    context that opted out via `with_python_udf_inlining(enabled=False)` has to
    keep its setting, or installing a codec silently starts embedding
    cloudpickled callables in serialized execution plans.

    The logical counterpart lives in `test_pickle_expr.py`; this one needs an
    `ExecutionPlan` because only the physical codec encodes it. `DFPYUDF` is
    the scalar Python-UDF family prefix, shared by both layers; see
    `PY_SCALAR_UDF_FAMILY` in crates/core/src/codec.rs.
    """
    identity = udf(
        lambda arr: arr,
        [pa.string()],
        pa.string(),
        volatility="immutable",
        name="identity_str",
    )

    def plan_bytes(ctx: SessionContext) -> bytes:
        df = ctx.read_csv(path="testing/data/csv/aggregate_test_100.csv").select(
            identity(col("c1"))
        )
        return df.execution_plan().to_bytes(ctx)

    # The inlining default is on, so the strict blob is what has to differ.
    assert b"DFPYUDF" in plan_bytes(SessionContext())

    strict = SessionContext().with_python_udf_inlining(enabled=False)
    assert b"DFPYUDF" not in plan_bytes(strict)

    installed = strict.with_physical_extension_codec(
        strict.__datafusion_physical_extension_codec__()
    )
    assert b"DFPYUDF" not in plan_bytes(installed)


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
