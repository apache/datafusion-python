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
import os
from typing import Any

import pyarrow as pa
from pyarrow.csv import write_csv
import pyarrow.parquet as pq
import pytest

from datafusion import functions as f
from datafusion import (
    DataFrame,
    SessionContext,
    WindowFrame,
    column,
    literal,
    udf,
)


@pytest.fixture
def ctx():
    return SessionContext()


@pytest.fixture
def df():
    ctx = SessionContext()

    # create a RecordBatch and a new DataFrame from it
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 5, 6]), pa.array([8, 5, 8])],
        names=["a", "b", "c"],
    )

    return ctx.from_arrow(batch)


@pytest.fixture
def struct_df():
    ctx = SessionContext()

    # create a RecordBatch and a new DataFrame from it
    batch = pa.RecordBatch.from_arrays(
        [pa.array([{"c": 1}, {"c": 2}, {"c": 3}]), pa.array([4, 5, 6])],
        names=["a", "b"],
    )

    return ctx.create_dataframe([[batch]])


@pytest.fixture
def nested_df():
    ctx = SessionContext()

    # create a RecordBatch and a new DataFrame from it
    # Intentionally make each array of different length
    batch = pa.RecordBatch.from_arrays(
        [pa.array([[1], [2, 3], [4, 5, 6], None]), pa.array([7, 8, 9, 10])],
        names=["a", "b"],
    )

    return ctx.create_dataframe([[batch]])


@pytest.fixture
def aggregate_df():
    ctx = SessionContext()
    ctx.register_csv("test", "testing/data/csv/aggregate_test_100.csv")
    return ctx.sql("select c1, sum(c2) from test group by c1")


@pytest.fixture
def partitioned_df():
    ctx = SessionContext()

    # create a RecordBatch and a new DataFrame from it
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array([0, 1, 2, 3, 4, 5, 6]),
            pa.array([7, None, 7, 8, 9, None, 9]),
            pa.array(["A", "A", "A", "A", "B", "B", "B"]),
        ],
        names=["a", "b", "c"],
    )

    return ctx.create_dataframe([[batch]])


def test_select(df):
    df = df.select(
        column("a") + column("b"),
        column("a") - column("b"),
    )

    # execute and collect the first (and only) batch
    result = df.collect()[0]

    assert result.column(0) == pa.array([5, 7, 9])
    assert result.column(1) == pa.array([-3, -3, -3])


def test_select_mixed_expr_string(df):
    df = df.select_columns(column("b"), "a")

    # execute and collect the first (and only) batch
    result = df.collect()[0]

    assert result.column(0) == pa.array([4, 5, 6])
    assert result.column(1) == pa.array([1, 2, 3])


def test_select_columns(df):
    df = df.select_columns("b", "a")

    # execute and collect the first (and only) batch
    result = df.collect()[0]

    assert result.column(0) == pa.array([4, 5, 6])
    assert result.column(1) == pa.array([1, 2, 3])


def test_filter(df):
    df1 = df.filter(column("a") > literal(2)).select(
        column("a") + column("b"),
        column("a") - column("b"),
    )

    # execute and collect the first (and only) batch
    result = df1.collect()[0]

    assert result.column(0) == pa.array([9])
    assert result.column(1) == pa.array([-3])

    df.show()
    # verify that if there is no filter applied, internal dataframe is unchanged
    df2 = df.filter()
    assert df.df == df2.df

    df3 = df.filter(column("a") > literal(1), column("b") != literal(6))
    result = df3.collect()[0]

    assert result.column(0) == pa.array([2])
    assert result.column(1) == pa.array([5])
    assert result.column(2) == pa.array([5])


def test_sort(df):
    df = df.sort(column("b").sort(ascending=False))

    table = pa.Table.from_batches(df.collect())
    expected = {"a": [3, 2, 1], "b": [6, 5, 4], "c": [8, 5, 8]}

    assert table.to_pydict() == expected


def test_limit(df):
    df = df.limit(1)

    # execute and collect the first (and only) batch
    result = df.collect()[0]

    assert len(result.column(0)) == 1
    assert len(result.column(1)) == 1


def test_limit_with_offset(df):
    # only 3 rows, but limit past the end to ensure that offset is working
    df = df.limit(5, offset=2)

    # execute and collect the first (and only) batch
    result = df.collect()[0]

    assert len(result.column(0)) == 1
    assert len(result.column(1)) == 1


def test_with_column(df):
    df = df.with_column("c", column("a") + column("b"))

    # execute and collect the first (and only) batch
    result = df.collect()[0]

    assert result.schema.field(0).name == "a"
    assert result.schema.field(1).name == "b"
    assert result.schema.field(2).name == "c"

    assert result.column(0) == pa.array([1, 2, 3])
    assert result.column(1) == pa.array([4, 5, 6])
    assert result.column(2) == pa.array([5, 7, 9])


def test_with_column_renamed(df):
    df = df.with_column("c", column("a") + column("b")).with_column_renamed("c", "sum")

    result = df.collect()[0]

    assert result.schema.field(0).name == "a"
    assert result.schema.field(1).name == "b"
    assert result.schema.field(2).name == "sum"


def test_unnest(nested_df):
    nested_df = nested_df.unnest_columns("a")

    # execute and collect the first (and only) batch
    result = nested_df.collect()[0]

    assert result.column(0) == pa.array([1, 2, 3, 4, 5, 6, None])
    assert result.column(1) == pa.array([7, 8, 8, 9, 9, 9, 10])


def test_unnest_without_nulls(nested_df):
    nested_df = nested_df.unnest_columns("a", preserve_nulls=False)

    # execute and collect the first (and only) batch
    result = nested_df.collect()[0]

    assert result.column(0) == pa.array([1, 2, 3, 4, 5, 6])
    assert result.column(1) == pa.array([7, 8, 8, 9, 9, 9])


def test_udf(df):
    # is_null is a pa function over arrays
    is_null = udf(
        lambda x: x.is_null(),
        [pa.int64()],
        pa.bool_(),
        volatility="immutable",
    )

    df = df.select(is_null(column("a")))
    result = df.collect()[0].column(0)

    assert result == pa.array([False, False, False])


def test_join():
    ctx = SessionContext()

    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
        names=["a", "b"],
    )
    df = ctx.create_dataframe([[batch]], "l")

    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2]), pa.array([8, 10])],
        names=["a", "c"],
    )
    df1 = ctx.create_dataframe([[batch]], "r")

    df = df.join(df1, join_keys=(["a"], ["a"]), how="inner")
    df.show()
    df = df.sort(column("l.a"))
    table = pa.Table.from_batches(df.collect())

    expected = {"a": [1, 2], "c": [8, 10], "b": [4, 5]}
    assert table.to_pydict() == expected


def test_distinct():
    ctx = SessionContext()

    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3, 1, 2, 3]), pa.array([4, 5, 6, 4, 5, 6])],
        names=["a", "b"],
    )
    df_a = ctx.create_dataframe([[batch]]).distinct().sort(column("a"))

    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
        names=["a", "b"],
    )
    df_b = ctx.create_dataframe([[batch]]).sort(column("a"))

    assert df_a.collect() == df_b.collect()


data_test_window_functions = [
    (
        "row",
        f.row_number(order_by=[column("b"), column("a").sort(ascending=False)]),
        [4, 2, 3, 5, 7, 1, 6],
    ),
    (
        "row_w_params",
        f.row_number(
            order_by=[column("b"), column("a")],
            partition_by=[column("c")],
        ),
        [2, 1, 3, 4, 2, 1, 3],
    ),
    ("rank", f.rank(order_by=[column("b")]), [3, 1, 3, 5, 6, 1, 6]),
    (
        "rank_w_params",
        f.rank(order_by=[column("b"), column("a")], partition_by=[column("c")]),
        [2, 1, 3, 4, 2, 1, 3],
    ),
    (
        "dense_rank",
        f.dense_rank(order_by=[column("b")]),
        [2, 1, 2, 3, 4, 1, 4],
    ),
    (
        "dense_rank_w_params",
        f.dense_rank(order_by=[column("b"), column("a")], partition_by=[column("c")]),
        [2, 1, 3, 4, 2, 1, 3],
    ),
    (
        "percent_rank",
        f.round(f.percent_rank(order_by=[column("b")]), literal(3)),
        [0.333, 0.0, 0.333, 0.667, 0.833, 0.0, 0.833],
    ),
    (
        "percent_rank_w_params",
        f.round(
            f.percent_rank(
                order_by=[column("b"), column("a")], partition_by=[column("c")]
            ),
            literal(3),
        ),
        [0.333, 0.0, 0.667, 1.0, 0.5, 0.0, 1.0],
    ),
    (
        "cume_dist",
        f.round(f.cume_dist(order_by=[column("b")]), literal(3)),
        [0.571, 0.286, 0.571, 0.714, 1.0, 0.286, 1.0],
    ),
    (
        "cume_dist_w_params",
        f.round(
            f.cume_dist(
                order_by=[column("b"), column("a")], partition_by=[column("c")]
            ),
            literal(3),
        ),
        [0.5, 0.25, 0.75, 1.0, 0.667, 0.333, 1.0],
    ),
    (
        "ntile",
        f.ntile(2, order_by=[column("b")]),
        [1, 1, 1, 2, 2, 1, 2],
    ),
    (
        "ntile_w_params",
        f.ntile(2, order_by=[column("b"), column("a")], partition_by=[column("c")]),
        [1, 1, 2, 2, 1, 1, 2],
    ),
    ("lead", f.lead(column("b"), order_by=[column("b")]), [7, None, 8, 9, 9, 7, None]),
    (
        "lead_w_params",
        f.lead(
            column("b"),
            shift_offset=2,
            default_value=-1,
            order_by=[column("b"), column("a")],
            partition_by=[column("c")],
        ),
        [8, 7, -1, -1, -1, 9, -1],
    ),
    ("lag", f.lag(column("b"), order_by=[column("b")]), [None, None, 7, 7, 8, None, 9]),
    (
        "lag_w_params",
        f.lag(
            column("b"),
            shift_offset=2,
            default_value=-1,
            order_by=[column("b"), column("a")],
            partition_by=[column("c")],
        ),
        [-1, -1, None, 7, -1, -1, None],
    ),
    # TODO update all aggregate functions as windows once upstream merges https://github.com/apache/datafusion-python/issues/833
    pytest.param(
        "first_value",
        f.window(
            "first_value",
            [column("a")],
            order_by=[f.order_by(column("b"))],
            partition_by=[column("c")],
        ),
        [1, 1, 1, 1, 5, 5, 5],
    ),
    pytest.param(
        "last_value",
        f.window("last_value", [column("a")])
        .window_frame(WindowFrame("rows", 0, None))
        .order_by(column("b"))
        .partition_by(column("c"))
        .build(),
        [3, 3, 3, 3, 6, 6, 6],
    ),
    pytest.param(
        "3rd_value",
        f.window(
            "nth_value",
            [column("b"), literal(3)],
            order_by=[f.order_by(column("a"))],
        ),
        [None, None, 7, 7, 7, 7, 7],
    ),
    pytest.param(
        "avg",
        f.round(f.window("avg", [column("b")], order_by=[column("a")]), literal(3)),
        [7.0, 7.0, 7.0, 7.333, 7.75, 7.75, 8.0],
    ),
]


@pytest.mark.parametrize("name,expr,result", data_test_window_functions)
def test_window_functions(partitioned_df, name, expr, result):
    df = partitioned_df.select(
        column("a"), column("b"), column("c"), f.alias(expr, name)
    )
    df.sort(column("a")).show()
    table = pa.Table.from_batches(df.collect())

    expected = {
        "a": [0, 1, 2, 3, 4, 5, 6],
        "b": [7, None, 7, 8, 9, None, 9],
        "c": ["A", "A", "A", "A", "B", "B", "B"],
        name: result,
    }

    assert table.sort_by("a").to_pydict() == expected


@pytest.mark.parametrize(
    ("units", "start_bound", "end_bound"),
    [
        (units, start_bound, end_bound)
        for units in ("rows", "range")
        for start_bound in (None, 0, 1)
        for end_bound in (None, 0, 1)
    ]
    + [
        ("groups", 0, 0),
    ],
)
def test_valid_window_frame(units, start_bound, end_bound):
    WindowFrame(units, start_bound, end_bound)


@pytest.mark.parametrize(
    ("units", "start_bound", "end_bound"),
    [
        ("invalid-units", 0, None),
        ("invalid-units", None, 0),
        ("invalid-units", None, None),
        ("groups", None, 0),
        ("groups", 0, None),
        ("groups", None, None),
    ],
)
def test_invalid_window_frame(units, start_bound, end_bound):
    with pytest.raises(RuntimeError):
        WindowFrame(units, start_bound, end_bound)


def test_get_dataframe(tmp_path):
    ctx = SessionContext()

    path = tmp_path / "test.csv"
    table = pa.Table.from_arrays(
        [
            [1, 2, 3, 4],
            ["a", "b", "c", "d"],
            [1.1, 2.2, 3.3, 4.4],
        ],
        names=["int", "str", "float"],
    )
    write_csv(table, path)

    ctx.register_csv("csv", path)

    df = ctx.table("csv")
    assert isinstance(df, DataFrame)


def test_struct_select(struct_df):
    df = struct_df.select(
        column("a")["c"] + column("b"),
        column("a")["c"] - column("b"),
    )

    # execute and collect the first (and only) batch
    result = df.collect()[0]

    assert result.column(0) == pa.array([5, 7, 9])
    assert result.column(1) == pa.array([-3, -3, -3])


def test_explain(df):
    df = df.select(
        column("a") + column("b"),
        column("a") - column("b"),
    )
    df.explain()


def test_logical_plan(aggregate_df):
    plan = aggregate_df.logical_plan()

    expected = "Projection: test.c1, sum(test.c2)"

    assert expected == plan.display()

    expected = (
        "Projection: test.c1, sum(test.c2)\n"
        "  Aggregate: groupBy=[[test.c1]], aggr=[[sum(test.c2)]]\n"
        "    TableScan: test"
    )

    assert expected == plan.display_indent()


def test_optimized_logical_plan(aggregate_df):
    plan = aggregate_df.optimized_logical_plan()

    expected = "Aggregate: groupBy=[[test.c1]], aggr=[[sum(test.c2)]]"

    assert expected == plan.display()

    expected = (
        "Aggregate: groupBy=[[test.c1]], aggr=[[sum(test.c2)]]\n"
        "  TableScan: test projection=[c1, c2]"
    )

    assert expected == plan.display_indent()


def test_execution_plan(aggregate_df):
    plan = aggregate_df.execution_plan()

    expected = (
        "AggregateExec: mode=FinalPartitioned, gby=[c1@0 as c1], aggr=[sum(test.c2)]\n"  # noqa: E501
    )

    assert expected == plan.display()

    # Check the number of partitions is as expected.
    assert isinstance(plan.partition_count, int)

    expected = (
        "ProjectionExec: expr=[c1@0 as c1, SUM(test.c2)@1 as SUM(test.c2)]\n"
        "  Aggregate: groupBy=[[test.c1]], aggr=[[SUM(test.c2)]]\n"
        "    TableScan: test projection=[c1, c2]"
    )

    indent = plan.display_indent()

    # indent plan will be different for everyone due to absolute path
    # to filename, so we just check for some expected content
    assert "AggregateExec:" in indent
    assert "CoalesceBatchesExec:" in indent
    assert "RepartitionExec:" in indent
    assert "CsvExec:" in indent

    ctx = SessionContext()
    stream = ctx.execute(plan, 0)
    # get the one and only batch
    batch = stream.next()
    assert batch is not None
    # there should be no more batches
    batch = stream.next()
    assert batch is None


def test_repartition(df):
    df.repartition(2)


def test_repartition_by_hash(df):
    df.repartition_by_hash(column("a"), num=2)


def test_intersect():
    ctx = SessionContext()

    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
        names=["a", "b"],
    )
    df_a = ctx.create_dataframe([[batch]])

    batch = pa.RecordBatch.from_arrays(
        [pa.array([3, 4, 5]), pa.array([6, 7, 8])],
        names=["a", "b"],
    )
    df_b = ctx.create_dataframe([[batch]])

    batch = pa.RecordBatch.from_arrays(
        [pa.array([3]), pa.array([6])],
        names=["a", "b"],
    )
    df_c = ctx.create_dataframe([[batch]]).sort(column("a"))

    df_a_i_b = df_a.intersect(df_b).sort(column("a"))

    assert df_c.collect() == df_a_i_b.collect()


def test_except_all():
    ctx = SessionContext()

    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
        names=["a", "b"],
    )
    df_a = ctx.create_dataframe([[batch]])

    batch = pa.RecordBatch.from_arrays(
        [pa.array([3, 4, 5]), pa.array([6, 7, 8])],
        names=["a", "b"],
    )
    df_b = ctx.create_dataframe([[batch]])

    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2]), pa.array([4, 5])],
        names=["a", "b"],
    )
    df_c = ctx.create_dataframe([[batch]]).sort(column("a"))

    df_a_e_b = df_a.except_all(df_b).sort(column("a"))

    assert df_c.collect() == df_a_e_b.collect()


def test_collect_partitioned():
    ctx = SessionContext()

    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
        names=["a", "b"],
    )

    assert [[batch]] == ctx.create_dataframe([[batch]]).collect_partitioned()


def test_union(ctx):
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
        names=["a", "b"],
    )
    df_a = ctx.create_dataframe([[batch]])

    batch = pa.RecordBatch.from_arrays(
        [pa.array([3, 4, 5]), pa.array([6, 7, 8])],
        names=["a", "b"],
    )
    df_b = ctx.create_dataframe([[batch]])

    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3, 3, 4, 5]), pa.array([4, 5, 6, 6, 7, 8])],
        names=["a", "b"],
    )
    df_c = ctx.create_dataframe([[batch]]).sort(column("a"))

    df_a_u_b = df_a.union(df_b).sort(column("a"))

    assert df_c.collect() == df_a_u_b.collect()


def test_union_distinct(ctx):
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
        names=["a", "b"],
    )
    df_a = ctx.create_dataframe([[batch]])

    batch = pa.RecordBatch.from_arrays(
        [pa.array([3, 4, 5]), pa.array([6, 7, 8])],
        names=["a", "b"],
    )
    df_b = ctx.create_dataframe([[batch]])

    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3, 4, 5]), pa.array([4, 5, 6, 7, 8])],
        names=["a", "b"],
    )
    df_c = ctx.create_dataframe([[batch]]).sort(column("a"))

    df_a_u_b = df_a.union(df_b, True).sort(column("a"))

    assert df_c.collect() == df_a_u_b.collect()
    assert df_c.collect() == df_a_u_b.collect()


def test_cache(df):
    assert df.cache().collect() == df.collect()


def test_count(df):
    # Get number of rows
    assert df.count() == 3


def test_to_pandas(df):
    # Skip test if pandas is not installed
    pd = pytest.importorskip("pandas")

    # Convert datafusion dataframe to pandas dataframe
    pandas_df = df.to_pandas()
    assert isinstance(pandas_df, pd.DataFrame)
    assert pandas_df.shape == (3, 3)
    assert set(pandas_df.columns) == {"a", "b", "c"}


def test_empty_to_pandas(df):
    # Skip test if pandas is not installed
    pd = pytest.importorskip("pandas")

    # Convert empty datafusion dataframe to pandas dataframe
    pandas_df = df.limit(0).to_pandas()
    assert isinstance(pandas_df, pd.DataFrame)
    assert pandas_df.shape == (0, 3)
    assert set(pandas_df.columns) == {"a", "b", "c"}


def test_to_polars(df):
    # Skip test if polars is not installed
    pl = pytest.importorskip("polars")

    # Convert datafusion dataframe to polars dataframe
    polars_df = df.to_polars()
    assert isinstance(polars_df, pl.DataFrame)
    assert polars_df.shape == (3, 3)
    assert set(polars_df.columns) == {"a", "b", "c"}


def test_empty_to_polars(df):
    # Skip test if polars is not installed
    pl = pytest.importorskip("polars")

    # Convert empty datafusion dataframe to polars dataframe
    polars_df = df.limit(0).to_polars()
    assert isinstance(polars_df, pl.DataFrame)
    assert polars_df.shape == (0, 3)
    assert set(polars_df.columns) == {"a", "b", "c"}


def test_to_arrow_table(df):
    # Convert datafusion dataframe to pyarrow Table
    pyarrow_table = df.to_arrow_table()
    assert isinstance(pyarrow_table, pa.Table)
    assert pyarrow_table.shape == (3, 3)
    assert set(pyarrow_table.column_names) == {"a", "b", "c"}


def test_execute_stream(df):
    stream = df.execute_stream()
    for s in stream:
        print(type(s))
    assert all(batch is not None for batch in stream)
    assert not list(stream)  # after one iteration the generator must be exhausted


@pytest.mark.parametrize("schema", [True, False])
def test_execute_stream_to_arrow_table(df, schema):
    stream = df.execute_stream()

    if schema:
        pyarrow_table = pa.Table.from_batches(
            (batch.to_pyarrow() for batch in stream), schema=df.schema()
        )
    else:
        pyarrow_table = pa.Table.from_batches((batch.to_pyarrow() for batch in stream))

    assert isinstance(pyarrow_table, pa.Table)
    assert pyarrow_table.shape == (3, 3)
    assert set(pyarrow_table.column_names) == {"a", "b", "c"}


def test_execute_stream_partitioned(df):
    streams = df.execute_stream_partitioned()
    assert all(batch is not None for stream in streams for batch in stream)
    assert all(
        not list(stream) for stream in streams
    )  # after one iteration all generators must be exhausted


def test_empty_to_arrow_table(df):
    # Convert empty datafusion dataframe to pyarrow Table
    pyarrow_table = df.limit(0).to_arrow_table()
    assert isinstance(pyarrow_table, pa.Table)
    assert pyarrow_table.shape == (0, 3)
    assert set(pyarrow_table.column_names) == {"a", "b", "c"}


def test_to_pylist(df):
    # Convert datafusion dataframe to Python list
    pylist = df.to_pylist()
    assert isinstance(pylist, list)
    assert pylist == [
        {"a": 1, "b": 4, "c": 8},
        {"a": 2, "b": 5, "c": 5},
        {"a": 3, "b": 6, "c": 8},
    ]


def test_to_pydict(df):
    # Convert datafusion dataframe to Python dictionary
    pydict = df.to_pydict()
    assert isinstance(pydict, dict)
    assert pydict == {"a": [1, 2, 3], "b": [4, 5, 6], "c": [8, 5, 8]}


def test_describe(df):
    # Calculate statistics
    df = df.describe()

    # Collect the result
    result = df.to_pydict()

    assert result == {
        "describe": [
            "count",
            "null_count",
            "mean",
            "std",
            "min",
            "max",
            "median",
        ],
        "a": [3.0, 0.0, 2.0, 1.0, 1.0, 3.0, 2.0],
        "b": [3.0, 0.0, 5.0, 1.0, 4.0, 6.0, 5.0],
        "c": [3.0, 0.0, 7.0, 1.7320508075688772, 5.0, 8.0, 8.0],
    }


@pytest.mark.parametrize("path_to_str", (True, False))
def test_write_csv(ctx, df, tmp_path, path_to_str):
    path = str(tmp_path) if path_to_str else tmp_path

    df.write_csv(path, with_header=True)

    ctx.register_csv("csv", path)
    result = ctx.table("csv").to_pydict()
    expected = df.to_pydict()

    assert result == expected


@pytest.mark.parametrize("path_to_str", (True, False))
def test_write_json(ctx, df, tmp_path, path_to_str):
    path = str(tmp_path) if path_to_str else tmp_path

    df.write_json(path)

    ctx.register_json("json", path)
    result = ctx.table("json").to_pydict()
    expected = df.to_pydict()

    assert result == expected


@pytest.mark.parametrize("path_to_str", (True, False))
def test_write_parquet(df, tmp_path, path_to_str):
    path = str(tmp_path) if path_to_str else tmp_path

    df.write_parquet(str(path))
    result = pq.read_table(str(path)).to_pydict()
    expected = df.to_pydict()

    assert result == expected


@pytest.mark.parametrize(
    "compression, compression_level",
    [("gzip", 6), ("brotli", 7), ("zstd", 15)],
)
def test_write_compressed_parquet(df, tmp_path, compression, compression_level):
    path = tmp_path

    df.write_parquet(
        str(path), compression=compression, compression_level=compression_level
    )

    # test that the actual compression scheme is the one written
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith(".parquet"):
                metadata = pq.ParquetFile(tmp_path / file).metadata.to_dict()
                for row_group in metadata["row_groups"]:
                    for columns in row_group["columns"]:
                        assert columns["compression"].lower() == compression

    result = pq.read_table(str(path)).to_pydict()
    expected = df.to_pydict()

    assert result == expected


@pytest.mark.parametrize(
    "compression, compression_level",
    [("gzip", 12), ("brotli", 15), ("zstd", 23), ("wrong", 12)],
)
def test_write_compressed_parquet_wrong_compression_level(
    df, tmp_path, compression, compression_level
):
    path = tmp_path

    with pytest.raises(ValueError):
        df.write_parquet(
            str(path),
            compression=compression,
            compression_level=compression_level,
        )


@pytest.mark.parametrize("compression", ["brotli", "zstd", "wrong"])
def test_write_compressed_parquet_missing_compression_level(df, tmp_path, compression):
    path = tmp_path

    with pytest.raises(ValueError):
        df.write_parquet(str(path), compression=compression)


def test_dataframe_export(df) -> None:
    # Guarantees that we have the canonical implementation
    # reading our dataframe export
    table = pa.table(df)
    assert table.num_columns == 3
    assert table.num_rows == 3

    desired_schema = pa.schema([("a", pa.int64())])

    # Verify we can request a schema
    table = pa.table(df, schema=desired_schema)
    assert table.num_columns == 1
    assert table.num_rows == 3

    # Expect a table of nulls if the schema don't overlap
    desired_schema = pa.schema([("g", pa.string())])
    table = pa.table(df, schema=desired_schema)
    assert table.num_columns == 1
    assert table.num_rows == 3
    for i in range(0, 3):
        assert table[0][i].as_py() is None

    # Expect an error when we cannot convert schema
    desired_schema = pa.schema([("a", pa.float32())])
    failed_convert = False
    try:
        table = pa.table(df, schema=desired_schema)
    except Exception:
        failed_convert = True
    assert failed_convert

    # Expect an error when we have a not set non-nullable
    desired_schema = pa.schema([("g", pa.string(), False)])
    failed_convert = False
    try:
        table = pa.table(df, schema=desired_schema)
    except Exception:
        failed_convert = True
    assert failed_convert


def test_dataframe_transform(df):
    def add_string_col(df_internal) -> DataFrame:
        return df_internal.with_column("string_col", literal("string data"))

    def add_with_parameter(df_internal, value: Any) -> DataFrame:
        return df_internal.with_column("new_col", literal(value))

    df = df.transform(add_string_col).transform(add_with_parameter, 3)

    result = df.to_pydict()

    assert result["a"] == [1, 2, 3]
    assert result["string_col"] == ["string data" for _i in range(0, 3)]
    assert result["new_col"] == [3 for _i in range(0, 3)]


def test_dataframe_repr_html(df) -> None:
    output = df._repr_html_()

    ref_html = """<table border='1'>
        <tr><th>a</td><th>b</td><th>c</td></tr>
        <tr><td>1</td><td>4</td><td>8</td></tr>
        <tr><td>2</td><td>5</td><td>5</td></tr>
        <tr><td>3</td><td>6</td><td>8</td></tr>
        </table>
        """

    # Ignore whitespace just to make this test look cleaner
    assert output.replace(" ", "") == ref_html.replace(" ", "")
