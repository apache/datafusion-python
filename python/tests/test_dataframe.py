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
import re
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from datafusion import (
    DataFrame,
    SessionContext,
    WindowFrame,
    column,
    literal,
)
from datafusion import functions as f
from datafusion.context import DataframeDisplayConfig
from datafusion.expr import Window
from pyarrow.csv import write_csv


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
def data():
    return [{"a": 1, "b": "x" * 50, "c": 3}] * 100


@pytest.fixture
def span_expandable_class():
    return '<span class="expandable" id="'


def test_display_config():
    # Test display_config initialization
    config = DataframeDisplayConfig(
        max_table_bytes=1024,
        min_table_rows=10,
        max_cell_length=15,
        max_table_rows_in_repr=5,
    )

    assert config.max_table_bytes == 1024
    assert config.min_table_rows == 10
    assert config.max_cell_length == 15
    assert config.max_table_rows_in_repr == 5

    # Test property setters
    config.max_table_bytes = 2048
    config.min_table_rows = 20
    config.max_cell_length = 30
    config.max_table_rows_in_repr = 10

    assert config.max_table_bytes == 2048
    assert config.min_table_rows == 20
    assert config.max_cell_length == 30
    assert config.max_table_rows_in_repr == 10

    # Test property setter validation
    with pytest.raises(ValueError, match="max_table_bytes must be greater than 0"):
        config.max_table_bytes = 0

    with pytest.raises(ValueError, match="min_table_rows must be greater than 0"):
        config.min_table_rows = -1

    with pytest.raises(ValueError, match="max_cell_length must be greater than 0"):
        config.max_cell_length = 0

    with pytest.raises(
        ValueError, match="max_table_rows_in_repr must be greater than 0"
    ):
        config.max_table_rows_in_repr = -5


def test_session_with_display_config(data, span_expandable_class):
    # Test with_display_config returns a new context with updated config
    ctx = SessionContext()

    # Verify the default values are used initially
    df = ctx.from_pylist(data)
    html_repr = df._repr_html_()

    # Create a new context with custom display config
    ctx2 = ctx.with_display_config(
        max_table_bytes=1024,
        min_table_rows=5,
        max_cell_length=10,
        max_table_rows_in_repr=3,
    )

    # Create a dataframe with the same data but using the new context
    df2 = ctx2.from_pylist(data)
    html_repr2 = df2._repr_html_()

    # The HTML representation should be different with different display configs
    assert html_repr != html_repr2

    # Check that the second representation has the short cell data based on the
    # configured length
    assert span_expandable_class in html_repr2
    assert f">{('x' * 10)}</span>" in html_repr2


def test_display_config_in_init(data):
    # Test default display config directly in SessionContext constructor

    ctx = SessionContext()
    df1 = ctx.from_pylist(data)
    html_repr1 = df1._repr_html_()

    # Create a context with custom display config through the with_display_config method
    ctx2 = ctx.with_display_config(
        max_table_bytes=1024,
        min_table_rows=5,
        max_cell_length=10,
        max_table_rows_in_repr=3,
    )
    df2 = ctx2.from_pylist(data)
    html_repr2 = df2._repr_html_()

    # Both methods should result in equivalent display configuration
    assert html_repr1 != html_repr2


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
    df_1 = df.select(
        column("a") + column("b"),
        column("a") - column("b"),
    )

    # execute and collect the first (and only) batch
    result = df_1.collect()[0]

    assert result.column(0) == pa.array([5, 7, 9])
    assert result.column(1) == pa.array([-3, -3, -3])

    df_2 = df.select("b", "a")

    # execute and collect the first (and only) batch
    result = df_2.collect()[0]

    assert result.column(0) == pa.array([4, 5, 6])
    assert result.column(1) == pa.array([1, 2, 3])


def test_select_mixed_expr_string(df):
    df = df.select(column("b"), "a")

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


def test_drop(df):
    df = df.drop("c")

    # execute and collect the first (and only) batch
    result = df.collect()[0]

    assert df.schema().names == ["a", "b"]
    assert result.column(0) == pa.array([1, 2, 3])
    assert result.column(1) == pa.array([4, 5, 6])


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


def test_head(df):
    df = df.head(1)

    # execute and collect the first (and only) batch
    result = df.collect()[0]

    assert result.column(0) == pa.array([1])
    assert result.column(1) == pa.array([4])
    assert result.column(2) == pa.array([8])


def test_tail(df):
    df = df.tail(1)

    # execute and collect the first (and only) batch
    result = df.collect()[0]

    assert result.column(0) == pa.array([3])
    assert result.column(1) == pa.array([6])
    assert result.column(2) == pa.array([8])


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


def test_with_columns(df):
    df = df.with_columns(
        (column("a") + column("b")).alias("c"),
        (column("a") + column("b")).alias("d"),
        [
            (column("a") + column("b")).alias("e"),
            (column("a") + column("b")).alias("f"),
        ],
        g=(column("a") + column("b")),
    )

    # execute and collect the first (and only) batch
    result = df.collect()[0]

    assert result.schema.field(0).name == "a"
    assert result.schema.field(1).name == "b"
    assert result.schema.field(2).name == "c"
    assert result.schema.field(3).name == "d"
    assert result.schema.field(4).name == "e"
    assert result.schema.field(5).name == "f"
    assert result.schema.field(6).name == "g"

    assert result.column(0) == pa.array([1, 2, 3])
    assert result.column(1) == pa.array([4, 5, 6])
    assert result.column(2) == pa.array([5, 7, 9])
    assert result.column(3) == pa.array([5, 7, 9])
    assert result.column(4) == pa.array([5, 7, 9])
    assert result.column(5) == pa.array([5, 7, 9])
    assert result.column(6) == pa.array([5, 7, 9])


def test_cast(df):
    df = df.cast({"a": pa.float16(), "b": pa.list_(pa.uint32())})
    expected = pa.schema(
        [("a", pa.float16()), ("b", pa.list_(pa.uint32())), ("c", pa.int64())]
    )

    assert df.schema() == expected


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


@pytest.mark.filterwarnings("ignore:`join_keys`:DeprecationWarning")
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

    df2 = df.join(df1, on="a", how="inner")
    df2.show()
    df2 = df2.sort(column("l.a"))
    table = pa.Table.from_batches(df2.collect())

    expected = {"a": [1, 2], "c": [8, 10], "b": [4, 5]}
    assert table.to_pydict() == expected

    df2 = df.join(df1, left_on="a", right_on="a", how="inner")
    df2.show()
    df2 = df2.sort(column("l.a"))
    table = pa.Table.from_batches(df2.collect())

    expected = {"a": [1, 2], "c": [8, 10], "b": [4, 5]}
    assert table.to_pydict() == expected

    # Verify we don't make a breaking change to pre-43.0.0
    # where users would pass join_keys as a positional argument
    df2 = df.join(df1, (["a"], ["a"]), how="inner")
    df2.show()
    df2 = df2.sort(column("l.a"))
    table = pa.Table.from_batches(df2.collect())

    expected = {"a": [1, 2], "c": [8, 10], "b": [4, 5]}
    assert table.to_pydict() == expected


def test_join_invalid_params():
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

    with pytest.deprecated_call():
        df2 = df.join(df1, join_keys=(["a"], ["a"]), how="inner")
        df2.show()
        df2 = df2.sort(column("l.a"))
        table = pa.Table.from_batches(df2.collect())

        expected = {"a": [1, 2], "c": [8, 10], "b": [4, 5]}
        assert table.to_pydict() == expected

    with pytest.raises(
        ValueError, match=r"`left_on` or `right_on` should not provided with `on`"
    ):
        df2 = df.join(df1, on="a", how="inner", right_on="test")

    with pytest.raises(
        ValueError, match=r"`left_on` and `right_on` should both be provided."
    ):
        df2 = df.join(df1, left_on="a", how="inner")

    with pytest.raises(
        ValueError, match=r"either `on` or `left_on` and `right_on` should be provided."
    ):
        df2 = df.join(df1, how="inner")


def test_join_on():
    ctx = SessionContext()

    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
        names=["a", "b"],
    )
    df = ctx.create_dataframe([[batch]], "l")

    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2]), pa.array([-8, 10])],
        names=["a", "c"],
    )
    df1 = ctx.create_dataframe([[batch]], "r")

    df2 = df.join_on(df1, column("l.a").__eq__(column("r.a")), how="inner")
    df2.show()
    df2 = df2.sort(column("l.a"))
    table = pa.Table.from_batches(df2.collect())

    expected = {"a": [1, 2], "c": [-8, 10], "b": [4, 5]}
    assert table.to_pydict() == expected

    df3 = df.join_on(
        df1,
        column("l.a").__eq__(column("r.a")),
        column("l.a").__lt__(column("r.c")),
        how="inner",
    )
    df3.show()
    df3 = df3.sort(column("l.a"))
    table = pa.Table.from_batches(df3.collect())
    expected = {"a": [2], "c": [10], "b": [5]}
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
    (
        "first_value",
        f.first_value(column("a")).over(
            Window(partition_by=[column("c")], order_by=[column("b")])
        ),
        [1, 1, 1, 1, 5, 5, 5],
    ),
    (
        "last_value",
        f.last_value(column("a")).over(
            Window(
                partition_by=[column("c")],
                order_by=[column("b")],
                window_frame=WindowFrame("rows", None, None),
            )
        ),
        [3, 3, 3, 3, 6, 6, 6],
    ),
    (
        "3rd_value",
        f.nth_value(column("b"), 3).over(Window(order_by=[column("a")])),
        [None, None, 7, 7, 7, 7, 7],
    ),
    (
        "avg",
        f.round(f.avg(column("b")).over(Window(order_by=[column("a")])), literal(3)),
        [7.0, 7.0, 7.0, 7.333, 7.75, 7.75, 8.0],
    ),
]


@pytest.mark.parametrize(("name", "expr", "result"), data_test_window_functions)
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


def test_window_frame_defaults_match_postgres(partitioned_df):
    # ref: https://github.com/apache/datafusion-python/issues/688

    window_frame = WindowFrame("rows", None, None)

    col_a = column("a")

    # Using `f.window` with or without an unbounded window_frame produces the same
    # results. These tests are included as a regression check but can be removed when
    # f.window() is deprecated in favor of using the .over() approach.
    no_frame = f.window("avg", [col_a]).alias("no_frame")
    with_frame = f.window("avg", [col_a], window_frame=window_frame).alias("with_frame")
    df_1 = partitioned_df.select(col_a, no_frame, with_frame)

    expected = {
        "a": [0, 1, 2, 3, 4, 5, 6],
        "no_frame": [3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0],
        "with_frame": [3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0],
    }

    assert df_1.sort(col_a).to_pydict() == expected

    # When order is not set, the default frame should be unounded preceeding to
    # unbounded following. When order is set, the default frame is unbounded preceeding
    # to current row.
    no_order = f.avg(col_a).over(Window()).alias("over_no_order")
    with_order = f.avg(col_a).over(Window(order_by=[col_a])).alias("over_with_order")
    df_2 = partitioned_df.select(col_a, no_order, with_order)

    expected = {
        "a": [0, 1, 2, 3, 4, 5, 6],
        "over_no_order": [3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0],
        "over_with_order": [0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0],
    }

    assert df_2.sort(col_a).to_pydict() == expected


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
        "AggregateExec: mode=FinalPartitioned, gby=[c1@0 as c1], aggr=[sum(test.c2)]\n"
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
    assert "DataSourceExec:" in indent
    assert "file_type=csv" in indent

    ctx = SessionContext()
    rows_returned = 0
    for idx in range(plan.partition_count):
        stream = ctx.execute(plan, idx)
        try:
            batch = stream.next()
            assert batch is not None
            rows_returned += len(batch.to_pyarrow()[0])
        except StopIteration:
            # This is one of the partitions with no values
            pass
        with pytest.raises(StopIteration):
            stream.next()

    assert rows_returned == 5


@pytest.mark.asyncio
async def test_async_iteration_of_df(aggregate_df):
    rows_returned = 0
    async for batch in aggregate_df.execute_stream():
        assert batch is not None
        rows_returned += len(batch.to_pyarrow()[0])

    assert rows_returned == 5


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

    df_a_u_b = df_a.union(df_b, distinct=True).sort(column("a"))

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
    assert all(batch is not None for batch in stream)
    assert not list(stream)  # after one iteration the generator must be exhausted


@pytest.mark.asyncio
async def test_execute_stream_async(df):
    stream = df.execute_stream()
    batches = [batch async for batch in stream]

    assert all(batch is not None for batch in batches)

    # After consuming all batches, the stream should be exhausted
    remaining_batches = [batch async for batch in stream]
    assert not remaining_batches


@pytest.mark.parametrize("schema", [True, False])
def test_execute_stream_to_arrow_table(df, schema):
    stream = df.execute_stream()

    if schema:
        pyarrow_table = pa.Table.from_batches(
            (batch.to_pyarrow() for batch in stream), schema=df.schema()
        )
    else:
        pyarrow_table = pa.Table.from_batches(batch.to_pyarrow() for batch in stream)

    assert isinstance(pyarrow_table, pa.Table)
    assert pyarrow_table.shape == (3, 3)
    assert set(pyarrow_table.column_names) == {"a", "b", "c"}


@pytest.mark.asyncio
@pytest.mark.parametrize("schema", [True, False])
async def test_execute_stream_to_arrow_table_async(df, schema):
    stream = df.execute_stream()

    if schema:
        pyarrow_table = pa.Table.from_batches(
            [batch.to_pyarrow() async for batch in stream], schema=df.schema()
        )
    else:
        pyarrow_table = pa.Table.from_batches(
            [batch.to_pyarrow() async for batch in stream]
        )

    assert isinstance(pyarrow_table, pa.Table)
    assert pyarrow_table.shape == (3, 3)
    assert set(pyarrow_table.column_names) == {"a", "b", "c"}


def test_execute_stream_partitioned(df):
    streams = df.execute_stream_partitioned()
    assert all(batch is not None for stream in streams for batch in stream)
    assert all(
        not list(stream) for stream in streams
    )  # after one iteration all generators must be exhausted


@pytest.mark.asyncio
async def test_execute_stream_partitioned_async(df):
    streams = df.execute_stream_partitioned()

    for stream in streams:
        batches = [batch async for batch in stream]
        assert all(batch is not None for batch in batches)

        # Ensure the stream is exhausted after iteration
        remaining_batches = [batch async for batch in stream]
        assert not remaining_batches


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


@pytest.mark.parametrize("path_to_str", [True, False])
def test_write_csv(ctx, df, tmp_path, path_to_str):
    path = str(tmp_path) if path_to_str else tmp_path

    df.write_csv(path, with_header=True)

    ctx.register_csv("csv", path)
    result = ctx.table("csv").to_pydict()
    expected = df.to_pydict()

    assert result == expected


@pytest.mark.parametrize("path_to_str", [True, False])
def test_write_json(ctx, df, tmp_path, path_to_str):
    path = str(tmp_path) if path_to_str else tmp_path

    df.write_json(path)

    ctx.register_json("json", path)
    result = ctx.table("json").to_pydict()
    expected = df.to_pydict()

    assert result == expected


@pytest.mark.parametrize("path_to_str", [True, False])
def test_write_parquet(df, tmp_path, path_to_str):
    path = str(tmp_path) if path_to_str else tmp_path

    df.write_parquet(str(path))
    result = pq.read_table(str(path)).to_pydict()
    expected = df.to_pydict()

    assert result == expected


@pytest.mark.parametrize(
    ("compression", "compression_level"),
    [("gzip", 6), ("brotli", 7), ("zstd", 15)],
)
def test_write_compressed_parquet(df, tmp_path, compression, compression_level):
    path = tmp_path

    df.write_parquet(
        str(path), compression=compression, compression_level=compression_level
    )

    # test that the actual compression scheme is the one written
    for _root, _dirs, files in os.walk(path):
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
    ("compression", "compression_level"),
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


@pytest.mark.parametrize("compression", ["wrong"])
def test_write_compressed_parquet_invalid_compression(df, tmp_path, compression):
    path = tmp_path

    with pytest.raises(ValueError):
        df.write_parquet(str(path), compression=compression)


# not testing lzo because it it not implemented yet
# https://github.com/apache/arrow-rs/issues/6970
@pytest.mark.parametrize("compression", ["zstd", "brotli", "gzip"])
def test_write_compressed_parquet_default_compression_level(df, tmp_path, compression):
    # Test write_parquet with zstd, brotli, gzip default compression level,
    # ie don't specify compression level
    # should complete without error
    path = tmp_path

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
    for i in range(3):
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
    assert result["string_col"] == ["string data" for _i in range(3)]
    assert result["new_col"] == [3 for _i in range(3)]


def test_dataframe_repr_html(df) -> None:
    output = df._repr_html_()

    # Since we've added a fair bit of processing to the html output, lets just verify
    # the values we are expecting in the table exist. Use regex and ignore everything
    # between the <th></th> and <td></td>. We also don't want the closing > on the
    # td and th segments because that is where the formatting data is written.

    headers = ["a", "b", "c"]
    headers = [f"<th(.*?)>{v}</th>" for v in headers]
    header_pattern = "(.*?)".join(headers)
    assert len(re.findall(header_pattern, output, re.DOTALL)) == 1

    body_data = [[1, 4, 8], [2, 5, 5], [3, 6, 8]]
    body_lines = [f"<td(.*?)>{v}</td>" for inner in body_data for v in inner]
    body_pattern = "(.*?)".join(body_lines)
    assert len(re.findall(body_pattern, output, re.DOTALL)) == 1


def test_display_config_affects_repr(data):
    max_table_rows_in_repr = 3
    # Create a context with custom display config
    ctx = SessionContext().with_display_config(
        max_table_rows_in_repr=max_table_rows_in_repr
    )

    # Create a DataFrame with more rows than the display limit
    df = ctx.from_pylist(data)

    repr_str = repr(df)

    # The representation should show truncated data (3 rows as specified)
    assert (
        # 5 = 1 header row + 3 separator line + 1 truncation message
        repr_str.count("\n")
        <= max_table_rows_in_repr + 5
    )
    assert "Data truncated" in repr_str

    # Create a context with larger display limit
    max_table_rows_in_repr = 100
    ctx2 = SessionContext().with_display_config(
        max_table_rows_in_repr=max_table_rows_in_repr
    )

    df2 = ctx2.from_pylist(data)
    repr_str2 = repr(df2)

    # Should show all data without truncation message
    assert (
        # 4 = 1 header row + 3 separator lines
        repr_str2.count("\n")
        == max_table_rows_in_repr + 4
    )  # All rows should be shown
    assert "Data truncated" not in repr_str2


def test_display_config_affects_html_repr(data, span_expandable_class):
    # Create a context with custom display config to show only a small cell length
    ctx = SessionContext().with_display_config(max_cell_length=5)

    # Create a DataFrame with a column containing long strings
    df = ctx.from_pylist(data)

    # Get the HTML representation
    html_str = df._repr_html_()

    # The cell should be truncated to 5 characters and have expansion button
    assert ">xxxxx" in html_str  # 5 character limit
    assert span_expandable_class in html_str

    # Create a context with larger cell length limit
    ctx2 = SessionContext().with_display_config(max_cell_length=60)

    df2 = ctx2.from_pylist(data)
    html_str2 = df2._repr_html_()

    # String shouldn't be truncated (or at least not in the same way)
    assert span_expandable_class not in html_str2


def test_display_config_rows_limit_in_html(data):
    max_table_rows = 5
    # Create a context with custom display config to limit rows
    ctx = SessionContext().with_display_config(
        max_table_rows_in_repr=max_table_rows,
    )

    # Create a DataFrame with 10 rows
    df = ctx.from_pylist(data)

    # Get the HTML representation
    html_str = df._repr_html_()

    # Only a few rows should be shown and there should be a truncation message
    row_count = html_str.count("<tr>") - 1  # Subtract 1 for header row
    assert row_count <= max_table_rows
    assert "Data truncated" in html_str

    # Create a context with larger row limit
    max_table_rows = 100
    ctx2 = SessionContext().with_display_config(
        max_table_rows_in_repr=max_table_rows
    )  # Show more rows

    df2 = ctx2.from_pylist(data)
    html_str2 = df2._repr_html_()

    # Should show all rows
    row_count2 = html_str2.count("<tr>") - 1  # Subtract 1 for header row
    assert row_count2 == max_table_rows
    assert "Data truncated" not in html_str2


def test_display_config_max_bytes_limit(data):
    min_table_rows = 10
    max_table_rows = 20
    # Create a context with custom display config with very small byte limit
    ctx = SessionContext().with_display_config(
        min_table_rows=min_table_rows,
        max_table_rows_in_repr=max_table_rows,
        max_table_bytes=100,
    )  # Very small limit

    # Create a DataFrame with large content
    df = ctx.from_pylist(data)

    # Get the HTML representation
    html_str = df._repr_html_()

    # Due to small byte limit, we should see truncation
    row_count = html_str.count("<tr>") - 1  # Subtract 1 for header row
    assert row_count <= min_table_rows  # Should not show all 10 rows
    assert "Data truncated" in html_str

    # With a larger byte limit
    ctx2 = SessionContext().with_display_config(
        max_table_bytes=10 * 1024 * 1024  # 10 MB, much more than needed
    )

    df2 = ctx2.from_pylist(data)
    html_str2 = df2._repr_html_()

    # Should show all rows
    row_count2 = html_str2.count("<tr>") - 1  # Subtract 1 for header row
    assert row_count2 >= min_table_rows  # Should show more than min_table_rows
