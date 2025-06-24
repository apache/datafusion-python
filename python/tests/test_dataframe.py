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
import ctypes
import datetime
import os
import re
import threading
import time
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from datafusion import (
    DataFrame,
    ParquetColumnOptions,
    ParquetWriterOptions,
    SessionContext,
    WindowFrame,
    column,
    literal,
)
from datafusion import (
    functions as f,
)
from datafusion.dataframe_formatter import (
    DataFrameHtmlFormatter,
    configure_formatter,
    get_formatter,
    reset_formatter,
    reset_styles_loaded_state,
)
from datafusion.expr import Window
from pyarrow.csv import write_csv

MB = 1024 * 1024


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
def large_df():
    ctx = SessionContext()

    rows = 100000
    data = {
        "a": list(range(rows)),
        "b": [f"s-{i}" for i in range(rows)],
        "c": [float(i + 0.1) for i in range(rows)],
    }
    batch = pa.record_batch(data)

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


@pytest.fixture
def clean_formatter_state():
    """Reset the HTML formatter after each test."""
    reset_formatter()


@pytest.fixture
def null_df():
    """Create a DataFrame with null values of different types."""
    ctx = SessionContext()

    # Create a RecordBatch with nulls across different types
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array([1, None, 3, None], type=pa.int64()),
            pa.array([4.5, 6.7, None, None], type=pa.float64()),
            pa.array(["a", None, "c", None], type=pa.string()),
            pa.array([True, None, False, None], type=pa.bool_()),
            pa.array(
                [10957, None, 18993, None], type=pa.date32()
            ),  # 2000-01-01, null, 2022-01-01, null
            pa.array(
                [946684800000, None, 1640995200000, None], type=pa.date64()
            ),  # 2000-01-01, null, 2022-01-01, null
        ],
        names=[
            "int_col",
            "float_col",
            "str_col",
            "bool_col",
            "date32_col",
            "date64_col",
        ],
    )

    return ctx.create_dataframe([[batch]])


# custom style for testing with html formatter
class CustomStyleProvider:
    def get_cell_style(self) -> str:
        return (
            "background-color: #f5f5f5; color: #333; padding: 8px; border: "
            "1px solid #ddd;"
        )

    def get_header_style(self) -> str:
        return (
            "background-color: #4285f4; color: white; font-weight: bold; "
            "padding: 10px; border: 1px solid #3367d6;"
        )


def count_table_rows(html_content: str) -> int:
    """Count the number of table rows in HTML content.
    Args:
        html_content: HTML string to analyze
    Returns:
        Number of table rows found (number of <tr> tags)
    """
    return len(re.findall(r"<tr", html_content))


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


def test_html_formatter_cell_dimension(df, clean_formatter_state):
    """Test configuring the HTML formatter with different options."""
    # Configure with custom settings
    configure_formatter(
        max_width=500,
        max_height=200,
        enable_cell_expansion=False,
    )

    html_output = df._repr_html_()

    # Verify our configuration was applied
    assert "max-height: 200px" in html_output
    assert "max-width: 500px" in html_output
    # With cell expansion disabled, we shouldn't see expandable-container elements
    assert "expandable-container" not in html_output


def test_html_formatter_custom_style_provider(df, clean_formatter_state):
    """Test using custom style providers with the HTML formatter."""

    # Configure with custom style provider
    configure_formatter(style_provider=CustomStyleProvider())

    html_output = df._repr_html_()

    # Verify our custom styles were applied
    assert "background-color: #4285f4" in html_output
    assert "color: white" in html_output
    assert "background-color: #f5f5f5" in html_output


def test_html_formatter_type_formatters(df, clean_formatter_state):
    """Test registering custom type formatters for specific data types."""

    # Get current formatter and register custom formatters
    formatter = get_formatter()

    # Format integers with color based on value
    # Using int as the type for the formatter will work since we convert
    # Arrow scalar values to Python native types in _get_cell_value
    def format_int(value):
        return f'<span style="color: {"red" if value > 2 else "blue"}">{value}</span>'

    formatter.register_formatter(int, format_int)

    html_output = df._repr_html_()

    # Our test dataframe has values 1,2,3 so we should see:
    assert '<span style="color: blue">1</span>' in html_output


def test_html_formatter_custom_cell_builder(df, clean_formatter_state):
    """Test using a custom cell builder function."""

    # Create a custom cell builder with distinct styling for different value ranges
    def custom_cell_builder(value, row, col, table_id):
        try:
            num_value = int(value)
            if num_value > 5:  # Values > 5 get green background with indicator
                return (
                    '<td style="background-color: #d9f0d3" '
                    f'data-test="high">{value}-high</td>'
                )
            if num_value < 3:  # Values < 3 get blue background with indicator
                return (
                    '<td style="background-color: #d3e9f0" '
                    f'data-test="low">{value}-low</td>'
                )
        except (ValueError, TypeError):
            pass

        # Default styling for other cells (3, 4, 5)
        return f'<td style="border: 1px solid #ddd" data-test="mid">{value}-mid</td>'

    # Set our custom cell builder
    formatter = get_formatter()
    formatter.set_custom_cell_builder(custom_cell_builder)

    html_output = df._repr_html_()

    # Extract cells with specific styling using regex
    low_cells = re.findall(
        r'<td style="background-color: #d3e9f0"[^>]*>(\d+)-low</td>', html_output
    )
    mid_cells = re.findall(
        r'<td style="border: 1px solid #ddd"[^>]*>(\d+)-mid</td>', html_output
    )
    high_cells = re.findall(
        r'<td style="background-color: #d9f0d3"[^>]*>(\d+)-high</td>', html_output
    )

    # Sort the extracted values for consistent comparison
    low_cells = sorted(map(int, low_cells))
    mid_cells = sorted(map(int, mid_cells))
    high_cells = sorted(map(int, high_cells))

    # Verify specific values have the correct styling applied
    assert low_cells == [1, 2]  # Values < 3
    assert mid_cells == [3, 4, 5, 5]  # Values 3-5
    assert high_cells == [6, 8, 8]  # Values > 5

    # Verify the exact content with styling appears in the output
    assert (
        '<td style="background-color: #d3e9f0" data-test="low">1-low</td>'
        in html_output
    )
    assert (
        '<td style="background-color: #d3e9f0" data-test="low">2-low</td>'
        in html_output
    )
    assert (
        '<td style="border: 1px solid #ddd" data-test="mid">3-mid</td>' in html_output
    )
    assert (
        '<td style="border: 1px solid #ddd" data-test="mid">4-mid</td>' in html_output
    )
    assert (
        '<td style="background-color: #d9f0d3" data-test="high">6-high</td>'
        in html_output
    )
    assert (
        '<td style="background-color: #d9f0d3" data-test="high">8-high</td>'
        in html_output
    )

    # Count occurrences to ensure all cells are properly styled
    assert html_output.count("-low</td>") == 2  # Two low values (1, 2)
    assert html_output.count("-mid</td>") == 4  # Four mid values (3, 4, 5, 5)
    assert html_output.count("-high</td>") == 3  # Three high values (6, 8, 8)

    # Create a custom cell builder that changes background color based on value
    def custom_cell_builder(value, row, col, table_id):
        # Handle numeric values regardless of their exact type
        try:
            num_value = int(value)
            if num_value > 5:  # Values > 5 get green background
                return f'<td style="background-color: #d9f0d3">{value}</td>'
            if num_value < 3:  # Values < 3 get light blue background
                return f'<td style="background-color: #d3e9f0">{value}</td>'
        except (ValueError, TypeError):
            pass

        # Default styling for other cells
        return f'<td style="border: 1px solid #ddd">{value}</td>'

    # Set our custom cell builder
    formatter = get_formatter()
    formatter.set_custom_cell_builder(custom_cell_builder)

    html_output = df._repr_html_()

    # Verify our custom cell styling was applied
    assert "background-color: #d3e9f0" in html_output  # For values 1,2


def test_html_formatter_custom_header_builder(df, clean_formatter_state):
    """Test using a custom header builder function."""

    # Create a custom header builder with tooltips
    def custom_header_builder(field):
        tooltips = {
            "a": "Primary key column",
            "b": "Secondary values",
            "c": "Additional data",
        }
        tooltip = tooltips.get(field.name, "")
        return (
            f'<th style="background-color: #333; color: white" '
            f'title="{tooltip}">{field.name}</th>'
        )

    # Set our custom header builder
    formatter = get_formatter()
    formatter.set_custom_header_builder(custom_header_builder)

    html_output = df._repr_html_()

    # Verify our custom headers were applied
    assert 'title="Primary key column"' in html_output
    assert 'title="Secondary values"' in html_output
    assert "background-color: #333; color: white" in html_output


def test_html_formatter_complex_customization(df, clean_formatter_state):
    """Test combining multiple customization options together."""

    # Create a dark mode style provider
    class DarkModeStyleProvider:
        def get_cell_style(self) -> str:
            return (
                "background-color: #222; color: #eee; "
                "padding: 8px; border: 1px solid #444;"
            )

        def get_header_style(self) -> str:
            return (
                "background-color: #111; color: #fff; padding: 10px; "
                "border: 1px solid #333;"
            )

    # Configure with dark mode style
    configure_formatter(
        max_cell_length=10,
        style_provider=DarkModeStyleProvider(),
        custom_css="""
            .datafusion-table {
                font-family: monospace;
                border-collapse: collapse;
            }
            .datafusion-table tr:hover td {
                background-color: #444 !important;
            }
        """,
    )

    # Add type formatters for special formatting - now working with native int values
    formatter = get_formatter()
    formatter.register_formatter(
        int,
        lambda n: f'<span style="color: {"#5af" if n % 2 == 0 else "#f5a"}">{n}</span>',
    )

    html_output = df._repr_html_()

    # Verify our customizations were applied
    assert "background-color: #222" in html_output
    assert "background-color: #111" in html_output
    assert ".datafusion-table" in html_output
    assert "color: #5af" in html_output  # Even numbers


def test_html_formatter_memory(df, clean_formatter_state):
    """Test the memory and row control parameters in DataFrameHtmlFormatter."""
    configure_formatter(max_memory_bytes=10, min_rows_display=1)
    html_output = df._repr_html_()

    # Count the number of table rows in the output
    tr_count = count_table_rows(html_output)
    # With a tiny memory limit of 10 bytes, the formatter should display
    # the minimum number of rows (1) plus a message about truncation
    assert tr_count == 2  # 1 for header row, 1 for data row
    assert "data truncated" in html_output.lower()

    configure_formatter(max_memory_bytes=10 * MB, min_rows_display=1)
    html_output = df._repr_html_()
    # With larger memory limit and min_rows=2, should display all rows
    tr_count = count_table_rows(html_output)
    # Table should have header row (1) + 3 data rows = 4 rows
    assert tr_count == 4
    # No truncation message should appear
    assert "data truncated" not in html_output.lower()


def test_html_formatter_repr_rows(df, clean_formatter_state):
    configure_formatter(min_rows_display=2, repr_rows=2)
    html_output = df._repr_html_()

    tr_count = count_table_rows(html_output)
    # Tabe should have header row (1) + 2 data rows = 3 rows
    assert tr_count == 3

    configure_formatter(min_rows_display=2, repr_rows=3)
    html_output = df._repr_html_()

    tr_count = count_table_rows(html_output)
    # Tabe should have header row (1) + 3 data rows = 4 rows
    assert tr_count == 4


def test_html_formatter_validation():
    # Test validation for invalid parameters

    with pytest.raises(ValueError, match="max_cell_length must be a positive integer"):
        DataFrameHtmlFormatter(max_cell_length=0)

    with pytest.raises(ValueError, match="max_width must be a positive integer"):
        DataFrameHtmlFormatter(max_width=0)

    with pytest.raises(ValueError, match="max_height must be a positive integer"):
        DataFrameHtmlFormatter(max_height=0)

    with pytest.raises(ValueError, match="max_memory_bytes must be a positive integer"):
        DataFrameHtmlFormatter(max_memory_bytes=0)

    with pytest.raises(ValueError, match="max_memory_bytes must be a positive integer"):
        DataFrameHtmlFormatter(max_memory_bytes=-100)

    with pytest.raises(ValueError, match="min_rows_display must be a positive integer"):
        DataFrameHtmlFormatter(min_rows_display=0)

    with pytest.raises(ValueError, match="min_rows_display must be a positive integer"):
        DataFrameHtmlFormatter(min_rows_display=-5)

    with pytest.raises(ValueError, match="repr_rows must be a positive integer"):
        DataFrameHtmlFormatter(repr_rows=0)

    with pytest.raises(ValueError, match="repr_rows must be a positive integer"):
        DataFrameHtmlFormatter(repr_rows=-10)


def test_configure_formatter(df, clean_formatter_state):
    """Test using custom style providers with the HTML formatter and configured
    parameters."""

    # these are non-default values
    max_cell_length = 10
    max_width = 500
    max_height = 30
    max_memory_bytes = 3 * MB
    min_rows_display = 2
    repr_rows = 2
    enable_cell_expansion = False
    show_truncation_message = False
    use_shared_styles = False

    reset_formatter()
    formatter_default = get_formatter()

    assert formatter_default.max_cell_length != max_cell_length
    assert formatter_default.max_width != max_width
    assert formatter_default.max_height != max_height
    assert formatter_default.max_memory_bytes != max_memory_bytes
    assert formatter_default.min_rows_display != min_rows_display
    assert formatter_default.repr_rows != repr_rows
    assert formatter_default.enable_cell_expansion != enable_cell_expansion
    assert formatter_default.show_truncation_message != show_truncation_message
    assert formatter_default.use_shared_styles != use_shared_styles

    # Configure with custom style provider and additional parameters
    configure_formatter(
        max_cell_length=max_cell_length,
        max_width=max_width,
        max_height=max_height,
        max_memory_bytes=max_memory_bytes,
        min_rows_display=min_rows_display,
        repr_rows=repr_rows,
        enable_cell_expansion=enable_cell_expansion,
        show_truncation_message=show_truncation_message,
        use_shared_styles=use_shared_styles,
    )
    formatter_custom = get_formatter()
    assert formatter_custom.max_cell_length == max_cell_length
    assert formatter_custom.max_width == max_width
    assert formatter_custom.max_height == max_height
    assert formatter_custom.max_memory_bytes == max_memory_bytes
    assert formatter_custom.min_rows_display == min_rows_display
    assert formatter_custom.repr_rows == repr_rows
    assert formatter_custom.enable_cell_expansion == enable_cell_expansion
    assert formatter_custom.show_truncation_message == show_truncation_message
    assert formatter_custom.use_shared_styles == use_shared_styles


def test_configure_formatter_invalid_params(clean_formatter_state):
    """Test that configure_formatter rejects invalid parameters."""
    with pytest.raises(ValueError, match="Invalid formatter parameters"):
        configure_formatter(invalid_param=123)

    # Test with multiple parameters, one valid and one invalid
    with pytest.raises(ValueError, match="Invalid formatter parameters"):
        configure_formatter(max_width=500, not_a_real_param="test")

    # Test with multiple invalid parameters
    with pytest.raises(ValueError, match="Invalid formatter parameters"):
        configure_formatter(fake_param1="test", fake_param2=456)


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


def test_write_parquet_with_options_default_compression(df, tmp_path):
    """Test that the default compression is ZSTD."""
    df.write_parquet(tmp_path)

    for file in tmp_path.rglob("*.parquet"):
        metadata = pq.ParquetFile(file).metadata.to_dict()
        for row_group in metadata["row_groups"]:
            for col in row_group["columns"]:
                assert col["compression"].lower() == "zstd"


@pytest.mark.parametrize(
    "compression",
    ["gzip(6)", "brotli(7)", "zstd(15)", "snappy", "uncompressed"],
)
def test_write_parquet_with_options_compression(df, tmp_path, compression):
    import re

    path = tmp_path
    df.write_parquet_with_options(
        str(path), ParquetWriterOptions(compression=compression)
    )

    # test that the actual compression scheme is the one written
    for _root, _dirs, files in os.walk(path):
        for file in files:
            if file.endswith(".parquet"):
                metadata = pq.ParquetFile(tmp_path / file).metadata.to_dict()
                for row_group in metadata["row_groups"]:
                    for col in row_group["columns"]:
                        assert col["compression"].lower() == re.sub(
                            r"\(\d+\)", "", compression
                        )

    result = pq.read_table(str(path)).to_pydict()
    expected = df.to_pydict()

    assert result == expected


@pytest.mark.parametrize(
    "compression",
    ["gzip(12)", "brotli(15)", "zstd(23)"],
)
def test_write_parquet_with_options_wrong_compression_level(df, tmp_path, compression):
    path = tmp_path

    with pytest.raises(Exception, match=r"valid compression range .*? exceeded."):
        df.write_parquet_with_options(
            str(path), ParquetWriterOptions(compression=compression)
        )


@pytest.mark.parametrize("compression", ["wrong", "wrong(12)"])
def test_write_parquet_with_options_invalid_compression(df, tmp_path, compression):
    path = tmp_path

    with pytest.raises(Exception, match="Unknown or unsupported parquet compression"):
        df.write_parquet_with_options(
            str(path), ParquetWriterOptions(compression=compression)
        )


@pytest.mark.parametrize(
    ("writer_version", "format_version"),
    [("1.0", "1.0"), ("2.0", "2.6"), (None, "1.0")],
)
def test_write_parquet_with_options_writer_version(
    df, tmp_path, writer_version, format_version
):
    """Test the Parquet writer version. Note that writer_version=2.0 results in
    format_version=2.6"""
    if writer_version is None:
        df.write_parquet_with_options(tmp_path, ParquetWriterOptions())
    else:
        df.write_parquet_with_options(
            tmp_path, ParquetWriterOptions(writer_version=writer_version)
        )

    for file in tmp_path.rglob("*.parquet"):
        parquet = pq.ParquetFile(file)
        metadata = parquet.metadata.to_dict()
        assert metadata["format_version"] == format_version


@pytest.mark.parametrize("writer_version", ["1.2.3", "custom-version", "0"])
def test_write_parquet_with_options_wrong_writer_version(df, tmp_path, writer_version):
    """Test that invalid writer versions in Parquet throw an exception."""
    with pytest.raises(
        Exception, match="Unknown or unsupported parquet writer version"
    ):
        df.write_parquet_with_options(
            tmp_path, ParquetWriterOptions(writer_version=writer_version)
        )


@pytest.mark.parametrize("dictionary_enabled", [True, False, None])
def test_write_parquet_with_options_dictionary_enabled(
    df, tmp_path, dictionary_enabled
):
    """Test enabling/disabling the dictionaries in Parquet."""
    df.write_parquet_with_options(
        tmp_path, ParquetWriterOptions(dictionary_enabled=dictionary_enabled)
    )
    # by default, the dictionary is enabled, so None results in True
    result = dictionary_enabled if dictionary_enabled is not None else True

    for file in tmp_path.rglob("*.parquet"):
        parquet = pq.ParquetFile(file)
        metadata = parquet.metadata.to_dict()

        for row_group in metadata["row_groups"]:
            for col in row_group["columns"]:
                assert col["has_dictionary_page"] == result


@pytest.mark.parametrize(
    ("statistics_enabled", "has_statistics"),
    [("page", True), ("chunk", True), ("none", False), (None, True)],
)
def test_write_parquet_with_options_statistics_enabled(
    df, tmp_path, statistics_enabled, has_statistics
):
    """Test configuring the statistics in Parquet. In pyarrow we can only check for
    column-level statistics, so "page" and "chunk" are tested in the same way."""
    df.write_parquet_with_options(
        tmp_path, ParquetWriterOptions(statistics_enabled=statistics_enabled)
    )

    for file in tmp_path.rglob("*.parquet"):
        parquet = pq.ParquetFile(file)
        metadata = parquet.metadata.to_dict()

        for row_group in metadata["row_groups"]:
            for col in row_group["columns"]:
                if has_statistics:
                    assert col["statistics"] is not None
                else:
                    assert col["statistics"] is None


@pytest.mark.parametrize("max_row_group_size", [1000, 5000, 10000, 100000])
def test_write_parquet_with_options_max_row_group_size(
    large_df, tmp_path, max_row_group_size
):
    """Test configuring the max number of rows per group in Parquet. These test cases
    guarantee that the number of rows for each row group is max_row_group_size, given
    the total number of rows is a multiple of max_row_group_size."""
    large_df.write_parquet_with_options(
        tmp_path, ParquetWriterOptions(max_row_group_size=max_row_group_size)
    )

    for file in tmp_path.rglob("*.parquet"):
        parquet = pq.ParquetFile(file)
        metadata = parquet.metadata.to_dict()
        for row_group in metadata["row_groups"]:
            assert row_group["num_rows"] == max_row_group_size


@pytest.mark.parametrize("created_by", ["datafusion", "datafusion-python", "custom"])
def test_write_parquet_with_options_created_by(df, tmp_path, created_by):
    """Test configuring the created by metadata in Parquet."""
    df.write_parquet_with_options(tmp_path, ParquetWriterOptions(created_by=created_by))

    for file in tmp_path.rglob("*.parquet"):
        parquet = pq.ParquetFile(file)
        metadata = parquet.metadata.to_dict()
        assert metadata["created_by"] == created_by


@pytest.mark.parametrize("statistics_truncate_length", [5, 25, 50])
def test_write_parquet_with_options_statistics_truncate_length(
    df, tmp_path, statistics_truncate_length
):
    """Test configuring the truncate limit in Parquet's row-group-level statistics."""
    ctx = SessionContext()
    data = {
        "a": [
            "a_the_quick_brown_fox_jumps_over_the_lazy_dog",
            "m_the_quick_brown_fox_jumps_over_the_lazy_dog",
            "z_the_quick_brown_fox_jumps_over_the_lazy_dog",
        ],
        "b": ["a_smaller", "m_smaller", "z_smaller"],
    }
    df = ctx.from_arrow(pa.record_batch(data))
    df.write_parquet_with_options(
        tmp_path,
        ParquetWriterOptions(statistics_truncate_length=statistics_truncate_length),
    )

    for file in tmp_path.rglob("*.parquet"):
        parquet = pq.ParquetFile(file)
        metadata = parquet.metadata.to_dict()

        for row_group in metadata["row_groups"]:
            for col in row_group["columns"]:
                statistics = col["statistics"]
                assert len(statistics["min"]) <= statistics_truncate_length
                assert len(statistics["max"]) <= statistics_truncate_length


def test_write_parquet_with_options_default_encoding(tmp_path):
    """Test that, by default, Parquet files are written with dictionary encoding.
    Note that dictionary encoding is not used for boolean values, so it is not tested
    here."""
    ctx = SessionContext()
    data = {
        "a": [1, 2, 3],
        "b": ["1", "2", "3"],
        "c": [1.01, 2.02, 3.03],
    }
    df = ctx.from_arrow(pa.record_batch(data))
    df.write_parquet_with_options(tmp_path, ParquetWriterOptions())

    for file in tmp_path.rglob("*.parquet"):
        parquet = pq.ParquetFile(file)
        metadata = parquet.metadata.to_dict()

        for row_group in metadata["row_groups"]:
            for col in row_group["columns"]:
                assert col["encodings"] == ("PLAIN", "RLE", "RLE_DICTIONARY")


@pytest.mark.parametrize(
    ("encoding", "data_types", "result"),
    [
        ("plain", ["int", "float", "str", "bool"], ("PLAIN", "RLE")),
        ("rle", ["bool"], ("RLE",)),
        ("delta_binary_packed", ["int"], ("RLE", "DELTA_BINARY_PACKED")),
        ("delta_length_byte_array", ["str"], ("RLE", "DELTA_LENGTH_BYTE_ARRAY")),
        ("delta_byte_array", ["str"], ("RLE", "DELTA_BYTE_ARRAY")),
        ("byte_stream_split", ["int", "float"], ("RLE", "BYTE_STREAM_SPLIT")),
    ],
)
def test_write_parquet_with_options_encoding(tmp_path, encoding, data_types, result):
    """Test different encodings in Parquet in their respective support column types."""
    ctx = SessionContext()

    data = {}
    for data_type in data_types:
        if data_type == "int":
            data["int"] = [1, 2, 3]
        elif data_type == "float":
            data["float"] = [1.01, 2.02, 3.03]
        elif data_type == "str":
            data["str"] = ["a", "b", "c"]
        elif data_type == "bool":
            data["bool"] = [True, False, True]

    df = ctx.from_arrow(pa.record_batch(data))
    df.write_parquet_with_options(
        tmp_path, ParquetWriterOptions(encoding=encoding, dictionary_enabled=False)
    )

    for file in tmp_path.rglob("*.parquet"):
        parquet = pq.ParquetFile(file)
        metadata = parquet.metadata.to_dict()

        for row_group in metadata["row_groups"]:
            for col in row_group["columns"]:
                assert col["encodings"] == result


@pytest.mark.parametrize("encoding", ["bit_packed"])
def test_write_parquet_with_options_unsupported_encoding(df, tmp_path, encoding):
    """Test that unsupported Parquet encodings do not work."""
    # BaseException is used since this throws a Rust panic: https://github.com/PyO3/pyo3/issues/3519
    with pytest.raises(BaseException, match="Encoding .*? is not supported"):
        df.write_parquet_with_options(tmp_path, ParquetWriterOptions(encoding=encoding))


@pytest.mark.parametrize("encoding", ["non_existent", "unknown", "plain123"])
def test_write_parquet_with_options_invalid_encoding(df, tmp_path, encoding):
    """Test that invalid Parquet encodings do not work."""
    with pytest.raises(Exception, match="Unknown or unsupported parquet encoding"):
        df.write_parquet_with_options(tmp_path, ParquetWriterOptions(encoding=encoding))


@pytest.mark.parametrize("encoding", ["plain_dictionary", "rle_dictionary"])
def test_write_parquet_with_options_dictionary_encoding_fallback(
    df, tmp_path, encoding
):
    """Test that the dictionary encoding cannot be used as fallback in Parquet."""
    # BaseException is used since this throws a Rust panic: https://github.com/PyO3/pyo3/issues/3519
    with pytest.raises(
        BaseException, match="Dictionary encoding can not be used as fallback encoding"
    ):
        df.write_parquet_with_options(tmp_path, ParquetWriterOptions(encoding=encoding))


def test_write_parquet_with_options_bloom_filter(df, tmp_path):
    """Test Parquet files with and without (default) bloom filters. Since pyarrow does
    not expose any information about bloom filters, the easiest way to confirm that they
    are actually written is to compare the file size."""
    path_no_bloom_filter = tmp_path / "1"
    path_bloom_filter = tmp_path / "2"

    df.write_parquet_with_options(path_no_bloom_filter, ParquetWriterOptions())
    df.write_parquet_with_options(
        path_bloom_filter, ParquetWriterOptions(bloom_filter_on_write=True)
    )

    size_no_bloom_filter = 0
    for file in path_no_bloom_filter.rglob("*.parquet"):
        size_no_bloom_filter += os.path.getsize(file)

    size_bloom_filter = 0
    for file in path_bloom_filter.rglob("*.parquet"):
        size_bloom_filter += os.path.getsize(file)

    assert size_no_bloom_filter < size_bloom_filter


def test_write_parquet_with_options_column_options(df, tmp_path):
    """Test writing Parquet files with different options for each column, which replace
    the global configs (when provided)."""
    data = {
        "a": [1, 2, 3],
        "b": ["a", "b", "c"],
        "c": [False, True, False],
        "d": [1.01, 2.02, 3.03],
        "e": [4, 5, 6],
    }

    column_specific_options = {
        "a": ParquetColumnOptions(statistics_enabled="none"),
        "b": ParquetColumnOptions(encoding="plain", dictionary_enabled=False),
        "c": ParquetColumnOptions(
            compression="snappy", encoding="rle", dictionary_enabled=False
        ),
        "d": ParquetColumnOptions(
            compression="zstd(6)",
            encoding="byte_stream_split",
            dictionary_enabled=False,
            statistics_enabled="none",
        ),
        # column "e" will use the global configs
    }

    results = {
        "a": {
            "statistics": False,
            "compression": "brotli",
            "encodings": ("PLAIN", "RLE", "RLE_DICTIONARY"),
        },
        "b": {
            "statistics": True,
            "compression": "brotli",
            "encodings": ("PLAIN", "RLE"),
        },
        "c": {
            "statistics": True,
            "compression": "snappy",
            "encodings": ("RLE",),
        },
        "d": {
            "statistics": False,
            "compression": "zstd",
            "encodings": ("RLE", "BYTE_STREAM_SPLIT"),
        },
        "e": {
            "statistics": True,
            "compression": "brotli",
            "encodings": ("PLAIN", "RLE", "RLE_DICTIONARY"),
        },
    }

    ctx = SessionContext()
    df = ctx.from_arrow(pa.record_batch(data))
    df.write_parquet_with_options(
        tmp_path,
        ParquetWriterOptions(
            compression="brotli(8)", column_specific_options=column_specific_options
        ),
    )

    for file in tmp_path.rglob("*.parquet"):
        parquet = pq.ParquetFile(file)
        metadata = parquet.metadata.to_dict()

        for row_group in metadata["row_groups"]:
            for col in row_group["columns"]:
                column_name = col["path_in_schema"]
                result = results[column_name]
                assert (col["statistics"] is not None) == result["statistics"]
                assert col["compression"].lower() == result["compression"].lower()
                assert col["encodings"] == result["encodings"]


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


def test_dataframe_repr_html_structure(df, clean_formatter_state) -> None:
    """Test that DataFrame._repr_html_ produces expected HTML output structure."""

    output = df._repr_html_()

    # Since we've added a fair bit of processing to the html output, lets just verify
    # the values we are expecting in the table exist. Use regex and ignore everything
    # between the <th></th> and <td></td>. We also don't want the closing > on the
    # td and th segments because that is where the formatting data is written.

    headers = ["a", "b", "c"]
    headers = [f"<th(.*?)>{v}</th>" for v in headers]
    header_pattern = "(.*?)".join(headers)
    header_matches = re.findall(header_pattern, output, re.DOTALL)
    assert len(header_matches) == 1

    # Update the pattern to handle values that may be wrapped in spans
    body_data = [[1, 4, 8], [2, 5, 5], [3, 6, 8]]

    body_lines = [
        f"<td(.*?)>(?:<span[^>]*?>)?{v}(?:</span>)?</td>"
        for inner in body_data
        for v in inner
    ]
    body_pattern = "(.*?)".join(body_lines)

    body_matches = re.findall(body_pattern, output, re.DOTALL)

    assert len(body_matches) == 1, "Expected pattern of values not found in HTML output"


def test_dataframe_repr_html_values(df, clean_formatter_state):
    """Test that DataFrame._repr_html_ contains the expected data values."""
    html = df._repr_html_()
    assert html is not None

    # Create a more flexible pattern that handles values being wrapped in spans
    # This pattern will match the sequence of values 1,4,8,2,5,5,3,6,8 regardless
    # of formatting
    pattern = re.compile(
        r"<td[^>]*?>(?:<span[^>]*?>)?1(?:</span>)?</td>.*?"
        r"<td[^>]*?>(?:<span[^>]*?>)?4(?:</span>)?</td>.*?"
        r"<td[^>]*?>(?:<span[^>]*?>)?8(?:</span>)?</td>.*?"
        r"<td[^>]*?>(?:<span[^>]*?>)?2(?:</span>)?</td>.*?"
        r"<td[^>]*?>(?:<span[^>]*?>)?5(?:</span>)?</td>.*?"
        r"<td[^>]*?>(?:<span[^>]*?>)?5(?:</span>)?</td>.*?"
        r"<td[^>]*?>(?:<span[^>]*?>)?3(?:</span>)?</td>.*?"
        r"<td[^>]*?>(?:<span[^>]*?>)?6(?:</span>)?</td>.*?"
        r"<td[^>]*?>(?:<span[^>]*?>)?8(?:</span>)?</td>",
        re.DOTALL,
    )

    # Print debug info if the test fails
    matches = re.findall(pattern, html)
    if not matches:
        print(f"HTML output snippet: {html[:500]}...")  # noqa: T201

    assert len(matches) > 0, "Expected pattern of values not found in HTML output"


def test_html_formatter_shared_styles(df, clean_formatter_state):
    """Test that shared styles work correctly across multiple tables."""

    # First, ensure we're using shared styles
    configure_formatter(use_shared_styles=True)

    # Get HTML output for first table - should include styles
    html_first = df._repr_html_()

    # Verify styles are included in first render
    assert "<style>" in html_first
    assert ".expandable-container" in html_first

    # Get HTML output for second table - should NOT include styles
    html_second = df._repr_html_()

    # Verify styles are NOT included in second render
    assert "<style>" not in html_second
    assert ".expandable-container" not in html_second

    # Reset the styles loaded state and verify styles are included again
    reset_styles_loaded_state()
    html_after_reset = df._repr_html_()

    # Verify styles are included after reset
    assert "<style>" in html_after_reset
    assert ".expandable-container" in html_after_reset


def test_html_formatter_no_shared_styles(df, clean_formatter_state):
    """Test that styles are always included when shared styles are disabled."""

    # Configure formatter to NOT use shared styles
    configure_formatter(use_shared_styles=False)

    # Generate HTML multiple times
    html_first = df._repr_html_()
    html_second = df._repr_html_()

    # Verify styles are included in both renders
    assert "<style>" in html_first
    assert "<style>" in html_second
    assert ".expandable-container" in html_first
    assert ".expandable-container" in html_second


def test_html_formatter_manual_format_html(clean_formatter_state):
    """Test direct usage of format_html method with shared styles."""

    # Create sample data
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
        names=["a", "b"],
    )

    formatter = get_formatter()

    # First call should include styles
    html_first = formatter.format_html([batch], batch.schema)
    assert "<style>" in html_first

    # Second call should not include styles (using shared styles by default)
    html_second = formatter.format_html([batch], batch.schema)
    assert "<style>" not in html_second

    # Reset loaded state
    reset_styles_loaded_state()

    # After reset, styles should be included again
    html_reset = formatter.format_html([batch], batch.schema)
    assert "<style>" in html_reset

    # Create a new formatter with shared_styles=False
    local_formatter = DataFrameHtmlFormatter(use_shared_styles=False)

    # Both calls should include styles
    local_html_1 = local_formatter.format_html([batch], batch.schema)
    local_html_2 = local_formatter.format_html([batch], batch.schema)

    assert "<style>" in local_html_1
    assert "<style>" in local_html_2


def test_fill_null_basic(null_df):
    """Test basic fill_null functionality with a single value."""
    # Fill all nulls with 0
    filled_df = null_df.fill_null(0)

    result = filled_df.collect()[0]

    # Check that nulls were filled with 0 (or equivalent)
    assert result.column(0) == pa.array([1, 0, 3, 0])
    assert result.column(1) == pa.array([4.5, 6.7, 0.0, 0.0])
    # String column should be filled with "0"
    assert result.column(2) == pa.array(["a", "0", "c", "0"])
    # Boolean column should be filled with False (0 converted to bool)
    assert result.column(3) == pa.array([True, False, False, False])


def test_fill_null_subset(null_df):
    """Test filling nulls only in a subset of columns."""
    # Fill nulls only in numeric columns
    filled_df = null_df.fill_null(0, subset=["int_col", "float_col"])

    result = filled_df.collect()[0]

    # Check that nulls were filled only in specified columns
    assert result.column(0) == pa.array([1, 0, 3, 0])
    assert result.column(1) == pa.array([4.5, 6.7, 0.0, 0.0])
    # These should still have nulls
    assert None in result.column(2).to_pylist()
    assert None in result.column(3).to_pylist()


def test_fill_null_str_column(null_df):
    """Test filling nulls in string columns with different values."""
    # Fill string nulls with a replacement string
    filled_df = null_df.fill_null("N/A", subset=["str_col"])

    result = filled_df.collect()[0]

    # Check that string nulls were filled with "N/A"
    assert result.column(2).to_pylist() == ["a", "N/A", "c", "N/A"]

    # Other columns should be unchanged
    assert None in result.column(0).to_pylist()
    assert None in result.column(1).to_pylist()
    assert None in result.column(3).to_pylist()

    # Fill with an empty string
    filled_df = null_df.fill_null("", subset=["str_col"])
    result = filled_df.collect()[0]
    assert result.column(2).to_pylist() == ["a", "", "c", ""]


def test_fill_null_bool_column(null_df):
    """Test filling nulls in boolean columns with different values."""
    # Fill bool nulls with True
    filled_df = null_df.fill_null(value=True, subset=["bool_col"])

    result = filled_df.collect()[0]

    # Check that bool nulls were filled with True
    assert result.column(3).to_pylist() == [True, True, False, True]

    # Other columns should be unchanged
    assert None in result.column(0).to_pylist()

    # Fill bool nulls with False
    filled_df = null_df.fill_null(value=False, subset=["bool_col"])
    result = filled_df.collect()[0]
    assert result.column(3).to_pylist() == [True, False, False, False]


def test_fill_null_date32_column(null_df):
    """Test filling nulls in date32 columns."""

    # Fill date32 nulls with a specific date (1970-01-01)
    epoch_date = datetime.date(1970, 1, 1)
    filled_df = null_df.fill_null(epoch_date, subset=["date32_col"])

    result = filled_df.collect()[0]

    # Check that date32 nulls were filled with epoch date
    dates = result.column(4).to_pylist()
    assert dates[0] == datetime.date(2000, 1, 1)  # Original value
    assert dates[1] == epoch_date  # Filled value
    assert dates[2] == datetime.date(2022, 1, 1)  # Original value
    assert dates[3] == epoch_date  # Filled value

    # Other date column should be unchanged
    assert None in result.column(5).to_pylist()


def test_fill_null_date64_column(null_df):
    """Test filling nulls in date64 columns."""

    # Fill date64 nulls with a specific date (1970-01-01)
    epoch_date = datetime.date(1970, 1, 1)
    filled_df = null_df.fill_null(epoch_date, subset=["date64_col"])

    result = filled_df.collect()[0]

    # Check that date64 nulls were filled with epoch date
    dates = result.column(5).to_pylist()
    assert dates[0] == datetime.date(2000, 1, 1)  # Original value
    assert dates[1] == epoch_date  # Filled value
    assert dates[2] == datetime.date(2022, 1, 1)  # Original value
    assert dates[3] == epoch_date  # Filled value

    # Other date column should be unchanged
    assert None in result.column(4).to_pylist()


def test_fill_null_type_coercion(null_df):
    """Test type coercion when filling nulls with values of different types."""
    # Try to fill string nulls with a number
    filled_df = null_df.fill_null(42, subset=["str_col"])

    result = filled_df.collect()[0]

    # String nulls should be filled with string representation of the number
    assert result.column(2).to_pylist() == ["a", "42", "c", "42"]

    # Try to fill bool nulls with a string that converts to True
    filled_df = null_df.fill_null("true", subset=["bool_col"])
    result = filled_df.collect()[0]

    # This behavior depends on the implementation - check it works without error
    # but don't make assertions about exact conversion behavior
    assert None not in result.column(3).to_pylist()


def test_fill_null_multiple_date_columns(null_df):
    """Test filling nulls in both date column types simultaneously."""

    # Fill both date column types with the same date
    test_date = datetime.date(2023, 12, 31)
    filled_df = null_df.fill_null(test_date, subset=["date32_col", "date64_col"])

    result = filled_df.collect()[0]

    # Check both date columns were filled correctly
    date32_vals = result.column(4).to_pylist()
    date64_vals = result.column(5).to_pylist()

    assert None not in date32_vals
    assert None not in date64_vals

    assert date32_vals[1] == test_date
    assert date32_vals[3] == test_date
    assert date64_vals[1] == test_date
    assert date64_vals[3] == test_date


def test_fill_null_specific_types(null_df):
    """Test filling nulls with type-appropriate values."""
    # Fill with type-specific values
    filled_df = null_df.fill_null("missing")

    result = filled_df.collect()[0]

    # Check that nulls were filled appropriately by type

    assert result.column(0).to_pylist() == [1, None, 3, None]
    assert result.column(1).to_pylist() == [4.5, 6.7, None, None]
    assert result.column(2).to_pylist() == ["a", "missing", "c", "missing"]
    assert result.column(3).to_pylist() == [True, None, False, None]  # Bool gets False
    assert result.column(4).to_pylist() == [
        datetime.date(2000, 1, 1),
        None,
        datetime.date(2022, 1, 1),
        None,
    ]
    assert result.column(5).to_pylist() == [
        datetime.date(2000, 1, 1),
        None,
        datetime.date(2022, 1, 1),
        None,
    ]


def test_fill_null_immutability(null_df):
    """Test that original DataFrame is unchanged after fill_null."""
    # Get original values with nulls
    original = null_df.collect()[0]
    original_int_nulls = original.column(0).to_pylist().count(None)

    # Apply fill_null
    _filled_df = null_df.fill_null(0)

    # Check that original is unchanged
    new_original = null_df.collect()[0]
    new_original_int_nulls = new_original.column(0).to_pylist().count(None)

    assert original_int_nulls == new_original_int_nulls
    assert original_int_nulls > 0  # Ensure we actually had nulls in the first place


def test_fill_null_empty_df(ctx):
    """Test fill_null on empty DataFrame."""
    # Create an empty DataFrame with schema
    batch = pa.RecordBatch.from_arrays(
        [pa.array([], type=pa.int64()), pa.array([], type=pa.string())],
        names=["a", "b"],
    )
    empty_df = ctx.create_dataframe([[batch]])

    # Fill nulls (should work without errors)
    filled_df = empty_df.fill_null(0)

    # Should still be empty but with same schema
    result = filled_df.collect()[0]
    assert len(result.column(0)) == 0
    assert len(result.column(1)) == 0
    assert result.schema.field(0).name == "a"
    assert result.schema.field(1).name == "b"


def test_fill_null_all_null_column(ctx):
    """Test fill_null on a column with all nulls."""
    # Create DataFrame with a column of all nulls
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([None, None, None], type=pa.string())],
        names=["a", "b"],
    )
    all_null_df = ctx.create_dataframe([[batch]])

    # Fill nulls with a value
    filled_df = all_null_df.fill_null("filled")

    # Check that all nulls were filled
    result = filled_df.collect()[0]
    assert result.column(1).to_pylist() == ["filled", "filled", "filled"]


def test_collect_interrupted():
    """Test that a long-running query can be interrupted with Ctrl-C.

    This test simulates a Ctrl-C keyboard interrupt by raising a KeyboardInterrupt
    exception in the main thread during a long-running query execution.
    """
    # Create a context and a DataFrame with a query that will run for a while
    ctx = SessionContext()

    # Create a recursive computation that will run for some time
    batches = []
    for i in range(10):
        batch = pa.RecordBatch.from_arrays(
            [
                pa.array(list(range(i * 1000, (i + 1) * 1000))),
                pa.array([f"value_{j}" for j in range(i * 1000, (i + 1) * 1000)]),
            ],
            names=["a", "b"],
        )
        batches.append(batch)

    # Register tables
    ctx.register_record_batches("t1", [batches])
    ctx.register_record_batches("t2", [batches])

    # Create a large join operation that will take time to process
    df = ctx.sql("""
        WITH t1_expanded AS (
            SELECT
                a,
                b,
                CAST(a AS DOUBLE) / 1.5 AS c,
                CAST(a AS DOUBLE) * CAST(a AS DOUBLE) AS d
            FROM t1
            CROSS JOIN (SELECT 1 AS dummy FROM t1 LIMIT 5)
        ),
        t2_expanded AS (
            SELECT
                a,
                b,
                CAST(a AS DOUBLE) * 2.5 AS e,
                CAST(a AS DOUBLE) * CAST(a AS DOUBLE) * CAST(a AS DOUBLE) AS f
            FROM t2
            CROSS JOIN (SELECT 1 AS dummy FROM t2 LIMIT 5)
        )
        SELECT
            t1.a, t1.b, t1.c, t1.d,
            t2.a AS a2, t2.b AS b2, t2.e, t2.f
        FROM t1_expanded t1
        JOIN t2_expanded t2 ON t1.a % 100 = t2.a % 100
        WHERE t1.a > 100 AND t2.a > 100
    """)

    # Flag to track if the query was interrupted
    interrupted = False
    interrupt_error = None
    main_thread = threading.main_thread()

    # Shared flag to indicate query execution has started
    query_started = threading.Event()
    max_wait_time = 5.0  # Maximum wait time in seconds

    # This function will be run in a separate thread and will raise
    # KeyboardInterrupt in the main thread
    def trigger_interrupt():
        """Poll for query start, then raise KeyboardInterrupt in the main thread"""
        # Poll for query to start with small sleep intervals
        start_time = time.time()
        while not query_started.is_set():
            time.sleep(0.1)  # Small sleep between checks
            if time.time() - start_time > max_wait_time:
                msg = f"Query did not start within {max_wait_time} seconds"
                raise RuntimeError(msg)

        # Check if thread ID is available
        thread_id = main_thread.ident
        if thread_id is None:
            msg = "Cannot get main thread ID"
            raise RuntimeError(msg)

        # Use ctypes to raise exception in main thread
        exception = ctypes.py_object(KeyboardInterrupt)
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_long(thread_id), exception
        )
        if res != 1:
            # If res is 0, the thread ID was invalid
            # If res > 1, we modified multiple threads
            ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_long(thread_id), ctypes.py_object(0)
            )
            msg = "Failed to raise KeyboardInterrupt in main thread"
            raise RuntimeError(msg)

    # Start a thread to trigger the interrupt
    interrupt_thread = threading.Thread(target=trigger_interrupt)
    # we mark as daemon so the test process can exit even if this thread doesn't finish
    interrupt_thread.daemon = True
    interrupt_thread.start()

    # Execute the query and expect it to be interrupted
    try:
        # Signal that we're about to start the query
        query_started.set()
        df.collect()
    except KeyboardInterrupt:
        interrupted = True
    except Exception as e:
        interrupt_error = e

    # Assert that the query was interrupted properly
    if not interrupted:
        pytest.fail(f"Query was not interrupted; got error: {interrupt_error}")

    # Make sure the interrupt thread has finished
    interrupt_thread.join(timeout=1.0)
