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

import pyarrow as pa
import pytest
from datafusion import SessionContext, column, lit, udwf
from datafusion import functions as f
from datafusion.expr import WindowFrame
from datafusion.user_defined import WindowEvaluator


class ExponentialSmoothDefault(WindowEvaluator):
    def __init__(self, alpha: float = 0.9) -> None:
        self.alpha = alpha

    def evaluate_all(self, values: list[pa.Array], num_rows: int) -> pa.Array:
        results = []
        curr_value = 0.0
        values = values[0]
        for idx in range(num_rows):
            if idx == 0:
                curr_value = values[idx].as_py()
            else:
                curr_value = values[idx].as_py() * self.alpha + curr_value * (
                    1.0 - self.alpha
                )
            results.append(curr_value)

        return pa.array(results)


class ExponentialSmoothBounded(WindowEvaluator):
    def __init__(self, alpha: float = 0.9) -> None:
        self.alpha = alpha

    def supports_bounded_execution(self) -> bool:
        return True

    def get_range(self, idx: int, num_rows: int) -> tuple[int, int]:
        # Override the default range of current row since uses_window_frame is False
        # So for the purpose of this test we just smooth from the previous row to
        # current.
        if idx == 0:
            return (0, 0)
        return (idx - 1, idx)

    def evaluate(
        self, values: list[pa.Array], eval_range: tuple[int, int]
    ) -> pa.Scalar:
        (start, stop) = eval_range
        curr_value = 0.0
        values = values[0]
        for idx in range(start, stop + 1):
            if idx == start:
                curr_value = values[idx].as_py()
            else:
                curr_value = values[idx].as_py() * self.alpha + curr_value * (
                    1.0 - self.alpha
                )
        return pa.scalar(curr_value).cast(pa.float64())


class ExponentialSmoothRank(WindowEvaluator):
    def __init__(self, alpha: float = 0.9) -> None:
        self.alpha = alpha

    def include_rank(self) -> bool:
        return True

    def evaluate_all_with_rank(
        self, num_rows: int, ranks_in_partition: list[tuple[int, int]]
    ) -> pa.Array:
        results = []
        for idx in range(num_rows):
            if idx == 0:
                prior_value = 1.0
            matching_row = [
                i
                for i in range(len(ranks_in_partition))
                if ranks_in_partition[i][0] <= idx and ranks_in_partition[i][1] > idx
            ][0] + 1
            curr_value = matching_row * self.alpha + prior_value * (1.0 - self.alpha)
            results.append(curr_value)
            prior_value = matching_row

        return pa.array(results)


class ExponentialSmoothFrame(WindowEvaluator):
    def __init__(self, alpha: float = 0.9) -> None:
        self.alpha = alpha

    def uses_window_frame(self) -> bool:
        return True

    def evaluate(
        self, values: list[pa.Array], eval_range: tuple[int, int]
    ) -> pa.Scalar:
        (start, stop) = eval_range
        curr_value = 0.0
        if len(values) > 1:
            order_by = values[1]  # noqa: F841
            values = values[0]
        else:
            values = values[0]
        for idx in range(start, stop):
            if idx == start:
                curr_value = values[idx].as_py()
            else:
                curr_value = values[idx].as_py() * self.alpha + curr_value * (
                    1.0 - self.alpha
                )
        return pa.scalar(curr_value).cast(pa.float64())


class SmoothTwoColumn(WindowEvaluator):
    """This class demonstrates using two columns.

    If the second column is above a threshold, then smooth over the first column from
    the previous and next rows.
    """

    def __init__(self, alpha: float = 0.9) -> None:
        self.alpha = alpha

    def evaluate_all(self, values: list[pa.Array], num_rows: int) -> pa.Array:
        results = []
        values_a = values[0]
        values_b = values[1]
        for idx in range(num_rows):
            if values_b[idx].as_py() > 7:
                if idx == 0:
                    results.append(values_a[1].cast(pa.float64()))
                elif idx == num_rows - 1:
                    results.append(values_a[num_rows - 2].cast(pa.float64()))
                else:
                    results.append(
                        pa.scalar(
                            values_a[idx - 1].as_py() * self.alpha
                            + values_a[idx + 1].as_py() * (1.0 - self.alpha)
                        )
                    )
            else:
                results.append(values_a[idx].cast(pa.float64()))

        return pa.array(results)


class SimpleWindowCount(WindowEvaluator):
    """A simple window evaluator that counts rows."""

    def __init__(self, base: int = 0) -> None:
        self.base = base

    def evaluate_all(self, values: list[pa.Array], num_rows: int) -> pa.Array:
        return pa.array([self.base + i for i in range(num_rows)])


class NotSubclassOfWindowEvaluator:
    pass


@pytest.fixture
def ctx():
    return SessionContext()


@pytest.fixture
def complex_window_df(ctx):
    # create a RecordBatch and a new DataFrame from it
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array([0, 1, 2, 3, 4, 5, 6]),
            pa.array([7, 4, 3, 8, 9, 1, 6]),
            pa.array(["A", "A", "A", "A", "B", "B", "B"]),
        ],
        names=["a", "b", "c"],
    )
    return ctx.create_dataframe([[batch]])


@pytest.fixture
def count_window_df(ctx):
    # create a RecordBatch and a new DataFrame from it
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 4, 6])],
        names=["a", "b"],
    )
    return ctx.create_dataframe([[batch]], name="test_table")


def test_udwf_errors(complex_window_df):
    with pytest.raises(TypeError):
        udwf(
            NotSubclassOfWindowEvaluator,
            pa.float64(),
            pa.float64(),
            volatility="immutable",
        )


def test_udwf_errors_with_message():
    """Test error cases for UDWF creation."""
    with pytest.raises(
        TypeError, match="`func` must implement the abstract base class WindowEvaluator"
    ):
        udwf(
            NotSubclassOfWindowEvaluator, pa.int64(), pa.int64(), volatility="immutable"
        )


def test_udwf_basic_usage(count_window_df):
    """Test basic UDWF usage with a simple counting window function."""
    simple_count = udwf(
        SimpleWindowCount, pa.int64(), pa.int64(), volatility="immutable"
    )

    df = count_window_df.select(
        simple_count(column("a"))
        .window_frame(WindowFrame("rows", None, None))
        .build()
        .alias("count")
    )
    result = df.collect()[0]
    assert result.column(0) == pa.array([0, 1, 2])


def test_udwf_with_args(count_window_df):
    """Test UDWF with constructor arguments."""
    count_base10 = udwf(
        lambda: SimpleWindowCount(10), pa.int64(), pa.int64(), volatility="immutable"
    )

    df = count_window_df.select(
        count_base10(column("a"))
        .window_frame(WindowFrame("rows", None, None))
        .build()
        .alias("count")
    )
    result = df.collect()[0]
    assert result.column(0) == pa.array([10, 11, 12])


def test_udwf_decorator_basic(count_window_df):
    """Test UDWF used as a decorator."""

    @udwf([pa.int64()], pa.int64(), "immutable")
    def window_count() -> WindowEvaluator:
        return SimpleWindowCount()

    df = count_window_df.select(
        window_count(column("a"))
        .window_frame(WindowFrame("rows", None, None))
        .build()
        .alias("count")
    )
    result = df.collect()[0]
    assert result.column(0) == pa.array([0, 1, 2])


def test_udwf_decorator_with_args(count_window_df):
    """Test UDWF decorator with constructor arguments."""

    @udwf([pa.int64()], pa.int64(), "immutable")
    def window_count_base10() -> WindowEvaluator:
        return SimpleWindowCount(10)

    df = count_window_df.select(
        window_count_base10(column("a"))
        .window_frame(WindowFrame("rows", None, None))
        .build()
        .alias("count")
    )
    result = df.collect()[0]
    assert result.column(0) == pa.array([10, 11, 12])


def test_register_udwf(ctx, count_window_df):
    """Test registering and using UDWF in SQL context."""
    window_count = udwf(
        SimpleWindowCount,
        [pa.int64()],
        pa.int64(),
        volatility="immutable",
        name="window_count",
    )

    ctx.register_udwf(window_count)
    result = ctx.sql(
        """
        SELECT window_count(a)
        OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED
        FOLLOWING) FROM test_table
        """
    ).collect()[0]
    assert result.column(0) == pa.array([0, 1, 2])


smooth_default = udwf(
    ExponentialSmoothDefault,
    pa.float64(),
    pa.float64(),
    volatility="immutable",
)

smooth_w_arguments = udwf(
    lambda: ExponentialSmoothDefault(0.8),
    pa.float64(),
    pa.float64(),
    volatility="immutable",
)

smooth_bounded = udwf(
    ExponentialSmoothBounded,
    pa.float64(),
    pa.float64(),
    volatility="immutable",
)

smooth_rank = udwf(
    ExponentialSmoothRank,
    pa.utf8(),
    pa.float64(),
    volatility="immutable",
)

smooth_frame = udwf(
    ExponentialSmoothFrame,
    pa.float64(),
    pa.float64(),
    volatility="immutable",
)

smooth_two_col = udwf(
    SmoothTwoColumn,
    [pa.int64(), pa.int64()],
    pa.float64(),
    volatility="immutable",
)

data_test_udwf_functions = [
    (
        "default_udwf_no_arguments",
        smooth_default(column("a")),
        [0, 0.9, 1.89, 2.889, 3.889, 4.889, 5.889],
    ),
    (
        "default_udwf_w_arguments",
        smooth_w_arguments(column("a")),
        [0, 0.8, 1.76, 2.752, 3.75, 4.75, 5.75],
    ),
    (
        "default_udwf_partitioned",
        smooth_default(column("a")).partition_by(column("c")).build(),
        [0, 0.9, 1.89, 2.889, 4.0, 4.9, 5.89],
    ),
    (
        "default_udwf_ordered",
        smooth_default(column("a")).order_by(column("b")).build(),
        [0.551, 1.13, 2.3, 2.755, 3.876, 5.0, 5.513],
    ),
    (
        "bounded_udwf",
        smooth_bounded(column("a")),
        [0, 0.9, 1.9, 2.9, 3.9, 4.9, 5.9],
    ),
    (
        "bounded_udwf_ignores_frame",
        smooth_bounded(column("a"))
        .window_frame(WindowFrame("rows", None, None))
        .build(),
        [0, 0.9, 1.9, 2.9, 3.9, 4.9, 5.9],
    ),
    (
        "rank_udwf",
        smooth_rank(column("c")).order_by(column("c")).build(),
        [1, 1, 1, 1, 1.9, 2, 2],
    ),
    (
        "frame_unbounded_udwf",
        smooth_frame(column("a")).window_frame(WindowFrame("rows", None, None)).build(),
        [5.889, 5.889, 5.889, 5.889, 5.889, 5.889, 5.889],
    ),
    (
        "frame_bounded_udwf",
        smooth_frame(column("a")).window_frame(WindowFrame("rows", None, 0)).build(),
        [0.0, 0.9, 1.89, 2.889, 3.889, 4.889, 5.889],
    ),
    (
        "frame_bounded_udwf",
        smooth_frame(column("a"))
        .window_frame(WindowFrame("rows", None, 0))
        .order_by(column("b"))
        .build(),
        [0.551, 1.13, 2.3, 2.755, 3.876, 5.0, 5.513],
    ),
    (
        "two_column_udwf",
        smooth_two_col(column("a"), column("b")),
        [0.0, 1.0, 2.0, 2.2, 3.2, 5.0, 6.0],
    ),
]


@pytest.mark.parametrize(("name", "expr", "expected"), data_test_udwf_functions)
def test_udwf_functions(complex_window_df, name, expr, expected):
    df = complex_window_df.select("a", "b", f.round(expr, lit(3)).alias(name))

    # execute and collect the first (and only) batch
    result = df.sort(column("a")).select(column(name)).collect()[0]

    assert result.column(0) == pa.array(expected)


@pytest.mark.parametrize(
    "udwf_func",
    [
        udwf(SimpleWindowCount, pa.int64(), pa.int64(), "immutable"),
        udwf(SimpleWindowCount, [pa.int64()], pa.int64(), "immutable"),
        udwf([pa.int64()], pa.int64(), "immutable")(lambda: SimpleWindowCount()),
        udwf(pa.int64(), pa.int64(), "immutable")(lambda: SimpleWindowCount()),
    ],
)
def test_udwf_overloads(udwf_func, count_window_df):
    df = count_window_df.select(
        udwf_func(column("a"))
        .window_frame(WindowFrame("rows", None, None))
        .build()
        .alias("count")
    )
    result = df.collect()[0]
    assert result.column(0) == pa.array([0, 1, 2])


def test_udwf_named_function(ctx, count_window_df):
    """Test UDWF with explicit name parameter."""
    window_count = udwf(
        SimpleWindowCount,
        pa.int64(),
        pa.int64(),
        volatility="immutable",
        name="my_custom_counter",
    )

    ctx.register_udwf(window_count)
    result = ctx.sql(
        """
        SELECT my_custom_counter(a)
        OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED
        FOLLOWING) FROM test_table"""
    ).collect()[0]
    assert result.column(0) == pa.array([0, 1, 2])
