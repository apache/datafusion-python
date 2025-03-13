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
from datafusion.expr import WindowFrame
from datafusion.udf import WindowEvaluator


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
def df(ctx):
    # create a RecordBatch and a new DataFrame from it
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 4, 6])],
        names=["a", "b"],
    )
    return ctx.create_dataframe([[batch]], name="test_table")


def test_udwf_errors():
    """Test error cases for UDWF creation."""
    with pytest.raises(
        TypeError, match="`func` must implement the abstract base class WindowEvaluator"
    ):
        udwf(
            NotSubclassOfWindowEvaluator, pa.int64(), pa.int64(), volatility="immutable"
        )


def test_udwf_basic_usage(df):
    """Test basic UDWF usage with a simple counting window function."""
    simple_count = udwf(
        SimpleWindowCount, pa.int64(), pa.int64(), volatility="immutable"
    )

    df = df.select(
        simple_count(column("a"))
        .window_frame(WindowFrame("rows", None, None))
        .build()
        .alias("count")
    )
    result = df.collect()[0]
    assert result.column(0) == pa.array([0, 1, 2])


def test_udwf_with_args(df):
    """Test UDWF with constructor arguments."""
    count_base10 = udwf(
        lambda: SimpleWindowCount(10), pa.int64(), pa.int64(), volatility="immutable"
    )

    df = df.select(
        count_base10(column("a"))
        .window_frame(WindowFrame("rows", None, None))
        .build()
        .alias("count")
    )
    result = df.collect()[0]
    assert result.column(0) == pa.array([10, 11, 12])


def test_udwf_decorator_basic(df):
    """Test UDWF used as a decorator."""

    @udwf([pa.int64()], pa.int64(), "immutable")
    def window_count() -> WindowEvaluator:
        return SimpleWindowCount()

    df = df.select(
        window_count(column("a"))
        .window_frame(WindowFrame("rows", None, None))
        .build()
        .alias("count")
    )
    result = df.collect()[0]
    assert result.column(0) == pa.array([0, 1, 2])


def test_udwf_decorator_with_args(df):
    """Test UDWF decorator with constructor arguments."""

    @udwf([pa.int64()], pa.int64(), "immutable")
    def window_count_base10() -> WindowEvaluator:
        return SimpleWindowCount(10)

    df = df.select(
        window_count_base10(column("a"))
        .window_frame(WindowFrame("rows", None, None))
        .build()
        .alias("count")
    )
    result = df.collect()[0]
    assert result.column(0) == pa.array([10, 11, 12])


def test_register_udwf(ctx, df):
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
        "SELECT window_count(a) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM test_table"
    ).collect()[0]
    assert result.column(0) == pa.array([0, 1, 2])
