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

import numpy as np
import pyarrow as pa
import pytest

from datafusion import SessionContext, column, lit
from datafusion import functions as f


@pytest.fixture
def df():
    ctx = SessionContext()

    # create a RecordBatch and a new DataFrame from it
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array([1, 2, 3]),
            pa.array([4, 4, 6]),
            pa.array([9, 8, 5]),
            pa.array([True, True, False]),
            pa.array([1, 2, None]),
        ],
        names=["a", "b", "c", "d", "e"],
    )
    return ctx.create_dataframe([[batch]])


@pytest.fixture
def df_aggregate_100():
    ctx = SessionContext()
    ctx.register_csv("aggregate_test_data", "./testing/data/csv/aggregate_test_100.csv")
    return ctx.table("aggregate_test_data")


@pytest.mark.parametrize(
    "agg_expr, calc_expected",
    [
        (f.avg(column("a")), lambda a, b, c, d: np.array(np.average(a))),
        (
            f.corr(column("a"), column("b")),
            lambda a, b, c, d: np.array(np.corrcoef(a, b)[0][1]),
        ),
        (f.count(column("a")), lambda a, b, c, d: pa.array([len(a)])),
        # Sample (co)variance -> ddof=1
        # Population (co)variance -> ddof=0
        (
            f.covar(column("a"), column("b")),
            lambda a, b, c, d: np.array(np.cov(a, b, ddof=1)[0][1]),
        ),
        (
            f.covar_pop(column("a"), column("c")),
            lambda a, b, c, d: np.array(np.cov(a, c, ddof=0)[0][1]),
        ),
        (
            f.covar_samp(column("b"), column("c")),
            lambda a, b, c, d: np.array(np.cov(b, c, ddof=1)[0][1]),
        ),
        # f.grouping(col_a),  # No physical plan implemented yet
        (f.max(column("a")), lambda a, b, c, d: np.array(np.max(a))),
        (f.mean(column("b")), lambda a, b, c, d: np.array(np.mean(b))),
        (f.median(column("b")), lambda a, b, c, d: np.array(np.median(b))),
        (f.min(column("a")), lambda a, b, c, d: np.array(np.min(a))),
        (f.sum(column("b")), lambda a, b, c, d: np.array(np.sum(b.to_pylist()))),
        # Sample stdev -> ddof=1
        # Population stdev -> ddof=0
        (f.stddev(column("a")), lambda a, b, c, d: np.array(np.std(a, ddof=1))),
        (f.stddev_pop(column("b")), lambda a, b, c, d: np.array(np.std(b, ddof=0))),
        (f.stddev_samp(column("c")), lambda a, b, c, d: np.array(np.std(c, ddof=1))),
        (f.var(column("a")), lambda a, b, c, d: np.array(np.var(a, ddof=1))),
        (f.var_pop(column("b")), lambda a, b, c, d: np.array(np.var(b, ddof=0))),
        (f.var_samp(column("c")), lambda a, b, c, d: np.array(np.var(c, ddof=1))),
    ],
)
def test_aggregation_stats(df, agg_expr, calc_expected):
    df = df.select("a", "b", "c", "d")
    agg_df = df.aggregate([], [agg_expr])
    result = agg_df.collect()[0]
    values_a, values_b, values_c, values_d = df.collect()[0]
    expected = calc_expected(values_a, values_b, values_c, values_d)
    np.testing.assert_array_almost_equal(result.column(0), expected)


@pytest.mark.parametrize(
    "agg_expr, expected, array_sort",
    [
        (f.approx_distinct(column("b")), pa.array([2], type=pa.uint64()), False),
        (
            f.approx_distinct(
                column("b"),
                filter=column("a") != lit(3),
            ),
            pa.array([1], type=pa.uint64()),
            False,
        ),
        (f.approx_median(column("b")), pa.array([4]), False),
        (f.approx_median(column("b"), filter=column("a") != 2), pa.array([5]), False),
        (f.approx_percentile_cont(column("b"), 0.5), pa.array([4]), False),
        (
            f.approx_percentile_cont_with_weight(column("b"), lit(0.6), 0.5),
            pa.array([6], type=pa.float64()),
            False,
        ),
        (
            f.approx_percentile_cont_with_weight(
                column("b"), lit(0.6), 0.5, filter=column("a") != lit(3)
            ),
            pa.array([4], type=pa.float64()),
            False,
        ),
        (f.array_agg(column("b")), pa.array([[4, 4, 6]]), False),
        (f.array_agg(column("b"), distinct=True), pa.array([[4, 6]]), True),
        (
            f.array_agg(column("e"), filter=column("e").is_not_null()),
            pa.array([[1, 2]]),
            False,
        ),
        (
            f.array_agg(column("b"), order_by=[column("c")]),
            pa.array([[6, 4, 4]]),
            False,
        ),
        (f.avg(column("b"), filter=column("a") != lit(1)), pa.array([5.0]), False),
        (f.count(column("b"), distinct=True), pa.array([2]), False),
        (f.count(column("b"), filter=column("a") != 3), pa.array([2]), False),
        (f.count(), pa.array([3]), False),
        (f.count(column("e")), pa.array([2]), False),
        (f.count_star(filter=column("a") != 3), pa.array([2]), False),
    ],
)
def test_aggregation(df, agg_expr, expected, array_sort):
    agg_df = df.aggregate([], [agg_expr.alias("agg_expr")])
    if array_sort:
        agg_df = agg_df.select(f.array_sort(column("agg_expr")))
    agg_df.show()
    result = agg_df.collect()[0]

    print(result)
    assert result.column(0) == expected


@pytest.mark.parametrize(
    "name,expr,expected",
    [
        (
            "approx_percentile_cont",
            f.approx_percentile_cont(column("c3"), 0.95, num_centroids=200),
            [73, 68, 122, 124, 115],
        ),
        (
            "approx_perc_cont_few_centroids",
            f.approx_percentile_cont(column("c3"), 0.95, num_centroids=5),
            [72, 68, 119, 124, 115],
        ),
        (
            "approx_perc_cont_filtered",
            f.approx_percentile_cont(
                column("c3"), 0.95, num_centroids=200, filter=column("c3") > lit(0)
            ),
            [83, 68, 122, 124, 117],
        ),
        (
            "corr",
            f.corr(column("c3"), column("c2")),
            [-0.1056, -0.2808, 0.0023, 0.0022, -0.2473],
        ),
        (
            "corr_w_filter",
            f.corr(column("c3"), column("c2"), filter=column("c3") > lit(0)),
            [-0.3298, 0.2925, 0.2467, -0.2269, 0.0358],
        ),
    ],
)
def test_aggregate_100(df_aggregate_100, name, expr, expected):
    # https://github.com/apache/datafusion/blob/bddb6415a50746d2803dd908d19c3758952d74f9/datafusion/sqllogictest/test_files/aggregate.slt#L1490-L1498

    df = (
        df_aggregate_100.aggregate(
            [column("c1")],
            [expr.alias(name)],
        )
        .select("c1", f.round(column(name), lit(4)).alias(name))
        .sort(column("c1").sort(ascending=True))
    )
    df.show()

    expected_dict = {
        "c1": ["a", "b", "c", "d", "e"],
        name: expected,
    }

    assert df.collect()[0].to_pydict() == expected_dict


data_test_bitwise_and_boolean_functions = [
    ("bit_and", f.bit_and(column("a")), [0]),
    ("bit_and_filter", f.bit_and(column("a"), filter=column("a") != lit(2)), [1]),
    ("bit_or", f.bit_or(column("b")), [6]),
    ("bit_or_filter", f.bit_or(column("b"), filter=column("a") != lit(3)), [4]),
    ("bit_xor", f.bit_xor(column("c")), [4]),
    ("bit_xor_distinct", f.bit_xor(column("b"), distinct=True), [2]),
    ("bit_xor_filter", f.bit_xor(column("b"), filter=column("a") != lit(3)), [0]),
    (
        "bit_xor_filter_distinct",
        f.bit_xor(column("b"), distinct=True, filter=column("a") != lit(3)),
        [4],
    ),
    ("bool_and", f.bool_and(column("d")), [False]),
    ("bool_and_filter", f.bool_and(column("d"), filter=column("a") != lit(3)), [True]),
    ("bool_or", f.bool_or(column("d")), [True]),
    ("bool_or_filter", f.bool_or(column("d"), filter=column("a") == lit(3)), [False]),
]


@pytest.mark.parametrize("name,expr,result", data_test_bitwise_and_boolean_functions)
def test_bit_and_bool_fns(df, name, expr, result):
    df = df.aggregate([], [expr.alias(name)])

    expected = {
        name: result,
    }

    assert df.collect()[0].to_pydict() == expected
