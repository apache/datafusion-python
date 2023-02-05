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
        ],
        names=["a", "b", "c"],
    )
    return ctx.create_dataframe([[batch]])


def test_built_in_aggregation(df):
    col_a = column("a")
    col_b = column("b")
    col_c = column("c")

    agg_df = df.aggregate(
        [],
        [
            f.approx_distinct(col_b),
            f.approx_median(col_b),
            f.approx_percentile_cont(col_b, lit(0.5)),
            f.approx_percentile_cont_with_weight(col_b, lit(0.6), lit(0.5)),
            f.array_agg(col_b),
            f.avg(col_a),
            f.corr(col_a, col_b),
            f.count(col_a),
            f.covar(col_a, col_b),
            f.covar_pop(col_a, col_c),
            f.covar_samp(col_b, col_c),
            # f.grouping(col_a),  # No physical plan implemented yet
            f.max(col_a),
            f.mean(col_b),
            f.median(col_b),
            f.min(col_a),
            f.sum(col_b),
            f.stddev(col_a),
            f.stddev_pop(col_b),
            f.stddev_samp(col_c),
            f.var(col_a),
            f.var_pop(col_b),
            f.var_samp(col_c),
        ],
    )
    result = agg_df.collect()[0]
    values_a, values_b, values_c = df.collect()[0]

    assert result.column(0) == pa.array([2], type=pa.uint64())
    assert result.column(1) == pa.array([4])
    assert result.column(2) == pa.array([4])
    assert result.column(3) == pa.array([6])
    assert result.column(4) == pa.array([[4, 4, 6]])
    np.testing.assert_array_almost_equal(
        result.column(5), np.average(values_a)
    )
    np.testing.assert_array_almost_equal(
        result.column(6), np.corrcoef(values_a, values_b)[0][1]
    )
    assert result.column(7) == pa.array([len(values_a)])
    # Sample (co)variance -> ddof=1
    # Population (co)variance -> ddof=0
    np.testing.assert_array_almost_equal(
        result.column(8), np.cov(values_a, values_b, ddof=1)[0][1]
    )
    np.testing.assert_array_almost_equal(
        result.column(9), np.cov(values_a, values_c, ddof=0)[0][1]
    )
    np.testing.assert_array_almost_equal(
        result.column(10), np.cov(values_b, values_c, ddof=1)[0][1]
    )
    np.testing.assert_array_almost_equal(result.column(11), np.max(values_a))
    np.testing.assert_array_almost_equal(result.column(12), np.mean(values_b))
    np.testing.assert_array_almost_equal(
        result.column(13), np.median(values_b)
    )
    np.testing.assert_array_almost_equal(result.column(14), np.min(values_a))
    np.testing.assert_array_almost_equal(
        result.column(15), np.sum(values_b.to_pylist())
    )
    np.testing.assert_array_almost_equal(
        result.column(16), np.std(values_a, ddof=1)
    )
    np.testing.assert_array_almost_equal(
        result.column(17), np.std(values_b, ddof=0)
    )
    np.testing.assert_array_almost_equal(
        result.column(18), np.std(values_c, ddof=1)
    )
    np.testing.assert_array_almost_equal(
        result.column(19), np.var(values_a, ddof=1)
    )
    np.testing.assert_array_almost_equal(
        result.column(20), np.var(values_b, ddof=0)
    )
    np.testing.assert_array_almost_equal(
        result.column(21), np.var(values_c, ddof=1)
    )
