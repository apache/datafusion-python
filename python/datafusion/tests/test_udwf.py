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

import pyarrow as pa
import pytest

from datafusion import SessionContext, column, udwf, lit, functions as f
from datafusion.udf import WindowEvaluator


class ExponentialSmooth(WindowEvaluator):
    """Interface of a user-defined accumulation."""

    def __init__(self, alpha: float) -> None:
        self.alpha = alpha

    def evaluate_all(self, values: pa.Array, num_rows: int) -> pa.Array:
        results = []
        curr_value = 0.0
        for idx in range(num_rows):
            if idx == 0:
                curr_value = values[idx].as_py()
            else:
                curr_value = values[idx].as_py() * self.alpha + curr_value * (
                    1.0 - self.alpha
                )
            results.append(curr_value)

        return pa.array(results)


class NotSubclassOfWindowEvaluator:
    pass


@pytest.fixture
def df():
    ctx = SessionContext()

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


def test_udwf_errors(df):
    with pytest.raises(TypeError):
        udwf(
            NotSubclassOfWindowEvaluator(),
            pa.float64(),
            pa.float64(),
            volatility="immutable",
        )


smooth = udwf(
    ExponentialSmooth(0.9),
    pa.float64(),
    pa.float64(),
    volatility="immutable",
)

data_test_udwf_functions = [
    ("smooth_udwf", smooth(column("a")), [0, 0.9, 1.89, 2.889, 3.889, 4.889, 5.889]),
    (
        "partitioned_udwf",
        smooth(column("a")).partition_by(column("c")).build(),
        [0, 0.9, 1.89, 2.889, 4.0, 4.9, 5.89],
    ),
    (
        "ordered_udwf",
        smooth(column("a")).order_by(column("b")).build(),
        [0.551, 1.13, 2.3, 2.755, 3.876, 5.0, 5.513],
    ),
]


@pytest.mark.parametrize("name,expr,expected", data_test_udwf_functions)
def test_udwf_functions(df, name, expr, expected):
    df = df.select("a", f.round(expr, lit(3)).alias(name))

    # execute and collect the first (and only) batch
    result = df.sort(column("a")).select(column(name)).collect()[0]

    assert result.column(0) == pa.array(expected)
