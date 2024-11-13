# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import pyarrow as pa
import datafusion
from datafusion import udwf, functions as f, col, lit
from datafusion.udf import WindowEvaluator
from datafusion.expr import WindowFrame

# This example creates five different examples of user defined window functions in order
# to demonstrate the variety of ways a user may need to implement.


class ExponentialSmoothDefault(WindowEvaluator):
    """Create a running smooth operation across an entire partition at once."""

    def __init__(self, alpha: float) -> None:
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


class SmoothBoundedFromPreviousRow(WindowEvaluator):
    """Smooth over from the previous to current row only."""

    def __init__(self, alpha: float) -> None:
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


class SmoothAcrossRank(WindowEvaluator):
    """Smooth over the rank from the previous rank to current."""

    def __init__(self, alpha: float) -> None:
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
    "Find the value across an entire frame using exponential smoothing"

    def __init__(self, alpha: float) -> None:
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
    """Smooth once column based on a condition of another column.

    If the second column is above a threshold, then smooth over the first column from
    the previous and next rows.
    """

    def __init__(self, alpha: float) -> None:
        self.alpha = alpha

    def evaluate_all(self, values: list[pa.Array], num_rows: int) -> pa.Array:
        results = []
        values_a = values[0]
        values_b = values[1]
        for idx in range(num_rows):
            if not values_b[idx].is_valid:
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


# create a context
ctx = datafusion.SessionContext()

# create a RecordBatch and a new DataFrame from it
batch = pa.RecordBatch.from_arrays(
    [
        pa.array([1.0, 2.1, 2.9, 4.0, 5.1, 6.0, 6.9, 8.0]),
        pa.array([1, 2, None, 4, 5, 6, None, 8]),
        pa.array(["A", "A", "A", "A", "A", "B", "B", "B"]),
    ],
    names=["a", "b", "c"],
)
df = ctx.create_dataframe([[batch]])

exp_smooth = udwf(
    lambda: ExponentialSmoothDefault(0.9),
    pa.float64(),
    pa.float64(),
    volatility="immutable",
)

smooth_two_row = udwf(
    lambda: SmoothBoundedFromPreviousRow(0.9),
    pa.float64(),
    pa.float64(),
    volatility="immutable",
)

smooth_rank = udwf(
    lambda: SmoothAcrossRank(0.9),
    pa.float64(),
    pa.float64(),
    volatility="immutable",
)

smooth_frame = udwf(
    lambda: ExponentialSmoothFrame(0.9),
    pa.float64(),
    pa.float64(),
    volatility="immutable",
    name="smooth_frame",
)

smooth_two_col = udwf(
    lambda: SmoothTwoColumn(0.9),
    [pa.float64(), pa.int64()],
    pa.float64(),
    volatility="immutable",
)

# These are done with separate statements instead of one large `select` because that will
# attempt to combine the window operations and our defined UDFs do not all support that.
(
    df.with_column("exp_smooth", exp_smooth(col("a")))
    .with_column("smooth_prior_row", smooth_two_row(col("a")))
    .with_column("smooth_rank", smooth_rank(col("a")).order_by(col("c")).build())
    .with_column("smooth_two_col", smooth_two_col(col("a"), col("b")))
    .with_column(
        "smooth_frame",
        smooth_frame(col("a")).window_frame(WindowFrame("rows", None, 0)).build(),
    )
    .select(
        "a",
        "b",
        "c",
        "exp_smooth",
        "smooth_prior_row",
        "smooth_rank",
        "smooth_two_col",
        "smooth_frame",
    )
).show()

assert df.select(f.round(exp_smooth(col("a")), lit(3))).collect()[0].column(
    0
) == pa.array([1, 1.99, 2.809, 3.881, 4.978, 5.898, 6.8, 7.88])


assert df.select(f.round(smooth_two_row(col("a")), lit(3))).collect()[0].column(
    0
) == pa.array([1.0, 1.99, 2.82, 3.89, 4.99, 5.91, 6.81, 7.89])


assert df.select(smooth_rank(col("a")).order_by(col("c")).build()).collect()[0].column(
    0
) == pa.array([1, 1, 1, 1, 1, 1.9, 2.0, 2.0])


assert df.select(smooth_two_col(col("a"), col("b"))).collect()[0].column(0) == pa.array(
    [1, 2.1, 2.29, 4, 5.1, 6, 6.2, 8.0]
)


assert df.select(
    f.round(
        smooth_frame(col("a")).window_frame(WindowFrame("rows", None, 0)).build(),
        lit(3),
    )
).collect()[0].column(0) == pa.array([1, 1.99, 2.809, 3.881, 4.978, 5.898, 6.8, 7.88])
