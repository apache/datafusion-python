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

from datafusion import udaf, SessionContext, Accumulator
import pyarrow as pa


# Define a user-defined aggregation function (UDAF)
class MyAccumulator(Accumulator):
    """
    Interface of a user-defined accumulation.
    """

    def __init__(self):
        self._sum = pa.scalar(0.0)

    def update(self, values: pa.Array) -> None:
        # not nice since pyarrow scalars can't be summed yet. This breaks on `None`
        self._sum = pa.scalar(
            self._sum.as_py() + pa.compute.sum(values).as_py()
        )

    def merge(self, states: pa.Array) -> None:
        # not nice since pyarrow scalars can't be summed yet. This breaks on `None`
        self._sum = pa.scalar(
            self._sum.as_py() + pa.compute.sum(states).as_py()
        )

    def state(self) -> pa.Array:
        return pa.array([self._sum.as_py()])

    def evaluate(self) -> pa.Scalar:
        return self._sum


my_udaf = udaf(
    MyAccumulator,
    pa.float64(),
    pa.float64(),
    [pa.float64()],
    "stable",
    # This will be the name of the UDAF in SQL
    # If not specified it will by default the same as accumulator class name
    name="my_accumulator",
)

# Create a context
ctx = SessionContext()

# Create a datafusion DataFrame from a Python dictionary
source_df = ctx.from_pydict({"a": [1, 1, 3], "b": [4, 5, 6]}, name="t")
# Dataframe:
# +---+---+
# | a | b |
# +---+---+
# | 1 | 4 |
# | 1 | 5 |
# | 3 | 6 |
# +---+---+

# Register UDF for use in SQL
ctx.register_udaf(my_udaf)

# Query the DataFrame using SQL
result_df = ctx.sql(
    "select a, my_accumulator(b) as b_aggregated from t group by a order by a"
)
# Dataframe:
# +---+--------------+
# | a | b_aggregated |
# +---+--------------+
# | 1 | 9            |
# | 3 | 6            |
# +---+--------------+
assert result_df.to_pydict()["a"] == [1, 3]
assert result_df.to_pydict()["b_aggregated"] == [9, 6]
