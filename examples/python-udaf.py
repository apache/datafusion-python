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

import datafusion
import pyarrow
import pyarrow.compute
from datafusion import Accumulator, col, udaf


class MyAccumulator(Accumulator):
    """
    Interface of a user-defined accumulation.
    """

    def __init__(self) -> None:
        self._sum = pyarrow.scalar(0.0)

    def update(self, values: pyarrow.Array) -> None:
        # not nice since pyarrow scalars can't be summed yet. This breaks on `None`
        self._sum = pyarrow.scalar(
            self._sum.as_py() + pyarrow.compute.sum(values).as_py()
        )

    def merge(self, states: pyarrow.Array) -> None:
        # not nice since pyarrow scalars can't be summed yet. This breaks on `None`
        self._sum = pyarrow.scalar(
            self._sum.as_py() + pyarrow.compute.sum(states).as_py()
        )

    def state(self) -> pyarrow.Array:
        return pyarrow.array([self._sum.as_py()])

    def evaluate(self) -> pyarrow.Scalar:
        return self._sum


# create a context
ctx = datafusion.SessionContext()

# create a RecordBatch and a new DataFrame from it
batch = pyarrow.RecordBatch.from_arrays(
    [pyarrow.array([1, 2, 3]), pyarrow.array([4, 5, 6])],
    names=["a", "b"],
)
df = ctx.create_dataframe([[batch]])

my_udaf = udaf(
    MyAccumulator,
    pyarrow.float64(),
    pyarrow.float64(),
    [pyarrow.float64()],
    "stable",
)

df = df.aggregate([], [my_udaf(col("a"))])

result = df.collect()[0]

assert result.column(0) == pyarrow.array([6.0])
