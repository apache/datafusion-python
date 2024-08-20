.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

User Defined Functions
======================

DataFusion provides powerful expressions and functions, reducing the need for custom Python functions.
However you can still incorporate your own functions, i.e. User-Defined Functions (UDFs), with the :py:func:`~datafusion.udf.ScalarUDF.udf` function.

.. ipython:: python

    import pyarrow
    import datafusion
    from datafusion import udf, col

    def is_null(array: pyarrow.Array) -> pyarrow.Array:
        return array.is_null()

    is_null_arr = udf(is_null, [pyarrow.int64()], pyarrow.bool_(), 'stable')

    ctx = datafusion.SessionContext()

    batch = pyarrow.RecordBatch.from_arrays(
        [pyarrow.array([1, 2, 3]), pyarrow.array([4, 5, 6])],
        names=["a", "b"],
    )
    df = ctx.create_dataframe([[batch]], name="batch_array")

    df.select(is_null_arr(col("a"))).to_pandas()

Additionally the :py:func:`~datafusion.udf.AggregateUDF.udaf` function allows you to define User-Defined Aggregate Functions (UDAFs)

.. code-block:: python

    import pyarrow
    import pyarrow.compute
    import datafusion
    from datafusion import col, udaf, Accumulator
    from typing import List

    class MyAccumulator(Accumulator):
        """
        Interface of a user-defined accumulation.
        """
        def __init__(self):
            self._sum = pyarrow.scalar(0.0)

        def update(self, values: pyarrow.Array) -> None:
            # not nice since pyarrow scalars can't be summed yet. This breaks on `None`
            self._sum = pyarrow.scalar(self._sum.as_py() + pyarrow.compute.sum(values).as_py())

        def merge(self, states: List[pyarrow.Array]) -> None:
            # not nice since pyarrow scalars can't be summed yet. This breaks on `None`
            self._sum = pyarrow.scalar(self._sum.as_py() + pyarrow.compute.sum(states[0]).as_py())

        def state(self) -> pyarrow.Array:
            return pyarrow.array([self._sum.as_py()])

        def evaluate(self) -> pyarrow.Scalar:
            return self._sum

    ctx = datafusion.SessionContext()
    df = ctx.from_pydict(
        {
            "a": [1, 2, 3],
            "b": [4, 5, 6],
        }
    )

    my_udaf = udaf(MyAccumulator, pyarrow.float64(), pyarrow.float64(), [pyarrow.float64()], 'stable')

    df.aggregate([],[my_udaf(col("a"))])
