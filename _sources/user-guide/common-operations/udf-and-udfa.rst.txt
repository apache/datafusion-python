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

User-Defined Functions
======================

DataFusion provides powerful expressions and functions, reducing the need for custom Python
functions. However you can still incorporate your own functions, i.e. User-Defined Functions (UDFs).

Scalar Functions
----------------

When writing a user-defined function that can operate on a row by row basis, these are called Scalar
Functions. You can define your own scalar function by calling
:py:func:`~datafusion.udf.ScalarUDF.udf` .

The basic definition of a scalar UDF is a python function that takes one or more
`pyarrow <https://arrow.apache.org/docs/python/index.html>`_ arrays and returns a single array as
output. DataFusion scalar UDFs operate on an entire batch of records at a time, though the
evaluation of those records should be on a row by row basis. In the following example, we compute
if the input array contains null values.

.. ipython:: python

    import pyarrow
    import datafusion
    from datafusion import udf, col

    def is_null(array: pyarrow.Array) -> pyarrow.Array:
        return array.is_null()

    is_null_arr = udf(is_null, [pyarrow.int64()], pyarrow.bool_(), 'stable')

    ctx = datafusion.SessionContext()

    batch = pyarrow.RecordBatch.from_arrays(
        [pyarrow.array([1, None, 3]), pyarrow.array([4, 5, 6])],
        names=["a", "b"],
    )
    df = ctx.create_dataframe([[batch]], name="batch_array")

    df.select(col("a"), is_null_arr(col("a")).alias("is_null")).show()

In the previous example, we used the fact that pyarrow provides a variety of built in array
functions such as ``is_null()``. There are additional pyarrow
`compute functions <https://arrow.apache.org/docs/python/compute.html>`_ available. When possible,
it is highly recommended to use these functions because they can perform computations without doing
any copy operations from the original arrays. This leads to greatly improved performance.

If you need to perform an operation in python that is not available with the pyarrow compute
functions, you will need to convert the record batch into python values, perform your operation,
and construct an array. This operation of converting the built in data type of the array into a
python object can be one of the slowest operations in DataFusion, so it should be done sparingly.

The following example performs the same operation as before with ``is_null`` but demonstrates
converting to Python objects to do the evaluation.

.. ipython:: python

    import pyarrow
    import datafusion
    from datafusion import udf, col

    def is_null(array: pyarrow.Array) -> pyarrow.Array:
        return pyarrow.array([value.as_py() is None for value in array])

    is_null_arr = udf(is_null, [pyarrow.int64()], pyarrow.bool_(), 'stable')

    ctx = datafusion.SessionContext()

    batch = pyarrow.RecordBatch.from_arrays(
        [pyarrow.array([1, None, 3]), pyarrow.array([4, 5, 6])],
        names=["a", "b"],
    )
    df = ctx.create_dataframe([[batch]], name="batch_array")

    df.select(col("a"), is_null_arr(col("a")).alias("is_null")).show()

Aggregate Functions
-------------------

The :py:func:`~datafusion.udf.AggregateUDF.udaf` function allows you to define User-Defined
Aggregate Functions (UDAFs). To use this you must implement an
:py:class:`~datafusion.udf.Accumulator` that determines how the aggregation is performed.

When defining a UDAF there are four methods you need to implement. The ``update`` function takes the
array(s) of input and updates the internal state of the accumulator. You should define this function
to have as many input arguments as you will pass when calling the UDAF. Since aggregation may be
split into multiple batches, we must have a method to combine multiple batches. For this, we have
two functions, ``state`` and ``merge``. ``state`` will return an array of scalar values that contain
the current state of a single batch accumulation. Then we must ``merge`` the results of these
different states. Finally ``evaluate`` is the call that will return the final result after the
``merge`` is complete.

In the following example we want to define a custom aggregate function that will return the
difference between the sum of two columns. The state can be represented by a single value and we can
also see how the inputs to ``update`` and ``merge`` differ.

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
            self._sum = 0.0

        def update(self, values_a: pyarrow.Array, values_b: pyarrow.Array) -> None:
            self._sum = self._sum + pyarrow.compute.sum(values_a).as_py() - pyarrow.compute.sum(values_b).as_py()

        def merge(self, states: List[pyarrow.Array]) -> None:
            self._sum = self._sum + pyarrow.compute.sum(states[0]).as_py()

        def state(self) -> pyarrow.Array:
            return pyarrow.array([self._sum])

        def evaluate(self) -> pyarrow.Scalar:
            return pyarrow.scalar(self._sum)

    ctx = datafusion.SessionContext()
    df = ctx.from_pydict(
        {
            "a": [4, 5, 6],
            "b": [1, 2, 3],
        }
    )

    my_udaf = udaf(MyAccumulator, [pyarrow.float64(), pyarrow.float64()], pyarrow.float64(), [pyarrow.float64()], 'stable')

    df.aggregate([], [my_udaf(col("a"), col("b")).alias("col_diff")])

Window Functions
----------------

To implement a User-Defined Window Function (UDWF) you must call the
:py:func:`~datafusion.udf.WindowUDF.udwf` function using a class that implements the abstract
class :py:class:`~datafusion.udf.WindowEvaluator`.

There are three methods of evaluation of UDWFs.

- ``evaluate`` is the simplest case, where you are given an array and are expected to calculate the
  value for a single row of that array. This is the simplest case, but also the least performant.
- ``evaluate_all`` computes the values for all rows for an input array at a single time.
- ``evaluate_all_with_rank`` computes the values for all rows, but you only have the rank
  information for the rows.

Which methods you implement are based upon which of these options are set.

.. list-table::
   :header-rows: 1

   * - ``uses_window_frame``
     - ``supports_bounded_execution``
     - ``include_rank``
     - function_to_implement
   * - False (default)
     - False (default)
     - False (default)
     - ``evaluate_all``
   * - False
     - True
     - False
     - ``evaluate``
   * - False
     - True
     - False
     - ``evaluate_all_with_rank``
   * - True
     - True/False
     - True/False
     - ``evaluate``

UDWF options
^^^^^^^^^^^^

When you define your UDWF you can override the functions that return these values. They will
determine which evaluate functions are called.

- ``uses_window_frame`` is set for functions that compute based on the specified window frame. If
  your function depends upon the specified frame, set this to ``True``.
- ``supports_bounded_execution`` specifies if your function can be incrementally computed.
- ``include_rank`` is set to ``True`` for window functions that can be computed only using the rank
  information.


.. code-block:: python

    import pyarrow as pa
    from datafusion import udwf, col, SessionContext
    from datafusion.udf import WindowEvaluator

    class ExponentialSmooth(WindowEvaluator):
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

    exp_smooth = udwf(
        ExponentialSmooth(0.9),
        pa.float64(),
        pa.float64(),
        volatility="immutable",
    )

    ctx = SessionContext()

    df = ctx.from_pydict({
        "a": [1.0, 2.1, 2.9, 4.0, 5.1, 6.0, 6.9, 8.0]
    })

    df.select("a", exp_smooth(col("a")).alias("smooth_a")).show()
