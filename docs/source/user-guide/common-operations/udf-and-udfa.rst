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
:py:func:`~datafusion.user_defined.ScalarUDF.udf` .

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

In this example we passed the PyArrow ``DataType`` when we defined the function
by calling ``udf()``. If you need additional control, such as specifying
metadata or nullability of the input or output, you can instead specify a
PyArrow ``Field``.

If you need to write a custom function but do not want to incur the performance
cost of converting to Python objects and back, a more advanced approach is to
write Rust based UDFs and to expose them to Python. There is an example in the
`DataFusion blog <https://datafusion.apache.org/blog/2024/11/19/datafusion-python-udf-comparisons/>`_
describing how to do this.

When not to use a UDF
^^^^^^^^^^^^^^^^^^^^^

A UDF is the right tool when the per-row computation genuinely cannot be
expressed with built-in functions. It is often the *wrong* tool for a
predicate that happens to be easier to write in Python. A UDF is opaque
to the optimizer, which means filters expressed as UDFs lose several
rewrites that the engine applies to filters built from native
expressions. The most visible of these is **Parquet predicate pushdown**:
a native predicate can prune entire row groups using the min/max
statistics in the Parquet footer, while a UDF predicate cannot.

The following example writes a small Parquet file, then filters it two
ways: first with a native expression, then with a UDF that computes the
same result. The filter itself is simple on purpose so we can compare
the plans side by side.

.. ipython:: python

    import tempfile, os
    import pyarrow as pa
    import pyarrow.parquet as pq
    from datafusion import SessionContext, col, lit, udf

    tmpdir = tempfile.mkdtemp()
    parquet_path = os.path.join(tmpdir, "items.parquet")
    pq.write_table(
        pa.table({
            "id":    list(range(100)),
            "brand": ["A", "B", "C", "D"] * 25,
            "qty":   [i * 10 for i in range(100)],
        }),
        parquet_path,
    )

    ctx = SessionContext()
    items = ctx.read_parquet(parquet_path)

**Native-expression predicate.** The filter is a plain boolean tree
over column references and literals, so the optimizer can analyze it:

.. ipython:: python

    native_filtered = items.filter(
        (col("brand") == lit("A")) & (col("qty") >= lit(150))
    )
    print(native_filtered.execution_plan().display_indent())

Notice the ``DataSourceExec`` line. It carries three annotations the
optimizer computed from the predicate:

- ``predicate=brand@1 = A AND qty@2 >= 150`` — the filter is pushed
  into the Parquet scan itself, so the scan only reads matching rows.
- ``pruning_predicate=... brand_min@0 <= A AND A <= brand_max@1 ...
  qty_max@4 >= 150`` — the scan prunes whole row groups by consulting
  the Parquet min/max statistics in the footer *before* reading any
  column data.
- ``required_guarantees=[brand in (A)]`` — the scan uses this when a
  bloom filter or dictionary is available to skip pages.

**UDF predicate.** Now wrap the same logic in a Python UDF:

.. ipython:: python

    def brand_qty_filter(brand_arr: pa.Array, qty_arr: pa.Array) -> pa.Array:
        return pa.array([
            b.as_py() == "A" and q.as_py() >= 150
            for b, q in zip(brand_arr, qty_arr)
        ])

    pred_udf = udf(
        brand_qty_filter, [pa.string(), pa.int64()], pa.bool_(), "stable",
    )
    udf_filtered = items.filter(pred_udf(col("brand"), col("qty")))
    print(udf_filtered.execution_plan().display_indent())

The ``DataSourceExec`` now carries only ``predicate=brand_qty_filter(...)``.
There is no ``pruning_predicate`` and no ``required_guarantees``: the
scan has to materialize every row group and hand each row to the
Python callback just to decide whether to keep it.

At small scale the cost difference is invisible; on a Parquet file with
many row groups, or data whose min/max statistics line up well with
the predicate, the native form can skip most of the file. The UDF form
reads all of it.

**Takeaway.** Reach for a UDF when the per-row computation is genuinely
not expressible as a tree of built-in functions (custom numerical work,
external lookups, complex business rules). When it *is* expressible —
even if the native form is a little more verbose — build the ``Expr``
tree directly so the optimizer can see through it. For disjunctive
predicates the idiom is to produce one clause per bucket and combine
them with ``|``:

.. code-block:: python

    from functools import reduce
    from operator import or_
    from datafusion import col, lit, functions as f

    buckets = {
        "Brand#12": {"containers": ["SM CASE", "SM BOX"], "min_qty": 1, "max_size": 5},
        "Brand#23": {"containers": ["MED BAG", "MED BOX"], "min_qty": 10, "max_size": 10},
    }

    def bucket_clause(brand, spec):
        return (
            (col("brand") == lit(brand))
            & f.in_list(col("container"), [lit(c) for c in spec["containers"]])
            & (col("quantity") >= lit(spec["min_qty"]))
            & (col("quantity") <= lit(spec["min_qty"] + 10))
            & (col("size") >= lit(1))
            & (col("size") <= lit(spec["max_size"]))
        )

    predicate = reduce(or_, (bucket_clause(b, s) for b, s in buckets.items()))
    df = df.filter(predicate)

Aggregate Functions
-------------------

The :py:func:`~datafusion.user_defined.AggregateUDF.udaf` function allows you to define User-Defined
Aggregate Functions (UDAFs). To use this you must implement an
:py:class:`~datafusion.user_defined.Accumulator` that determines how the aggregation is performed.

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

    import pyarrow as pa
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

        def update(self, values_a: pa.Array, values_b: pa.Array) -> None:
            self._sum = self._sum + pyarrow.compute.sum(values_a).as_py() - pyarrow.compute.sum(values_b).as_py()

        def merge(self, states: list[pa.Array]) -> None:
            self._sum = self._sum + pyarrow.compute.sum(states[0]).as_py()

        def state(self) -> list[pa.Scalar]:
            return [pyarrow.scalar(self._sum)]

        def evaluate(self) -> pa.Scalar:
            return pyarrow.scalar(self._sum)

    ctx = datafusion.SessionContext()
    df = ctx.from_pydict(
        {
            "a": [4, 5, 6],
            "b": [1, 2, 3],
        }
    )

    my_udaf = udaf(MyAccumulator, [pa.float64(), pa.float64()], pa.float64(), [pa.float64()], 'stable')

    df.aggregate([], [my_udaf(col("a"), col("b")).alias("col_diff")])

FAQ
^^^

**How do I return a list from a UDAF?**

Both the ``evaluate`` and the ``state`` functions expect to return scalar values.
If you wish to return a list array as a scalar value, the best practice is to
wrap the values in a ``pyarrow.Scalar`` object. For example, you can return a
timestamp list with ``pa.scalar([...], type=pa.list_(pa.timestamp("ms")))`` and
register the appropriate return or state types as
``return_type=pa.list_(pa.timestamp("ms"))`` and
``state_type=[pa.list_(pa.timestamp("ms"))]``, respectively.

As of DataFusion 52.0.0 , you can pass return any Python object, including a
PyArrow array, as the return value(s) for these functions and DataFusion will
attempt to create a scalar type from the value. DataFusion has been tested to
convert PyArrow, nanoarrow, and arro3 objects as well as primitive data types
like integers, strings, and so on.

Window Functions
----------------

To implement a User-Defined Window Function (UDWF) you must call the
:py:func:`~datafusion.user_defined.WindowUDF.udwf` function using a class that implements the abstract
class :py:class:`~datafusion.user_defined.WindowEvaluator`.

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
    from datafusion.user_defined import WindowEvaluator

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

Table Functions
---------------

User Defined Table Functions are slightly different than the other functions
described here. These functions take any number of `Expr` arguments, but only
literal expressions are supported. Table functions must return a Table
Provider as described in the ref:`_io_custom_table_provider` page.

Once you have a table function, you can register it with the session context
by using :py:func:`datafusion.context.SessionContext.register_udtf`.

There are examples of both rust backed and python based table functions in the
examples folder of the repository. If you have a rust backed table function
that you wish to expose via PyO3, you need to expose it as a ``PyCapsule``.

.. code-block:: rust

    #[pymethods]
    impl MyTableFunction {
        fn __datafusion_table_function__<'py>(
            &self,
            py: Python<'py>,
        ) -> PyResult<Bound<'py, PyCapsule>> {
            let name = cr"datafusion_table_function".into();

            let func = self.clone();
            let provider = FFI_TableFunction::new(Arc::new(func), None);

            PyCapsule::new(py, provider, Some(name))
        }
    }
