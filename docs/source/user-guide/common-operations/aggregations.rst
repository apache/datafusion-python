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

.. _aggregation:

Aggregation
============

An aggregate or aggregation is a function where the values of multiple rows are processed together to form a single summary value.
For performing an aggregation, DataFusion provides the :py:func:`~datafusion.dataframe.DataFrame.aggregate`

.. ipython:: python

    from datafusion import SessionContext
    from datafusion import column, lit
    from datafusion import functions as f
    import random

    ctx = SessionContext()
    df = ctx.from_pydict(
        {
            "a": ["foo", "bar", "foo", "bar", "foo", "bar", "foo", "foo"],
            "b": ["one", "one", "two", "three", "two", "two", "one", "three"],
            "c": [random.randint(0, 100) for _ in range(8)],
            "d": [random.random() for _ in range(8)],
        },
        name="foo_bar"
    )

    col_a = column("a")
    col_b = column("b")
    col_c = column("c")
    col_d = column("d")

    df.aggregate([], [f.approx_distinct(col_c), f.approx_median(col_d), f.approx_percentile_cont(col_d, lit(0.5))])

When the :code:`group_by` list is empty the aggregation is done over the whole :class:`.DataFrame`. For grouping
the :code:`group_by` list must contain at least one column

.. ipython:: python

    df.aggregate([col_a], [f.sum(col_c), f.max(col_d), f.min(col_d)])

More than one column can be used for grouping

.. ipython:: python

    df.aggregate([col_a, col_b], [f.sum(col_c), f.max(col_d), f.min(col_d)])
