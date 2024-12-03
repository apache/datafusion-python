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

.. _window_functions:

Window Functions
================

In this section you will learn about window functions. A window function utilizes values from one or
multiple rows to produce a result for each individual row, unlike an aggregate function that
provides a single value for multiple rows.

The window functions are availble in the :py:mod:`~datafusion.functions` module.

We'll use the pokemon dataset (from Ritchie Vink) in the following examples.

.. ipython:: python

    from datafusion import SessionContext
    from datafusion import col
    from datafusion import functions as f

    ctx = SessionContext()
    df = ctx.read_csv("pokemon.csv")

Here is an example that shows how you can compare each pokemon's speed to the speed of the
previous row in the DataFrame.

.. ipython:: python

    df.select(
        col('"Name"'),
        col('"Speed"'),
        f.lag(col('"Speed"')).alias("Previous Speed")
    )

Setting Parameters
------------------


Ordering
^^^^^^^^

You can control the order in which rows are processed by window functions by providing
a list of ``order_by`` functions for the ``order_by`` parameter.

.. ipython:: python

    df.select(
        col('"Name"'),
        col('"Attack"'),
        col('"Type 1"'),
        f.rank(
            partition_by=[col('"Type 1"')],
            order_by=[col('"Attack"').sort(ascending=True)],
        ).alias("rank"),
    ).sort(col('"Type 1"'), col('"Attack"'))

Partitions
^^^^^^^^^^

A window function can take a list of ``partition_by`` columns similar to an
:ref:`Aggregation Function<aggregation>`. This will cause the window values to be evaluated
independently for each of the partitions. In the example above, we found the rank of each
Pokemon per ``Type 1`` partitions. We can see the first couple of each partition if we do
the following:

.. ipython:: python

    df.select(
        col('"Name"'),
        col('"Attack"'),
        col('"Type 1"'),
        f.rank(
            partition_by=[col('"Type 1"')],
            order_by=[col('"Attack"').sort(ascending=True)],
        ).alias("rank"),
    ).filter(col("rank") < lit(3)).sort(col('"Type 1"'), col("rank"))

Window Frame
^^^^^^^^^^^^

When using aggregate functions, the Window Frame of defines the rows over which it operates.
If you do not specify a Window Frame, the frame will be set depending on the following
criteria.

* If an ``order_by`` clause is set, the default window frame is defined as the rows between
  unbounded preceeding and the current row.
* If an ``order_by`` is not set, the default frame is defined as the rows betwene unbounded
  and unbounded following (the entire partition).

Window Frames are defined by three parameters: unit type, starting bound, and ending bound.

The unit types available are:

* Rows: The starting and ending boundaries are defined by the number of rows relative to the
  current row.
* Range: When using Range, the ``order_by`` clause must have exactly one term. The boundaries
  are defined bow how close the rows are to the value of the expression in the ``order_by``
  parameter.
* Groups: A "group" is the set of all rows that have equivalent values for all terms in the
  ``order_by`` clause.

In this example we perform a "rolling average" of the speed of the current Pokemon and the
two preceeding rows.

.. ipython:: python

    from datafusion.expr import WindowFrame

    df.select(
        col('"Name"'),
        col('"Speed"'),
        f.window("avg",
            [col('"Speed"')],
            order_by=[col('"Speed"')],
            window_frame=WindowFrame("rows", 2, 0)
        ).alias("Previous Speed")
    )

Null Treatment
^^^^^^^^^^^^^^

When using aggregate functions as window functions, it is often useful to specify how null values
should be treated. In order to do this you need to use the builder function. In future releases
we expect this to be simplified in the interface.

One common usage for handling nulls is the case where you want to find the last value up to the
current row. In the following example we demonstrate how setting the null treatment to ignore
nulls will fill in with the value of the most recent non-null row. To do this, we also will set
the window frame so that we only process up to the current row.

In this example, we filter down to one specific type of Pokemon that does have some entries in
it's ``Type 2`` column that are null.

.. ipython:: python

    from datafusion.common import NullTreatment

    df.filter(col('"Type 1"') ==  lit("Bug")).select(
        '"Name"',
        '"Type 2"',
        f.window("last_value", [col('"Type 2"')])
            .window_frame(WindowFrame("rows", None, 0))
            .order_by(col('"Speed"'))
            .null_treatment(NullTreatment.IGNORE_NULLS)
            .build()
            .alias("last_wo_null"),
        f.window("last_value", [col('"Type 2"')])
            .window_frame(WindowFrame("rows", None, 0))
            .order_by(col('"Speed"'))
            .null_treatment(NullTreatment.RESPECT_NULLS)
            .build()
            .alias("last_with_null")
    )

Aggregate Functions
-------------------

You can use any :ref:`Aggregation Function<aggregation>` as a window function. Currently
aggregate functions must use the deprecated
:py:func:`datafusion.functions.window` API but this should be resolved in
DataFusion 42.0 (`Issue Link <https://github.com/apache/datafusion-python/issues/833>`_). Here
is an example that shows how to compare each pokemons’s attack power with the average attack
power in its ``"Type 1"`` using the :py:func:`datafusion.functions.avg` function.

.. ipython:: python
    :okwarning:

    df.select(
        col('"Name"'),
        col('"Attack"'),
        col('"Type 1"'),
        f.window("avg", [col('"Attack"')])
            .partition_by(col('"Type 1"'))
            .build()
            .alias("Average Attack"),
    )

Available Functions
-------------------

The possible window functions are:

1. Rank Functions
    - :py:func:`datafusion.functions.rank`
    - :py:func:`datafusion.functions.dense_rank`
    - :py:func:`datafusion.functions.ntile`
    - :py:func:`datafusion.functions.row_number`

2. Analytical Functions
    - :py:func:`datafusion.functions.cume_dist`
    - :py:func:`datafusion.functions.percent_rank`
    - :py:func:`datafusion.functions.lag`
    - :py:func:`datafusion.functions.lead`

3. Aggregate Functions
    - All :ref:`Aggregation Functions<aggregation>` can be used as window functions.
