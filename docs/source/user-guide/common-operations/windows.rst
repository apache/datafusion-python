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

    import urllib.request
    from datafusion import SessionContext
    from datafusion import col
    from datafusion import functions as f

    urllib.request.urlretrieve(
        "https://gist.githubusercontent.com/ritchie46/cac6b337ea52281aa23c049250a4ff03/raw/89a957ff3919d90e6ef2d34235e6bf22304f3366/pokemon.csv",
        "pokemon.csv",
    )

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

You can control the order in which rows are processed by window functions by providing
a list of ``order_by`` functions for the ``order_by`` parameter.

.. ipython:: python

    df.select(
        col('"Name"'),
        col('"Attack"'),
        col('"Type 1"'),
        f.rank()
            .partition_by(col('"Type 1"'))
            .order_by(col('"Attack"').sort(ascending=True))
            .build()
            .alias("rank"),
    ).sort(col('"Type 1"').sort(), col('"Attack"').sort())

Window Functions can be configured using a builder approach to set a few parameters.
To create a builder you simply need to call any one of these functions

- :py:func:`datafusion.expr.Expr.order_by` to set the window ordering.
- :py:func:`datafusion.expr.Expr.null_treatment` to set how ``null`` values should be handled.
- :py:func:`datafusion.expr.Expr.partition_by` to set the partitions for processing.
- :py:func:`datafusion.expr.Expr.window_frame` to set boundary of operation.

After these parameters are set, you must call ``build()`` on the resultant object to get an
expression as shown in the example above.

Aggregate Functions
-------------------

You can use any  :ref:`Aggregation Function<aggregation>` as a window function. Currently
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
