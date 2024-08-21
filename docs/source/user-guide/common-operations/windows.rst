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

Here is an example that shows how to compare each pokemons’s attack power with the average attack
power in its ``"Type 1"``

.. ipython:: python

    df.select(
        col('"Name"'),
        col('"Attack"'),
        #f.alias(
        #    f.window("avg", [col('"Attack"')], partition_by=[col('"Type 1"')]),
        #    "Average Attack",
        #)
    )

You can also control the order in which rows are processed by window functions by providing
a list of ``order_by`` functions for the ``order_by`` parameter.

.. ipython:: python

    df.select(
        col('"Name"'),
        col('"Attack"'),
        #f.alias(
        #    f.window(
        #        "rank",
        #        [],
        #        partition_by=[col('"Type 1"')],
        #        order_by=[f.order_by(col('"Attack"'))],
        #    ),
        #    "rank",
        #),
    )

The possible window functions are:

1. Rank Functions
    - rank
    - dense_rank
    - row_number
    - ntile

2. Analytical Functions
    - cume_dist
    - percent_rank
    - lag
    - lead
    - first_value
    - last_value
    - nth_value

3. Aggregate Functions
    - All aggregate functions can be used as window functions.
