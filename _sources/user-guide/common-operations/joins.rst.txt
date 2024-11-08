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

Joins
=====

DataFusion supports the following join variants via the method :py:func:`~datafusion.dataframe.DataFrame.join`

- Inner Join
- Left Join
- Right Join
- Full Join
- Left Semi Join
- Left Anti Join

For the examples in this section we'll use the following two DataFrames

.. ipython:: python

    from datafusion import SessionContext

    ctx = SessionContext()

    left = ctx.from_pydict(
        {
            "customer_id": [1, 2, 3],
            "customer": ["Alice", "Bob", "Charlie"],
        }
    )

    right = ctx.from_pylist([
        {"id": 1, "name": "CityCabs"},
        {"id": 2, "name": "MetroRide"},
        {"id": 5, "name": "UrbanGo"},
    ])

Inner Join
----------

When using an inner join, only rows containing the common values between the two join columns present in both DataFrames
will be included in the resulting DataFrame.

.. ipython:: python

    left.join(right, left_on="customer_id", right_on="id", how="inner")

The parameter ``join_keys`` specifies the columns from the left DataFrame and right DataFrame that contains the values
that should match.

Left Join
---------

A left join combines rows from two DataFrames using the key columns. It returns all rows from the left DataFrame and
matching rows from the right DataFrame. If there's no match in the right DataFrame, it returns null
values for the corresponding columns.

.. ipython:: python

    left.join(right, left_on="customer_id", right_on="id", how="left")

Full Join
---------

A full join merges rows from two tables based on a related column, returning all rows from both tables, even if there
is no match. Unmatched rows will have null values.

.. ipython:: python

    left.join(right, left_on="customer_id", right_on="id", how="full")

Left Semi Join
--------------

A left semi join retrieves matching rows from the left table while
omitting duplicates with multiple matches in the right table.

.. ipython:: python

    left.join(right, left_on="customer_id", right_on="id", how="semi")

Left Anti Join
--------------

A left anti join shows all rows from the left table without any matching rows in the right table,
based on a the specified matching columns. It excludes rows from the left table that have at least one matching row in
the right table.

.. ipython:: python

    left.join(right, left_on="customer_id", right_on="id", how="anti")