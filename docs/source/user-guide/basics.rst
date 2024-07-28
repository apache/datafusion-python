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

.. _user_guide_concepts:

Concepts
========

In this section, we will cover a basic example to introduce a few key concepts.

.. code-block:: python

    import datafusion
    from datafusion import col
    import pyarrow

    # create a context
    ctx = datafusion.SessionContext()

    # create a RecordBatch and a new DataFrame from it
    batch = pyarrow.RecordBatch.from_arrays(
        [pyarrow.array([1, 2, 3]), pyarrow.array([4, 5, 6])],
        names=["a", "b"],
    )
    df = ctx.create_dataframe([[batch]])

    # create a new statement
    df = df.select(
        col("a") + col("b"),
        col("a") - col("b"),
    )

    # execute and collect the first (and only) batch
    result = df.collect()[0]

The first statement group:

.. code-block:: python

    # create a context
    ctx = datafusion.SessionContext()

creates a :py:class:`~datafusion.context.SessionContext`, that is, the main interface for executing queries with DataFusion. It maintains the state
of the connection between a user and an instance of the DataFusion engine. Additionally it provides the following functionality:

- Create a DataFrame from a CSV or Parquet data source.
- Register a CSV or Parquet data source as a table that can be referenced from a SQL query.
- Register a custom data source that can be referenced from a SQL query.
- Execute a SQL query

The second statement group creates a :code:`DataFrame`,

.. code-block:: python

    # create a RecordBatch and a new DataFrame from it
    batch = pyarrow.RecordBatch.from_arrays(
        [pyarrow.array([1, 2, 3]), pyarrow.array([4, 5, 6])],
        names=["a", "b"],
    )
    df = ctx.create_dataframe([[batch]])

A DataFrame refers to a (logical) set of rows that share the same column names, similar to a `Pandas DataFrame <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html>`_.
DataFrames are typically created by calling a method on :py:class:`~datafusion.context.SessionContext`, such as :code:`read_csv`, and can then be modified by
calling the transformation methods, such as :py:func:`~datafusion.dataframe.DataFrame.filter`, :py:func:`~datafusion.dataframe.DataFrame.select`, :py:func:`~datafusion.dataframe.DataFrame.aggregate`,
and :py:func:`~datafusion.dataframe.DataFrame.limit` to build up a query definition.

The third statement uses :code:`Expressions` to build up a query definition.

.. code-block:: python

    df = df.select(
        col("a") + col("b"),
        col("a") - col("b"),
    )

Finally the :py:func:`~datafusion.dataframe.DataFrame.collect` method converts the logical plan represented by the DataFrame into a physical plan and execute it,
collecting all results into a list of `RecordBatch <https://arrow.apache.org/docs/python/generated/pyarrow.RecordBatch.html>`_.
