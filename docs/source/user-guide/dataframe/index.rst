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

DataFrames
==========

Overview
--------

The ``DataFrame`` class is the core abstraction in DataFusion that represents tabular data and operations
on that data. DataFrames provide a flexible API for transforming data through various operations such as
filtering, projection, aggregation, joining, and more.

A DataFrame represents a logical plan that is lazily evaluated. The actual execution occurs only when 
terminal operations like ``collect()``, ``show()``, or ``to_pandas()`` are called.

Creating DataFrames
-------------------

DataFrames can be created in several ways:

* From SQL queries via a ``SessionContext``:

  .. code-block:: python

      from datafusion import SessionContext
      
      ctx = SessionContext()
      df = ctx.sql("SELECT * FROM your_table")

* From registered tables:

  .. code-block:: python

      df = ctx.table("your_table")

* From various data sources:

  .. code-block:: python

      # From CSV files (see :ref:`io_csv` for detailed options)
      df = ctx.read_csv("path/to/data.csv")
      
      # From Parquet files (see :ref:`io_parquet` for detailed options)
      df = ctx.read_parquet("path/to/data.parquet")
      
      # From JSON files (see :ref:`io_json` for detailed options)
      df = ctx.read_json("path/to/data.json")
      
      # From Avro files (see :ref:`io_avro` for detailed options)
      df = ctx.read_avro("path/to/data.avro")
      
      # From Pandas DataFrame
      import pandas as pd
      pandas_df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
      df = ctx.from_pandas(pandas_df)
      
      # From Arrow data
      import pyarrow as pa
      batch = pa.RecordBatch.from_arrays(
          [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
          names=["a", "b"]
      )
      df = ctx.from_arrow(batch)

For detailed information about reading from different data sources, see the :doc:`I/O Guide <../io/index>`.
For custom data sources, see :ref:`io_custom_table_provider`.

Common DataFrame Operations
---------------------------

DataFusion's DataFrame API offers a wide range of operations:

.. code-block:: python

    from datafusion import column, literal
    
    # Select specific columns
    df = df.select("col1", "col2")
    
    # Select with expressions
    df = df.select(column("a") + column("b"), column("a") - column("b"))
    
    # Filter rows
    df = df.filter(column("age") > literal(25))
    
    # Add computed columns
    df = df.with_column("full_name", column("first_name") + literal(" ") + column("last_name"))
    
    # Multiple column additions
    df = df.with_columns(
        (column("a") + column("b")).alias("sum"),
        (column("a") * column("b")).alias("product")
    )
    
    # Sort data
    df = df.sort(column("age").sort(ascending=False))
    
    # Join DataFrames
    df = df1.join(df2, on="user_id", how="inner")
    
    # Aggregate data
    from datafusion import functions as f
    df = df.aggregate(
        [],  # Group by columns (empty for global aggregation)
        [f.sum(column("amount")).alias("total_amount")]
    )
    
    # Limit rows
    df = df.limit(100)
    
    # Drop columns
    df = df.drop("temporary_column")

String Columns and Expressions
------------------------------

Some ``DataFrame`` methods accept plain strings when an argument refers to an
existing column. These include:

* :py:meth:`~datafusion.DataFrame.select`
* :py:meth:`~datafusion.DataFrame.sort`
* :py:meth:`~datafusion.DataFrame.drop`
* :py:meth:`~datafusion.DataFrame.join` (``on`` argument)
* :py:meth:`~datafusion.DataFrame.aggregate` (grouping columns)

Note that :py:meth:`~datafusion.DataFrame.join_on` expects ``col()``/``column()`` expressions rather than plain strings.

For such methods, you can pass column names directly:

.. code-block:: python

    from datafusion import col, functions as f

    df.sort('id')
    df.aggregate('id', [f.count(col('value'))])

The same operation can also be written with explicit column expressions, using either ``col()`` or ``column()``:

.. code-block:: python

    from datafusion import col, column, functions as f

    df.sort(col('id'))
    df.aggregate(column('id'), [f.count(col('value'))])

Note that ``column()`` is an alias of ``col()``, so you can use either name; the example above shows both in action.

Whenever an argument represents an expression—such as in
:py:meth:`~datafusion.DataFrame.filter` or
:py:meth:`~datafusion.DataFrame.with_column`—use ``col()`` to reference columns
and wrap constant values with ``lit()`` (also available as ``literal()``):

.. code-block:: python

    from datafusion import col, lit
    df.filter(col('age') > lit(21))

Without ``lit()`` DataFusion would treat ``21`` as a column name rather than a
constant value.

Terminal Operations
-------------------

To materialize the results of your DataFrame operations:

.. code-block:: python

    # Collect all data as PyArrow RecordBatches
    result_batches = df.collect()
    
    # Convert to various formats
    pandas_df = df.to_pandas()        # Pandas DataFrame
    polars_df = df.to_polars()        # Polars DataFrame
    arrow_table = df.to_arrow_table() # PyArrow Table
    py_dict = df.to_pydict()          # Python dictionary
    py_list = df.to_pylist()          # Python list of dictionaries
    
    # Display results
    df.show()                         # Print tabular format to console
    
    # Count rows
    count = df.count()

HTML Rendering
--------------

When working in Jupyter notebooks or other environments that support HTML rendering, DataFrames will
automatically display as formatted HTML tables. For detailed information about customizing HTML 
rendering, formatting options, and advanced styling, see :doc:`rendering`.

Core Classes
------------

**DataFrame**
    The main DataFrame class for building and executing queries.

    See: :py:class:`datafusion.DataFrame`

**SessionContext**
    The primary entry point for creating DataFrames from various data sources.

    Key methods for DataFrame creation:

    * :py:meth:`~datafusion.SessionContext.read_csv` - Read CSV files
    * :py:meth:`~datafusion.SessionContext.read_parquet` - Read Parquet files
    * :py:meth:`~datafusion.SessionContext.read_json` - Read JSON files
    * :py:meth:`~datafusion.SessionContext.read_avro` - Read Avro files
    * :py:meth:`~datafusion.SessionContext.table` - Access registered tables
    * :py:meth:`~datafusion.SessionContext.sql` - Execute SQL queries
    * :py:meth:`~datafusion.SessionContext.from_pandas` - Create from Pandas DataFrame
    * :py:meth:`~datafusion.SessionContext.from_arrow` - Create from Arrow data

    See: :py:class:`datafusion.SessionContext`

Expression Classes
------------------

**Expr**
    Represents expressions that can be used in DataFrame operations.

    See: :py:class:`datafusion.Expr`

**Functions for creating expressions:**

* :py:func:`datafusion.column` - Reference a column by name
* :py:func:`datafusion.literal` - Create a literal value expression

Built-in Functions
------------------

DataFusion provides many built-in functions for data manipulation:

* :py:mod:`datafusion.functions` - Mathematical, string, date/time, and aggregation functions

For a complete list of available functions, see the :py:mod:`datafusion.functions` module documentation.


.. toctree::
   :maxdepth: 1

   rendering
