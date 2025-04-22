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

DataFrame Operations
===================

Working with DataFrames
----------------------

A DataFrame in DataFusion represents a logical plan that defines a series of operations to be performed on data. 
This logical plan is not executed until you call a terminal operation like :py:func:`~datafusion.dataframe.DataFrame.collect` 
or :py:func:`~datafusion.dataframe.DataFrame.show`.

DataFrames provide a familiar API for data manipulation:

.. ipython:: python

    import datafusion
    from datafusion import col, lit, functions as f
    
    ctx = datafusion.SessionContext()
    
    # Create a DataFrame from a CSV file
    df = ctx.read_csv("example.csv")
    
    # Add transformations
    df = df.filter(col("age") > lit(30)) \
           .select([col("name"), col("age"), (col("salary") * lit(1.1)).alias("new_salary")]) \
           .sort("age")
    
    # Execute the plan
    df.show()

Common DataFrame Operations
--------------------------

DataFusion supports a wide range of operations on DataFrames:

Filtering and Selection
~~~~~~~~~~~~~~~~~~~~~~~

.. ipython:: python

    # Filter rows
    df = df.filter(col("age") > lit(30))
    
    # Select columns
    df = df.select([col("name"), col("age")])
    
    # Select by column name
    df = df.select_columns(["name", "age"])
    
    # Select using column indexing
    df = df["name", "age"]

Aggregation
~~~~~~~~~~

.. ipython:: python

    # Group by and aggregate
    df = df.aggregate(
        [col("category")],  # Group by columns
        [f.sum(col("amount")).alias("total"), 
         f.avg(col("price")).alias("avg_price")]
    )

Joins
~~~~~

.. ipython:: python

    # Join two DataFrames
    df_joined = df1.join(
        df2,
        how="inner",
        left_on=["id"], 
        right_on=["id"]
    )
    
    # Join with custom expressions
    df_joined = df1.join_on(
        df2,
        [col("df1.id") == col("df2.id")],
        how="left"
    )

DataFrame Visualization
----------------------

Jupyter Notebook Integration
~~~~~~~~~~~~~~~~~~~~~~~~~~~

When working in Jupyter notebooks, DataFrames automatically display as HTML tables. This is 
handled by the :code:`_repr_html_` method, which provides a rich, formatted view of your data.

.. ipython:: python

    # DataFrames render as HTML tables in notebooks
    df  # Just displaying the DataFrame renders it as HTML

Customizing DataFrame Display
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can customize how DataFrames are displayed using the HTML formatter:

.. ipython:: python

    from datafusion.html_formatter import configure_formatter
    
    # Change display settings
    configure_formatter(
        max_rows=100,          # Show more rows
        truncate_width=30,     # Allow longer strings
        theme="light",         # Use light theme
        precision=2            # Set decimal precision
    )
    
    # Now display uses the new format
    df.show()

Creating a Custom Style Provider
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For advanced styling needs:

.. code-block:: python

    from datafusion.html_formatter import StyleProvider, configure_formatter
    
    class CustomStyleProvider(StyleProvider):
        def get_table_styles(self):
            return {
                "table": "border-collapse: collapse; width: 100%;",
                "th": "background-color: #4CAF50; color: white; padding: 10px;",
                "td": "border: 1px solid #ddd; padding: 8px;",
                "tr:hover": "background-color: #f5f5f5;",
            }
            
        def get_value_styles(self, dtype, value):
            if dtype == "float" and value < 0:
                return "color: red; font-weight: bold;"
            return None
    
    # Apply custom styling
    configure_formatter(style_provider=CustomStyleProvider())

Managing Display Settings
~~~~~~~~~~~~~~~~~~~~~~~

You can temporarily change formatting settings with context managers:

.. code-block:: python

    from datafusion.html_formatter import formatting_context
    
    # Use different formatting temporarily
    with formatting_context(max_rows=5, theme="dark"):
        df.show()  # Will show only 5 rows with dark theme
    
    # Reset to default formatting
    from datafusion.html_formatter import reset_formatter
    reset_formatter()

Converting to Other Formats
--------------------------

DataFusion DataFrames can be easily converted to other popular formats:

.. ipython:: python

    # Convert to Arrow Table
    arrow_table = df.to_arrow_table()
    
    # Convert to Pandas DataFrame
    pandas_df = df.to_pandas()
    
    # Convert to Polars DataFrame
    polars_df = df.to_polars()
    
    # Convert to Python data structures
    python_dict = df.to_pydict()
    python_list = df.to_pylist()

Saving DataFrames
---------------

You can write DataFrames to various file formats:

.. ipython:: python

    # Write to CSV
    df.write_csv("output.csv", with_header=True)
    
    # Write to Parquet
    df.write_parquet("output.parquet", compression="zstd")
    
    # Write to JSON
    df.write_json("output.json")
