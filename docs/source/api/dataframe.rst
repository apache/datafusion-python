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

=================
DataFrame API
=================

Overview
--------

The ``DataFrame`` class is the core abstraction in DataFusion that represents tabular data and operations
on that data. DataFrames provide a flexible API for transforming data through various operations such as
filtering, projection, aggregation, joining, and more.

A DataFrame represents a logical plan that is lazily evaluated. The actual execution occurs only when 
terminal operations like ``collect()``, ``show()``, or ``to_pandas()`` are called.

Creating DataFrames
------------------

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

      # From CSV files
      df = ctx.read_csv("path/to/data.csv")
      
      # From Parquet files
      df = ctx.read_parquet("path/to/data.parquet")
      
      # From JSON files
      df = ctx.read_json("path/to/data.json")
      
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

Common DataFrame Operations
--------------------------

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

Terminal Operations
------------------

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

HTML Rendering in Jupyter
------------------------

When working in Jupyter notebooks or other environments that support rich HTML display, 
DataFusion DataFrames automatically render as nicely formatted HTML tables. This functionality
is provided by the ``_repr_html_`` method, which is automatically called by Jupyter.

Basic HTML Rendering
~~~~~~~~~~~~~~~~~~~

In a Jupyter environment, simply displaying a DataFrame object will trigger HTML rendering:

.. code-block:: python

    # Will display as HTML table in Jupyter
    df

    # Explicit display also uses HTML rendering
    display(df)

HTML Rendering Customization
---------------------------

DataFusion provides extensive customization options for HTML table rendering through the
``datafusion.html_formatter`` module.

Configuring the HTML Formatter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can customize how DataFrames are rendered by configuring the formatter:

.. code-block:: python

    from datafusion.html_formatter import configure_formatter
    
    configure_formatter(
        max_cell_length=30,              # Maximum length of cell content before truncation
        max_width=800,                   # Maximum width of table in pixels
        max_height=400,                  # Maximum height of table in pixels
        max_memory_bytes=2 * 1024 * 1024,# Maximum memory used for rendering (2MB)
        min_rows_display=10,             # Minimum rows to display
        repr_rows=20,                    # Number of rows to display in representation
        enable_cell_expansion=True,      # Allow cells to be expandable on click
        custom_css=None,                 # Custom CSS to apply
        show_truncation_message=True,    # Show message when data is truncated
        style_provider=None,             # Custom style provider class
        use_shared_styles=True           # Share styles across tables to reduce duplication
    )

Custom Style Providers
~~~~~~~~~~~~~~~~~~~~~

For advanced styling needs, you can create a custom style provider class:

.. code-block:: python

    from datafusion.html_formatter import configure_formatter
    
    class CustomStyleProvider:
        def get_cell_style(self) -> str:
            return "background-color: #f5f5f5; color: #333; padding: 8px; border: 1px solid #ddd;"
    
        def get_header_style(self) -> str:
            return "background-color: #4285f4; color: white; font-weight: bold; padding: 10px;"
    
    # Apply custom styling
    configure_formatter(style_provider=CustomStyleProvider())

Custom Type Formatters
~~~~~~~~~~~~~~~~~~~~~

You can register custom formatters for specific data types:

.. code-block:: python

    from datafusion.html_formatter import get_formatter
    
    formatter = get_formatter()
    
    # Format integers with color based on value
    def format_int(value):
        return f'<span style="color: {"red" if value > 100 else "blue"}">{value}</span>'
    
    formatter.register_formatter(int, format_int)
    
    # Format date values
    def format_date(value):
        return f'<span class="date-value">{value.isoformat()}</span>'
    
    formatter.register_formatter(datetime.date, format_date)

Custom Cell Builders
~~~~~~~~~~~~~~~~~~~

For complete control over cell rendering:

.. code-block:: python

    formatter = get_formatter()
    
    def custom_cell_builder(value, row, col, table_id):
        try:
            num_value = float(value)
            if num_value > 0:  # Positive values get green
                return f'<td style="background-color: #d9f0d3">{value}</td>'
            if num_value < 0:  # Negative values get red
                return f'<td style="background-color: #f0d3d3">{value}</td>'
        except (ValueError, TypeError):
            pass
        
        # Default styling for non-numeric or zero values
        return f'<td style="border: 1px solid #ddd">{value}</td>'
    
    formatter.set_custom_cell_builder(custom_cell_builder)

Custom Header Builders
~~~~~~~~~~~~~~~~~~~~~

Similarly, you can customize the rendering of table headers:

.. code-block:: python

    def custom_header_builder(field):
        tooltip = f"Type: {field.type}"
        return f'<th style="background-color: #333; color: white" title="{tooltip}">{field.name}</th>'
    
    formatter.set_custom_header_builder(custom_header_builder)

Managing Formatter State
-----------------------

The HTML formatter maintains global state that can be managed:

.. code-block:: python

    from datafusion.html_formatter import reset_formatter, reset_styles_loaded_state, get_formatter
    
    # Reset the formatter to default settings
    reset_formatter()
    
    # Reset only the styles loaded state (useful when styles were loaded but need reloading)
    reset_styles_loaded_state()
    
    # Get the current formatter instance to make changes
    formatter = get_formatter()

Advanced Example: Dashboard-Style Formatting
------------------------------------------

This example shows how to create a dashboard-like styling for your DataFrames:

.. code-block:: python

    from datafusion.html_formatter import configure_formatter, get_formatter
    
    # Define custom CSS
    custom_css = """
    .datafusion-table {
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        border-collapse: collapse;
        width: 100%;
        box-shadow: 0 2px 3px rgba(0,0,0,0.1);
    }
    .datafusion-table th {
        position: sticky;
        top: 0;
        z-index: 10;
    }
    .datafusion-table tr:hover td {
        background-color: #f1f7fa !important;
    }
    .datafusion-table .numeric-positive {
        color: #0a7c00;
    }
    .datafusion-table .numeric-negative {
        color: #d13438;
    }
    """
    
    class DashboardStyleProvider:
        def get_cell_style(self) -> str:
            return "padding: 8px 12px; border-bottom: 1px solid #e0e0e0;"
        
        def get_header_style(self) -> str:
            return ("background-color: #0078d4; color: white; font-weight: 600; "
                    "padding: 12px; text-align: left; border-bottom: 2px solid #005a9e;")
    
    # Apply configuration
    configure_formatter(
        max_height=500,
        enable_cell_expansion=True,
        custom_css=custom_css,
        style_provider=DashboardStyleProvider(),
        max_cell_length=50
    )
    
    # Add custom formatters for numbers
    formatter = get_formatter()
    
    def format_number(value):
        try:
            num = float(value)
            cls = "numeric-positive" if num > 0 else "numeric-negative" if num < 0 else ""
            return f'<span class="{cls}">{value:,}</span>' if cls else f'{value:,}'
        except (ValueError, TypeError):
            return str(value)
    
    formatter.register_formatter(int, format_number)
    formatter.register_formatter(float, format_number)

Best Practices
-------------

1. **Memory Management**: For large datasets, use ``max_memory_bytes`` to limit memory usage.

2. **Responsive Design**: Set reasonable ``max_width`` and ``max_height`` values to ensure tables display well on different screens.

3. **Style Optimization**: Use ``use_shared_styles=True`` to avoid duplicate style definitions when displaying multiple tables.

4. **Reset When Needed**: Call ``reset_formatter()`` when you want to start fresh with default settings.

5. **Cell Expansion**: Use ``enable_cell_expansion=True`` when cells might contain longer content that users may want to see in full.

Additional Resources
-------------------

* `DataFusion User Guide <../user-guide/dataframe.html>`_ - Complete guide to using DataFrames
* `API Reference <https://arrow.apache.org/datafusion-python/api/index.html>`_ - Full API reference
