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

DataFusion's DataFrame API provides a powerful interface for building and executing queries against data sources. 
It offers a familiar API similar to pandas and other DataFrame libraries, but with the performance benefits of Rust 
and Arrow.

A DataFrame represents a logical plan that can be composed through operations like filtering, projection, and aggregation.
The actual execution happens when terminal operations like ``collect()`` or ``show()`` are called.

Basic Usage
-----------

.. code-block:: python

    import datafusion
    from datafusion import col, lit

    # Create a context and register a data source
    ctx = datafusion.SessionContext()
    ctx.register_csv("my_table", "path/to/data.csv")
    
    # Create and manipulate a DataFrame
    df = ctx.sql("SELECT * FROM my_table")
    
    # Or use the DataFrame API directly
    df = (ctx.table("my_table")
          .filter(col("age") > lit(25))
          .select([col("name"), col("age")]))
    
    # Execute and collect results
    result = df.collect()
    
    # Display the first few rows
    df.show()

HTML Rendering
--------------

When working in Jupyter notebooks or other environments that support HTML rendering, DataFrames will
automatically display as formatted HTML tables, making it easier to visualize your data.

The ``_repr_html_`` method is called automatically by Jupyter to render a DataFrame. This method 
controls how DataFrames appear in notebook environments, providing a richer visualization than
plain text output.

Customizing HTML Rendering
--------------------------

You can customize how DataFrames are rendered in HTML by configuring the formatter:

.. code-block:: python

    from datafusion.html_formatter import configure_formatter
    
    # Change the default styling
    configure_formatter(
        max_cell_length=25,        # Maximum characters in a cell before truncation
        max_width=1000,            # Maximum width in pixels
        max_height=300,            # Maximum height in pixels
        max_memory_bytes=2097152,  # Maximum memory for rendering (2MB)
        min_rows_display=20,       # Minimum number of rows to display
        repr_rows=10,              # Number of rows to display in __repr__
        enable_cell_expansion=True,# Allow expanding truncated cells
        custom_css=None,           # Additional custom CSS
        show_truncation_message=True, # Show message when data is truncated
        style_provider=None,       # Custom styling provider
        use_shared_styles=True     # Share styles across tables
    )

The formatter settings affect all DataFrames displayed after configuration.

Performance Optimization with Shared Styles
------------------------------------------

The ``use_shared_styles`` parameter (enabled by default) optimizes performance when displaying 
multiple DataFrames in notebook environments:

.. code-block:: python

    # Default: Use shared styles (recommended for notebooks)
    configure_formatter(use_shared_styles=True)
    
    # Disable shared styles (each DataFrame includes its own styles)
    configure_formatter(use_shared_styles=False)

When ``use_shared_styles=True``:

- CSS styles and JavaScript are included only once per notebook session
- This reduces HTML output size and prevents style duplication
- Improves rendering performance with many DataFrames
- Applies consistent styling across all DataFrames

If you switch between notebooks or need to refresh styles:

.. code-block:: python

    from datafusion.html_formatter import reset_styles_loaded_state
    
    # Force styles to be included in the next DataFrame display
    reset_styles_loaded_state()

Memory and Display Controls
--------------------------

You can control how much data is displayed and how much memory is used for rendering:

.. code-block:: python

    configure_formatter(
        max_memory_bytes=4 * 1024 * 1024,  # 4MB maximum memory for display
        min_rows_display=50,               # Always show at least 50 rows
        repr_rows=20                       # Show 20 rows in __repr__ output
    )

These parameters help balance comprehensive data display against performance considerations.
