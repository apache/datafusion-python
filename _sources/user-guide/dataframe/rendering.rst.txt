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

DataFrame Rendering
===================

DataFusion provides configurable rendering for DataFrames in both plain text and HTML
formats. The ``datafusion.dataframe_formatter`` module controls how DataFrames are
displayed in Jupyter notebooks (via ``_repr_html_``), in the terminal (via ``__repr__``),
and anywhere else a string or HTML representation is needed.

Basic Rendering
---------------

In a Jupyter environment, displaying a DataFrame triggers HTML rendering:

.. code-block:: python

    # Will display as HTML table in Jupyter
    df

    # Explicit display also uses HTML rendering
    display(df)

In a terminal or when converting to string, plain text rendering is used:

.. code-block:: python

    # Plain text table output
    print(df)

Configuring the Formatter
-------------------------

You can customize how DataFrames are rendered by configuring the global formatter:

.. code-block:: python

    from datafusion.dataframe_formatter import configure_formatter

    configure_formatter(
        max_cell_length=25,           # Maximum characters in a cell before truncation
        max_width=1000,               # Maximum width in pixels (HTML only)
        max_height=300,               # Maximum height in pixels (HTML only)
        max_memory_bytes=2097152,     # Maximum memory for rendering (2MB)
        min_rows=10,                  # Minimum number of rows to display
        max_rows=10,                  # Maximum rows to display
        enable_cell_expansion=True,   # Allow expanding truncated cells (HTML only)
        custom_css=None,              # Additional custom CSS (HTML only)
        show_truncation_message=True, # Show message when data is truncated
        style_provider=None,          # Custom styling provider (HTML only)
        use_shared_styles=True,       # Share styles across tables (HTML only)
    )

The formatter settings affect all DataFrames displayed after configuration.

Custom Style Providers
----------------------

For HTML styling, you can create a custom style provider that implements the
``StyleProvider`` protocol:

.. code-block:: python

    from datafusion.dataframe_formatter import configure_formatter

    class MyStyleProvider:
        def get_cell_style(self):
            """Return CSS style string for table data cells."""
            return "border: 1px solid #ddd; padding: 8px; text-align: left;"

        def get_header_style(self):
            """Return CSS style string for table header cells."""
            return (
                "background-color: #007bff; color: white; "
                "padding: 8px; text-align: left;"
            )

    # Apply the custom style provider
    configure_formatter(style_provider=MyStyleProvider())

Custom Cell Formatters
----------------------

You can register custom formatters for specific Python types. A cell formatter is any
callable that takes a value and returns a string:

.. code-block:: python

    from datafusion.dataframe_formatter import get_formatter

    formatter = get_formatter()

    # Format floats to 2 decimal places
    formatter.register_formatter(float, lambda v: f"{v:.2f}")

    # Format dates in a custom way
    from datetime import date
    formatter.register_formatter(date, lambda v: v.strftime("%B %d, %Y"))

Custom Cell and Header Builders
-------------------------------

For full control over the HTML of individual cells or headers, you can set custom
builder functions:

.. code-block:: python

    from datafusion.dataframe_formatter import get_formatter

    formatter = get_formatter()

    # Custom cell builder receives (value, row, col, table_id) and returns HTML
    def my_cell_builder(value, row, col, table_id):
        color = "red" if isinstance(value, (int, float)) and value < 0 else "black"
        return f"<td style='color: {color}; padding: 8px;'>{value}</td>"

    formatter.set_custom_cell_builder(my_cell_builder)

    # Custom header builder receives a schema field and returns HTML
    def my_header_builder(field):
        return f"<th style='background: #333; color: white; padding: 8px;'>{field.name}</th>"

    formatter.set_custom_header_builder(my_header_builder)

Performance Optimization with Shared Styles
--------------------------------------------

The ``use_shared_styles`` parameter (enabled by default) optimizes performance when
displaying multiple DataFrames in notebook environments:

.. code-block:: python

    from datafusion.dataframe_formatter import configure_formatter

    # Default: Use shared styles (recommended for notebooks)
    configure_formatter(use_shared_styles=True)

    # Disable shared styles (each DataFrame includes its own styles)
    configure_formatter(use_shared_styles=False)

When ``use_shared_styles=True``:

- CSS styles and JavaScript are included only once per notebook session
- This reduces HTML output size and prevents style duplication
- Improves rendering performance with many DataFrames
- Applies consistent styling across all DataFrames

Working with the Formatter Directly
------------------------------------

You can use ``get_formatter()`` and ``set_formatter()`` for direct access to the global
formatter instance:

.. code-block:: python

    from datafusion.dataframe_formatter import (
        DataFrameHtmlFormatter,
        get_formatter,
        set_formatter,
    )

    # Get and modify the current formatter
    formatter = get_formatter()
    print(formatter.max_rows)
    print(formatter.max_cell_length)

    # Create and set a fully custom formatter
    custom_formatter = DataFrameHtmlFormatter(
        max_cell_length=50,
        max_rows=20,
        enable_cell_expansion=False,
    )
    set_formatter(custom_formatter)

Reset to default formatting:

.. code-block:: python

    from datafusion.dataframe_formatter import reset_formatter

    # Reset to default settings
    reset_formatter()

Memory and Display Controls
---------------------------

You can control how much data is displayed and how much memory is used for rendering:

.. code-block:: python

    from datafusion.dataframe_formatter import configure_formatter

    configure_formatter(
        max_memory_bytes=4 * 1024 * 1024,  # 4MB maximum memory for display
        min_rows=20,                       # Always show at least 20 rows
        max_rows=50,                       # Show up to 50 rows in output
    )

These parameters help balance comprehensive data display against performance considerations.

Best Practices
--------------

1. **Global Configuration**: Use ``configure_formatter()`` at the beginning of your notebook to set up consistent formatting for all DataFrames.

2. **Memory Management**: Set appropriate ``max_memory_bytes`` limits to prevent performance issues with large datasets.

3. **Shared Styles**: Keep ``use_shared_styles=True`` (default) for better performance in notebooks with multiple DataFrames.

4. **Reset When Needed**: Call ``reset_formatter()`` when you want to start fresh with default settings.

5. **Cell Expansion**: Use ``enable_cell_expansion=True`` when cells might contain longer content that users may want to see in full.

Additional Resources
--------------------

* :doc:`../dataframe/index` - Complete guide to using DataFrames
* :doc:`../io/index` - I/O Guide for reading data from various sources
* :doc:`../data-sources` - Comprehensive data sources guide
* :ref:`io_csv` - CSV file reading
* :ref:`io_parquet` - Parquet file reading
* :ref:`io_json` - JSON file reading
* :ref:`io_avro` - Avro file reading
* :ref:`io_custom_table_provider` - Custom table providers
* `API Reference <https://arrow.apache.org/datafusion-python/api/index.html>`_ - Full API reference
