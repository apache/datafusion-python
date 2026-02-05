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

HTML Rendering in Jupyter
=========================

When working in Jupyter notebooks or other environments that support rich HTML display, 
DataFusion DataFrames automatically render as nicely formatted HTML tables. This functionality
is provided by the ``_repr_html_`` method, which is automatically called by Jupyter to provide
a richer visualization than plain text output.

Basic HTML Rendering
--------------------

In a Jupyter environment, simply displaying a DataFrame object will trigger HTML rendering:

.. code-block:: python

    # Will display as HTML table in Jupyter
    df

    # Explicit display also uses HTML rendering
    display(df)

Customizing HTML Rendering
---------------------------

DataFusion provides extensive customization options for HTML table rendering through the
``datafusion.html_formatter`` module.

Configuring the HTML Formatter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can customize how DataFrames are rendered by configuring the formatter:

.. code-block:: python

    from datafusion.html_formatter import configure_formatter
    
    # Change the default styling
    configure_formatter(
        max_cell_length=25,        # Maximum characters in a cell before truncation
        max_width=1000,            # Maximum width in pixels
        max_height=300,            # Maximum height in pixels
        max_memory_bytes=2097152,  # Maximum memory for rendering (2MB)
        min_rows=10,               # Minimum number of rows to display
        max_rows=10,               # Maximum rows to display in __repr__
        enable_cell_expansion=True,# Allow expanding truncated cells
        custom_css=None,           # Additional custom CSS
        show_truncation_message=True, # Show message when data is truncated
        style_provider=None,       # Custom styling provider
        use_shared_styles=True     # Share styles across tables
    )

The formatter settings affect all DataFrames displayed after configuration.

Custom Style Providers
-----------------------

For advanced styling needs, you can create a custom style provider:

.. code-block:: python

    from datafusion.html_formatter import StyleProvider, configure_formatter
    
    class MyStyleProvider(StyleProvider):
        def get_table_styles(self):
            return {
                "table": "border-collapse: collapse; width: 100%;",
                "th": "background-color: #007bff; color: white; padding: 8px; text-align: left;",
                "td": "border: 1px solid #ddd; padding: 8px;",
                "tr:nth-child(even)": "background-color: #f2f2f2;",
            }
            
        def get_value_styles(self, dtype, value):
            """Return custom styles for specific values"""
            if dtype == "float" and value < 0:
                return "color: red;"
            return None
    
    # Apply the custom style provider
    configure_formatter(style_provider=MyStyleProvider())

Performance Optimization with Shared Styles
--------------------------------------------

The ``use_shared_styles`` parameter (enabled by default) optimizes performance when displaying 
multiple DataFrames in notebook environments:

.. code-block:: python

    from datafusion.html_formatter import StyleProvider, configure_formatter
    # Default: Use shared styles (recommended for notebooks)
    configure_formatter(use_shared_styles=True)

    # Disable shared styles (each DataFrame includes its own styles)
    configure_formatter(use_shared_styles=False)

When ``use_shared_styles=True``:
- CSS styles and JavaScript are included only once per notebook session
- This reduces HTML output size and prevents style duplication
- Improves rendering performance with many DataFrames
- Applies consistent styling across all DataFrames

Creating a Custom Formatter
----------------------------

For complete control over rendering, you can implement a custom formatter:

.. code-block:: python

    from datafusion.html_formatter import Formatter, get_formatter
    
    class MyFormatter(Formatter):
        def format_html(self, batches, schema, has_more=False, table_uuid=None):
            # Create your custom HTML here
            html = "<div class='my-custom-table'>"
            # ... formatting logic ...
            html += "</div>"
            return html
    
    # Set as the global formatter
    configure_formatter(formatter_class=MyFormatter)
    
    # Or use the formatter just for specific operations
    formatter = get_formatter()
    custom_html = formatter.format_html(batches, schema)

Managing Formatters
-------------------

Reset to default formatting:

.. code-block:: python

    from datafusion.html_formatter import reset_formatter
    
    # Reset to default settings
    reset_formatter()

Get the current formatter settings:

.. code-block:: python

    from datafusion.html_formatter import get_formatter
    
    formatter = get_formatter()
    print(formatter.max_rows)
    print(formatter.theme)

Contextual Formatting
----------------------

You can also use a context manager to temporarily change formatting settings:

.. code-block:: python

    from datafusion.html_formatter import formatting_context
    
    # Default formatting
    df.show()
    
    # Temporarily use different formatting
    with formatting_context(max_rows=100, theme="dark"):
        df.show()  # Will use the temporary settings
    
    # Back to default formatting
    df.show()

Memory and Display Controls
---------------------------

You can control how much data is displayed and how much memory is used for rendering:

.. code-block:: python

    configure_formatter(
        max_memory_bytes=4 * 1024 * 1024,  # 4MB maximum memory for display
        min_rows=20,                       # Always show at least 20 rows
        max_rows=50                        # Show up to 50 rows in output
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
