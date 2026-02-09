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

.. _io_csv:

CSV
===

Reading a csv is very straightforward with :py:func:`~datafusion.context.SessionContext.read_csv`

.. code-block:: python


    from datafusion import SessionContext

    ctx = SessionContext()
    df = ctx.read_csv("file.csv")

An alternative is to use :py:func:`~datafusion.context.SessionContext.register_csv`

.. code-block:: python

    ctx.register_csv("file", "file.csv")
    df = ctx.table("file")

If you require additional control over how to read the CSV file, you can use
:py:class:`~datafusion.options.CsvReadOptions` to set a variety of options.

.. code-block:: python

    from datafusion import CsvReadOptions
    options = (
        CsvReadOptions()
        .with_has_header(True) # File contains a header row
        .with_delimiter(";") # Use ; as the delimiter instead of ,
        .with_comment("#")  # Skip lines starting with #
        .with_escape("\\")  # Escape character
        .with_null_regex(r"^(null|NULL|N/A)$")  # Treat these as NULL
        .with_truncated_rows(True) # Allow rows to have incomplete columns
        .with_file_compression_type("gzip")  # Read gzipped CSV
        .with_file_extension(".gz") # File extension other than .csv
    )
    df = ctx.read_csv("data.csv.gz", options=options)

Details for all CSV reading options can be found on the
`DataFusion documentation site <https://datafusion.apache.org/library-user-guide/custom-table-providers.html>`_.
