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

DataFrame API Reference
=======================

This page provides quick access to DataFusion's DataFrame API documentation.

For comprehensive usage patterns and examples, see the main :doc:`DataFrame Guide <index>`.

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
