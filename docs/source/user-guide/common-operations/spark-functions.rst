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

Spark-Compatible Functions
==========================

DataFusion ships Spark-compatible versions of a wide set of SQL functions
(string, math, datetime, hash, array, aggregate) through the upstream
``datafusion-spark`` crate. ``datafusion-python`` exposes these under
``datafusion.functions.spark`` for the DataFrame API and via
:py:meth:`~datafusion.SessionContext.enable_spark_functions` for SQL.

Why a Separate Namespace?
-------------------------

Several Spark functions share names with DataFusion built-ins but differ in
semantics. The most common divergences:

- ``concat`` propagates NULL. ``concat('a', NULL, 'b')`` returns NULL under
  Spark semantics, whereas the DataFusion default returns ``'ab'``.
- ``substring`` is 1-indexed and supports negative positions counting from
  the end of the string.
- ``round`` uses HALF_UP rounding mode (``round(2.5, 0) == 3``).
- Numeric functions (``floor``, ``ceil``, ``mod``) follow Spark's edge-case
  handling for negative values and decimals.

Enabling Spark functions does not affect the DataFrame API: you choose which
implementation to call by which module you import from.

DataFrame API
-------------

Import the submodule and call functions directly. Returned values are
:py:class:`~datafusion.expr.Expr` instances that compose with the rest of
the DataFrame API.

.. code-block:: python

    from datafusion import SessionContext, col, lit
    from datafusion.functions import spark

    ctx = SessionContext()
    df = ctx.from_pydict({"s": ["hello", "world"]})

    # SHA-256 hash with Spark semantics
    df.select(spark.sha2(col("s"), lit(256)).alias("h")).show()

    # 1-indexed substring
    df.select(spark.substring(col("s"), lit(1), lit(3)).alias("p")).show()

SQL
---

To use Spark functions in SQL queries, call
:py:meth:`~datafusion.SessionContext.enable_spark_functions` on the context.
This registers every Spark UDF/UDAF/UDWF, overriding any DataFusion built-in
of the same name.

.. code-block:: python

    from datafusion import SessionContext

    ctx = SessionContext()
    ctx.enable_spark_functions()

    ctx.sql("SELECT sha2('hello', 256)").show()
    ctx.sql("SELECT concat('a', NULL, 'b')").show()   # -> NULL, not 'ab'

The override applies for the lifetime of the session. To call DataFusion's
built-in versions afterwards, create a fresh ``SessionContext``.

Function Categories
-------------------

The full list is available in the
:py:mod:`API reference <datafusion.functions.spark>`. Highlights by
category:

- **String**: ``ascii``, ``base64``, ``char``, ``concat``, ``elt``,
  ``format_string``, ``ilike``, ``is_valid_utf8``, ``length``, ``like``,
  ``luhn_check``, ``make_valid_utf8``, ``soundex``, ``space``,
  ``substring``, ``unbase64``.
- **Math**: ``abs``, ``bin``, ``ceil``, ``csc``, ``expm1``, ``factorial``,
  ``floor``, ``hex``, ``modulus``, ``negative``, ``pmod``, ``rint``,
  ``round``, ``sec``, ``unhex``, ``width_bucket``.
- **Datetime**: ``add_months``, ``date_add``, ``date_diff``, ``date_part``,
  ``date_sub``, ``date_trunc``, ``from_utc_timestamp``, ``hour``,
  ``last_day``, ``make_dt_interval``, ``make_interval``, ``minute``,
  ``next_day``, ``second``, ``time_trunc``, ``to_utc_timestamp``,
  ``trunc``, ``unix_date``, ``unix_micros``, ``unix_millis``,
  ``unix_seconds``.
- **Hash**: ``crc32``, ``sha1``, ``sha2``, ``xxhash64``.
- **Array**: ``array``, ``array_contains``, ``array_repeat``, ``shuffle``,
  ``slice``.
- **Aggregate**: ``avg``, ``collect_list``, ``collect_set``, ``try_sum``.
- **Bitwise**: ``bit_count``, ``bit_get``, ``bitwise_not``, ``shiftleft``,
  ``shiftright``, ``shiftrightunsigned``.
- **Bitmap**: ``bitmap_bit_position``, ``bitmap_bucket_number``,
  ``bitmap_count``.
- **Collection**: ``size``.
- **Conditional**: ``if_`` (exposed under that name because ``if`` is a
  Python keyword).
- **Conversion**: ``spark_cast``.
- **JSON**: ``json_tuple``.
- **Map**: ``map_from_arrays``, ``map_from_entries``, ``str_to_map``.
- **URL**: ``parse_url``, ``try_parse_url``, ``url_decode``,
  ``try_url_decode``, ``url_encode``.
