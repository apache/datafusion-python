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

Functions
=========

DataFusion provides a large number of built-in functions for performing complex queries without requiring user-defined functions.
In here we will cover some of the more popular use cases. If you want to view all the functions go to the :py:mod:`Functions <datafusion.functions>` API Reference.

We'll use the pokemon dataset in the following examples.

.. ipython:: python

    from datafusion import SessionContext

    ctx = SessionContext()
    ctx.register_csv("pokemon", "pokemon.csv")
    df = ctx.table("pokemon")

Mathematical
------------

DataFusion offers mathematical functions such as :py:func:`~datafusion.functions.pow` or :py:func:`~datafusion.functions.log`

.. ipython:: python

    from datafusion import col, literal, string_literal, str_lit
    from datafusion import functions as f

    df.select(
        f.pow(col('"Attack"'), literal(2)) - f.pow(col('"Defense"'), literal(2))
    ).limit(10)


Conditional
-----------

There 3 conditional functions in DataFusion :py:func:`~datafusion.functions.coalesce`, :py:func:`~datafusion.functions.nullif` and :py:func:`~datafusion.functions.case`.

.. ipython:: python

    df.select(
        f.coalesce(col('"Type 1"'), col('"Type 2"')).alias("dominant_type")
    ).limit(10)

Temporal
--------

For selecting the current time use :py:func:`~datafusion.functions.now`

.. ipython:: python

    df.select(f.now())

Convert to timestamps using :py:func:`~datafusion.functions.to_timestamp`

.. ipython:: python

    df.select(f.to_timestamp(col('"Total"')).alias("timestamp"))

Extracting parts of a date using :py:func:`~datafusion.functions.date_part` (alias :py:func:`~datafusion.functions.extract`)

.. ipython:: python

     df.select(
        f.date_part(literal("month"), f.to_timestamp(col('"Total"'))).alias("month"),
        f.extract(literal("day"), f.to_timestamp(col('"Total"'))).alias("day")
     )
  
String
------

In the field of data science, working with textual data is a common task. To make string manipulation easier,
DataFusion offers a range of helpful options.

.. ipython:: python

    df.select(
        f.char_length(col('"Name"')).alias("len"),
        f.lower(col('"Name"')).alias("lower"),
        f.left(col('"Name"'), literal(4)).alias("code")
    )

This also includes the functions for regular expressions like :py:func:`~datafusion.functions.regexp_replace` and :py:func:`~datafusion.functions.regexp_match`

.. ipython:: python

    df.select(
        f.regexp_match(col('"Name"'), literal("Char")).alias("dragons"),
        f.regexp_replace(col('"Name"'), literal("saur"), literal("fleur")).alias("flowers")
    )

Casting
-------

Casting expressions to different data types using :py:func:`~datafusion.functions.arrow_cast`

.. ipython:: python

    df.select(
        f.arrow_cast(col('"Total"'), string_literal("Float64")).alias("total_as_float"),
        f.arrow_cast(col('"Total"'), str_lit("Int32")).alias("total_as_int")
    )

Other
-----

The function :py:func:`~datafusion.functions.in_list` allows to check a column for the presence of multiple values:

.. ipython:: python

    types = [literal("Grass"), literal("Fire"), literal("Water")]
    (
        df.select(f.in_list(col('"Type 1"'), types, negated=False).alias("basic_types"))
          .limit(20)
          .to_pandas()
    )
