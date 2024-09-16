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

.. _expressions:

Expressions
===========

In DataFusion an expression is an abstraction that represents a computation.
Expressions are used as the primary inputs and outputs for most functions within
DataFusion. As such, expressions can be combined to create expression trees, a
concept shared across most compilers and databases.

Column
------

The first expression most new users will interact with is the Column, which is created by calling :py:func:`~datafusion.col`.
This expression represents a column within a DataFrame. The function :py:func:`~datafusion.col` takes as in input a string
and returns an expression as it's output.

Literal
-------

Literal expressions represent a single value. These are helpful in a wide range of operations where
a specific, known value is of interest. You can create a literal expression using the function :py:func:`~datafusion.lit`.
The type of the object passed to the :py:func:`~datafusion.lit` function will be used to convert it to a known data type.

In the following example we create expressions for the column named `color` and the literal scalar string `red`.
The resultant variable `red_units` is itself also an expression.

.. ipython:: python

    red_units = col("color") == lit("red")

Boolean
-------

When combining expressions that evaluate to a boolean value, you can combine these expressions using boolean operators.
It is important to note that in order to combine these expressions, you *must* use bitwise operators. See the following
examples for the and, or, and not operations.


.. ipython:: python

    red_or_green_units = (col("color") == lit("red")) | (col("color") == lit("green"))
    heavy_red_units = (col("color") == lit("red")) & (col("weight") > lit(42))
    not_red_units = ~(col("color") == lit("red"))

Arrays
------

For columns that contain arrays of values, you can access individual elements of the array by index
using bracket indexing. This is similar to callling the function
:py:func:`datafusion.functions.array_element`, except that array indexing using brackets is 0 based,
similar to Python arrays and ``array_element`` is 1 based indexing to be compatible with other SQL
approaches.

.. ipython:: python

    from datafusion import SessionContext, col

    ctx = SessionContext()
    df = ctx.from_pydict({"a": [[1, 2, 3], [4, 5, 6]]})
    df.select(col("a")[0].alias("a0"))


.. warning::

    Indexing an element of an array via ``[]`` starts at index 0 whereas
    :py:func:`~datafusion.functions.array_element` starts at index 1.

Structs
-------

Columns that contain struct elements can be accessed using the bracket notation as if they were
Python dictionary style objects. This expects a string key as the parameter passed.

.. ipython:: python

    ctx = SessionContext()
    data = {"a": [{"size": 15, "color": "green"}, {"size": 10, "color": "blue"}]}
    df = ctx.from_pydict(data)
    df.select(col("a")["size"].alias("a_size"))


Functions
---------

As mentioned before, most functions in DataFusion return an expression at their output. This allows us to create
a wide variety of expressions built up from other expressions. For example, :py:func:`~datafusion.expr.Expr.alias` is a function that takes
as it input a single expression and returns an expression in which the name of the expression has changed.

The following example shows a series of expressions that are built up from functions operating on expressions.

.. ipython:: python

    from datafusion import SessionContext
    from datafusion import column, lit
    from datafusion import functions as f
    import random

    ctx = SessionContext()
    df = ctx.from_pydict(
        {
            "name": ["Albert", "Becca", "Carlos", "Dante"],
            "age": [42, 67, 27, 71],
            "years_in_position": [13, 21, 10, 54],
        },
        name="employees"
    )

    age_col = col("age")
    renamed_age = age_col.alias("age_in_years")
    start_age = age_col - col("years_in_position")
    started_young = start_age < lit(18)
    can_retire = age_col > lit(65)
    long_timer = started_young & can_retire

    df.filter(long_timer).select(col("name"), renamed_age, col("years_in_position"))
