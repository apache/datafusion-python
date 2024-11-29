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

.. _aggregation:

Aggregation
============

An aggregate or aggregation is a function where the values of multiple rows are processed together
to form a single summary value. For performing an aggregation, DataFusion provides the
:py:func:`~datafusion.dataframe.DataFrame.aggregate`

.. ipython:: python

    from datafusion import SessionContext, col, lit, functions as f

    ctx = SessionContext()
    df = ctx.read_csv("pokemon.csv")

    col_type_1 = col('"Type 1"')
    col_type_2 = col('"Type 2"')
    col_speed = col('"Speed"')
    col_attack = col('"Attack"')

    df.aggregate([col_type_1], [
        f.approx_distinct(col_speed).alias("Count"),
        f.approx_median(col_speed).alias("Median Speed"),
        f.approx_percentile_cont(col_speed, 0.9).alias("90% Speed")])

When the :code:`group_by` list is empty the aggregation is done over the whole :class:`.DataFrame`.
For grouping the :code:`group_by` list must contain at least one column.

.. ipython:: python

    df.aggregate([col_type_1], [
        f.max(col_speed).alias("Max Speed"),
        f.avg(col_speed).alias("Avg Speed"),
        f.min(col_speed).alias("Min Speed")])

More than one column can be used for grouping

.. ipython:: python

    df.aggregate([col_type_1, col_type_2], [
        f.max(col_speed).alias("Max Speed"),
        f.avg(col_speed).alias("Avg Speed"),
        f.min(col_speed).alias("Min Speed")])



Setting Parameters
------------------

Each of the built in aggregate functions provides arguments for the parameters that affect their
operation. These can also be overridden using the builder approach to setting any of the following
parameters. When you use the builder, you must call ``build()`` to finish. For example, these two
expressions are equivalent.

.. ipython:: python

    first_1 = f.first_value(col("a"), order_by=[col("a")])
    first_2 = f.first_value(col("a")).order_by(col("a")).build()

Ordering
^^^^^^^^

You can control the order in which rows are processed by window functions by providing
a list of ``order_by`` functions for the ``order_by`` parameter. In the following example, we
sort the Pokemon by their attack in increasing order and take the first value, which gives us the
Pokemon with the smallest attack value in each ``Type 1``.

.. ipython:: python

    df.aggregate(
        [col('"Type 1"')],
        [f.first_value(
            col('"Name"'),
            order_by=[col('"Attack"').sort(ascending=True)]
            ).alias("Smallest Attack")
        ])

Distinct
^^^^^^^^

When you set the parameter ``distinct`` to ``True``, then unique values will only be evaluated one
time each. Suppose we want to create an array of all of the ``Type 2`` for each ``Type 1`` of our
Pokemon set. Since there will be many entries of ``Type 2`` we only one each distinct value.

.. ipython:: python

    df.aggregate([col_type_1], [f.array_agg(col_type_2, distinct=True).alias("Type 2 List")])

In the output of the above we can see that there are some ``Type 1`` for which the ``Type 2`` entry
is ``null``. In reality, we probably want to filter those out. We can do this in two ways. First,
we can filter DataFrame rows that have no ``Type 2``. If we do this, we might have some ``Type 1``
entries entirely removed. The second is we can use the ``filter`` argument described below.

.. ipython:: python

    df.filter(col_type_2.is_not_null()).aggregate([col_type_1], [f.array_agg(col_type_2, distinct=True).alias("Type 2 List")])

    df.aggregate([col_type_1], [f.array_agg(col_type_2, distinct=True, filter=col_type_2.is_not_null()).alias("Type 2 List")])

Which approach you take should depend on your use case.

Null Treatment
^^^^^^^^^^^^^^

This option allows you to either respect or ignore null values.

One common usage for handling nulls is the case where you want to find the first value within a
partition. By setting the null treatment to ignore nulls, we can find the first non-null value
in our partition.


.. ipython:: python

    from datafusion.common import NullTreatment

    df.aggregate([col_type_1], [
        f.first_value(
            col_type_2,
            order_by=[col_attack],
            null_treatment=NullTreatment.RESPECT_NULLS
        ).alias("Lowest Attack Type 2")])

    df.aggregate([col_type_1], [
        f.first_value(
            col_type_2,
            order_by=[col_attack],
            null_treatment=NullTreatment.IGNORE_NULLS
        ).alias("Lowest Attack Type 2")])

Filter
^^^^^^

Using the filter option is useful for filtering results to include in the aggregate function. It can
be seen in the example above on how this can be useful to only filter rows evaluated by the
aggregate function without filtering rows from the entire DataFrame.

Filter takes a single expression.

Suppose we want to find the speed values for only Pokemon that have low Attack values.

.. ipython:: python

    df.aggregate([col_type_1], [
        f.avg(col_speed).alias("Avg Speed All"),
        f.avg(col_speed, filter=col_attack < lit(50)).alias("Avg Speed Low Attack")])


Aggregate Functions
-------------------

The available aggregate functions are:

1. Comparison Functions
    - :py:func:`datafusion.functions.min`
    - :py:func:`datafusion.functions.max`
2. Math Functions
    - :py:func:`datafusion.functions.sum`
    - :py:func:`datafusion.functions.avg`
    - :py:func:`datafusion.functions.median`
3. Array Functions
    - :py:func:`datafusion.functions.array_agg`
4. Logical Functions
    - :py:func:`datafusion.functions.bit_and`
    - :py:func:`datafusion.functions.bit_or`
    - :py:func:`datafusion.functions.bit_xor`
    - :py:func:`datafusion.functions.bool_and`
    - :py:func:`datafusion.functions.bool_or`
5. Statistical Functions
    - :py:func:`datafusion.functions.count`
    - :py:func:`datafusion.functions.corr`
    - :py:func:`datafusion.functions.covar_samp`
    - :py:func:`datafusion.functions.covar_pop`
    - :py:func:`datafusion.functions.stddev`
    - :py:func:`datafusion.functions.stddev_pop`
    - :py:func:`datafusion.functions.var_samp`
    - :py:func:`datafusion.functions.var_pop`
6. Linear Regression Functions
    - :py:func:`datafusion.functions.regr_count`
    - :py:func:`datafusion.functions.regr_slope`
    - :py:func:`datafusion.functions.regr_intercept`
    - :py:func:`datafusion.functions.regr_r2`
    - :py:func:`datafusion.functions.regr_avgx`
    - :py:func:`datafusion.functions.regr_avgy`
    - :py:func:`datafusion.functions.regr_sxx`
    - :py:func:`datafusion.functions.regr_syy`
    - :py:func:`datafusion.functions.regr_slope`
7. Positional Functions
    - :py:func:`datafusion.functions.first_value`
    - :py:func:`datafusion.functions.last_value`
    - :py:func:`datafusion.functions.nth_value`
8. String Functions
    - :py:func:`datafusion.functions.string_agg`
9. Approximation Functions
    - :py:func:`datafusion.functions.approx_distinct`
    - :py:func:`datafusion.functions.approx_median`
    - :py:func:`datafusion.functions.approx_percentile_cont`
    - :py:func:`datafusion.functions.approx_percentile_cont_with_weight`

