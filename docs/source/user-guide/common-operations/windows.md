---
jupytext:
  text_representation:
    extension: .md
    format_name: myst
kernelspec:
  name: python3
  display_name: Python 3
---
<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

(window_functions)=

# Window Functions

In this section you will learn about window functions. A window function utilizes values from one or
multiple rows to produce a result for each individual row, unlike an aggregate function that
provides a single value for multiple rows.

The window functions are available in the {py:mod}`~datafusion.functions` module.

We'll use the pokemon dataset (from Ritchie Vink) in the following examples.

```{code-cell} ipython3
from datafusion import SessionContext
from datafusion import col, lit
from datafusion import functions as f

ctx = SessionContext()
df = ctx.read_csv("pokemon.csv")
```

Here is an example that shows how you can compare each pokemon's speed to the speed of the
previous row in the DataFrame.

```{code-cell} ipython3
df.select(
    col('"Name"'),
    col('"Speed"'),
    f.lag(col('"Speed"')).alias("Previous Speed")
)
```

## Setting Parameters

### Ordering

You can control the order in which rows are processed by window functions by providing
a list of `order_by` functions for the `order_by` parameter.

```{code-cell} ipython3
df.select(
    col('"Name"'),
    col('"Attack"'),
    col('"Type 1"'),
    f.rank(
        partition_by=[col('"Type 1"')],
        order_by=[col('"Attack"').sort(ascending=True)],
    ).alias("rank"),
).sort(col('"Type 1"'), col('"Attack"'))
```

### Partitions

A window function can take a list of `partition_by` columns similar to an
{ref}`Aggregation Function<aggregation>`. This will cause the window values to be evaluated
independently for each of the partitions. In the example above, we found the rank of each
Pokemon per `Type 1` partitions. We can see the first couple of each partition if we do
the following:

```{code-cell} ipython3
df.select(
    col('"Name"'),
    col('"Attack"'),
    col('"Type 1"'),
    f.rank(
        partition_by=[col('"Type 1"')],
        order_by=[col('"Attack"').sort(ascending=True)],
    ).alias("rank"),
).filter(col("rank") < lit(3)).sort(col('"Type 1"'), col("rank"))
```

### Window Frame

When using aggregate functions, the Window Frame of defines the rows over which it operates.
If you do not specify a Window Frame, the frame will be set depending on the following
criteria.

- If an `order_by` clause is set, the default window frame is defined as the rows between
  unbounded preceding and the current row.
- If an `order_by` is not set, the default frame is defined as the rows between unbounded
  and unbounded following (the entire partition).

Window Frames are defined by three parameters: unit type, starting bound, and ending bound.

The unit types available are:

- Rows: The starting and ending boundaries are defined by the number of rows relative to the
  current row.
- Range: When using Range, the `order_by` clause must have exactly one term. The boundaries
  are defined bow how close the rows are to the value of the expression in the `order_by`
  parameter.
- Groups: A "group" is the set of all rows that have equivalent values for all terms in the
  `order_by` clause.

In this example we perform a "rolling average" of the speed of the current Pokemon and the
two preceding rows.

```{code-cell} ipython3
from datafusion.expr import Window, WindowFrame

df.select(
    col('"Name"'),
    col('"Speed"'),
    f.avg(col('"Speed"'))
    .over(Window(window_frame=WindowFrame("rows", 2, 0), order_by=[col('"Speed"')]))
    .alias("Previous Speed"),
)
```

(window_function_chaining)=

#### Chaining onto a window function

Set a window function's `partition_by`, `order_by`, and `window_frame` in one
place: its keyword arguments, a single `over()`, or one builder chain ending in
`build()`. Chaining a builder method or `over()` onto a window function that
already has any of them raises:

```python
# Raises: lead already has window options (order_by)
f.lead(col("v"), order_by="t").over(Window(partition_by=[col("g")]))

# Set them together instead.
f.lead(col("v")).over(Window(partition_by=[col("g")], order_by="t"))
```

A built window function stores a concrete frame with no record of whether you
chose it or it was derived from `order_by`, so the options cannot be merged
without guessing. Merging may become possible once
[apache/datafusion#25934](https://github.com/apache/datafusion/issues/25934)
is resolved.

The `null_treatment` already set is kept, as are `filter` and `distinct` on an
aggregate used as a window function. The whole-partition frame counts as no
frame, so adding an `order_by` derives the running frame, even when you passed
that frame explicitly:

```python
whole = WindowFrame("rows", None, None)  # same as the no-order_by default

# Both give a running sum.
f.sum(col("v")).over(Window()).order_by(col("v")).build()
f.sum(col("v")).over(Window(window_frame=whole)).order_by(col("v")).build()
```

To keep that frame, set it after the `order_by`, or pass both in one `Window`:

```python
f.sum(col("v")).over(Window()).order_by(col("v")).window_frame(whole).build()
f.sum(col("v")).over(Window(order_by=col("v"), window_frame=whole))
```

### Null Treatment

When using aggregate functions as window functions, it is often useful to specify how null values
should be treated. In order to do this you need to use the builder function. In future releases
we expect this to be simplified in the interface.

One common usage for handling nulls is the case where you want to find the last value up to the
current row. In the following example we demonstrate how setting the null treatment to ignore
nulls will fill in with the value of the most recent non-null row. To do this, we also will set
the window frame so that we only process up to the current row.

In this example, we filter down to one specific type of Pokemon that does have some entries in
it's `Type 2` column that are null.

```{code-cell} ipython3
from datafusion.common import NullTreatment

df.filter(col('"Type 1"') == lit("Bug")).select(
    '"Name"',
    '"Type 2"',
    f.last_value(col('"Type 2"'))
    .over(
        Window(
            window_frame=WindowFrame("rows", None, 0),
            order_by=[col('"Speed"')],
            null_treatment=NullTreatment.IGNORE_NULLS,
        )
    )
    .alias("last_wo_null"),
    f.last_value(col('"Type 2"'))
    .over(
        Window(
            window_frame=WindowFrame("rows", None, 0),
            order_by=[col('"Speed"')],
            null_treatment=NullTreatment.RESPECT_NULLS,
        )
    )
    .alias("last_with_null"),
)
```

## Aggregate Functions

You can use any {ref}`Aggregation Function<aggregation>` as a window function. Here
is an example that shows how to compare each pokemons’s attack power with the average attack
power in its `"Type 1"` using the {py:func}`datafusion.functions.avg` function.

```{code-cell} ipython3
df.select(
    col('"Name"'),
    col('"Attack"'),
    col('"Type 1"'),
    f.avg(col('"Attack"')).over(
        Window(
            window_frame=WindowFrame("rows", None, None),
            partition_by=[col('"Type 1"')],
        )
    ).alias("Average Attack"),
)
```

(aggregate_over_options)=

### Options set on the aggregate

`over()` keeps the `filter`, `distinct`, and `null_treatment` options an
aggregate was built with:

```python
# Averages the distinct values 1.0 and 4.0.
f.avg(col("v"), distinct=True).over(Window())
```

An aggregate's `order_by` raises, as `ORDER BY` inside an aggregate call does
with `OVER` in SQL. A window does not pass an ordering to the aggregate, so
the `order_by` in the `Window` only sets the frame and the order of rows. The
one exception is a `WITHIN GROUP` function such as
{py:func}`~datafusion.functions.percentile_cont`, which computes ascending as a
window: an ascending `sort_expression` is accepted, and a descending one raises.

```python
f.percentile_cont(col("v"), 0.25).over(Window())  # ascending, accepted
f.percentile_cont(col("v").sort(ascending=False), 0.25).over(Window())  # raises
```

## Available Functions

The possible window functions are:

1. Rank Functions
   : - {py:func}`datafusion.functions.rank`
     - {py:func}`datafusion.functions.dense_rank`
     - {py:func}`datafusion.functions.ntile`
     - {py:func}`datafusion.functions.row_number`
2. Analytical Functions
   : - {py:func}`datafusion.functions.cume_dist`
     - {py:func}`datafusion.functions.percent_rank`
     - {py:func}`datafusion.functions.lag`
     - {py:func}`datafusion.functions.lead`
3. Aggregate Functions
   : - All {ref}`Aggregation Functions<aggregation>` can be used as window functions.

## User-Defined Window Functions

You can ship custom window functions to the engine by subclassing
{py:class}`~datafusion.user_defined.WindowEvaluator` and registering it
via {py:func}`~datafusion.udwf`. See {py:mod}`datafusion.user_defined`
for the evaluator interface and worked examples.

:::{note}
Serialization

Python window UDFs travel inline inside pickled or
{py:meth}`~datafusion.expr.Expr.to_bytes`-serialized expressions —
the evaluator class is captured by value via {mod}`cloudpickle`, so
worker processes do not need to pre-register the UDF. Any names the
evaluator resolves via `import` are captured **by reference** and
must be importable on the receiving worker. See
{py:mod}`datafusion.ipc` for the full IPC model and security caveats.
:::
