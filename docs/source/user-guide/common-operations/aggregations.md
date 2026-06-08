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


# Aggregation

An aggregate or aggregation is a function where the values of multiple rows are processed together
to form a single summary value. For performing an aggregation, DataFusion provides the
[`aggregate`][datafusion.dataframe.DataFrame.aggregate]

```python exec="1" source="material-block" result="text" session="aggregations"
ctx = SessionContext()
df = ctx.read_csv("pokemon.csv")

col_type_1 = col('"Type 1"')
col_type_2 = col('"Type 2"')
col_speed = col('"Speed"')
col_attack = col('"Attack"')

print(df.aggregate(
    [col_type_1],
    [
        f.approx_distinct(col_speed).alias("Count"),
        f.approx_median(col_speed).alias("Median Speed"),
        f.approx_percentile_cont(col_speed, 0.9).alias("90% Speed"),
    ],
))
```


When `group_by` is `None` or an empty list, the aggregation is done over the whole
[`DataFrame`][datafusion.dataframe.DataFrame]. For grouping the `group_by` list must contain at least one column.

```python exec="1" source="material-block" result="text" session="aggregations"
print(df.aggregate(
    [col_type_1],
    [
        f.max(col_speed).alias("Max Speed"),
        f.avg(col_speed).alias("Avg Speed"),
        f.min(col_speed).alias("Min Speed"),
    ],
))
```


More than one column can be used for grouping

```python exec="1" source="material-block" result="text" session="aggregations"
print(df.aggregate(
    [col_type_1, col_type_2],
    [
        f.max(col_speed).alias("Max Speed"),
        f.avg(col_speed).alias("Avg Speed"),
        f.min(col_speed).alias("Min Speed"),
    ],
))
```


## Setting Parameters

Each of the built in aggregate functions provides arguments for the parameters that affect their
operation. These can also be overridden using the builder approach to setting any of the following
parameters. When you use the builder, you must call `build()` to finish. For example, these two
expressions are equivalent.

```python exec="1" source="material-block" result="text" session="aggregations"
first_1 = f.first_value(col("a"), order_by=[col("a")])
first_2 = f.first_value(col("a")).order_by(col("a")).build()
```


### Ordering

You can control the order in which rows are processed by window functions by providing
a list of `order_by` functions for the `order_by` parameter. In the following example, we
sort the Pokemon by their attack in increasing order and take the first value, which gives us the
Pokemon with the smallest attack value in each `Type 1`.

```python exec="1" source="material-block" result="text" session="aggregations"
print(df.aggregate(
    [col('"Type 1"')],
    [
        f.first_value(
            col('"Name"'), order_by=[col('"Attack"').sort(ascending=True)]
        ).alias("Smallest Attack")
    ],
))
```


### Distinct

When you set the parameter `distinct` to `True`, then unique values will only be evaluated one
time each. Suppose we want to create an array of all of the `Type 2` for each `Type 1` of our
Pokemon set. Since there will be many entries of `Type 2` we only one each distinct value.

```python exec="1" source="material-block" result="text" session="aggregations"
print(df.aggregate(
    [col_type_1], [f.array_agg(col_type_2, distinct=True).alias("Type 2 List")]
))
```


In the output of the above we can see that there are some `Type 1` for which the `Type 2` entry
is `null`. In reality, we probably want to filter those out. We can do this in two ways. First,
we can filter DataFrame rows that have no `Type 2`. If we do this, we might have some `Type 1`
entries entirely removed. The second is we can use the `filter` argument described below.

```python exec="1" source="material-block" result="text" session="aggregations"
df.filter(col_type_2.is_not_null()).aggregate(
    [col_type_1], [f.array_agg(col_type_2, distinct=True).alias("Type 2 List")]
)

print(df.aggregate(
    [col_type_1],
    [
        f.array_agg(col_type_2, distinct=True, filter=col_type_2.is_not_null()).alias(
            "Type 2 List"
        )
    ],
))
```


Which approach you take should depend on your use case.

### Null Treatment

This option allows you to either respect or ignore null values.

One common usage for handling nulls is the case where you want to find the first value within a
partition. By setting the null treatment to ignore nulls, we can find the first non-null value
in our partition.

```python exec="1" source="material-block" result="text" session="aggregations"
from datafusion.common import NullTreatment

df.aggregate(
    [col_type_1],
    [
        f.first_value(
            col_type_2,
            order_by=[col_attack],
            null_treatment=NullTreatment.RESPECT_NULLS,
        ).alias("Lowest Attack Type 2")
    ],
)

print(df.aggregate(
    [col_type_1],
    [
        f.first_value(
            col_type_2, order_by=[col_attack], null_treatment=NullTreatment.IGNORE_NULLS
        ).alias("Lowest Attack Type 2")
    ],
))
```


### Filter

Using the filter option is useful for filtering results to include in the aggregate function. It can
be seen in the example above on how this can be useful to only filter rows evaluated by the
aggregate function without filtering rows from the entire DataFrame.

Filter takes a single expression.

Suppose we want to find the speed values for only Pokemon that have low Attack values.

```python exec="1" source="material-block" result="text" session="aggregations"
print(df.aggregate(
    [col_type_1],
    [
        f.avg(col_speed).alias("Avg Speed All"),
        f.avg(col_speed, filter=col_attack < lit(50)).alias("Avg Speed Low Attack"),
    ],
))
```


### Comparing subsets within a group

Sometimes you need to compare the full membership of a group against a
subset that meets some condition — for example, "which groups have at least
one failure, but not every member failed?". The `filter` argument on an
aggregate restricts the rows that contribute to *that* aggregate without
dropping the group, so a single pass can produce both the full set and the
filtered subset side by side. Pairing
[`array_agg`][datafusion.functions.array_agg] with `distinct=True` and
`filter=` is a compact way to express this: collect the distinct values
of the group, collect the distinct values that satisfy the condition, then
compare the two arrays.

Suppose each row records a line item with the supplier that fulfilled it and
a flag for whether that supplier met the commit date. We want to identify
*partially failed* orders — orders where at least one supplier failed but
not every supplier failed:

```python exec="1" source="material-block" result="text" session="aggregations"
orders_df = ctx.from_pydict(
    {
        "order_id": [1, 1, 1, 2, 2, 3, 4, 4],
        "supplier_id": [100, 101, 102, 200, 201, 300, 400, 401],
        "failed": [False, True, False, False, False, True, True, True],
    },
)

grouped = orders_df.aggregate(
    [col("order_id")],
    [
        f.array_agg(col("supplier_id"), distinct=True).alias("all_suppliers"),
        f.array_agg(
            col("supplier_id"),
            filter=col("failed"),
            distinct=True,
        ).alias("failed_suppliers"),
    ],
)

print(grouped.filter(
    (f.array_length(col("failed_suppliers")) > lit(0))
    & (f.array_length(col("failed_suppliers")) < f.array_length(col("all_suppliers")))
).select(col("order_id"), col("failed_suppliers")))
```


Order 1 is partial (one of three suppliers failed). Order 2 is excluded
because no supplier failed, order 3 because its only supplier failed, and
order 4 because both of its suppliers failed.

## Grouping Sets

The default style of aggregation produces one row per group. Sometimes you want a single query to
produce rows at multiple levels of detail — for example, totals per type *and* an overall grand
total, or subtotals for every combination of two columns plus the individual column totals. Writing
separate queries and concatenating them is tedious and runs the data multiple times. Grouping sets
solve this by letting you specify several grouping levels in one pass.

DataFusion supports three grouping set styles through the
[`GroupingSet`][datafusion.expr.GroupingSet] class:

- [`rollup`][datafusion.expr.GroupingSet.rollup] — hierarchical subtotals, like a drill-down report
- [`cube`][datafusion.expr.GroupingSet.cube] — every possible subtotal combination, like a pivot table
- [`grouping_sets`][datafusion.expr.GroupingSet.grouping_sets] — explicitly list exactly which grouping levels you want

Because result rows come from different grouping levels, a column that is *not* part of a
particular level will be `null` in that row. Use [`grouping`][datafusion.functions.grouping] to
distinguish a real `null` in the data from one that means "this column was aggregated across."
It returns `0` when the column is a grouping key for that row, and `1` when it is not.

### Rollup

[`rollup`][datafusion.expr.GroupingSet.rollup] creates a hierarchy. `rollup(a, b)` produces
grouping sets `(a, b)`, `(a)`, and `()` — like nested subtotals in a report. This is useful
when your columns have a natural hierarchy, such as region → city or type → subtype.

Suppose we want to summarize Pokemon stats by `Type 1` with subtotals and a grand total. With
the default aggregation style we would need two separate queries. With `rollup` we get it all at
once:

```python exec="1" source="material-block" result="text" session="aggregations"
from datafusion.expr import GroupingSet

print(df.aggregate(
    [GroupingSet.rollup(col_type_1)],
    [
        f.count(col_speed).alias("Count"),
        f.avg(col_speed).alias("Avg Speed"),
        f.max(col_speed).alias("Max Speed"),
    ],
).sort(col_type_1.sort(ascending=True, nulls_first=True)))
```


The first row — where `Type 1` is `null` — is the grand total across all types. But how do you
tell a grand-total `null` apart from a Pokemon that genuinely has no type? The
[`grouping`][datafusion.functions.grouping] function returns `0` when the column is a grouping key
for that row and `1` when it is aggregated across.

Use `.alias()` to give the column a readable name:

```python exec="1" source="material-block" result="text" session="aggregations"
print(df.aggregate(
    [GroupingSet.rollup(col_type_1)],
    [
        f.count(col_speed).alias("Count"),
        f.avg(col_speed).alias("Avg Speed"),
        f.grouping(col_type_1).alias("Is Total"),
    ],
).sort(col_type_1.sort(ascending=True, nulls_first=True)))
```


With two columns the hierarchy becomes more apparent. `rollup(Type 1, Type 2)` produces:

- one row per `(Type 1, Type 2)` pair — the most detailed level
- one row per `Type 1` — subtotals
- one grand total row

```python exec="1" source="material-block" result="text" session="aggregations"
print(df.aggregate(
    [GroupingSet.rollup(col_type_1, col_type_2)],
    [f.count(col_speed).alias("Count"), f.avg(col_speed).alias("Avg Speed")],
).sort(
    col_type_1.sort(ascending=True, nulls_first=True),
    col_type_2.sort(ascending=True, nulls_first=True),
))
```


### Cube

[`cube`][datafusion.expr.GroupingSet.cube] produces every possible subset. `cube(a, b)`
produces grouping sets `(a, b)`, `(a)`, `(b)`, and `()` — one more than `rollup` because
it also includes `(b)` alone. This is useful when neither column is "above" the other in a
hierarchy and you want all cross-tabulations.

For our Pokemon data, `cube(Type 1, Type 2)` gives us stats broken down by the type pair,
by `Type 1` alone, by `Type 2` alone, and a grand total — all in one query:

```python exec="1" source="material-block" result="text" session="aggregations"
print(df.aggregate(
    [GroupingSet.cube(col_type_1, col_type_2)],
    [f.count(col_speed).alias("Count"), f.avg(col_speed).alias("Avg Speed")],
).sort(
    col_type_1.sort(ascending=True, nulls_first=True),
    col_type_2.sort(ascending=True, nulls_first=True),
))
```


Compared to the `rollup` example above, notice the extra rows where `Type 1` is `null` but
`Type 2` has a value — those are the per-`Type 2` subtotals that `rollup` does not include.

### Explicit Grouping Sets

[`grouping_sets`][datafusion.expr.GroupingSet.grouping_sets] lets you list exactly which grouping levels
you need when `rollup` or `cube` would produce too many or too few. Each argument is a list of
columns forming one grouping set.

For example, if we want only the per-`Type 1` totals and per-`Type 2` totals — but *not* the
full `(Type 1, Type 2)` detail rows or the grand total — we can ask for exactly that:

```python exec="1" source="material-block" result="text" session="aggregations"
print(df.aggregate(
    [GroupingSet.grouping_sets([col_type_1], [col_type_2])],
    [f.count(col_speed).alias("Count"), f.avg(col_speed).alias("Avg Speed")],
).sort(
    col_type_1.sort(ascending=True, nulls_first=True),
    col_type_2.sort(ascending=True, nulls_first=True),
))
```


Each row belongs to exactly one grouping level. The [`grouping`][datafusion.functions.grouping]
function tells you which level each row comes from:

```python exec="1" source="material-block" result="text" session="aggregations"
print(df.aggregate(
    [GroupingSet.grouping_sets([col_type_1], [col_type_2])],
    [
        f.count(col_speed).alias("Count"),
        f.avg(col_speed).alias("Avg Speed"),
        f.grouping(col_type_1).alias("grouping(Type 1)"),
        f.grouping(col_type_2).alias("grouping(Type 2)"),
    ],
).sort(
    col_type_1.sort(ascending=True, nulls_first=True),
    col_type_2.sort(ascending=True, nulls_first=True),
))
```


Where `grouping(Type 1)` is `0` the row is a per-`Type 1` total (and `Type 2` is `null`).
Where `grouping(Type 2)` is `0` the row is a per-`Type 2` total (and `Type 1` is `null`).

## Aggregate Functions

The available aggregate functions are:

01. Comparison Functions
    : - [`min`][datafusion.functions.min]
      - [`max`][datafusion.functions.max]
02. Math Functions
    : - [`sum`][datafusion.functions.sum]
      - [`avg`][datafusion.functions.avg]
      - [`median`][datafusion.functions.median]
03. Array Functions
    : - [`array_agg`][datafusion.functions.array_agg]
04. Logical Functions
    : - [`bit_and`][datafusion.functions.bit_and]
      - [`bit_or`][datafusion.functions.bit_or]
      - [`bit_xor`][datafusion.functions.bit_xor]
      - [`bool_and`][datafusion.functions.bool_and]
      - [`bool_or`][datafusion.functions.bool_or]
05. Statistical Functions
    : - [`count`][datafusion.functions.count]
      - [`corr`][datafusion.functions.corr]
      - [`covar_samp`][datafusion.functions.covar_samp]
      - [`covar_pop`][datafusion.functions.covar_pop]
      - [`stddev`][datafusion.functions.stddev]
      - [`stddev_pop`][datafusion.functions.stddev_pop]
      - [`var_samp`][datafusion.functions.var_samp]
      - [`var_pop`][datafusion.functions.var_pop]
      - [`var_population`][datafusion.functions.var_population]
06. Linear Regression Functions
    : - [`regr_count`][datafusion.functions.regr_count]
      - [`regr_slope`][datafusion.functions.regr_slope]
      - [`regr_intercept`][datafusion.functions.regr_intercept]
      - [`regr_r2`][datafusion.functions.regr_r2]
      - [`regr_avgx`][datafusion.functions.regr_avgx]
      - [`regr_avgy`][datafusion.functions.regr_avgy]
      - [`regr_sxx`][datafusion.functions.regr_sxx]
      - [`regr_syy`][datafusion.functions.regr_syy]
      - [`regr_slope`][datafusion.functions.regr_slope]
07. Positional Functions
    : - [`first_value`][datafusion.functions.first_value]
      - [`last_value`][datafusion.functions.last_value]
      - [`nth_value`][datafusion.functions.nth_value]
08. String Functions
    : - [`string_agg`][datafusion.functions.string_agg]
09. Percentile Functions
    : - [`percentile_cont`][datafusion.functions.percentile_cont]
      - [`quantile_cont`][datafusion.functions.quantile_cont]
      - [`approx_distinct`][datafusion.functions.approx_distinct]
      - [`approx_median`][datafusion.functions.approx_median]
      - [`approx_percentile_cont`][datafusion.functions.approx_percentile_cont]
      - [`approx_percentile_cont_with_weight`][datafusion.functions.approx_percentile_cont_with_weight]
10. Grouping Set Functions
    \- [`grouping`][datafusion.functions.grouping]
    \- [`rollup`][datafusion.expr.GroupingSet.rollup]
    \- [`cube`][datafusion.expr.GroupingSet.cube]
    \- [`grouping_sets`][datafusion.expr.GroupingSet.grouping_sets]

## User-Defined Aggregate Functions

You can ship custom aggregations to the engine by subclassing
[`Accumulator`][datafusion.user_defined.Accumulator] and registering it via
[`udaf`][datafusion.user_defined.udaf]. See [`user_defined`](../../reference/datafusion/user_defined.md)
for the accumulator interface and worked examples.

<div class="admonition note">
<p class="admonition-title">Note</p>

Serialization

</div>

    Python aggregate UDFs travel inline inside pickled or
    [`to_bytes`][datafusion.expr.Expr.to_bytes]-serialized expressions —
    the accumulator class is captured by value via [`cloudpickle`][cloudpickle],
    so worker processes do not need to pre-register the UDF. Any names
    the accumulator resolves via `import` are captured **by reference**
    and must be importable on the receiving worker. See
    [`ipc`][datafusion.ipc] for the full IPC model and security caveats.
