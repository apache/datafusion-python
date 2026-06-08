```python exec="1" session="expressions"
import os
import pathlib

import datafusion  # noqa: F401
from datafusion import (  # noqa: F401
    SessionContext,
    col,
    column,
    lit,
    literal,
)
from datafusion import functions as f  # noqa: F401
from datafusion.dataframe_formatter import configure_formatter

# mkdocs runs from the repo root; the demo data lives at docs/source/.
for candidate in ("docs/source", ".."):
    p = pathlib.Path(candidate)
    if (p / "pokemon.csv").exists():
        os.chdir(p)
        break

configure_formatter(max_rows=10, show_truncation_message=False)
```

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


# Expressions

In DataFusion an expression is an abstraction that represents a computation.
Expressions are used as the primary inputs and outputs for most functions within
DataFusion. As such, expressions can be combined to create expression trees, a
concept shared across most compilers and databases.

## Column

The first expression most new users will interact with is the Column, which is created by calling [`col`][datafusion.col.col].
This expression represents a column within a DataFrame. The function [`col`][datafusion.col.col] takes as in input a string
and returns an expression as it's output.

## Literal

Literal expressions represent a single value. These are helpful in a wide range of operations where
a specific, known value is of interest. You can create a literal expression using the function [`lit`][datafusion.lit].
The type of the object passed to the [`lit`][datafusion.lit] function will be used to convert it to a known data type.

In the following example we create expressions for the column named `color` and the literal scalar string `red`.
The resultant variable `red_units` is itself also an expression.

```python exec="1" source="material-block" result="text" session="expressions"
red_units = col("color") == lit("red")
```


## Boolean

When combining expressions that evaluate to a boolean value, you can combine these expressions using boolean operators.
It is important to note that in order to combine these expressions, you *must* use bitwise operators. See the following
examples for the and, or, and not operations.

```python exec="1" source="material-block" result="text" session="expressions"
red_or_green_units = (col("color") == lit("red")) | (col("color") == lit("green"))
heavy_red_units = (col("color") == lit("red")) & (col("weight") > lit(42))
not_red_units = ~(col("color") == lit("red"))
```


## Arrays

For columns that contain arrays of values, you can access individual elements of the array by index
using bracket indexing. This is similar to calling the function
[`array_element`][datafusion.functions.array_element], except that array indexing using brackets is 0 based,
similar to Python arrays and `array_element` is 1 based indexing to be compatible with other SQL
approaches.

```python exec="1" source="material-block" result="text" session="expressions"
from datafusion import col

ctx = SessionContext()
df = ctx.from_pydict({"a": [[1, 2, 3], [4, 5, 6]]})
df.select(col("a")[0].alias("a0"))
```


<div class="admonition warning">
<p class="admonition-title">Warning</p>

Indexing an element of an array via `[]` starts at index 0 whereas
[`array_element`][datafusion.functions.array_element] starts at index 1.

</div>

Starting in DataFusion 49.0.0 you can also create slices of array elements using
slice syntax from Python.

```python exec="1" source="material-block" result="text" session="expressions"
df.select(col("a")[1:3].alias("second_two_elements"))
```


To check if an array is empty, you can use the function [`array_empty`][datafusion.functions.array_empty] or `datafusion.functions.empty`.
This function returns a boolean indicating whether the array is empty.

```python exec="1" source="material-block" result="text" session="expressions"
from datafusion import SessionContext, col
from datafusion.functions import array_empty

ctx = SessionContext()
df = ctx.from_pydict({"a": [[], [1, 2, 3]]})
df.select(array_empty(col("a")).alias("is_empty"))
```


In this example, the `is_empty` column will contain `True` for the first row and `False` for the second row.

To get the total number of elements in an array, you can use the function [`cardinality`][datafusion.functions.cardinality].
This function returns an integer indicating the total number of elements in the array.

```python exec="1" source="material-block" result="text" session="expressions"
from datafusion import SessionContext, col
from datafusion.functions import cardinality

ctx = SessionContext()
df = ctx.from_pydict({"a": [[1, 2, 3], [4, 5, 6]]})
df.select(cardinality(col("a")).alias("num_elements"))
```


In this example, the `num_elements` column will contain `3` for both rows.

To concatenate two arrays, you can use the function [`array_cat`][datafusion.functions.array_cat] or [`array_concat`][datafusion.functions.array_concat].
These functions return a new array that is the concatenation of the input arrays.

```python exec="1" source="material-block" result="text" session="expressions"
from datafusion import SessionContext, col
from datafusion.functions import array_cat

ctx = SessionContext()
df = ctx.from_pydict({"a": [[1, 2, 3]], "b": [[4, 5, 6]]})
df.select(array_cat(col("a"), col("b")).alias("concatenated_array"))
```


In this example, the `concatenated_array` column will contain `[1, 2, 3, 4, 5, 6]`.

To repeat the elements of an array a specified number of times, you can use the function [`array_repeat`][datafusion.functions.array_repeat].
This function returns a new array with the elements repeated.

```python exec="1" source="material-block" result="text" session="expressions"
from datafusion import SessionContext, col
from datafusion.functions import array_repeat

ctx = SessionContext()
df = ctx.from_pydict({"a": [[1, 2, 3]]})
df.select(array_repeat(col("a"), literal(2)).alias("repeated_array"))
```


In this example, the `repeated_array` column will contain `[[1, 2, 3], [1, 2, 3]]`.

## Lambda functions

Some array functions take a *lambda function*: a small function that runs once
per element. [`array_transform`][datafusion.functions.array_transform] maps a lambda over
every element, [`array_filter`][datafusion.functions.array_filter] keeps the elements
for which a predicate lambda is true, and
[`array_any_match`][datafusion.functions.array_any_match] returns whether any element
satisfies a predicate lambda. (Functions that take another function as an
argument are sometimes called *higher-order* functions.)

The simplest way to supply a lambda is a Python `lambda`. Its parameter names
become the lambda parameters, and its return value becomes the body.

```python exec="1" source="material-block" result="text" session="expressions"
from datafusion import SessionContext, col

ctx = SessionContext()
df = ctx.from_pydict({"a": [[1, 2, 3], [4, 5]]})
df.select(f.array_transform(col("a"), lambda v: v * 2).alias("doubled"))
df.select(f.array_filter(col("a"), lambda v: v > 2).alias("big_only"))
df.select(f.array_any_match(col("a"), lambda v: v > 3).alias("has_big"))
```


If you need explicit control over parameter names, build the lambda with
[`lambda_`][datafusion.functions.lambda_] and reference its parameters with
[`lambda_var`][datafusion.functions.lambda_var]. The following is equivalent to the
`array_transform` call above.

```python exec="1" source="material-block" result="text" session="expressions"
from datafusion import lit

double_fn = f.lambda_(["v"], f.lambda_var("v") * lit(2))
df.select(f.array_transform(col("a"), double_fn).alias("doubled"))
```


<div class="admonition note">
<p class="admonition-title">Note</p>

Lambda expressions cannot yet be serialized: calling
[`to_bytes`][datafusion.expr.Expr.to_bytes] or pickling an expression that
contains a lambda raises `Lambda not implemented`. SQL lambda syntax is
only parsed by dialects that support lambdas; set
`datafusion.sql_parser.dialect` to one of `DuckDB`, `ClickHouse`,
`Snowflake`, or `Databricks`. Both arrow syntax (`x -> x * 2`) and
keyword syntax (`lambda x: x * 2`) parse. DuckDB will drop the arrow
form in v2.1, so prefer `lambda x: x * 2` for forward compatibility.
The Python expression builder shown above works regardless of dialect.

</div>

## Testing membership in a list

A common need is filtering rows where a column equals *any* of a small set of
values. DataFusion offers three forms; they differ in readability and in how
they scale:

1. A compound boolean using `|` across explicit equalities.
2. [`in_list`][datafusion.functions.in_list], which accepts a list of
   expressions and tests equality against all of them in one call.
3. A trick with [`array_position`][datafusion.functions.array_position] and
   [`make_array`][datafusion.functions.make_array], which returns the 1-based
   index of the value in a constructed array, or null if it is not present.

```python exec="1" source="material-block" result="text" session="expressions"
from datafusion import SessionContext, col, lit
from datafusion import functions as f

ctx = SessionContext()
df = ctx.from_pydict({"shipmode": ["MAIL", "SHIP", "AIR", "TRUCK", "RAIL"]})

# Option 1: compound boolean. Fine for two values; awkward past three.
df.filter((col("shipmode") == lit("MAIL")) | (col("shipmode") == lit("SHIP")))

# Option 2: in_list. Preferred for readability as the set grows.
df.filter(f.in_list(col("shipmode"), [lit("MAIL"), lit("SHIP")]))

# Option 3: array_position / make_array. Useful when you already have the
# set as an array column and want "is in that array" semantics.
df.filter(
    ~f.array_position(f.make_array(lit("MAIL"), lit("SHIP")), col("shipmode")).is_null()
)
```


Use `in_list` as the default. It is explicit, readable, and matches the
semantics users expect from SQL's `IN (...)`. Reach for the
`array_position` form only when the membership set is itself an array
column rather than a literal list.

## Conditional expressions

DataFusion provides [`case`][datafusion.functions.case] for the SQL
`CASE` expression in both its switched and searched forms, along with
[`when`][datafusion.functions.when] as a standalone builder for the
searched form.

**Switched CASE** (one expression compared against several literal values):

```python exec="1" source="material-block" result="text" session="expressions"
df = ctx.from_pydict(
    {"priority": ["1-URGENT", "2-HIGH", "3-MEDIUM", "5-LOW"]},
)

df.select(
    col("priority"),
    f.case(col("priority"))
    .when(lit("1-URGENT"), lit(1))
    .when(lit("2-HIGH"), lit(1))
    .otherwise(lit(0))
    .alias("is_high_priority"),
)
```


**Searched CASE** (an independent boolean predicate per branch). Use this
form whenever a branch tests more than simple equality — for example,
checking whether a joined column is `NULL` to gate a computed value:

```python exec="1" source="material-block" result="text" session="expressions"
df = ctx.from_pydict(
    {"volume": [10.0, 20.0, 30.0], "supplier_id": [1, None, 2]},
)

df.select(
    col("volume"),
    col("supplier_id"),
    f.when(col("supplier_id").is_not_null(), col("volume"))
    .otherwise(lit(0.0))
    .alias("attributed_volume"),
)
```


This searched-CASE pattern is idiomatic for "attribute the measure to the
matching side of a left join, otherwise contribute zero" — a shape that
appears in TPC-H Q08 and similar market-share calculations.

If a switched CASE only groups several equality matches into one bucket,
`f.when(f.in_list(col(...), [...]), value).otherwise(default)` is often
simpler than the full `case` builder.

## Structs

Columns that contain struct elements can be accessed using the bracket notation as if they were
Python dictionary style objects. This expects a string key as the parameter passed.

```python exec="1" source="material-block" result="text" session="expressions"
ctx = SessionContext()
data = {"a": [{"size": 15, "color": "green"}, {"size": 10, "color": "blue"}]}
df = ctx.from_pydict(data)
df.select(col("a")["size"].alias("a_size"))
```


## Functions

As mentioned before, most functions in DataFusion return an expression at their output. This allows us to create
a wide variety of expressions built up from other expressions. For example, [`alias`][datafusion.expr.Expr.alias] is a function that takes
as it input a single expression and returns an expression in which the name of the expression has changed.

The following example shows a series of expressions that are built up from functions operating on expressions.

```python exec="1" source="material-block" result="text" session="expressions"
from datafusion import SessionContext, lit
from datafusion import functions as f

ctx = SessionContext()
df = ctx.from_pydict(
    {
        "name": ["Albert", "Becca", "Carlos", "Dante"],
        "age": [42, 67, 27, 71],
        "years_in_position": [13, 21, 10, 54],
    },
    name="employees",
)

age_col = col("age")
renamed_age = age_col.alias("age_in_years")
start_age = age_col - col("years_in_position")
started_young = start_age < lit(18)
can_retire = age_col > lit(65)
long_timer = started_young & can_retire

df.filter(long_timer).select(col("name"), renamed_age, col("years_in_position"))
```
