```python exec="1" session="windows"
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


# Window Functions

In this section you will learn about window functions. A window function utilizes values from one or
multiple rows to produce a result for each individual row, unlike an aggregate function that
provides a single value for multiple rows.

The window functions are available in the [`functions`][datafusion.functions] module.

We'll use the pokemon dataset (from Ritchie Vink) in the following examples.

```python exec="1" source="material-block" result="text" session="windows"
ctx = SessionContext()
df = ctx.read_csv("pokemon.csv")
```


Here is an example that shows how you can compare each pokemon's speed to the speed of the
previous row in the DataFrame.

```python exec="1" source="material-block" result="text" session="windows"
df.select(col('"Name"'), col('"Speed"'), f.lag(col('"Speed"')).alias("Previous Speed"))
```


## Setting Parameters

### Ordering

You can control the order in which rows are processed by window functions by providing
a list of `order_by` functions for the `order_by` parameter.

```python exec="1" source="material-block" result="text" session="windows"
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
[Aggregation Function](../aggregations/). This will cause the window values to be evaluated
independently for each of the partitions. In the example above, we found the rank of each
Pokemon per `Type 1` partitions. We can see the first couple of each partition if we do
the following:

```python exec="1" source="material-block" result="text" session="windows"
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

```python exec="1" source="material-block" result="text" session="windows"
from datafusion.expr import Window, WindowFrame

df.select(
    col('"Name"'),
    col('"Speed"'),
    f.avg(col('"Speed"'))
    .over(Window(window_frame=WindowFrame("rows", 2, 0), order_by=[col('"Speed"')]))
    .alias("Previous Speed"),
)
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

```python exec="1" source="material-block" result="text" session="windows"
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

You can use any [Aggregation Function](../aggregations/) as a window function. Here
is an example that shows how to compare each pokemons’s attack power with the average attack
power in its `"Type 1"` using the [`avg`][datafusion.functions.avg] function.

```python exec="1" source="material-block" result="text" session="windows"
df.select(
    col('"Name"'),
    col('"Attack"'),
    col('"Type 1"'),
    f.avg(col('"Attack"'))
    .over(
        Window(
            window_frame=WindowFrame("rows", None, None),
            partition_by=[col('"Type 1"')],
        )
    )
    .alias("Average Attack"),
)
```


## Available Functions

The possible window functions are:

1. Rank Functions
   : - [`rank`][datafusion.functions.rank]
     - [`dense_rank`][datafusion.functions.dense_rank]
     - [`ntile`][datafusion.functions.ntile]
     - [`row_number`][datafusion.functions.row_number]
2. Analytical Functions
   : - [`cume_dist`][datafusion.functions.cume_dist]
     - [`percent_rank`][datafusion.functions.percent_rank]
     - [`lag`][datafusion.functions.lag]
     - [`lead`][datafusion.functions.lead]
3. Aggregate Functions
   : - All [Aggregation Functions](../aggregations/) can be used as window functions.

## User-Defined Window Functions

You can ship custom window functions to the engine by subclassing
[`WindowEvaluator`][datafusion.user_defined.WindowEvaluator] and registering it
via [`udwf`][datafusion.user_defined.udwf]. See [`user_defined`][datafusion.user_defined]
for the evaluator interface and worked examples.

<div class="admonition note">
<p class="admonition-title">Note</p>

Serialization

</div>

    Python window UDFs travel inline inside pickled or
    [`to_bytes`][datafusion.expr.Expr.to_bytes]-serialized expressions —
    the evaluator class is captured by value via [`cloudpickle`][cloudpickle], so
    worker processes do not need to pre-register the UDF. Any names the
    evaluator resolves via `import` are captured **by reference** and
    must be importable on the receiving worker. See
    [`ipc`][datafusion.ipc] for the full IPC model and security caveats.
