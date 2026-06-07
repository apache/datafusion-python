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

# DataFrames

## Overview

The [`DataFrame`][datafusion.dataframe.DataFrame] class is the core abstraction in DataFusion that represents tabular data and operations
on that data. DataFrames provide a flexible API for transforming data through various operations such as
filtering, projection, aggregation, joining, and more.

A DataFrame represents a logical plan that is lazily evaluated. The actual execution occurs only when
terminal operations like [`collect()`][datafusion.dataframe.DataFrame.collect], [`show()`][datafusion.dataframe.DataFrame.show], or [`to_pandas()`][datafusion.dataframe.DataFrame.to_pandas] are called.

## Creating DataFrames

DataFrames can be created in several ways:

- From SQL queries via a [`SessionContext`][datafusion.context.SessionContext]:

  ```python
  from datafusion import SessionContext

  ctx = SessionContext()
  df = ctx.sql("SELECT * FROM your_table")
  ```

- From registered tables:

  ```python
  df = ctx.table("your_table")
  ```

- From various data sources:

  ```python
  # From CSV files (see [io_csv](/python/user-guide/io/csv/) for detailed options)
  df = ctx.read_csv("path/to/data.csv")

  # From Parquet files (see [io_parquet](/python/user-guide/io/parquet/) for detailed options)
  df = ctx.read_parquet("path/to/data.parquet")

  # From JSON files (see [io_json](/python/user-guide/io/json/) for detailed options)
  df = ctx.read_json("path/to/data.json")

  # From Avro files (see [io_avro](/python/user-guide/io/avro/) for detailed options)
  df = ctx.read_avro("path/to/data.avro")

  # From Pandas DataFrame
  import pandas as pd
  pandas_df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
  df = ctx.from_pandas(pandas_df)

  # From Arrow data
  import pyarrow as pa
  batch = pa.RecordBatch.from_arrays(
      [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
      names=["a", "b"]
  )
  df = ctx.from_arrow(batch)
  ```

For detailed information about reading from different data sources, see the [I/O Guide](../io/index.md).
For custom data sources, see [io_custom_table_provider](../../io/table_provider/).

## Common DataFrame Operations

DataFusion's DataFrame API offers a wide range of operations:

```python
from datafusion import column, literal

# Select specific columns
df = df.select("col1", "col2")

# Select with expressions
df = df.select(column("a") + column("b"), column("a") - column("b"))

# Filter rows (expressions or SQL strings)
df = df.filter(column("age") > literal(25))
df = df.filter("age > 25")

# Add computed columns
df = df.with_column("full_name", column("first_name") + literal(" ") + column("last_name"))

# Multiple column additions
df = df.with_columns(
    (column("a") + column("b")).alias("sum"),
    (column("a") * column("b")).alias("product")
)

# Sort data
df = df.sort(column("age").sort(ascending=False))

# Join DataFrames
df = df1.join(df2, on="user_id", how="inner")

# Aggregate data
from datafusion import functions as f
df = df.aggregate(
    [],  # Group by columns (empty for global aggregation)
    [f.sum(column("amount")).alias("total_amount")]
)

# Limit rows
df = df.limit(100)

# Drop columns
df = df.drop("temporary_column")
```

## Column Names as Function Arguments

Some [`DataFrame`][datafusion.dataframe.DataFrame] methods accept column names when an argument refers to an
existing column. These include:

- [`select`][datafusion.dataframe.DataFrame.select]
- [`sort`][datafusion.dataframe.DataFrame.sort]
- [`drop`][datafusion.dataframe.DataFrame.drop]
- [`join`][datafusion.dataframe.DataFrame.join] (`on` argument)
- [`aggregate`][datafusion.dataframe.DataFrame.aggregate] (grouping columns)

See the full function documentation for details on any specific function.

Note that [`join_on`][datafusion.dataframe.DataFrame.join_on] expects [`col()`][datafusion.col.col]/[`column()`][datafusion.col.column] expressions rather than plain strings.

For such methods, you can pass column names directly:

```python
from datafusion import col, functions as f

df.sort('id')
df.aggregate('id', [f.count(col('value'))])
```

The same operation can also be written with explicit column expressions, using either [`col()`][datafusion.col.col] or [`column()`][datafusion.col.column]:

```python
from datafusion import col, column, functions as f

df.sort(col('id'))
df.aggregate(column('id'), [f.count(col('value'))])
```

Note that [`column()`][datafusion.col.column] is an alias of [`col()`][datafusion.col.col], so you can use either name; the example above shows both in action.

Whenever an argument represents an expression—such as in
[`filter`][datafusion.dataframe.DataFrame.filter] or
[`with_column`][datafusion.dataframe.DataFrame.with_column]—use [`col()`][datafusion.col.col] to reference
columns. The comparison and arithmetic operators on [`Expr`][datafusion.expr.Expr] will automatically
convert any non-[`Expr`][datafusion.expr.Expr] value into a literal expression, so writing

```python
from datafusion import col
df.filter(col("age") > 21)
```

is equivalent to using `lit(21)` explicitly. Use [`lit()`][datafusion.lit] (also available
as [`literal()`][datafusion.literal]) when you need to construct a literal expression directly.

## Terminal Operations

To materialize the results of your DataFrame operations:

```python
# Collect all data as PyArrow RecordBatches
result_batches = df.collect()

# Convert to various formats
pandas_df = df.to_pandas()        # Pandas DataFrame
polars_df = df.to_polars()        # Polars DataFrame
arrow_table = df.to_arrow_table() # PyArrow Table
py_dict = df.to_pydict()          # Python dictionary
py_list = df.to_pylist()          # Python list of dictionaries

# Display results
df.show()                         # Print tabular format to console

# Count rows
count = df.count()

# Collect a single column of data as a PyArrow Array
arr = df.collect_column("age")
```

## Zero-copy streaming to Arrow-based Python libraries

DataFusion DataFrames implement the `__arrow_c_stream__` protocol, enabling
zero-copy, lazy streaming into Arrow-based Python libraries. With the streaming
protocol, batches are produced on demand.

!!! note

    The protocol is implementation-agnostic and works with any Python library
    that understands the Arrow C streaming interface (for example, PyArrow
    or other Arrow-compatible implementations). The sections below provide a
    short PyArrow-specific example and general guidance for other
    implementations.

## PyArrow

```python
import pyarrow as pa

# Create a PyArrow RecordBatchReader without materializing all batches
reader = pa.RecordBatchReader.from_stream(df)
for batch in reader:
    ...  # process each batch as it is produced
```

DataFrames are also iterable, yielding [`RecordBatch`][datafusion.RecordBatch]
objects lazily so you can loop over results directly without importing
PyArrow:

```python
for batch in df:
    ...  # each batch is a ``datafusion.RecordBatch``
```

Each batch exposes [`to_pyarrow()`][datafusion.record_batch.RecordBatch.to_pyarrow], allowing conversion to a PyArrow
table. `pa.table(df)` collects the entire DataFrame eagerly into a
PyArrow table:

```python
import pyarrow as pa
table = pa.table(df)
```

Asynchronous iteration is supported as well, allowing integration with
`asyncio` event loops:

```python
async for batch in df:
    ...  # process each batch as it is produced
```

To work with the stream directly, use [`execute_stream()`][datafusion.dataframe.DataFrame.execute_stream], which returns a
[`RecordBatchStream`][datafusion.RecordBatchStream].

```python
stream = df.execute_stream()
for batch in stream:
    ...
```

### Execute as Stream

For finer control over streaming execution, use
[`execute_stream`][datafusion.dataframe.DataFrame.execute_stream] to obtain a
[`RecordBatchStream`][datafusion.record_batch.RecordBatchStream]:

```python
stream = df.execute_stream()
for batch in stream:
    ...  # process each batch as it is produced
```

!!! tip

    To get a PyArrow reader instead, call

    `pa.RecordBatchReader.from_stream(df)`.

When partition boundaries are important,
[`execute_stream_partitioned`][datafusion.dataframe.DataFrame.execute_stream_partitioned]
returns an iterable of [`RecordBatchStream`][datafusion.record_batch.RecordBatchStream] objects, one per
partition:

```python
for stream in df.execute_stream_partitioned():
    for batch in stream:
        ...  # each stream yields RecordBatches
```

To process partitions concurrently, first collect the streams into a list
and then poll each one in a separate `asyncio` task:

```python
import asyncio

async def consume(stream):
    async for batch in stream:
        ...

streams = list(df.execute_stream_partitioned())
await asyncio.gather(*(consume(s) for s in streams))
```

See [../io/arrow](../io/arrow.ipynb) for additional details on the Arrow interface.

## HTML Rendering

When working in Jupyter notebooks or other environments that support HTML rendering, DataFrames will
automatically display as formatted HTML tables. For detailed information about customizing HTML
rendering, formatting options, and advanced styling, see [rendering](rendering.md).

## Core Classes

**DataFrame**

: The main DataFrame class for building and executing queries.

  See: [`DataFrame`][datafusion.dataframe.DataFrame]

**SessionContext**

: The primary entry point for creating DataFrames from various data sources.

  Key methods for DataFrame creation:

  - [`read_csv`][datafusion.context.SessionContext.read_csv] - Read CSV files
  - [`read_parquet`][datafusion.context.SessionContext.read_parquet] - Read Parquet files
  - [`read_json`][datafusion.context.SessionContext.read_json] - Read JSON files
  - [`read_avro`][datafusion.context.SessionContext.read_avro] - Read Avro files
  - [`table`][datafusion.context.SessionContext.table] - Access registered tables
  - [`sql`][datafusion.context.SessionContext.sql] - Execute SQL queries
  - [`from_pandas`][datafusion.context.SessionContext.from_pandas] - Create from Pandas DataFrame
  - [`from_arrow`][datafusion.context.SessionContext.from_arrow] - Create from Arrow data

  See: [`SessionContext`][datafusion.context.SessionContext]

## Expression Classes

**Expr**

: Represents expressions that can be used in DataFrame operations.

  See: [`Expr`][datafusion.expr.Expr]

**Functions for creating expressions:**

- [`column`][datafusion.col.column] - Reference a column by name
- [`literal`][datafusion.literal] - Create a literal value expression

## Built-in Functions

DataFusion provides many built-in functions for data manipulation:

- [`functions`][datafusion.functions] - Mathematical, string, date/time, and aggregation functions

For a complete list of available functions, see the [`functions`][datafusion.functions] module documentation.

## Execution Metrics

After executing a DataFrame (via [`collect()`][datafusion.dataframe.DataFrame.collect], [`execute_stream()`][datafusion.dataframe.DataFrame.execute_stream], etc.),
DataFusion populates per-operator runtime statistics such as row counts and
compute time. See [execution-metrics](execution-metrics.md) for a full explanation and
worked example.

## Further reading

- [Rendering](rendering.md) — Jupyter HTML repr customization.
- [Execution Metrics](execution-metrics.md) — per-operator row counts,
  compute time, spill events.
