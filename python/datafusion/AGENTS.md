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

# DataFusion Python DataFrame API Guide

## What Is DataFusion?

DataFusion is an **in-process query engine** built on Apache Arrow. It is not a
database -- there is no server, no connection string, and no external
dependencies. You create a `SessionContext`, point it at data (Parquet, CSV,
JSON, Arrow IPC, Pandas, Polars, or raw Python dicts/lists), and run queries
using either SQL or the DataFrame API described below.

All data flows through **Apache Arrow**. The canonical Python implementation is
PyArrow (`pyarrow.RecordBatch` / `pyarrow.Table`), but any library that
conforms to the [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)
can interoperate with DataFusion.

## Core Abstractions

| Abstraction | Role | Key import |
|---|---|---|
| `SessionContext` | Entry point. Loads data, runs SQL, produces DataFrames. | `from datafusion import SessionContext` |
| `DataFrame` | Lazy query builder. Each method returns a new DataFrame. | Returned by context methods |
| `Expr` | Expression tree node (column ref, literal, function call, ...). | `from datafusion import col, lit` |
| `functions` | 290+ built-in scalar, aggregate, and window functions. | `from datafusion import functions as F` |

## Import Conventions

```python
from datafusion import SessionContext, col, lit
from datafusion import functions as F
```

## Data Loading

```python
ctx = SessionContext()

# From files
df = ctx.read_parquet("path/to/data.parquet")
df = ctx.read_csv("path/to/data.csv")
df = ctx.read_json("path/to/data.json")

# From Python objects
df = ctx.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]})
df = ctx.from_pylist([{"a": 1, "b": "x"}, {"a": 2, "b": "y"}])
df = ctx.from_pandas(pandas_df)
df = ctx.from_polars(polars_df)
df = ctx.from_arrow(arrow_table)

# From SQL
df = ctx.sql("SELECT a, b FROM my_table WHERE a > 1")
```

To make a DataFrame queryable by name in SQL, register it first:

```python
ctx.register_parquet("my_table", "path/to/data.parquet")
ctx.register_csv("my_table", "path/to/data.csv")
```

## DataFrame Operations Quick Reference

Every method returns a **new** DataFrame (immutable/lazy). Chain them fluently.

### Projection

```python
df.select("a", "b")                         # by column name
df.select(col("a"), col("b"))               # by Expr
df.select(col("a"), (col("b") + 1).alias("b_plus_1"))  # with expression

df.with_column("new_col", col("a") + lit(10))  # add one column
df.with_columns(
    col("a").alias("x"),
    y=col("b") + lit(1),                    # named keyword form
)

df.drop("unwanted_col")
df.with_column_renamed("old_name", "new_name")
```

### Filtering

```python
df.filter(col("a") > lit(10))
df.filter(col("a") > lit(10), col("b") == lit("x"))   # multiple = AND
df.filter("a > 10")                                     # SQL expression string
```

### Aggregation

```python
# GROUP BY a, compute sum(b) and count(*)
df.aggregate([col("a")], [F.sum(col("b")), F.count(col("a"))])

# HAVING equivalent: use the filter keyword on the aggregate function
df.aggregate(
    [col("region")],
    [F.sum(col("sales"), filter=col("sales") > lit(1000)).alias("large_sales")],
)
```

Most aggregate functions accept an optional `filter` keyword argument. When
provided, only rows where the filter expression is true contribute to the
aggregate.

### Sorting

```python
df.sort(col("a").sort(ascending=True, nulls_first=False))
df.sort(col("a").sort(ascending=False))      # descending
```

### Joining

```python
# Equi-join on shared column name
df1.join(df2, on="key")
df1.join(df2, on="key", how="left")

# Different column names
df1.join(df2, left_on="id", right_on="fk_id", how="inner")

# Expression-based join (supports inequality predicates)
df1.join_on(df2, col("a") == col("b"), how="inner")

# Semi join: keep rows from left where a match exists in right (like EXISTS)
df1.join(df2, on="key", how="semi")

# Anti join: keep rows from left where NO match exists in right (like NOT EXISTS)
df1.join(df2, on="key", how="anti")
```

Join types: `"inner"`, `"left"`, `"right"`, `"full"`, `"semi"`, `"anti"`.

### Window Functions

```python
from datafusion import WindowFrame

# Row number partitioned by group, ordered by value
df.window(
    F.row_number(
        partition_by=[col("group")],
        order_by=[col("value")],
    ).alias("rn")
)

# Using a Window object for reuse
from datafusion.expr import Window

win = Window(
    partition_by=[col("group")],
    order_by=[col("value").sort(ascending=True)],
)
df.select(
    col("group"),
    col("value"),
    F.sum(col("value")).over(win).alias("running_total"),
)

# With explicit frame bounds
win = Window(
    partition_by=[col("group")],
    order_by=[col("value").sort(ascending=True)],
    window_frame=WindowFrame("rows", 0, None),  # current row to unbounded following
)
```

### Set Operations

```python
df1.union(df2)                          # UNION ALL (by position)
df1.union(df2, distinct=True)           # UNION DISTINCT
df1.union_by_name(df2)                  # match columns by name, not position
df1.intersect(df2)                      # INTERSECT ALL
df1.except_all(df2)                     # EXCEPT ALL
```

### Limit and Offset

```python
df.limit(10)            # first 10 rows
df.limit(10, offset=20) # skip 20, then take 10
```

### Deduplication

```python
df.distinct()           # remove duplicate rows
df.distinct_on(         # keep first row per group (like DISTINCT ON in Postgres)
    [col("a")],                     # uniqueness columns
    [col("a"), col("b")],           # output columns
    [col("b").sort(ascending=True)], # which row to keep
)
```

## Executing and Collecting Results

DataFrames are lazy until you collect.

```python
df.show()                               # print formatted table to stdout
batches = df.collect()                  # list[pa.RecordBatch]
table = df.to_arrow_table()             # pa.Table
pandas_df = df.to_pandas()              # pd.DataFrame
polars_df = df.to_polars()              # pl.DataFrame
py_dict = df.to_pydict()                # dict[str, list]
py_list = df.to_pylist()                # list[dict]
count = df.count()                      # int

# Streaming
stream = df.execute_stream()            # RecordBatchStream (single partition)
for batch in stream:
    process(batch)
```

### Writing Results

```python
df.write_parquet("output/")
df.write_csv("output/")
df.write_json("output/")
```

## Expression Building

### Column References and Literals

```python
col("column_name")              # reference a column
lit(42)                          # integer literal
lit("hello")                     # string literal
lit(3.14)                        # float literal
lit(pa.scalar(value))            # PyArrow scalar (preserves Arrow type)
```

`lit()` accepts PyArrow scalars directly -- prefer this over converting Arrow
data to Python and back when working with values extracted from query results.

### Arithmetic

```python
col("price") * col("quantity")            # multiplication
col("a") + lit(1)                          # addition
col("a") - col("b")                        # subtraction
col("a") / lit(2)                          # division
col("a") % lit(3)                          # modulo
```

### Date Arithmetic

`Date32` columns require `Interval` types for arithmetic, not `Duration`. Use
PyArrow's `month_day_nano_interval` type, which takes a `(months, days, nanos)`
tuple:

```python
import pyarrow as pa

# Subtract 90 days from a date column
col("ship_date") - lit(pa.scalar((0, 90, 0), type=pa.month_day_nano_interval()))

# Subtract 3 months
col("ship_date") - lit(pa.scalar((3, 0, 0), type=pa.month_day_nano_interval()))
```

**Important**: `lit(datetime.timedelta(days=90))` creates a `Duration(µs)`
literal, which is **not** compatible with `Date32` arithmetic. Always use
`pa.month_day_nano_interval()` for date operations.

### Comparisons

```python
col("a") > lit(10)
col("a") >= lit(10)
col("a") < lit(10)
col("a") <= lit(10)
col("a") == lit("x")
col("a") != lit("x")
col("a") == None                           # same as col("a").is_null()
col("a") != None                           # same as col("a").is_not_null()
```

### Boolean Logic

**Important**: Python's `and`, `or`, `not` keywords do NOT work with Expr
objects. You must use the bitwise operators:

```python
(col("a") > lit(1)) & (col("b") < lit(10))   # AND
(col("a") > lit(1)) | (col("b") < lit(10))   # OR
~(col("a") > lit(1))                           # NOT
```

Always wrap each comparison in parentheses when combining with `&`, `|`, `~`
because Python's operator precedence for bitwise operators is different from
logical operators.

### Null Handling

```python
col("a").is_null()
col("a").is_not_null()
col("a").fill_null(lit(0))          # replace NULL with a value
F.coalesce(col("a"), col("b"))     # first non-null value
F.nullif(col("a"), lit(0))         # return NULL if a == 0
```

### CASE / WHEN

```python
# Simple CASE (matching on a single expression)
F.case(col("status"))
    .when(lit("A"), lit("Active"))
    .when(lit("I"), lit("Inactive"))
    .otherwise(lit("Unknown"))

# Searched CASE (each branch has its own predicate)
F.when(col("value") > lit(100), lit("high"))
    .when(col("value") > lit(50), lit("medium"))
    .otherwise(lit("low"))
```

### Casting

```python
import pyarrow as pa

col("a").cast(pa.float64())
col("a").cast(pa.utf8())
col("a").cast(pa.date32())
```

### Aliasing

```python
(col("a") + col("b")).alias("total")
```

### BETWEEN and IN

```python
col("a").between(lit(1), lit(10))                       # 1 <= a <= 10
F.in_list(col("a"), [lit(1), lit(2), lit(3)])           # a IN (1, 2, 3)
F.in_list(col("a"), [lit(1), lit(2)], negated=True)     # a NOT IN (1, 2)
```

### Struct and Array Access

```python
col("struct_col")["field_name"]     # access struct field
col("array_col")[0]                  # access array element (0-indexed)
col("array_col")[1:3]                # array slice (0-indexed)
```

## SQL-to-DataFrame Reference

| SQL | DataFrame API |
|---|---|
| `SELECT a, b` | `df.select("a", "b")` |
| `SELECT a, b + 1 AS c` | `df.select(col("a"), (col("b") + lit(1)).alias("c"))` |
| `SELECT *, a + 1 AS c` | `df.with_column("c", col("a") + lit(1))` |
| `WHERE a > 10` | `df.filter(col("a") > lit(10))` |
| `GROUP BY a` with `SUM(b)` | `df.aggregate([col("a")], [F.sum(col("b"))])` |
| `SUM(b) FILTER (WHERE b > 100)` | `F.sum(col("b"), filter=col("b") > lit(100))` |
| `ORDER BY a DESC` | `df.sort(col("a").sort(ascending=False))` |
| `LIMIT 10 OFFSET 5` | `df.limit(10, offset=5)` |
| `DISTINCT` | `df.distinct()` |
| `a INNER JOIN b ON a.id = b.id` | `a.join(b, on="id")` |
| `a LEFT JOIN b ON a.id = b.fk` | `a.join(b, left_on="id", right_on="fk", how="left")` |
| `WHERE EXISTS (SELECT ...)` | `a.join(b, on="key", how="semi")` |
| `WHERE NOT EXISTS (SELECT ...)` | `a.join(b, on="key", how="anti")` |
| `UNION ALL` | `df1.union(df2)` |
| `UNION` (distinct) | `df1.union(df2, distinct=True)` |
| `INTERSECT` | `df1.intersect(df2)` |
| `EXCEPT` | `df1.except_all(df2)` |
| `CASE x WHEN 1 THEN 'a' END` | `F.case(col("x")).when(lit(1), lit("a")).end()` |
| `CASE WHEN x > 1 THEN 'a' END` | `F.when(col("x") > lit(1), lit("a")).end()` |
| `x IN (1, 2, 3)` | `F.in_list(col("x"), [lit(1), lit(2), lit(3)])` |
| `x BETWEEN 1 AND 10` | `col("x").between(lit(1), lit(10))` |
| `CAST(x AS DOUBLE)` | `col("x").cast(pa.float64())` |
| `ROW_NUMBER() OVER (...)` | `F.row_number(partition_by=[...], order_by=[...])` |
| `SUM(x) OVER (...)` | `F.sum(col("x")).over(window)` |
| `x IS NULL` | `col("x").is_null()` |
| `COALESCE(a, b)` | `F.coalesce(col("a"), col("b"))` |

## Common Pitfalls

1. **Boolean operators**: Use `&`, `|`, `~` -- not Python's `and`, `or`, `not`.
   Always parenthesize: `(col("a") > lit(1)) & (col("b") < lit(2))`.

2. **Wrapping scalars with `lit()`**: Comparisons with raw Python values work
   (e.g., `col("a") > 10`) because the Expr operators auto-wrap them, but
   standalone scalars in function calls need `lit()`:
   `F.coalesce(col("a"), lit(0))`, not `F.coalesce(col("a"), 0)`.

3. **Column name quoting**: Column names are normalized to lowercase by default.
   To reference a column with uppercase letters, use double quotes inside the
   string: `col('"MyColumn"')`.

4. **DataFrames are immutable**: Every method returns a **new** DataFrame. You
   must capture the return value:
   ```python
   df = df.filter(col("a") > lit(1))   # correct
   df.filter(col("a") > lit(1))         # WRONG -- result is discarded
   ```

5. **Window frame defaults**: When using `order_by` in a window, the default
   frame is `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`. For a full
   partition frame, set `window_frame=WindowFrame("rows", None, None)`.

## Idiomatic Patterns

### Fluent Chaining

```python
result = (
    ctx.read_parquet("data.parquet")
    .filter(col("year") >= lit(2020))
    .select(col("region"), col("sales"))
    .aggregate([col("region")], [F.sum(col("sales")).alias("total")])
    .sort(col("total").sort(ascending=False))
    .limit(10)
)
result.show()
```

### Using Variables as CTEs

Instead of SQL CTEs (`WITH ... AS`), assign intermediate DataFrames to
variables:

```python
base = ctx.read_parquet("orders.parquet").filter(col("status") == lit("shipped"))
by_region = base.aggregate([col("region")], [F.sum(col("amount")).alias("total")])
top_regions = by_region.filter(col("total") > lit(10000))
```

### Reusing Expressions as Variables

Just like DataFrames, expressions (`Expr`) can be stored in variables and used
anywhere an `Expr` is expected. This is useful for building up complex
expressions or reusing a computed value across multiple operations:

```python
# Build an expression and reuse it
disc_price = col("price") * (lit(1) - col("discount"))
df = df.select(
    col("id"),
    disc_price.alias("disc_price"),
    (disc_price * (lit(1) + col("tax"))).alias("total"),
)

# Use a collected scalar as an expression
max_val = result_batch[0].column("max_price")[0]  # PyArrow scalar
cutoff = lit(max_val) - lit(pa.scalar((0, 90, 0), type=pa.month_day_nano_interval()))
df = df.filter(col("ship_date") <= cutoff)         # cutoff is already an Expr
```

**Important**: Do not wrap an `Expr` in `lit()`. `lit()` is for converting
Python/PyArrow values into expressions. If a value is already an `Expr`, use it
directly.

### Window Functions for Scalar Subqueries

Where SQL uses a correlated scalar subquery, the idiomatic DataFrame approach
is a window function:

```sql
-- SQL scalar subquery
SELECT *, (SELECT SUM(b) FROM t WHERE t.group = s.group) AS group_total FROM s
```

```python
# DataFrame: window function
win = Window(partition_by=[col("group")])
df = df.with_column("group_total", F.sum(col("b")).over(win))
```

### Semi/Anti Joins for EXISTS / NOT EXISTS

```sql
-- SQL: WHERE EXISTS (SELECT 1 FROM other WHERE other.key = main.key)
-- DataFrame:
result = main.join(other, on="key", how="semi")

-- SQL: WHERE NOT EXISTS (SELECT 1 FROM other WHERE other.key = main.key)
-- DataFrame:
result = main.join(other, on="key", how="anti")
```

### Computed Columns

```python
# Add computed columns while keeping all originals
df = df.with_column("full_name", F.concat(col("first"), lit(" "), col("last")))
df = df.with_column("discounted", col("price") * lit(0.9))
```

## Available Functions (Categorized)

The `functions` module (imported as `F`) provides 290+ functions. Key categories:

**Aggregate**: `sum`, `avg`, `min`, `max`, `count`, `count_star`, `median`,
`stddev`, `stddev_pop`, `var_samp`, `var_pop`, `corr`, `covar`, `approx_distinct`,
`approx_median`, `approx_percentile_cont`, `array_agg`, `string_agg`,
`first_value`, `last_value`, `bit_and`, `bit_or`, `bit_xor`, `bool_and`,
`bool_or`, `grouping`, `regr_*` (9 regression functions)

**Window**: `row_number`, `rank`, `dense_rank`, `percent_rank`, `cume_dist`,
`ntile`, `lag`, `lead`, `first_value`, `last_value`, `nth_value`

**String**: `length`, `lower`, `upper`, `trim`, `ltrim`, `rtrim`, `lpad`,
`rpad`, `starts_with`, `ends_with`, `contains`, `substr`, `replace`, `reverse`,
`repeat`, `split_part`, `concat`, `concat_ws`, `initcap`, `ascii`, `chr`,
`left`, `right`, `strpos`, `translate`, `overlay`, `levenshtein`

**Math**: `abs`, `ceil`, `floor`, `round`, `trunc`, `sqrt`, `cbrt`, `exp`,
`ln`, `log`, `log2`, `log10`, `pow`, `signum`, `pi`, `random`, `factorial`,
`gcd`, `lcm`, `greatest`, `least`, sin/cos/tan and inverse/hyperbolic variants

**Date/Time**: `now`, `today`, `current_date`, `current_time`,
`current_timestamp`, `date_part`, `date_trunc`, `date_bin`, `extract`,
`to_timestamp`, `to_timestamp_millis`, `to_timestamp_micros`,
`to_timestamp_nanos`, `to_timestamp_seconds`, `to_unixtime`, `from_unixtime`,
`make_date`, `make_time`, `to_date`, `to_time`, `to_local_time`, `date_format`

**Conditional**: `case`, `when`, `coalesce`, `nullif`, `ifnull`, `nvl`, `nvl2`

**Array/List**: `array`, `make_array`, `array_agg`, `array_length`,
`array_element`, `array_slice`, `array_append`, `array_prepend`,
`array_concat`, `array_has`, `array_has_all`, `array_has_any`, `array_position`,
`array_remove`, `array_distinct`, `array_sort`, `array_reverse`, `flatten`,
`array_to_string`, `array_intersect`, `array_union`, `array_except`,
`generate_series`
(Most `array_*` functions also have `list_*` aliases.)

**Struct/Map**: `struct`, `named_struct`, `get_field`, `make_map`, `map_keys`,
`map_values`, `map_entries`, `map_extract`

**Regex**: `regexp_like`, `regexp_match`, `regexp_replace`, `regexp_count`,
`regexp_instr`

**Hash**: `md5`, `sha224`, `sha256`, `sha384`, `sha512`, `digest`

**Type**: `arrow_typeof`, `arrow_cast`, `arrow_metadata`

**Other**: `in_list`, `order_by`, `alias`, `col`, `encode`, `decode`,
`to_hex`, `to_char`, `uuid`, `version`, `bit_length`, `octet_length`
