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


# Concepts

In this section, we will cover a basic example to introduce a few key concepts. We will use the
2021 Yellow Taxi Trip Records ([download](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet)),
from the [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

```python exec="1" source="material-block" result="text" session="concepts"
ctx = SessionContext()

df = ctx.read_parquet("yellow_tripdata_2021-01.parquet")

df = df.select(
    "trip_distance",
    col("total_amount").alias("total"),
    (f.round(lit(100.0) * col("tip_amount") / col("total_amount"), lit(1))).alias(
        "tip_percent"
    ),
)

df.show()
```


## Session Context

The first statement group creates a [`SessionContext`][datafusion.context.SessionContext].

```python
# create a context
ctx = datafusion.SessionContext()
```

A Session Context is the main interface for executing queries with DataFusion. It maintains the state
of the connection between a user and an instance of the DataFusion engine. Additionally it provides
the following functionality:

- Create a DataFrame from a data source.
- Register a data source as a table that can be referenced from a SQL query.
- Execute a SQL query

## DataFrame

The second statement group creates a [`DataFrame`][datafusion.dataframe.DataFrame],

```python
# Create a DataFrame from a file
df = ctx.read_parquet("yellow_tripdata_2021-01.parquet")
```

A DataFrame refers to a (logical) set of rows that share the same column names, similar to a [Pandas DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html).
DataFrames are typically created by calling a method on [`SessionContext`][datafusion.context.SessionContext], such as [`read_csv`][datafusion.context.SessionContext.read_csv], and can then be modified by
calling the transformation methods, such as [`filter`][datafusion.dataframe.DataFrame.filter], [`select`][datafusion.dataframe.DataFrame.select], [`aggregate`][datafusion.dataframe.DataFrame.aggregate],
and [`limit`][datafusion.dataframe.DataFrame.limit] to build up a query definition.

For more details on working with DataFrames, including visualization options and conversion to other formats, see [dataframe/index](dataframe/index.md).

## Expressions

The third statement uses [Expressions](common-operations/expressions.md) to build up a query definition. You can find
explanations for what the functions below do in the user documentation for
[`col`][datafusion.col.col], [`lit`][datafusion.lit], [`round`][datafusion.functions.round],
and [`alias`][datafusion.expr.Expr.alias].

```python
df = df.select(
    "trip_distance",
    col("total_amount").alias("total"),
    (f.round(lit(100.0) * col("tip_amount") / col("total_amount"), lit(1))).alias("tip_percent"),
)
```

Finally the [`show`][datafusion.dataframe.DataFrame.show] method converts the logical plan
represented by the DataFrame into a physical plan and execute it, collecting all results and
displaying them to the user. It is important to note that DataFusion performs lazy evaluation
of the DataFrame. Until you call a method such as [`show`][datafusion.dataframe.DataFrame.show]
or [`collect`][datafusion.dataframe.DataFrame.collect], DataFusion will not perform the query.
