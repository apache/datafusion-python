# DataFusion Create Table

It's easy to create DataFusion tables from a variety of data sources.

## Create table from Python Dictionary

Here's how to create a DataFusion table from a Python dictionary:

```python
from datafusion import SessionContext

ctx = SessionContext()

df = ctx.from_pydict({"a": [1, 2, 3, 1], "b": [4, 5, 6, 7]}, name="my_table")
```

Supplying the `name` parameter is optional.  You only need to name the table if you'd like to query it with the SQL API.

You can also create a DataFrame without a name that can be queried with the Python API:

```python
from datafusion import SessionContext

ctx = SessionContext()

df = ctx.from_pydict({"a": [1, 2, 3, 1], "b": [4, 5, 6, 7]})
```

## Create table from CSV

You can read a CSV into a DataFusion DataFrame.  Here's how to read the `G1_1e8_1e2_0_0.csv` file into a table named `csv_1e8`:

```python
ctx.register_csv("csv_1e8", "G1_1e8_1e2_0_0.csv")
```

## Create table from Parquet

You can read a Parquet file into a DataFusion DataFrame.  Here's how to read the `yellow_tripdata_2021-01.parquet` file into a table named `taxi`.

```python
ctx.register_parquet("taxi", "yellow_tripdata_2021-01.parquet")
```
