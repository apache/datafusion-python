# DataFusion Quickstart

You can easily query a DataFusion table with the Python API or with pure SQL.

Let's create a small DataFrame and then run some queries with both APIs.

Start by creating a DataFrame with four rows of data and two columns: `a` and `b`.

```python
from datafusion import SessionContext

ctx = SessionContext()

df = ctx.from_pydict({"a": [1, 2, 3, 1], "b": [4, 5, 6, 7]}, name="my_table")
```

Let's append a column to this DataFrame that adds columns `a` and `b` with the SQL API.

```
ctx.sql("select a, b, a + b as sum_a_b from my_table")

+---+---+---------+
| a | b | sum_a_b |
+---+---+---------+
| 1 | 4 | 5       |
| 2 | 5 | 7       |
| 3 | 6 | 9       |
| 1 | 7 | 8       |
+---+---+---------+
```

DataFusion makes it easy to run SQL queries on DataFrames.

Now let's run the same query with the DataFusion Python API:

```python
from datafusion import col

df.select(
    col("a"),
    col("b"),
    col("a") + col("b"),
)
```

We get the same result as before:

```
+---+---+-------------------------+
| a | b | my_table.a + my_table.b |
+---+---+-------------------------+
| 1 | 4 | 5                       |
| 2 | 5 | 7                       |
| 3 | 6 | 9                       |
| 1 | 7 | 8                       |
+---+---+-------------------------+
```

DataFusion also allows you to query data with a well-designed Python interface.

Python users have two great ways to query DataFusion tables.
